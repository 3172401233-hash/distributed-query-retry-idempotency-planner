// 分布式查询重试 / 幂等合并规划器。
// 本模块只做决策与状态管理，不执行任何真实查询。

export const AttemptState = Object.freeze({
  PENDING: 'pending',
  ACCEPTED: 'accepted',
  SUPERSEDED: 'superseded',
  CANCELLED: 'cancelled',
});

export const TerminationReason = Object.freeze({
  COMPLETED: 'completed',
  CANCELLED: 'cancelled',
  DEADLINE_EXCEEDED: 'deadline_exceeded',
  RETRY_BUDGET_EXHAUSTED: 'retry_budget_exhausted',
});

export const Action = Object.freeze({
  RETRY: 'retry',
  WAIT: 'wait',
  DISCARD: 'discard',
  ACCEPT: 'accept',
  ACCUMULATE: 'accumulate',
  TERMINATE: 'terminate',
});

function normalizeRows(rows) {
  return (rows ?? []).map((row) =>
    typeof row === 'object' && row !== null ? row : { key: String(row) },
  );
}

class Attempt {
  constructor(attemptId, startedAt, leaseMs) {
    this.attemptId = attemptId;
    this.state = AttemptState.PENDING;
    this.startedAt = startedAt;
    this.leaseUntil = startedAt + leaseMs;
    // 幂等合并缓冲：按行键去重，只允许同一 attempt 的 partial 进入。
    this.parts = [];
    this.seenRowKeys = new Set();
    this.rows = [];
    this.duplicatesDropped = 0;
    this.coveredRanges = [];
    this.finalized = false;
  }

  addPart(part) {
    this.parts.push(part.partId ?? this.parts.length);
    if (part.shardRange) this.coveredRanges.push(part.shardRange);
    for (const row of normalizeRows(part.rows)) {
      const key = String(row.key);
      if (this.seenRowKeys.has(key)) {
        this.duplicatesDropped += 1;
        continue;
      }
      this.seenRowKeys.add(key);
      this.rows.push(row);
    }
    if (part.final) this.finalized = true;
  }

  toJSON() {
    return {
      attemptId: this.attemptId,
      state: this.state,
      startedAt: this.startedAt,
      leaseUntil: this.leaseUntil,
      parts: this.parts,
      rows: this.rows,
      duplicatesDropped: this.duplicatesDropped,
      coveredRanges: this.coveredRanges,
      finalized: this.finalized,
    };
  }

  static fromJSON(data) {
    const attempt = new Attempt(data.attemptId, data.startedAt, 0);
    attempt.state = data.state;
    attempt.leaseUntil = data.leaseUntil;
    attempt.parts = data.parts ?? [];
    attempt.rows = data.rows ?? [];
    attempt.seenRowKeys = new Set(attempt.rows.map((row) => String(row.key)));
    attempt.duplicatesDropped = data.duplicatesDropped ?? 0;
    attempt.coveredRanges = data.coveredRanges ?? [];
    attempt.finalized = data.finalized ?? false;
    return attempt;
  }
}

class FragmentState {
  constructor(fragmentId, shardRange) {
    this.fragmentId = fragmentId;
    this.shardRange = shardRange;
    this.attempts = new Map();
    this.activeAttemptId = null;
    this.acceptedAttemptId = null;
  }

  get accepted() {
    return this.acceptedAttemptId !== null;
  }

  toJSON() {
    return {
      fragmentId: this.fragmentId,
      shardRange: this.shardRange,
      attempts: [...this.attempts.values()].map((a) => a.toJSON()),
      activeAttemptId: this.activeAttemptId,
      acceptedAttemptId: this.acceptedAttemptId,
    };
  }

  static fromJSON(data) {
    const fragment = new FragmentState(data.fragmentId, data.shardRange);
    for (const attemptData of data.attempts) {
      const attempt = Attempt.fromJSON(attemptData);
      fragment.attempts.set(attempt.attemptId, attempt);
    }
    fragment.activeAttemptId = data.activeAttemptId;
    fragment.acceptedAttemptId = data.acceptedAttemptId;
    return fragment;
  }
}

export class RetryPlanner {
  constructor({ queryId, plan, deadlineMs, retryBudget, leaseMs }) {
    if (!plan?.fragments?.length) throw new Error('plan.fragments 不能为空');
    this.queryId = queryId ?? 'query';
    this.deadlineMs = deadlineMs;
    this.retryBudget = retryBudget;
    this.leaseMs = leaseMs;
    this.retriesUsed = 0;
    this.nextAttemptSeq = 1;
    this.termination = null;
    this.fragments = new Map();
    for (const { fragmentId, shardRange } of plan.fragments) {
      this.fragments.set(fragmentId, new FragmentState(fragmentId, shardRange));
    }
  }

  start(now = 0) {
    const decisions = [];
    for (const fragmentId of this.fragments.keys()) {
      decisions.push(this.#startAttempt(fragmentId, now));
    }
    return decisions;
  }

  #startAttempt(fragmentId, now) {
    const fragment = this.fragments.get(fragmentId);
    const attemptId = `${fragmentId}#a${this.nextAttemptSeq++}`;
    const attempt = new Attempt(attemptId, now, this.leaseMs);
    fragment.attempts.set(attemptId, attempt);
    fragment.activeAttemptId = attemptId;
    return {
      action: 'schedule',
      fragmentId,
      attemptId,
      shardRange: fragment.shardRange,
      leaseUntil: attempt.leaseUntil,
    };
  }

  #terminate(reason) {
    if (this.termination) return;
    this.termination = reason;
    if (reason !== TerminationReason.COMPLETED) {
      for (const fragment of this.fragments.values()) {
        for (const attempt of fragment.attempts.values()) {
          if (attempt.state === AttemptState.PENDING) {
            attempt.state = AttemptState.CANCELLED;
          }
        }
      }
    }
  }

  // 某个 attempt 超时（未在租约内返回）时，由协调器调用以决定下一步。
  onTimeout(fragmentId, attemptId, now) {
    if (this.termination) {
      return { action: Action.DISCARD, reason: 'query_terminated', termination: this.termination };
    }
    const fragment = this.fragments.get(fragmentId);
    const attempt = fragment?.attempts.get(attemptId);
    if (!fragment || !attempt) {
      return { action: Action.DISCARD, reason: 'unknown_attempt' };
    }
    if (attempt.state !== AttemptState.PENDING) {
      return { action: Action.DISCARD, reason: 'stale_timeout', attemptState: attempt.state };
    }
    if (now >= this.deadlineMs) {
      this.#terminate(TerminationReason.DEADLINE_EXCEEDED);
      return { action: Action.TERMINATE, reason: TerminationReason.DEADLINE_EXCEEDED };
    }
    if (now < attempt.leaseUntil) {
      // 租约未到期：旧 attempt 仍可能合法返回，选择等待而不是立即重试。
      return { action: Action.WAIT, fragmentId, attemptId, waitUntil: attempt.leaseUntil };
    }
    if (this.retriesUsed >= this.retryBudget) {
      this.#terminate(TerminationReason.RETRY_BUDGET_EXHAUSTED);
      return { action: Action.TERMINATE, reason: TerminationReason.RETRY_BUDGET_EXHAUSTED };
    }
    // 租约到期且预算充足：作废旧 attempt，签发新 attempt。
    attempt.state = AttemptState.SUPERSEDED;
    this.retriesUsed += 1;
    const scheduled = this.#startAttempt(fragmentId, now);
    return {
      action: Action.RETRY,
      fragmentId,
      supersededAttemptId: attemptId,
      attemptId: scheduled.attemptId,
      leaseUntil: scheduled.leaseUntil,
      retriesUsed: this.retriesUsed,
      retryBudget: this.retryBudget,
    };
  }

  // 收到某个 attempt 的 partial 结果。不同 attempt 的 partial 绝不混入同一缓冲。
  onPartialResult({ fragmentId, attemptId, partId, shardRange, rows, final = false }, now = 0) {
    if (this.termination) {
      return { action: Action.DISCARD, reason: 'query_terminated', termination: this.termination };
    }
    const fragment = this.fragments.get(fragmentId);
    const attempt = fragment?.attempts.get(attemptId);
    if (!fragment || !attempt) {
      return { action: Action.DISCARD, reason: 'unknown_attempt' };
    }
    if (fragment.accepted) {
      // 晚到的旧结果：同一片段已有结果参与合并，直接丢弃，避免同一行统计两次。
      return {
        action: Action.DISCARD,
        reason: 'duplicate_fragment_result',
        acceptedAttemptId: fragment.acceptedAttemptId,
      };
    }
    if (attempt.state !== AttemptState.PENDING || attemptId !== fragment.activeAttemptId) {
      return { action: Action.DISCARD, reason: 'stale_attempt', attemptState: attempt.state };
    }
    if (now >= this.deadlineMs) {
      this.#terminate(TerminationReason.DEADLINE_EXCEEDED);
      return { action: Action.TERMINATE, reason: TerminationReason.DEADLINE_EXCEEDED };
    }

    attempt.addPart({ partId, shardRange, rows, final });
    if (!attempt.finalized) {
      return {
        action: Action.ACCUMULATE,
        fragmentId,
        attemptId,
        rowCount: attempt.rows.length,
        duplicatesDropped: attempt.duplicatesDropped,
      };
    }

    attempt.state = AttemptState.ACCEPTED;
    fragment.acceptedAttemptId = attemptId;
    const completed = [...this.fragments.values()].every((f) => f.accepted);
    if (completed) this.#terminate(TerminationReason.COMPLETED);
    return {
      action: Action.ACCEPT,
      fragmentId,
      attemptId,
      rowCount: attempt.rows.length,
      duplicatesDropped: attempt.duplicatesDropped,
      queryCompleted: completed,
    };
  }

  cancel() {
    this.#terminate(TerminationReason.CANCELLED);
    return { action: Action.TERMINATE, reason: TerminationReason.CANCELLED };
  }

  // 协调器崩溃恢复：快照可持久化，重启后据此继续决策。
  snapshot() {
    return JSON.stringify({
      queryId: this.queryId,
      deadlineMs: this.deadlineMs,
      retryBudget: this.retryBudget,
      leaseMs: this.leaseMs,
      retriesUsed: this.retriesUsed,
      nextAttemptSeq: this.nextAttemptSeq,
      termination: this.termination,
      fragments: [...this.fragments.values()].map((f) => f.toJSON()),
    });
  }

  static restore(snapshot) {
    const data = typeof snapshot === 'string' ? JSON.parse(snapshot) : snapshot;
    const planner = new RetryPlanner({
      queryId: data.queryId,
      plan: {
        fragments: data.fragments.map((f) => ({
          fragmentId: f.fragmentId,
          shardRange: f.shardRange,
        })),
      },
      deadlineMs: data.deadlineMs,
      retryBudget: data.retryBudget,
      leaseMs: data.leaseMs,
    });
    planner.retriesUsed = data.retriesUsed;
    planner.nextAttemptSeq = data.nextAttemptSeq;
    planner.termination = data.termination;
    planner.fragments = new Map();
    for (const fragmentData of data.fragments) {
      const fragment = FragmentState.fromJSON(fragmentData);
      planner.fragments.set(fragment.fragmentId, fragment);
    }
    return planner;
  }

  // 输出：每个 attempt 的状态、允许参与合并的结果、最终终止原因。
  report() {
    const attempts = {};
    const mergeable = {};
    for (const fragment of this.fragments.values()) {
      attempts[fragment.fragmentId] = [...fragment.attempts.values()].map((attempt) => ({
        attemptId: attempt.attemptId,
        state: attempt.state,
        rowCount: attempt.rows.length,
        duplicatesDropped: attempt.duplicatesDropped,
      }));
      if (fragment.accepted) {
        const attempt = fragment.attempts.get(fragment.acceptedAttemptId);
        mergeable[fragment.fragmentId] = {
          attemptId: attempt.attemptId,
          shardRange: fragment.shardRange,
          rows: attempt.rows,
          rowCount: attempt.rows.length,
          duplicatesDropped: attempt.duplicatesDropped,
        };
      }
    }
    return { queryId: this.queryId, attempts, mergeable, termination: this.termination };
  }
}
