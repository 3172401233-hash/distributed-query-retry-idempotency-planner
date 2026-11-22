import test from 'node:test';
import assert from 'node:assert/strict';
import {
  RetryPlanner,
  Action,
  AttemptState,
  TerminationReason,
} from '../src/planner.js';

function makePlanner(overrides = {}) {
  return new RetryPlanner({
    queryId: 'q1',
    plan: {
      fragments: [
        { fragmentId: 'f1', shardRange: [0, 100] },
        { fragmentId: 'f2', shardRange: [100, 200] },
      ],
    },
    deadlineMs: 10_000,
    retryBudget: 2,
    leaseMs: 1_000,
    ...overrides,
  });
}

test('旧结果晚到：重试后旧 attempt 的结果被丢弃，同一行不会被统计两次', () => {
  const planner = makePlanner();
  const [s1] = planner.start(0);

  // 租约内不立即重试，而是等待旧 attempt。
  const early = planner.onTimeout('f1', s1.attemptId, 500);
  assert.equal(early.action, Action.WAIT);
  assert.equal(early.waitUntil, 1_000);

  // 租约到期后重试，旧 attempt 被作废。
  const retry = planner.onTimeout('f1', s1.attemptId, 1_000);
  assert.equal(retry.action, Action.RETRY);
  assert.equal(retry.supersededAttemptId, s1.attemptId);

  // 新 attempt 返回完整结果并被接受。
  const accepted = planner.onPartialResult(
    {
      fragmentId: 'f1',
      attemptId: retry.attemptId,
      rows: [{ key: 'r1' }, { key: 'r2' }],
      final: true,
    },
    1_200,
  );
  assert.equal(accepted.action, Action.ACCEPT);

  // 旧 attempt 的结果晚到：必须被丢弃，不能进入合并。
  const late = planner.onPartialResult(
    {
      fragmentId: 'f1',
      attemptId: s1.attemptId,
      rows: [{ key: 'r1' }, { key: 'r2' }],
      final: true,
    },
    1_500,
  );
  assert.equal(late.action, Action.DISCARD);
  assert.equal(late.reason, 'duplicate_fragment_result');

  const report = planner.report();
  assert.equal(report.mergeable.f1.rowCount, 2);
  assert.deepEqual(
    report.mergeable.f1.rows.map((r) => r.key),
    ['r1', 'r2'],
  );
  const states = Object.fromEntries(
    report.attempts.f1.map((a) => [a.attemptId, a.state]),
  );
  assert.equal(states[s1.attemptId], AttemptState.SUPERSEDED);
  assert.equal(states[retry.attemptId], AttemptState.ACCEPTED);
});

test('重复行：同一 attempt 的 partial 按行键去重', () => {
  const planner = makePlanner();
  const [s1] = planner.start(0);

  planner.onPartialResult(
    { fragmentId: 'f1', attemptId: s1.attemptId, partId: 'p1', rows: [{ key: 'a' }, { key: 'b' }] },
    100,
  );
  const second = planner.onPartialResult(
    {
      fragmentId: 'f1',
      attemptId: s1.attemptId,
      partId: 'p2',
      rows: [{ key: 'b' }, { key: 'c' }],
      final: true,
    },
    200,
  );
  assert.equal(second.action, Action.ACCEPT);
  assert.equal(second.duplicatesDropped, 1);

  const report = planner.report();
  assert.equal(report.mergeable.f1.rowCount, 3);
  assert.deepEqual(
    report.mergeable.f1.rows.map((r) => r.key),
    ['a', 'b', 'c'],
  );
});

test('分片部分成功：同一 attempt 的多个 partial 累积，旧 attempt 的 partial 不得数组合并', () => {
  const planner = makePlanner();
  const [s1] = planner.start(0);

  // 旧 attempt 交付了分片的一半。
  planner.onPartialResult(
    {
      fragmentId: 'f1',
      attemptId: s1.attemptId,
      shardRange: [0, 50],
      rows: [{ key: 'r1' }, { key: 'r2' }],
    },
    100,
  );

  // 超时重试，旧 attempt 作废。
  const retry = planner.onTimeout('f1', s1.attemptId, 1_000);
  assert.equal(retry.action, Action.RETRY);

  // 旧 attempt 剩余的 partial 晚到，必须丢弃而不是拼进新缓冲。
  const stalePart = planner.onPartialResult(
    {
      fragmentId: 'f1',
      attemptId: s1.attemptId,
      shardRange: [50, 100],
      rows: [{ key: 'r3' }, { key: 'r4' }],
    },
    1_100,
  );
  assert.equal(stalePart.action, Action.DISCARD);
  assert.equal(stalePart.reason, 'stale_attempt');

  // 新 attempt 分两个 partial 交付整个分片范围。
  planner.onPartialResult(
    {
      fragmentId: 'f1',
      attemptId: retry.attemptId,
      shardRange: [0, 50],
      rows: [{ key: 'r1' }, { key: 'r2' }],
    },
    1_200,
  );
  const done = planner.onPartialResult(
    {
      fragmentId: 'f1',
      attemptId: retry.attemptId,
      shardRange: [50, 100],
      rows: [{ key: 'r3' }, { key: 'r4' }],
      final: true,
    },
    1_300,
  );
  assert.equal(done.action, Action.ACCEPT);

  const report = planner.report();
  // 合并结果只来自被接受的新 attempt，不与旧 attempt 的 partial 拼接。
  assert.equal(report.mergeable.f1.attemptId, retry.attemptId);
  assert.equal(report.mergeable.f1.rowCount, 4);
});

test('预算耗尽：重试次数用完后终止整查询', () => {
  const planner = makePlanner({ retryBudget: 1 });
  const [s1, s2] = planner.start(0);

  const retry = planner.onTimeout('f1', s1.attemptId, 1_000);
  assert.equal(retry.action, Action.RETRY);
  assert.equal(retry.retriesUsed, 1);

  // 第二次超时：预算已耗尽，终止整查询。
  const exhausted = planner.onTimeout('f1', retry.attemptId, 2_000);
  assert.equal(exhausted.action, Action.TERMINATE);
  assert.equal(exhausted.reason, TerminationReason.RETRY_BUDGET_EXHAUSTED);

  // 终止后任何结果（包括其他分片）都被丢弃。
  const late = planner.onPartialResult(
    { fragmentId: 'f2', attemptId: s2.attemptId, rows: [{ key: 'x' }], final: true },
    2_100,
  );
  assert.equal(late.action, Action.DISCARD);
  assert.equal(late.reason, 'query_terminated');

  const report = planner.report();
  assert.equal(report.termination, TerminationReason.RETRY_BUDGET_EXHAUSTED);
  assert.deepEqual(Object.keys(report.mergeable), []);
});

test('协调器崩溃：从快照恢复后，崩溃前旧 attempt 的晚到结果仍被丢弃', () => {
  const planner = makePlanner();
  const [s1] = planner.start(0);

  const retry = planner.onTimeout('f1', s1.attemptId, 1_000);
  assert.equal(retry.action, Action.RETRY);

  // 协调器崩溃：持久化快照并模拟重启恢复。
  const snapshot = planner.snapshot();
  const restored = RetryPlanner.restore(snapshot);

  // 崩溃前旧 attempt 的结果晚到，恢复后依然识别为过期并丢弃。
  const late = restored.onPartialResult(
    { fragmentId: 'f1', attemptId: s1.attemptId, rows: [{ key: 'r1' }], final: true },
    1_500,
  );
  assert.equal(late.action, Action.DISCARD);
  assert.equal(late.reason, 'stale_attempt');

  // 新 attempt 在恢复后继续完成，重试预算消耗也被保留。
  const accepted = restored.onPartialResult(
    { fragmentId: 'f1', attemptId: retry.attemptId, rows: [{ key: 'r1' }], final: true },
    1_600,
  );
  assert.equal(accepted.action, Action.ACCEPT);
  assert.equal(restored.retriesUsed, 1);

  const report = restored.report();
  assert.equal(report.mergeable.f1.rowCount, 1);
  assert.equal(report.mergeable.f1.attemptId, retry.attemptId);
});

test('取消与 deadline：取消后拒绝新结果，超过 deadline 终止整查询', () => {
  const cancelled = makePlanner();
  cancelled.start(0);
  cancelled.cancel();
  const report = cancelled.report();
  assert.equal(report.termination, TerminationReason.CANCELLED);
  for (const attempts of Object.values(report.attempts)) {
    for (const attempt of attempts) assert.equal(attempt.state, AttemptState.CANCELLED);
  }

  const expired = makePlanner({ deadlineMs: 500 });
  const [s1] = expired.start(0);
  const decision = expired.onTimeout('f1', s1.attemptId, 600);
  assert.equal(decision.action, Action.TERMINATE);
  assert.equal(decision.reason, TerminationReason.DEADLINE_EXCEEDED);
});

test('全部分片被接受后查询完成，终止原因为 completed', () => {
  const planner = makePlanner();
  const [s1, s2] = planner.start(0);
  planner.onPartialResult(
    { fragmentId: 'f1', attemptId: s1.attemptId, rows: [{ key: 'a' }], final: true },
    100,
  );
  const done = planner.onPartialResult(
    { fragmentId: 'f2', attemptId: s2.attemptId, rows: [{ key: 'b' }], final: true },
    200,
  );
  assert.equal(done.queryCompleted, true);
  const report = planner.report();
  assert.equal(report.termination, TerminationReason.COMPLETED);
  assert.deepEqual(Object.keys(report.mergeable).sort(), ['f1', 'f2']);
});
