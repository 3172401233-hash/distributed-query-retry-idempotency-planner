# distributed-query-retry-idempotency-planner
规划分布式查询重试、分片结果去重和取消

## 背景

分布式查询中，某个分片超时后协调器会重试整个片段；旧片段可能稍后返回，
如果合并阶段不做幂等处理，同一行会被统计两次。本项目实现一个纯决策层的
重试规划器（`src/planner.js`），解决这一问题。

**本项目不执行任何真实查询**：没有网络 IO、没有存储引擎、没有 SQL 执行。
它只根据输入（查询计划、片段 ID、attempt ID、分片范围、部分结果摘要、
deadline、取消状态、重试预算）输出决策与合并许可，供真实的协调器集成。

## 决策模型

- `onTimeout(fragmentId, attemptId, now)`：租约未到期 → `wait`（等待旧尝试）；
  租约到期且预算充足 → `retry`（作废旧 attempt，签发新 attempt）；
  预算耗尽或超过 deadline → `terminate`（终止整查询）。
- `onPartialResult(...)`：只有当前活跃 attempt 的 partial 能进入合并缓冲；
  同一片段已有结果被接受后，晚到的旧结果一律 `discard`，
  保证同一片段只有一个结果参与合并。
- 不同 attempt 的 partial rows 绝不简单数组合并：合并缓冲按
  `(fragmentId, attemptId)` 隔离，同一 attempt 内按行键去重。
- `cancel()`：取消后所有 pending attempt 置为 `cancelled`，后续结果全部丢弃。
- `snapshot()` / `RetryPlanner.restore()`：协调器崩溃后可从快照恢复，
  重试预算消耗与 attempt 状态完整保留。

## 输出

`report()` 返回：

- `attempts`：每个片段下所有 attempt 的状态
  （`pending` / `accepted` / `superseded` / `cancelled`）与行数；
- `mergeable`：允许参与合并的结果（每片段至多一个 attempt）；
- `termination`：最终终止原因
  （`completed` / `cancelled` / `deadline_exceeded` / `retry_budget_exhausted`）。

## 运行测试

```sh
node --test
```

测试覆盖：旧结果晚到、重复行去重、分片部分成功、重试预算耗尽、
协调器崩溃恢复，以及取消与 deadline 终止。
