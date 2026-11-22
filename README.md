# paxos-snapshot-compaction-validator

验证 Paxos 日志压缩（快照）安全点的 Python 模拟器：给定 acceptor 的
accepted 状态、chosen 索引、快照内容边界、leader term、成员日志位置与
快照安装状态，计算可截断范围并生成落后成员的追赶计划。

兼容 Python 3.8.2（仅使用标准库，`dataclasses` / `enum` / `unittest`）。

## 核心安全规则

1. **已 chosen 未入快照的提案不能删除**：快照边界 `last_included_index`
   不得超过 chosen 索引，且边界内每个索引的 chosen term 必须能从日志或
   多数派 acceptor 的 accepted 记录中重建，否则拒绝（`chosen_not_in_snapshot`）。
2. **法定人数验证**：快照记录的 `last_included_term` 必须与多数派
   acceptor 在该索引上的 accepted term 一致（`term_mismatch`）。
3. **learner 保护**：任何成员的持久化位置（`max(snapshot_index,
   match_index)`）落后于截断点时拒绝截断；快照安装失败
   （`member_install_failed`）或尚未安装（`member_needs_snapshot`）都会阻塞。
4. **leader 切换后重新验证边界**：快照带有 `created_term`，term 过期的
   快照会被标记 `stale_leader_term`，新 leader 必须重建并重新验证后才能截断。
5. **截断后可继续接受新提案**：新提案索引在截断边界之后递增，acceptor
   的 accepted 元数据保留，用于后续边界验证。

## 输出内容

- **快照候选 `snapshot_candidates`**：1..chosen 每个索引的候选，含合法性
  与拒绝原因。
- **可截断日志 `truncatable_indices` / `safe_upper_bound`**：请求安全时
  给出可删除索引；被拒绝时给出当前状态下真正安全的上界，且不删除任何日志。
- **落后成员 `lagging_members` 与追赶计划 `catch_up_tasks`**：三种模式
  - `install_snapshot`：成员日志早于截断点，先全量安装快照；
  - `resume_install`：安装失败/进行中，从 `resume_chunk` 断点续传；
  - `send_entries`：快照就位后追加同步剩余日志条目。
- **拒绝原因 `reject_reasons`**：机器可读 code + 中文说明，定义见
  `paxos_compaction/models.py` 的 `RejectReason`。

## 目录结构

```
paxos_compaction/
  models.py      # LogEntry / AcceptorState / MemberState / Snapshot / RejectReason 等
  analyzer.py    # 纯分析器：快照候选、截断计划、追赶计划、leader 切换重验
  simulator.py   # 有状态模拟器：提案、建快照、截断、分块安装/失败/续传
tests/
  test_compaction.py
```

## 快速上手

```python
from paxos_compaction import CompactionSimulator

sim = CompactionSimulator(["a1", "a2", "a3"], ["l1", "l2", "l3"])
for i in range(1, 6):
    sim.propose("cmd-%d" % i)

sim.members["l1"].match_index = 5
sim.members["l2"].match_index = 5
sim.members["l3"].match_index = 2          # 落后的 learner

snapshot = sim.create_snapshot(5)
plan = sim.truncate(snapshot)              # 不安全：l3 尚未保存快照
assert not plan.safe

sim.begin_snapshot_install("l3", snapshot, total_chunks=3)
for chunk in range(3):
    sim.deliver_snapshot_chunk("l3", chunk)
plan = sim.truncate(snapshot)              # 安全：截断 1..5
sim.propose("cmd-6")                       # 截断后继续接受新提案

sim.change_leader(2)
sim.revalidate_snapshot(snapshot)          # leader 切换后重新验证边界
```

## 测试

```bash
python -m unittest
```

覆盖场景：

- **截断竞争**：learner 未追上时截断被拒，日志一条不删；安装完成后截断成功；
  截断后 leader 继续接受新提案。
- **leader 切换**：旧 term 快照重验失败（`stale_leader_term`），新 term
  重建快照后通过；旧 term 快照不能用于截断。
- **快照安装失败**：失败状态阻塞截断并生成 `resume_install` 追赶任务。
- **learner 断点续传**：从最后成功块的下一块续传，完成后追加日志同步；
  乱序块被拒；无快照覆盖已截断历史时抛出 `HistoryUnavailableError`。

## 关于网络通信

**本模拟器不实现任何网络通信。** 它是纯内存的状态推演工具：Paxos 的
Prepare/Accept/ chosen 过程、快照安装（`InstallSnapshot`）与日志追加
（`AppendEntries`）都被抽象为本地方法调用，不包含 socket、RPC、消息
序列化、超时重试或集群进程。它只回答“在给定集群状态下，在哪里截断是
安全的、谁落后、如何追赶”这一安全点分析问题。
