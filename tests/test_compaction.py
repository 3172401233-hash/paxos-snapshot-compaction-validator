"""压缩安全点模拟器的单元测试。

覆盖：截断竞争、leader 切换、快照安装失败、learner 断点续传，
以及 chosen 未入快照保护、空日志与 term 不一致等边界。
"""

import unittest

from paxos_compaction import (
    CompactionSimulator,
    HistoryUnavailableError,
    InstallState,
    RejectReason,
)


def build_cluster(entries=5, acceptors=("a1", "a2", "a3"),
                  members=("l1", "l2", "l3")):
    """构造一个产生 ``entries`` 条 chosen 日志的集群。"""

    sim = CompactionSimulator(
        acceptor_names=list(acceptors), member_names=list(members)
    )
    for index in range(1, entries + 1):
        sim.propose("cmd-%d" % index)
    return sim


def codes(reasons):
    return [reason.code for reason in reasons]


class SnapshotCandidateTest(unittest.TestCase):

    def test_all_chosen_indices_are_valid_candidates(self):
        sim = build_cluster(entries=5)
        result = sim.analyze()
        self.assertEqual(
            [c.last_included_index for c in result.snapshot_candidates],
            [1, 2, 3, 4, 5],
        )
        self.assertTrue(all(c.valid for c in result.snapshot_candidates))

    def test_beyond_chosen_is_rejected(self):
        sim = build_cluster(entries=3)
        with self.assertRaises(ValueError):
            sim.create_snapshot(4)

    def test_empty_log_has_no_candidate(self):
        sim = build_cluster(entries=0)
        result = sim.analyze()
        self.assertEqual(result.snapshot_candidates, [])
        self.assertIsNone(result.plan)


class ChosenNotInSnapshotTest(unittest.TestCase):

    def test_chosen_entry_without_quorum_cannot_be_snapshotted(self):
        # 抹掉索引 3 的部分 acceptor 记录，制造无法重建的 chosen 状态
        sim = build_cluster(entries=3)
        del sim.acceptors["a1"].accepted_terms[3]
        sim.chosen_index = 3
        result = sim.analyze()
        candidate_3 = result.snapshot_candidates[2]
        self.assertFalse(candidate_3.valid)
        self.assertIn(
            RejectReason.CHOSEN_NOT_IN_SNAPSHOT.code,
            codes(candidate_3.reject_reasons),
        )

    def test_wrong_snapshot_term_is_rejected(self):
        sim = build_cluster(entries=3)
        snapshot = sim.create_snapshot(3)
        snapshot.last_included_term = 99
        result = sim.analyze(snapshot)
        plan = result.plan
        self.assertFalse(plan.safe)
        self.assertIn(RejectReason.TERM_MISMATCH.code,
                      codes(plan.reject_reasons))


class TruncationRaceTest(unittest.TestCase):
    """截断竞争：落后 learner 尚未追上时领导者尝试截断。"""

    def test_truncation_blocked_while_learner_lags(self):
        sim = build_cluster(entries=5, members=("l1", "l2", "l3"))
        sim.members["l1"].match_index = 5
        sim.members["l2"].match_index = 5
        sim.members["l3"].match_index = 2       # 落后的 learner
        snapshot = sim.create_snapshot(5)

        plan = sim.truncate(snapshot)
        self.assertFalse(plan.safe)
        self.assertIn(RejectReason.MEMBER_NEEDS_SNAPSHOT.code,
                      codes(plan.reject_reasons))
        self.assertEqual(plan.lagging_members, ["l3"])
        # 拒绝截断时日志一条都不能删
        self.assertEqual([e.index for e in sim.log], [1, 2, 3, 4, 5])
        self.assertEqual(sim.truncated_up_to, 0)
        # 给出的追赶计划是先装快照
        task = plan.catch_up_tasks[0]
        self.assertEqual(task.member, "l3")
        self.assertEqual(task.mode, "install_snapshot")
        self.assertEqual(task.to_index, 5)

    def test_truncation_succeeds_after_snapshot_installed(self):
        sim = build_cluster(entries=5, members=("l1", "l2", "l3"))
        sim.members["l1"].match_index = 5
        sim.members["l2"].match_index = 5
        sim.members["l3"].match_index = 2
        snapshot = sim.create_snapshot(5)

        sim.begin_snapshot_install("l3", snapshot, total_chunks=3)
        for chunk in range(3):
            sim.deliver_snapshot_chunk("l3", chunk)
        self.assertEqual(sim.members["l3"].install_state,
                         InstallState.COMPLETE)

        plan = sim.truncate(snapshot)
        self.assertTrue(plan.safe, codes(plan.reject_reasons))
        self.assertEqual(plan.truncatable_indices, [1, 2, 3, 4, 5])
        self.assertEqual(sim.truncated_up_to, 5)
        self.assertEqual([e.index for e in sim.log], [])

    def test_leader_keeps_accepting_after_truncation(self):
        sim = build_cluster(entries=3, members=("l1", "l2", "l3"))
        for member in sim.members.values():
            member.match_index = 3
        snapshot = sim.create_snapshot(3)
        plan = sim.truncate(snapshot)
        self.assertTrue(plan.safe)

        new_entry = sim.propose("cmd-4")
        self.assertEqual(new_entry.index, 4)
        self.assertEqual(sim.chosen_index, 4)
        self.assertEqual([e.index for e in sim.log], [4])


class LeaderChangeTest(unittest.TestCase):

    def test_snapshot_from_old_term_must_be_revalidated(self):
        sim = build_cluster(entries=4)
        snapshot = sim.create_snapshot(4)

        candidate = sim.revalidate_after_leader_change(2, snapshot)
        self.assertFalse(candidate.valid)
        self.assertIn(RejectReason.STALE_LEADER_TERM.code,
                      codes(candidate.reject_reasons))

    def test_rebuilt_snapshot_passes_in_new_term(self):
        sim = build_cluster(entries=4)
        sim.create_snapshot(4)
        sim.change_leader(2)
        # 新 leader 用当前 term 重建快照边界
        rebuilt = sim.create_snapshot(4)
        candidate = sim.revalidate_snapshot(rebuilt)
        self.assertTrue(candidate.valid, codes(candidate.reject_reasons))

    def test_truncation_uses_latest_leader_term(self):
        sim = build_cluster(entries=3, members=("l1", "l2", "l3"))
        for member in sim.members.values():
            member.match_index = 3
        stale = sim.create_snapshot(3)
        sim.change_leader(2)
        plan = sim.truncate(stale)
        self.assertFalse(plan.safe)
        self.assertIn(RejectReason.STALE_LEADER_TERM.code,
                      codes(plan.reject_reasons))
        self.assertEqual(sim.truncated_up_to, 0)


class SnapshotInstallFailureTest(unittest.TestCase):

    def test_failed_install_blocks_truncation(self):
        sim = build_cluster(entries=4, members=("l1", "l2", "l3"))
        sim.members["l1"].match_index = 4
        sim.members["l2"].match_index = 4
        sim.members["l3"].match_index = 1
        snapshot = sim.create_snapshot(4)

        sim.begin_snapshot_install("l3", snapshot, total_chunks=4)
        sim.deliver_snapshot_chunk("l3", 0)
        sim.fail_snapshot_install("l3")

        plan = sim.truncate(snapshot)
        self.assertFalse(plan.safe)
        self.assertIn(RejectReason.MEMBER_INSTALL_FAILED.code,
                      codes(plan.reject_reasons))
        task = next(t for t in plan.catch_up_tasks if t.member == "l3")
        self.assertEqual(task.mode, "resume_install")
        self.assertEqual(task.resume_chunk, 1)


class LearnerResumeTest(unittest.TestCase):

    def test_resume_from_last_chunk_then_catch_up_entries(self):
        sim = build_cluster(entries=5, members=("l1", "l2", "l3"))
        sim.members["l1"].match_index = 5
        sim.members["l2"].match_index = 5
        sim.members["l3"].match_index = 1
        snapshot = sim.create_snapshot(4)

        sim.begin_snapshot_install("l3", snapshot, total_chunks=4)
        sim.deliver_snapshot_chunk("l3", 0)
        sim.deliver_snapshot_chunk("l3", 1)
        sim.fail_snapshot_install("l3")

        next_chunk = sim.resume_snapshot_install("l3")
        self.assertEqual(next_chunk, 2)
        for chunk in range(2, 4):
            sim.deliver_snapshot_chunk("l3", chunk)
        self.assertEqual(sim.members["l3"].snapshot_index, 4)
        self.assertEqual(sim.members["l3"].install_state,
                         InstallState.COMPLETE)

        # 快照覆盖 1..4，只剩索引 5 需要追加同步
        sent = sim.send_entries("l3")
        self.assertEqual([entry.index for entry in sent], [5])
        self.assertEqual(sim.members["l3"].match_index, 5)

        # 全员就位后截断才安全
        plan = sim.truncate(snapshot)
        self.assertTrue(plan.safe, codes(plan.reject_reasons))
        self.assertEqual(sim.truncated_up_to, 4)

    def test_out_of_order_chunk_is_rejected(self):
        sim = build_cluster(entries=3)
        snapshot = sim.create_snapshot(3)
        sim.begin_snapshot_install("l1", snapshot, total_chunks=3)
        with self.assertRaises(ValueError):
            sim.deliver_snapshot_chunk("l1", 1)

    def test_send_entries_without_snapshot_raises_after_truncation(self):
        sim = build_cluster(entries=4, members=("l1", "l2", "l3"))
        sim.members["l1"].match_index = 4
        sim.members["l2"].match_index = 4
        # l3 保持 match_index=0 且不安装快照；通过手动推进边界模拟
        # “不安全截断已发生”的事故现场
        snapshot = sim.create_snapshot(4)
        sim.members["l3"].snapshot_index = 0
        sim.truncated_up_to = 4
        sim.log = []
        with self.assertRaises(HistoryUnavailableError):
            sim.send_entries("l3")


class SummaryOutputTest(unittest.TestCase):

    def test_summary_contains_required_sections(self):
        sim = build_cluster(entries=3, members=("l1", "l2", "l3"))
        snapshot = sim.create_snapshot(2)
        summary = sim.summary(sim.analyze(snapshot))
        self.assertEqual(summary["chosen_index"], 3)
        self.assertEqual(summary["leader_term"], 1)
        self.assertEqual(len(summary["snapshot_candidates"]), 3)
        plan = summary["plan"]
        self.assertEqual(plan["requested_index"], 2)
        self.assertIn("reject_reasons", plan)
        self.assertIn("lagging_members", plan)
        self.assertIn("catch_up_tasks", plan)


if __name__ == "__main__":
    unittest.main()
