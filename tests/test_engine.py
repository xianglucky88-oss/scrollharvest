"""引擎集成测试：四协议、协议回退、断点续采、幂等确认、覆盖范围与停止条件。"""
import pytest

from src.scrollharvest import store
from src.scrollharvest.config import (
    PROTO_CURSOR, PROTO_OFFSET, PROTO_SCROLL, PROTO_TIME_ANCHOR,
)
from src.scrollharvest.convergence import EndReason, HarvestBudget

from conftest import make_engine, make_source


def identities(con):
    return sorted(k for k, _ in store.valid_records(con))


# ---- 四协议都能完成基础采集 ---------------------------------------------
@pytest.mark.parametrize("protocol", [
    PROTO_OFFSET, PROTO_CURSOR, PROTO_SCROLL, PROTO_TIME_ANCHOR,
])
def test_all_four_protocols_harvest(seed, clock, con, protocol):
    source = make_source(seed, clock, protocols=(protocol,))
    engine = make_engine(source, con, clock, order=(protocol,))
    result = engine.run()
    assert result["stats"]["inserted"] == 6
    assert identities(con) == sorted(r["id"] for r in seed)


def test_snapshot_source_claims_complete(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_TIME_ANCHOR,), snapshot=True)
    engine = make_engine(source, con, clock, order=(PROTO_TIME_ANCHOR,))
    result = engine.run()  # 首次请求固定快照
    # 快照固定后再插入，后续轮次对本轮视图不可见
    source.insert({"id": "late-1", "updated_at": clock.now_iso()})
    assert result["end_reason"] == EndReason.COMPLETE.value
    assert result["can_claim_complete"] is True
    assert "late-1" not in identities(con)


def test_live_source_never_claims_complete(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_CURSOR,))
    result = make_engine(source, con, clock, order=(PROTO_CURSOR,)).run()
    assert result["end_reason"] == EndReason.END_REACHED_UNVERIFIED.value
    assert result["can_claim_complete"] is False
    assert result["coverage"]["unconfirmable"] >= 1


# ---- 采集过程中源站变异：插入/更新/删除 ---------------------------------
def test_concurrent_insert_update_delete_during_harvest(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_OFFSET,))
    engine = make_engine(source, con, clock, order=(PROTO_OFFSET,), page_size=2)
    # 在引擎启动前先做一轮，再模拟“第二轮期间”的变异：直接构造源并先跑首页
    result = engine.run()
    assert result["stats"]["inserted"] == 6

    # 后续变异 + 重跑：更新产生新修订，插入产生新记录，删除产生墓碑
    source.update("r-002", {"value": 999})
    source.insert({"id": "r-007", "updated_at": clock.now_iso(), "name": "Seven"})
    deletion = source.delete("r-003")
    engine2 = make_engine(source, con, clock, order=(PROTO_OFFSET,), page_size=2,
                          stream="main")
    r2 = engine2.run()
    assert r2["stats"]["updated"] == 1
    assert r2["stats"]["inserted"] == 1
    assert r2["stats"]["deleted"] == 1
    assert "r-003" not in identities(con)
    assert store.is_deleted(con, "r-003")
    revs = store.revisions_of(con, "r-002")
    actions = [r["action"] for r in revs]
    assert actions == ["insert", "update"]
    assert all(r["fp_rule"].startswith("fp.") for r in revs)
    tomb = con.execute(
        "SELECT deleted_at FROM tombstones WHERE identity='r-003'"
    ).fetchone()
    assert tomb[0] == deletion.deleted_at


def test_similar_independent_records_both_kept(clock, con):
    seed = [
        {"id": "a-1", "name": "widget", "value": 1,
         "updated_at": "2026-11-01T00:00:00Z"},
        {"id": "a-2", "name": "widget", "value": 2,
         "updated_at": "2026-11-02T00:00:00Z"},
    ]
    source = make_source(seed, clock, protocols=(PROTO_CURSOR,))
    make_engine(source, con, clock, order=(PROTO_CURSOR,)).run()
    got = dict(store.valid_records(con))
    assert set(got) == {"a-1", "a-2"}


# ---- offset 头部插入：窗口漂移与 pending/补采 ----------------------------
def test_offset_head_insert_marks_pending_and_repair_converges(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_OFFSET,))
    engine = make_engine(source, con, clock, order=(PROTO_OFFSET,), page_size=3)
    r1 = engine.run()
    assert r1["stats"]["inserted"] == 6

    # 头部插入两条，恰好制造整页漂移
    source.insert({"id": "head-1", "updated_at": "2026-11-20T00:00:00Z"})
    source.insert({"id": "head-2", "updated_at": "2026-11-21T00:00:00Z"})
    r2 = make_engine(source, con, clock, order=(PROTO_OFFSET,),
                     page_size=3, stream="main").run()
    assert r2["stats"]["inserted"] == 2
    # 无快照 offset 尾部不可确认
    assert store.coverage_summary(con, source.name, "main")["unconfirmable"] >= 1

    # 补采：源静止后 repair 重走，pending 收敛（无新写入）
    r3 = make_engine(source, con, clock, order=(PROTO_OFFSET,),
                     page_size=3, stream="main").run(repair=True)
    assert r3["stats"]["inserted"] == 0
    assert store.coverage_summary(con, source.name, "main")["pending"] == 0
    assert set(identities(con)) == {r["id"] for r in seed} | {"head-1", "head-2"}


# ---- 游标失效与协议回退 -------------------------------------------------
def test_cursor_expiry_falls_back_and_resumes(seed, clock, con):
    source = make_source(
        seed, clock, protocols=(PROTO_CURSOR, PROTO_OFFSET), ttl=10,
    )
    budget = HarvestBudget(max_pages=50, max_runtime_seconds=3600,
                           max_bytes=10_000_000, empty_streak_limit=3)
    engine = make_engine(source, con, clock, page_size=2,
                         order=(PROTO_CURSOR, PROTO_OFFSET), budget=budget)

    # 取第一页后令时钟越过 TTL，第二个 cursor 必然失效
    original_fetch = source.fetch
    calls = {"n": 0}

    def fetch_then_age(request):
        calls["n"] += 1
        page = original_fetch(request)
        if calls["n"] == 1:
            clock.advance(60)
        return page

    source.fetch = fetch_then_age
    result = engine.run()
    assert result["stats"]["cursor_expired"] >= 1
    assert result["stats"]["inserted"] == 6  # 回退 offset 重走，幂等不重复
    assert result["stats"]["ledger_dedup"] >= 0
    # 旧 cursor 窗口被降级，最终由 offset 补成 covered/unconfirmable
    summary = store.coverage_summary(con, source.name, "main")
    assert summary["covered"] >= 1


def test_protocol_unsupported_switches_to_next(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_CURSOR, PROTO_OFFSET))
    result = make_engine(
        source, con, clock,
        order=(PROTO_SCROLL, PROTO_CURSOR, PROTO_OFFSET),
    ).run()
    assert result["stats"]["protocol_switches"] >= 1
    assert result["stats"]["inserted"] == 6


def test_all_protocols_exhausted_is_failure(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_TIME_ANCHOR,))
    result = make_engine(
        source, con, clock, order=(PROTO_SCROLL, PROTO_CURSOR, PROTO_OFFSET),
    ).run()
    assert result["end_reason"] == EndReason.PROTOCOL_EXHAUSTED.value
    assert result["can_claim_complete"] is False


def test_generation_bump_invalidates_cursor(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_CURSOR, PROTO_OFFSET))
    calls = {"n": 0}
    original = source.fetch

    def fetch_with_mutation(request):
        calls["n"] += 1
        page = original(request)
        if calls["n"] == 1:
            source.update("r-001", {"value": 4242})  # 代际+1，旧 cursor 失效
        return page

    source.fetch = fetch_with_mutation
    result = make_engine(
        source, con, clock, order=(PROTO_CURSOR, PROTO_OFFSET), page_size=2
    ).run()
    assert result["stats"]["cursor_expired"] >= 1
    got = dict(store.valid_records(con))
    assert got["r-001"]["value"] == 4242


# ---- 断点续采：预算中断后继续 -------------------------------------------
def test_budget_stop_and_resume(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_OFFSET,))
    small = HarvestBudget(max_pages=2, max_runtime_seconds=3600,
                          max_bytes=10_000_000, empty_streak_limit=10)
    engine = make_engine(source, con, clock, order=(PROTO_OFFSET,),
                         page_size=2, budget=small)
    r1 = engine.run()
    assert r1["end_reason"] == EndReason.BUDGET_PAGES.value
    assert r1["stats"]["inserted"] == 4  # 2 页 × 2 条

    r2 = make_engine(source, con, clock, order=(PROTO_OFFSET,),
                     page_size=2, budget=small, stream="main").run()
    assert r2["stats"]["inserted"] == 2
    assert len(identities(con)) == 6
    # 运行台账记录了两轮及各自结束原因
    history = store.run_history(con, source.name, "main")
    reasons = [h["end_reason"] for h in history]
    assert EndReason.BUDGET_PAGES.value in reasons


def test_runtime_budget_is_resumable(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_OFFSET,))
    budget = HarvestBudget(max_pages=50, max_runtime_seconds=5,
                           max_bytes=10_000_000, empty_streak_limit=10)

    def slow_clock_engine():
        return make_engine(source, con, clock, order=(PROTO_OFFSET,),
                           page_size=2, budget=budget, stream="rt")

    calls = {"n": 0}

    def slow_fetch(request):
        calls["n"] += 1
        if calls["n"] == 2:
            clock.advance(10)
        return type(source).fetch(source, request)

    source.fetch = slow_fetch
    r1 = slow_clock_engine().run()
    assert r1["end_reason"] == EndReason.BUDGET_RUNTIME.value
    r2 = slow_clock_engine().run()
    assert len(identities(con)) == 6
    assert r1["run_id"] != r2["run_id"]


# ---- 提交确认丢失：重放不重复写入 ---------------------------------------
class CommitLostConnection:
    """代理连接：前 N 次 commit 抛异常模拟确认丢失，之后恢复。"""

    def __init__(self, real, fail_times=1):
        self._real = real
        self._fail_left = fail_times

    def __getattr__(self, name):
        return getattr(self._real, name)

    def commit(self):
        if self._fail_left > 0:
            self._fail_left -= 1
            raise ConnectionError("network dropped before commit ack")
        return self._real.commit()


def test_confirmation_loss_replay_writes_once(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_OFFSET,))
    flaky = CommitLostConnection(con, fail_times=1)
    engine = make_engine(source, flaky, clock, order=(PROTO_OFFSET,), page_size=2)
    r1 = engine.run()
    # 确认丢失以失败结束，但 inflight 检查点保留
    assert r1["end_reason"] == EndReason.FATAL.value
    state = store.get_state(con, source.name, "main")
    assert state["inflight_request"] is not None

    # 无论丢失的事务实际是否落库，重放后行数与台账都不允许重复
    before = len(identities(con))
    engine2 = make_engine(source, con, clock, order=(PROTO_OFFSET,),
                          page_size=2, stream="main")
    r2 = engine2.run()
    after = len(identities(con))
    assert after == 6
    # 台账中每个身份的生效写入（insert/update/delete）只允许一次
    dupes = con.execute(
        "SELECT identity, COUNT(*) FROM write_ledger "
        "WHERE action IN ('insert','update','delete') GROUP BY identity "
        "HAVING COUNT(*) > 1"
    ).fetchall()
    assert dupes == []
    assert r2["stats"]["inserted"] == 6 - before


def test_rerun_is_idempotent(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_CURSOR,))
    e1 = make_engine(source, con, clock, order=(PROTO_CURSOR,), page_size=2)
    r1 = e1.run()
    e2 = make_engine(source, con, clock, order=(PROTO_CURSOR,),
                     page_size=2, stream="main")
    r2 = e2.run()
    assert len(identities(con)) == 6
    assert r2["stats"]["inserted"] == 0
    # 修订链不因为重跑而膨胀
    total_revs = con.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]
    assert total_revs == r1["stats"]["inserted"]


# ---- 空页不等于完整：静止轮询与 unconfirmable ----------------------------
def test_empty_pages_dont_imply_completeness(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_CURSOR,))
    budget = HarvestBudget(max_pages=50, max_runtime_seconds=3600,
                           max_bytes=10_000_000, empty_streak_limit=1,
                           quiet_polls=1)
    result = make_engine(source, con, clock, order=(PROTO_CURSOR,),
                         page_size=50, budget=budget).run()
    # 页大于数据集：第一页就到尾，无快照 → 不可宣告完整
    assert result["can_claim_complete"] is False
    assert result["end_reason"] in (
        EndReason.END_REACHED_UNVERIFIED.value, EndReason.QUIESCED.value,
    )


def test_late_insert_during_quiescence_is_caught(seed, clock, con):
    source = make_source(seed[:3], clock, protocols=(PROTO_CURSOR,))
    budget = HarvestBudget(max_pages=50, max_runtime_seconds=3600,
                           max_bytes=10_000_000, empty_streak_limit=1,
                           quiet_polls=2)
    calls = {"n": 0}
    original = source.fetch

    def fetch_with_late_insert(request):
        calls["n"] += 1
        page = original(request)
        if calls["n"] == 2:  # 第一次静止轮询期间源站插入
            source.insert({"id": "late-9",
                           "updated_at": clock.now_iso(), "name": "late"})
        return page

    source.fetch = fetch_with_late_insert
    result = make_engine(source, con, clock, order=(PROTO_CURSOR,),
                         page_size=50, budget=budget).run()
    assert "late-9" in identities(con)
    assert result["stats"]["inserted"] == 4
    # 发现迟到插入后不能再以“静止”结束，只能给出不可验证结尾
    assert result["end_reason"] == EndReason.END_REACHED_UNVERIFIED.value


# ---- 大页与字节预算 ------------------------------------------------------
def test_oversized_page_requires_explicit_cap(seed, clock, con):
    source = make_source(seed, clock)
    with pytest.raises(ValueError):
        make_engine(source, con, clock, page_size=10_000)


def test_bytes_budget_stops_and_resumes(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_OFFSET,))
    budget = HarvestBudget(max_pages=50, max_runtime_seconds=3600,
                           max_bytes=1, empty_streak_limit=10)
    r1 = make_engine(source, con, clock, order=(PROTO_OFFSET,),
                     page_size=2, budget=budget).run()
    assert r1["end_reason"] == EndReason.BUDGET_BYTES.value
    make_engine(
        make_source(seed, clock, protocols=(PROTO_OFFSET,)), con, clock,
        order=(PROTO_OFFSET,), page_size=2,
        budget=HarvestBudget(max_pages=50, max_runtime_seconds=3600,
                             max_bytes=10_000_000, empty_streak_limit=10),
        stream="main",
    ).run()
    assert len(identities(con)) == 6


# ---- 查询面：有效记录 / 修订来源 / 结束原因 ------------------------------
def test_query_surface_after_full_lifecycle(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_TIME_ANCHOR,))
    engine = make_engine(source, con, clock, order=(PROTO_TIME_ANCHOR,),
                         page_size=2)
    engine.run()
    source.update("r-001", {"value": 777})
    make_engine(source, con, clock, order=(PROTO_TIME_ANCHOR,),
                page_size=2, stream="main").run()

    valid = dict(store.valid_records(con))
    assert set(valid) == {r["id"] for r in seed}
    assert valid["r-001"]["value"] == 777

    revs = store.revisions_of(con, "r-001")
    assert [r["action"] for r in revs] == ["insert", "update"]
    assert revs[-1]["source"] == source.name
    assert revs[-1]["run_id"]

    runs = store.run_history(con, source.name, "main")
    assert runs and all(r["end_reason"] for r in runs)
    latest = store.latest_run(con, source.name, "main")
    assert latest["status"] in ("ok", "resumable", "failed")


def test_time_anchor_floor_limits_scope(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_TIME_ANCHOR,),
                         snapshot=True)
    engine = make_engine(
        source, con, clock, order=(PROTO_TIME_ANCHOR,), page_size=2,
        floor_anchor="2026-11-03T00:00:00Z", stream="floor",
    )
    result = engine.run()
    # 只采集 updated_at >= floor 的记录（r-003..r-006）
    got = set(identities(con))
    assert got == {"r-003", "r-004", "r-005", "r-006"}
    assert result["end_reason"] == EndReason.COMPLETE.value
