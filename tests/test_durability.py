"""可靠性边界：崩溃重放、字段顺序、touch 不产生修订、scroll TTL、状态保留。"""
import pytest

from src.scrollharvest import store
from src.scrollharvest.config import (
    FP_V2, PROTO_CURSOR, PROTO_OFFSET, PROTO_SCROLL, PROTO_TIME_ANCHOR,
)
from src.scrollharvest.convergence import EndReason, HarvestBudget

from conftest import make_engine, make_source


def identities(con):
    return sorted(k for k, _ in store.valid_records(con))


class CrashAfterInflight:
    """模拟在 inflight 落盘后、取页返回前进程崩溃（抛 KeyboardInterrupt 类）。"""

    def __init__(self, real_source, crash_on: int = 1):
        self._real = real_source
        self._left = crash_on

    def __getattr__(self, name):
        return getattr(self._real, name)

    def fetch(self, request):
        if self._left > 0:
            self._left -= 1
            raise RuntimeError("simulated process crash after inflight persisted")
        return self._real.fetch(request)


def test_crash_after_inflight_replay_does_not_duplicate(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_OFFSET,))
    crashing = CrashAfterInflight(source, crash_on=1)
    r1 = make_engine(crashing, con, clock, order=(PROTO_OFFSET,),
                     page_size=2).run()
    assert r1["end_reason"] == EndReason.FATAL.value

    # inflight 请求必须仍在状态表中，不能被伪装成协议失效
    state = store.get_state(con, source.name, "main")
    assert state["phase"] == "inflight"
    assert state["inflight_request"] is not None

    # 用健康源从同一状态续采
    healthy = make_source(seed, clock, protocols=(PROTO_OFFSET,))
    r2 = make_engine(healthy, con, clock, order=(PROTO_OFFSET,),
                     page_size=2, stream="main").run()
    assert len(identities(con)) == 6
    # 台账中每个身份只有一条生效写入
    rows = con.execute(
        "SELECT identity, COUNT(*) FROM write_ledger "
        "WHERE action='insert' GROUP BY identity"
    ).fetchall()
    assert all(n == 1 for _, n in rows)
    assert r2["end_reason"] in (
        EndReason.END_REACHED_UNVERIFIED.value, EndReason.QUIESCED.value,
    )


def test_field_reorder_on_source_side_creates_no_new_revision(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_CURSOR,))
    make_engine(source, con, clock, order=(PROTO_CURSOR,)).run()
    revs_before = con.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]
    keys_before = {k for k, _ in store.all_records(con)}

    # 源站对同批记录仅打乱字段顺序（内容指纹基于规范化 JSON，顺序无关）
    for identity, rec in list(source._items.items()):
        source._items[identity] = dict(reversed(list(rec.items())))

    result = make_engine(source, con, clock, order=(PROTO_CURSOR,),
                         stream="main").run()
    assert result["stats"]["inserted"] == 0
    assert result["stats"]["updated"] == 0
    assert con.execute("SELECT COUNT(*) FROM revisions").fetchone()[0] == revs_before
    assert {k for k, _ in store.all_records(con)} == keys_before
    assert len(identities(con)) == 6


def test_url_tracking_change_keeps_identity_but_records_revision(seed, clock, con):
    from src.scrollharvest.keyer import stable_key
    source = make_source(seed, clock, protocols=(PROTO_CURSOR,))
    make_engine(source, con, clock, order=(PROTO_CURSOR,)).run()

    for identity, rec in list(source._items.items()):
        changed = dict(rec)
        changed["url"] = rec["url"] + "&utm_campaign=spring"
        source._items[identity] = changed
        # 身份键吸收跟踪参数：不产生新身份
        assert stable_key(changed)[0] == stable_key(rec)[0]

    r2 = make_engine(source, con, clock, order=(PROTO_CURSOR,),
                     stream="main").run()
    assert r2["stats"]["inserted"] == 0          # 没有新记录
    assert r2["stats"]["updated"] == 6           # 但 URL 内容变化留修订
    assert len(identities(con)) == 6
    assert all(
        [rev["action"] for rev in store.revisions_of(con, k)][-1] == "update"
        for k in identities(con)
    )


def test_touch_only_refresh_is_not_content_update(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_CURSOR,))
    make_engine(source, con, clock, order=(PROTO_CURSOR,),
                fp_version=FP_V2).run()
    revs_before = len(store.revisions_of(con, "r-001"))

    source.touch("r-001")  # 只改 updated_at
    r2 = make_engine(source, con, clock, order=(PROTO_CURSOR,),
                     fp_version=FP_V2, stream="main").run()
    assert r2["stats"]["updated"] == 0
    assert len(store.revisions_of(con, "r-001")) == revs_before


def test_scroll_ttl_expiry_falls_back(seed, clock, con):
    source = make_source(
        seed, clock, protocols=(PROTO_SCROLL, PROTO_OFFSET), ttl=10,
    )
    calls = {"n": 0}
    original = source.fetch

    def aging_fetch(request):
        calls["n"] += 1
        page = original(request)
        if calls["n"] == 1:
            clock.advance(30)
        return page

    source.fetch = aging_fetch
    result = make_engine(
        source, con, clock, order=(PROTO_SCROLL, PROTO_OFFSET), page_size=2
    ).run()
    assert result["stats"]["cursor_expired"] >= 1
    assert len(identities(con)) == 6


def test_state_retained_across_runs_with_protocol_and_request(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_OFFSET,))
    budget = HarvestBudget(max_pages=1, max_runtime_seconds=3600,
                           max_bytes=10_000_000, empty_streak_limit=10)
    make_engine(source, con, clock, order=(PROTO_OFFSET,),
                page_size=2, budget=budget).run()
    state = store.get_state(con, source.name, "main")
    assert state["protocol"] == PROTO_OFFSET
    assert state["resume_request"] is not None
    assert '"offset": 2' in state["resume_request"]

    full_budget = HarvestBudget(max_pages=50, max_runtime_seconds=3600,
                                max_bytes=10_000_000, empty_streak_limit=10)
    make_engine(source, con, clock, order=(PROTO_OFFSET,),
                page_size=2, budget=full_budget, stream="main").run()
    assert len(identities(con)) == 6


def test_coverage_three_states_are_queryable(seed, clock, con):
    # 无快照 + cursor 失效：同时出现 covered/pending/unconfirmable
    source = make_source(
        seed, clock, protocols=(PROTO_CURSOR, PROTO_OFFSET),
    )
    calls = {"n": 0}
    original = source.fetch

    def mutate_after_first(request):
        calls["n"] += 1
        page = original(request)
        if calls["n"] == 1:
            source.update("r-002", {"value": 555})
        return page

    source.fetch = mutate_after_first
    make_engine(source, con, clock,
                order=(PROTO_CURSOR, PROTO_OFFSET), page_size=2).run()
    summary = store.coverage_summary(con, source.name, "main")
    assert set(summary) == {"covered", "pending", "unconfirmable"}
    assert summary["covered"] >= 1
    # cursor 被代际变化击穿，至少一个窗口回退 pending
    assert summary["pending"] >= 1
    assert summary["unconfirmable"] >= 1


def test_runs_table_records_end_reason_and_stats(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_TIME_ANCHOR,),
                         snapshot=True)
    make_engine(source, con, clock, order=(PROTO_TIME_ANCHOR,)).run()
    history = store.run_history(con, source.name, "main")
    assert len(history) == 1
    h = history[0]
    assert h["end_reason"] == EndReason.COMPLETE.value
    assert h["status"] == "ok"
    assert h["stats"]["inserted"] == 6
    assert h["started_at"] and h["ended_at"]


def test_big_page_streaming_budget_respected(seed, clock, con):
    # 大页被硬上限拦截，防止有限资源下一次性拉取失控
    source = make_source(seed, clock)
    with pytest.raises(ValueError):
        make_engine(source, con, clock, page_size=10_000)
    # 显式放宽后可以运行
    from src.scrollharvest.engine import HarvestConfig, HarvestEngine
    from src.scrollharvest.pagination import PaginationPlan
    cfg = HarvestConfig(
        page_size=10_000, max_page_size=10_000,
        budget=HarvestBudget(max_pages=50, max_runtime_seconds=3600,
                             max_bytes=10_000_000, empty_streak_limit=10,
                             quiet_polls=0),
        plan=PaginationPlan(page_size=10_000,
                            preferred_order=(PROTO_OFFSET,)),
    )
    result = HarvestEngine(source, cfg, con=con, clock=clock).run()
    # 6 条记录一页装下：真实分页请求恰好 1 次
    assert result["stats"]["pages_fetched"] - result["stats"]["poll_fetches"] == 1
    assert len(identities(con)) == 6


def test_similar_independent_records_flagged_and_both_kept(clock, con):
    # 同名不同值的两条：不同身份独立保留，不做相似度合并
    seed = [
        {"id": "w-1", "name": "widget", "value": 1,
         "updated_at": "2026-11-01T00:00:00Z"},
        {"id": "w-2", "name": "widget", "value": 2,
         "updated_at": "2026-11-02T00:00:00Z"},
    ]
    source = make_source(seed, clock, protocols=(PROTO_CURSOR,))
    make_engine(source, con, clock, order=(PROTO_CURSOR,),
                stream="sim").run()
    assert len(identities(con)) == 2

    # 内容完全相同但身份不同（不同 id）的两条：标 SIMILAR，仍各自独立入库
    dup_seed = [
        {"id": "x-1", "name": "gadget", "value": 9,
         "updated_at": "2026-11-01T00:00:00Z"},
        {"id": "x-2", "name": "gadget", "value": 9,
         "updated_at": "2026-11-01T00:00:00Z"},
    ]
    source2 = make_source(dup_seed, clock, protocols=(PROTO_CURSOR,))
    r2 = make_engine(source2, con, clock, order=(PROTO_CURSOR,),
                     stream="sim2").run()
    assert r2["stats"]["similar"] == 1
    assert len(identities(con)) == 4
    revs = con.execute(
        "SELECT DISTINCT action FROM revisions WHERE run_id=?",
        (r2["run_id"],)
    ).fetchall()
    assert ("similar",) in revs


def test_deleted_record_reappearing_is_distinct_event(seed, clock, con):
    source = make_source(seed, clock, protocols=(PROTO_CURSOR,))
    make_engine(source, con, clock, order=(PROTO_CURSOR,)).run()
    source.delete("r-004")
    make_engine(source, con, clock, order=(PROTO_CURSOR,),
                stream="main").run()
    assert store.is_deleted(con, "r-004")

    # 源站重新放出同 id 记录（合法复活）：应作为新修订 upsert，墓碑保留可追溯
    source.insert({"id": "r-004", "name": "Record Four Reborn", "value": 400,
                   "updated_at": clock.now_iso()})
    r3 = make_engine(source, con, clock, order=(PROTO_CURSOR,),
                     stream="main").run()
    assert r3["stats"]["inserted"] == 1
    assert "r-004" in identities(con)
    actions = [r["action"] for r in store.revisions_of(con, "r-004")]
    assert "delete" in actions and actions[-1] == "insert"
    # 墓碑行仍在，删除历史不被覆盖性抹掉
    assert con.execute(
        "SELECT 1 FROM tombstones WHERE identity='r-004'"
    ).fetchone() is not None
