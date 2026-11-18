"""身份键、内容指纹、规范化、合并决策与分页计划的单元测试。"""
import pytest

from src.scrollharvest.config import FP_V1, FP_V2
from src.scrollharvest.fingerprint import fingerprint
from src.scrollharvest.keyer import key_of, stable_key
from src.scrollharvest.merge import MergeAction, classify_record, decide_deletion
from src.scrollharvest.models import Deletion
from src.scrollharvest.normalizer import canonical_json, canonical_url
from src.scrollharvest.pagination import (
    OffsetPaginator, PaginationPlan, TimeAnchorPaginator,
)
from src.scrollharvest.convergence import HarvestBudget
from src.scrollharvest.errors import BudgetExceeded

from conftest import make_source


# ---- 身份键：URL/字段顺序变化不产生新记录 -------------------------------
def test_field_order_does_not_change_identity():
    a = {"name": "x", "value": 1, "region": "cn"}
    b = {"region": "cn", "value": 1, "name": "x"}
    ka, _ = stable_key(a)
    kb, _ = stable_key(b)
    assert ka == kb and ka.startswith("h:")


def test_url_tracking_params_and_case_are_same_identity():
    a = {"url": "https://Example.com/a?b=2&utm_source=mail#frag"}
    b = {"url": "https://example.com/a?b=2"}
    assert stable_key(a)[0] == stable_key(b)[0]
    assert canonical_url("HTTPS://X.COM/P?z=1&a=2") == "https://x.com/P?a=2&z=1"


def test_explicit_id_wins_over_url():
    assert stable_key({"id": 42, "url": "https://x.com/a"})[0] == "42"
    assert key_of({"id": "r-1"}) == "r-1"  # 兼容旧流水线


def test_similar_distinct_records_kept_apart():
    # 无 id/url 的兜底键：一条业务字段不同就是不同身份，不做相似度合并
    a = {"name": "widget", "value": 1}
    b = {"name": "widget", "value": 2}
    assert stable_key(a)[0] != stable_key(b)[0]


# ---- 内容指纹：版本化 ---------------------------------------------------
def test_fingerprint_ignores_volatile_and_order():
    a = {"name": "x", "value": 1, "_fetch_at": "t1"}
    b = {"value": 1, "name": "x", "_fetch_at": "t2"}
    assert fingerprint(a, FP_V1)[0] == fingerprint(b, FP_V1)[0]


def test_fp_v2_ignores_touch_time_but_v1_does_not():
    a = {"name": "x", "value": 1, "updated_at": "2026-11-01T00:00:00Z"}
    b = dict(a, updated_at="2026-11-02T00:00:00Z")
    assert fingerprint(a, FP_V1)[0] != fingerprint(b, FP_V1)[0]
    assert fingerprint(a, FP_V2)[0] == fingerprint(b, FP_V2)[0]


def test_unknown_fp_version_rejected():
    with pytest.raises(ValueError):
        fingerprint({"a": 1}, "fp.v9")


def test_canonical_json_deterministic():
    # 字典键排序后序列化结果一致（列表顺序是业务序，不做重排）
    assert canonical_json({"a": 1, "b": [1, {"c": 2}]}) == \
        canonical_json({"b": [1, {"c": 2}], "a": 1})
    assert canonical_json({"z": 1, "a": 2}) == canonical_json({"a": 2, "z": 1})


# ---- 合并决策 -----------------------------------------------------------
def test_merge_insert_update_unchanged():
    raw = {"id": "r-1", "name": "n", "value": 1}
    d1 = classify_record(raw, None)
    assert d1.action == MergeAction.INSERT

    stored = dict(d1.record, _fp=d1.fingerprint, _fp_rule=d1.fp_rule)
    d2 = classify_record(raw, stored)
    assert d2.action == MergeAction.UNCHANGED

    changed = dict(raw, value=2)
    d3 = classify_record(changed, stored)
    assert d3.action == MergeAction.UPDATE
    assert d3.previous_fingerprint == d1.fingerprint


def test_merge_rule_migration_keeps_history_interpretable():
    raw = {"id": "r-1", "name": "n", "value": 1,
           "updated_at": "2026-11-01T00:00:00Z"}
    fp1, _ = fingerprint(raw, FP_V1)
    # 模拟 v1 时代入库的记录
    stored = dict(raw, _fp=fp1, _fp_rule=FP_V1)
    # 仅触碰时间变化：v1 下指纹会变，但切到 v2 后规范化内容体一致 → UNCHANGED
    touched = dict(raw, updated_at="2026-11-02T00:00:00Z")
    d = classify_record(touched, stored, FP_V2)
    assert d.action == MergeAction.UNCHANGED and "migration" in d.reason

    # 业务内容真的变了：换版也必须判为 UPDATE
    changed = dict(raw, value=2)
    d2 = classify_record(changed, stored, FP_V2)
    assert d2.action == MergeAction.UPDATE


def test_delete_requires_explicit_event():
    d = decide_deletion(Deletion("r-1", "2026-11-18T00:00:00Z"), None)
    assert d.action == MergeAction.UNCHANGED  # 缺席不推断删除
    existing = {"id": "r-1", "_fp": "x", "_fp_rule": FP_V1}
    d2 = decide_deletion(Deletion("r-1", "2026-11-18T00:00:00Z"), existing)
    assert d2.action == MergeAction.DELETE


# ---- 分页计划 -----------------------------------------------------------
def test_plan_keeps_full_fallback_chain(seed, clock):
    source = make_source(seed, clock, protocols=("offset", "time_anchor"))
    plan = PaginationPlan(preferred_order=("time_anchor", "cursor", "offset"),
                          page_size=10)
    # 完整回退链保留（不支持的协议留给 fetch 阶段拒绝，引擎计数切换）
    assert [p.protocol for p in plan.candidates(source)] == \
        ["time_anchor", "cursor", "offset"]
    # 能力预检视图只留支持项
    assert [p.protocol for p in plan.supported_candidates(source)] == \
        ["time_anchor", "offset"]
    assert isinstance(plan.fallback_request(plan.candidates(source)[0]),
                      type(plan.candidates(source)[0].first_request()))


def test_plan_no_supported_protocol(seed, clock):
    source = make_source(seed, clock, protocols=("offset",))
    plan = PaginationPlan(preferred_order=("cursor",), page_size=10)
    assert plan.supported_candidates(source) == []


def test_offset_paginator_respects_end_of_list_signal():
    p = OffsetPaginator(100)
    first = p.first_request()
    assert first.offset == 0 and first.limit == 100
    from src.scrollharvest.models import Page, PageRequest
    # 源站明确给出下一页 → 采用
    hinted = Page(
        items=[{"id": "x"}], page_index=0,
        next_request=PageRequest(protocol="offset", limit=100, offset=100,
                                 page_index=1),
    )
    nxt = p.next_request(hinted)
    assert nxt.offset == 100 and nxt.page_index == 1
    # 源站明确无下一页（None）→ 不得按页码盲推，避免反复翻到空页
    end_page = Page(items=[{"id": "x"}], next_request=None, page_index=0)
    assert p.next_request(end_page) is None


def test_budget_enforce_raises_typed_error():
    budget = HarvestBudget(max_pages=1, max_runtime_seconds=3600,
                           max_bytes=10_000_000, empty_streak_limit=10)
    assert budget.check(0, 0, 0, 0) is None
    with pytest.raises(BudgetExceeded):
        budget.enforce(1, 0, 0, 0)


def test_time_anchor_respects_floor(seed, clock):
    p = TimeAnchorPaginator(10, floor_anchor="2026-11-03T00:00:00Z")
    from src.scrollharvest.models import Page, PageRequest
    nxt_req = PageRequest(protocol="time_anchor", limit=10,
                          anchor="2026-11-01T00:00:00Z")
    page = Page(items=[{"id": "x"}], next_request=nxt_req)
    assert p.next_request(page) is None
