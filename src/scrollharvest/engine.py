"""采集引擎：把分页、合并、检查点、收敛与可靠落库串成完整流程。

职责边界：引擎只做编排与事务边界，业务判断分别在 pagination / merge /
cursor_state / convergence / store 内完成。

关键正确性约定：
1. 每个请求先持久化为 inflight 再发往源站；只有整页在单事务内确认入库后，
   恢复位置才推进。崩溃重放 inflight，由 write_ledger 去重，绝不重复写入。
2. 提交确认丢失（ConfirmationLost）不推进检查点；重跑时台账命中即短路。
3. 游标失效/协议不支持 → 旧协议窗口回退 pending，按 PaginationPlan 换协议
   从首页重走；已确认身份由幂等层短路。
4. 无稳定快照时，结尾窗口只能标 unconfirmable，连续空页只触发静止轮询，
   不宣告完整；pending 窗口由后续补采（repair）收敛。
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime

from . import store as _store
from .clock import Clock, SystemClock
from .config import (
    COVERAGE_COVERED, COVERAGE_PENDING, COVERAGE_UNCONFIRMABLE,
    DEFAULT_FP_VERSION, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, PROTO_OFFSET,
)
from .convergence import (
    EndReason, RESUMABLE, TERMINAL_OK, ConvergenceVerdict, HarvestBudget,
    evaluate_tail,
)
from .cursor_state import (
    CheckpointPhase, CoverageWindow, deserialize_request,
    mark_pending_on_cursor_dead, serialize_request,
    unconfirmable_without_snapshot, window_scope,
)
from .errors import (
    ConfirmationLost, CursorExpired, FatalHarvestError, ProtocolUnsupported,
)
from .fingerprint import business_body_fingerprint
from .keyer import stable_key
from .merge import MergeAction, classify_record, decide_deletion
from .models import PageRequest
from .pagination import PaginationPlan
from .sources import ListSource

_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass
class HarvestConfig:
    stream: str = "default"
    page_size: int = DEFAULT_PAGE_SIZE
    max_page_size: int = MAX_PAGE_SIZE
    fp_version: str = DEFAULT_FP_VERSION
    plan: PaginationPlan | None = None
    budget: HarvestBudget = field(default_factory=HarvestBudget)
    start_anchor: str | None = None
    floor_anchor: str | None = None


class HarvestEngine:
    def __init__(self, source: ListSource, config: HarvestConfig | None = None,
                 con=None, clock: Clock | None = None):
        self.source = source
        self.config = config or HarvestConfig()
        if self.config.page_size > self.config.max_page_size:
            raise ValueError(
                f"page_size {self.config.page_size} exceeds cap "
                f"{self.config.max_page_size}: 大页必须显式放宽上限"
            )
        self._own_con = con is None
        self.con = con or _store.connect()
        self.clock = clock or SystemClock()
        self.plan = config.plan or PaginationPlan(
            page_size=self.config.page_size,
            start_anchor=self.config.start_anchor,
            floor_anchor=self.config.floor_anchor,
        )
        if self.plan.page_size > self.config.max_page_size:
            raise ValueError(
                f"plan page_size {self.plan.page_size} exceeds cap "
                f"{self.config.max_page_size}"
            )
        self.paginators = self.plan.candidates(source)
        self.proto_index = 0
        self.windows: list[CoverageWindow] = []
        self._last_confirmed: PageRequest | None = None
        self._inflight: PageRequest | None = None
        self._polling = False
        self.repair_mode = False

    # ------------------------------------------------------------------ #
    # 入口
    # ------------------------------------------------------------------ #
    def run(self, run_id: str | None = None, *, repair: bool = False) -> dict:
        run_id = run_id or f"run-{uuid.uuid4().hex[:12]}"
        self.repair_mode = repair
        started = self.clock.now_iso()
        _store.start_run(self.con, run_id, self.source.name,
                         self.config.stream, started)
        stats = self._new_stats()
        verdict: ConvergenceVerdict | None = None
        try:
            request = self._resume_or_start(repair)
            quiet_polls_seen = 0
            empty_streak = 0
            first_seen = self.clock.now_iso()

            while True:
                # 1) 预算检查（发请求之前）
                budget_hit = self.config.budget.check(
                    stats["pages_fetched"], stats["bytes_fetched"],
                    self._elapsed(first_seen), empty_streak,
                )
                if budget_hit is not None:
                    verdict = evaluate_tail(
                        source_has_snapshot=self.source.has_stable_snapshot,
                        reached_end=False, empty_streak=empty_streak,
                        quiet_polls_seen=quiet_polls_seen,
                        budget=self.config.budget, budget_hit=budget_hit,
                        protocols_left=len(self.paginators) - self.proto_index,
                    )
                    break

                # 2) inflight 先落盘（崩溃后可重放）
                is_poll = self._is_poll_request(request)
                self._save_inflight(request)

                # 3) 取页（处理游标失效 / 协议不支持）
                try:
                    page = self.source.fetch(request)
                except CursorExpired as exc:
                    request = self._handle_cursor_dead(request, str(exc), stats)
                    continue
                except ProtocolUnsupported as exc:
                    request = self._handle_protocol_switch(str(exc), stats)
                    if request is None:
                        verdict = evaluate_tail(
                            source_has_snapshot=self.source.has_stable_snapshot,
                            reached_end=False, empty_streak=empty_streak,
                            quiet_polls_seen=quiet_polls_seen,
                            budget=self.config.budget, budget_hit=None,
                            protocols_left=0,
                        )
                        break
                    continue

                stats["pages_fetched"] += 1
                stats["bytes_fetched"] += page.bytes_est
                if is_poll:
                    stats["poll_fetches"] += 1

                # 4) 单事务确认整页（提交确认丢失会抛 ConfirmationLost）
                page_changes = self._apply_page(page, request, run_id, stats)

                # 5) 覆盖窗口
                self._record_window(request, page)

                # 6) 推进恢复位置（仅在事务已提交之后）
                next_request = self._paginator().next_request(page)
                self._save_confirmed(next_request)

                # 7) 收敛统计
                if page_changes == 0 and not page.deletions:
                    empty_streak += 1
                else:
                    empty_streak = 0
                stats["empty_streak"] = empty_streak

                if next_request is None:
                    self._mark_tail(request)
                    # 无稳定快照：到尾不等于完整，先做静止轮询，
                    # 轮询期发现变化会重置计数并继续采集；
                    # quiet_polls=0 表示显式关闭轮询，直接给不可验证结论
                    if not self.source.has_stable_snapshot \
                            and self.config.budget.quiet_polls > 0 \
                            and quiet_polls_seen < self.config.budget.quiet_polls:
                        quiet_polls_seen += 1
                        empty_streak = 0
                        # 到尾后的首页重取属于静止轮询，不是新的分页推进
                        self._polling = True
                        request = self._paginator().first_request()
                        continue
                    if self.repair_mode and not self._pending_left():
                        verdict = ConvergenceVerdict(
                            EndReason.COMPLETE
                            if self.source.has_stable_snapshot
                            else EndReason.END_REACHED_UNVERIFIED,
                            self.source.has_stable_snapshot,
                            "repair pass reconciled all pending windows",
                        )
                        break
                    verdict = evaluate_tail(
                        source_has_snapshot=self.source.has_stable_snapshot,
                        reached_end=True, empty_streak=empty_streak,
                        quiet_polls_seen=quiet_polls_seen,
                        budget=self.config.budget, budget_hit=None,
                        protocols_left=len(self.paginators) - self.proto_index,
                    )
                    break

                # 8) 无快照源：连续空页（服务端仍给下一页）只触发静止轮询
                if empty_streak >= self.config.budget.empty_streak_limit \
                        and self.config.budget.quiet_polls > 0:
                    quiet_polls_seen += 1
                    if quiet_polls_seen >= self.config.budget.quiet_polls:
                        verdict = evaluate_tail(
                            source_has_snapshot=self.source.has_stable_snapshot,
                            reached_end=False, empty_streak=empty_streak,
                            quiet_polls_seen=quiet_polls_seen,
                            budget=self.config.budget, budget_hit=None,
                            protocols_left=len(self.paginators) - self.proto_index,
                        )
                        break
                    # 静止轮询：回首页重查，幂等层负责短路
                    empty_streak = 0
                    self._polling = True
                    request = self._paginator().first_request()
                    continue

                request = next_request
                self._polling = False

        except ConfirmationLost as exc:
            # 提交确认丢失：不推进检查点，inflight 保留，台账决定重放结果
            verdict = ConvergenceVerdict(
                EndReason.BUDGET_EMPTY_STREAK, False,
                f"commit confirmation lost: {exc}; resume replays inflight",
            )
        except (CursorExpired, ProtocolUnsupported):
            raise  # 已在循环内处理；走到这里属于缺陷，原样抛出
        except FatalHarvestError as exc:
            verdict = ConvergenceVerdict(EndReason.FATAL, False, str(exc))
        except Exception as exc:
            # 取页/应用阶段崩溃：保留 inflight 供重放，绝不伪装成协议失效
            verdict = ConvergenceVerdict(
                EndReason.FATAL, False,
                f"aborted with inflight retained: {exc!r}",
            )
        finally:
            ended = self.clock.now_iso()
            if verdict is None:
                verdict = ConvergenceVerdict(EndReason.FATAL, False, "aborted")
            if verdict.end_reason in TERMINAL_OK:
                status, phase = "ok", CheckpointPhase.DONE.value
            elif verdict.end_reason in RESUMABLE:
                status, phase = "resumable", CheckpointPhase.INFLIGHT.value
            elif verdict.end_reason == EndReason.FATAL:
                # 崩溃时保留崩溃前的 inflight 检查点，供下次重放
                status, phase = "failed", CheckpointPhase.INFLIGHT.value
            else:
                status, phase = "failed", CheckpointPhase.CURSOR_DEAD.value
            _store.finish_run(
                self.con, run_id, ended, status, verdict.end_reason.value,
                verdict.detail, stats, [p.protocol for p in self.paginators],
            )
            _store.save_state(
                self.con, self.source.name, self.config.stream, phase,
                self._current_protocol(),
                serialize_request(self._last_confirmed),
                serialize_request(self._inflight),
                verdict.end_reason.value, ended,
            )
            coverage = _store.coverage_summary(
                self.con, self.source.name, self.config.stream
            )
            _store.commit(self.con)
            if self._own_con:
                self.con.close()

        return {
            "run_id": run_id,
            "end_reason": verdict.end_reason.value,
            "can_claim_complete": verdict.can_claim_complete,
            "detail": verdict.detail,
            "stats": stats,
            "coverage": coverage,
        }

    # ------------------------------------------------------------------ #
    # 恢复 / 启动
    # ------------------------------------------------------------------ #
    def _resume_or_start(self, force_repair: bool) -> PageRequest:
        state = _store.get_state(self.con, self.source.name, self.config.stream)
        if state is None:
            return self._paginator().first_request()
        inflight = deserialize_request(state["inflight_request"])
        resume = deserialize_request(state["resume_request"])
        proto = state.get("protocol")
        for i, p in enumerate(self.paginators):
            if p.protocol == proto:
                self.proto_index = i
                break
        pending = _store.pending_scopes(
            self.con, self.source.name, self.config.stream
        )
        if force_repair or pending:
            self.repair_mode = True
            return self._paginator().first_request()
        if inflight and state["phase"] == CheckpointPhase.INFLIGHT.value:
            return inflight  # 重放在途请求
        if resume is not None:
            return resume
        return self._paginator().first_request()

    # ------------------------------------------------------------------ #
    # 协议失效与切换
    # ------------------------------------------------------------------ #
    def _handle_cursor_dead(self, request: PageRequest, detail: str,
                            stats: dict) -> PageRequest:
        stats["cursor_expired"] += 1
        self.windows = mark_pending_on_cursor_dead(self.windows, request.protocol)
        self._downgrade_covered_to_pending(request.protocol)
        _store.save_state(
            self.con, self.source.name, self.config.stream,
            CheckpointPhase.CURSOR_DEAD.value, request.protocol, None,
            serialize_request(request), "cursor_expired", self.clock.now_iso(),
        )
        _store.commit(self.con)
        return self._switch_protocol(detail, stats)

    def _handle_protocol_switch(self, detail: str, stats: dict) -> PageRequest | None:
        stats["protocol_switches"] += 1
        return self._switch_protocol(detail, stats)

    def _switch_protocol(self, detail: str, stats: dict) -> PageRequest | None:
        self.proto_index += 1
        if self.proto_index >= len(self.paginators):
            stats["protocols_exhausted"] = 1
            return None
        paginator = self._paginator()
        _store.save_state(
            self.con, self.source.name, self.config.stream,
            CheckpointPhase.CURSOR_DEAD.value, paginator.protocol, None, None,
            f"fallback: {detail}", self.clock.now_iso(),
        )
        _store.commit(self.con)
        return self.plan.fallback_request(paginator)

    def _downgrade_covered_to_pending(self, protocol: str) -> None:
        rows = self.con.execute(
            "SELECT scope FROM coverage WHERE source=? AND stream=? AND protocol=? "
            "AND state='covered'",
            (self.source.name, self.config.stream, protocol),
        ).fetchall()
        for (scope,) in rows:
            _store.resolve_pending(
                self.con, self.source.name, self.config.stream, scope,
                COVERAGE_PENDING, "cursor invalidated; re-verify",
                self.clock.now_iso(),
            )

    # ------------------------------------------------------------------ #
    # 单页事务（SAVEPOINT 保证原子；commit 失败=确认丢失，不吞）
    # ------------------------------------------------------------------ #
    def _apply_page(self, page, request: PageRequest, run_id: str,
                    stats: dict) -> int:
        con = self.con
        con.execute("SAVEPOINT page_txn")
        try:
            changes = 0
            # 业务内容指纹 → 本页首个持有该内容的身份，用于相似独立记录检测
            page_bodies: dict[str, str] = {}
            for raw in page.items:
                identity, _ = stable_key(raw)
                idem_key = self._idem_key(run_id, request, identity)
                if _store.ledger_seen(con, idem_key):
                    stats["ledger_dedup"] += 1
                    continue
                existing = _store.get_active_record(con, identity)
                if existing is not None and existing.get("_is_deleted"):
                    # 同 id 合法复活：墓碑之上的再插入，修订链保留 delete 历史
                    existing = None
                decision = classify_record(raw, existing, self.config.fp_version)
                # 相似独立记录：剔除身份字段后业务内容相同、但身份不同
                # → 标 SIMILAR，仍然独立入库，绝不按相似度合并
                if decision.action == MergeAction.INSERT:
                    body_fp = business_body_fingerprint(decision.record or {})
                    owner = page_bodies.get(body_fp)
                    if owner is not None and owner != identity:
                        decision.action = MergeAction.SIMILAR
                        decision.reason = (
                            f"same business content as {owner} "
                            "but distinct identity; kept independently"
                        )
                        stats["similar"] += 1
                    else:
                        page_bodies.setdefault(body_fp, identity)
                if decision.action == MergeAction.UNCHANGED:
                    _store.ledger_add(
                        con, idem_key, run_id, identity,
                        decision.action.value, self.clock.now_iso(),
                    )
                    stats["unchanged"] += 1
                    continue
                self._write_decision(con, decision, run_id, idem_key, stats)
                changes += 1

            for deletion in page.deletions:
                idem_key = self._idem_key(
                    run_id, request, "del:" + deletion.identity
                )
                if _store.ledger_seen(con, idem_key):
                    stats["ledger_dedup"] += 1
                    continue
                existing = _store.get_active_record(con, deletion.identity)
                decision = decide_deletion(deletion, existing)
                _store.ledger_add(
                    con, idem_key, run_id, deletion.identity,
                    decision.action.value, self.clock.now_iso(),
                )
                if decision.action == MergeAction.DELETE:
                    _store.add_tombstone(
                        con, deletion.identity, deletion.deleted_at,
                        deletion.source_revision, run_id, self.clock.now_iso(),
                    )
                    if existing is not None:
                        _store.put_record(
                            con, deletion.identity, existing,
                            existing.get("_fp", ""),
                            existing.get("_fp_rule", self.config.fp_version),
                            deletion.deleted_at, is_deleted=True,
                        )
                    rev_no = _store.next_revision_no(con, deletion.identity)
                    _store.add_revision(
                        con, deletion.identity, rev_no,
                        existing.get("_fp", "") if existing else "",
                        existing.get("_fp_rule", self.config.fp_version)
                        if existing else self.config.fp_version,
                        "key.v1", MergeAction.DELETE.value, existing or {},
                        self.source.name, run_id, decision.reason,
                        self.clock.now_iso(),
                    )
                    stats["deleted"] += 1
                    changes += 1

            con.execute("RELEASE SAVEPOINT page_txn")
            try:
                con.commit()
            except Exception as exc:  # 提交确认丢失：状态不可知，交给重放
                raise ConfirmationLost(str(exc)) from exc
            return changes
        except ConfirmationLost:
            raise
        except Exception:
            con.execute("ROLLBACK TO SAVEPOINT page_txn")
            con.execute("RELEASE SAVEPOINT page_txn")
            raise

    def _write_decision(self, con, decision, run_id: str, idem_key: str,
                        stats: dict) -> None:
        now = self.clock.now_iso()
        _store.ledger_add(
            con, idem_key, run_id, decision.identity,
            decision.action.value, now,
        )
        rec = dict(decision.record or {})
        rec["_fp"] = decision.fingerprint
        rec["_fp_rule"] = decision.fp_rule
        _store.put_record(
            con, decision.identity, rec, decision.fingerprint or "",
            decision.fp_rule, rec.get("updated_at") or rec.get("ts"),
            is_deleted=False,
        )
        rev_no = _store.next_revision_no(con, decision.identity)
        _store.add_revision(
            con, decision.identity, rev_no, decision.fingerprint or "",
            decision.fp_rule, decision.identity_rule, decision.action.value,
            rec, self.source.name, run_id, decision.reason, now,
        )
        if decision.action == MergeAction.INSERT:
            stats["inserted"] += 1
        elif decision.action == MergeAction.UPDATE:
            stats["updated"] += 1
        elif decision.action == MergeAction.SIMILAR:
            # 相似独立记录照常入库，但统计上单列，便于审计“为什么有两条像的”
            stats["inserted"] += 1

    # ------------------------------------------------------------------ #
    # 覆盖窗口
    # ------------------------------------------------------------------ #
    def _record_window(self, request: PageRequest, page) -> None:
        scope = window_scope(request.protocol, request.page_index, request)
        lower, upper, basis = self._window_bounds(request, page)
        note = "repair pass" if self.repair_mode else ""
        state = COVERAGE_COVERED
        if not self.source.has_stable_snapshot and \
                unconfirmable_without_snapshot(request.protocol) and \
                page.next_request is None:
            state = COVERAGE_UNCONFIRMABLE
            note = (note + " | no stable snapshot; tail unverifiable").strip(" |")
        _store.upsert_coverage(
            self.con, self.source.name, self.config.stream, scope,
            request.protocol, lower, upper, state, basis, note,
            self.clock.now_iso(),
        )
        self.windows.append(
            CoverageWindow(scope, request.protocol, lower, upper, state, basis, note)
        )
        if self.repair_mode:
            _store.resolve_pending(
                self.con, self.source.name, self.config.stream, scope,
                state, "repaired: " + note, self.clock.now_iso(),
            )

    def _window_bounds(self, request: PageRequest, page):
        if request.protocol == PROTO_OFFSET:
            start = request.offset or 0
            return str(start), str(start + request.limit), "offset-index"
        if request.anchor:
            return request.anchor, page.source_cursor, "time-anchor"
        return page.source_cursor, None, "server-cursor"

    def _mark_tail(self, request: PageRequest) -> None:
        if self.source.has_stable_snapshot:
            return
        scope = f"tail:{request.protocol}:after-p{request.page_index}"
        _store.upsert_coverage(
            self.con, self.source.name, self.config.stream, scope,
            request.protocol, request.anchor, None,
            COVERAGE_UNCONFIRMABLE, "no-snapshot",
            "beyond end-of-list; backdated inserts unprovable",
            self.clock.now_iso(),
        )

    def _pending_left(self) -> bool:
        return bool(_store.pending_scopes(
            self.con, self.source.name, self.config.stream
        ))

    # ------------------------------------------------------------------ #
    # 辅助
    # ------------------------------------------------------------------ #
    def _paginator(self):
        return self.paginators[self.proto_index]

    def _current_protocol(self) -> str | None:
        if self.proto_index < len(self.paginators):
            return self.paginators[self.proto_index].protocol
        return self.paginators[-1].protocol if self.paginators else None

    def _is_poll_request(self, request: PageRequest) -> bool:
        """静止轮询请求 = 某协议首页，但该协议此前已有确认进展。"""
        first = self._paginator().first_request()
        if request.protocol != first.protocol or request.page_index != 0:
            return False
        return self._last_confirmed is not None or self._polling

    def _idem_key(self, run_id: str, request: PageRequest, identity: str) -> str:
        return f"{run_id}|{request.protocol}:{request.page_index}|{identity}"

    def _save_inflight(self, request: PageRequest) -> None:
        self._inflight = request
        _store.save_state(
            self.con, self.source.name, self.config.stream,
            CheckpointPhase.INFLIGHT.value, request.protocol,
            serialize_request(self._last_confirmed),
            serialize_request(request), None, self.clock.now_iso(),
        )
        _store.commit(self.con)

    def _save_confirmed(self, next_request: PageRequest | None) -> None:
        self._last_confirmed = next_request
        self._inflight = None
        _store.save_state(
            self.con, self.source.name, self.config.stream,
            CheckpointPhase.CONFIRMED.value, self._paginator().protocol,
            serialize_request(next_request), None, None, self.clock.now_iso(),
        )
        _store.commit(self.con)

    def _elapsed(self, start_iso: str) -> float:
        return (
            datetime.strptime(self.clock.now_iso(), _TS_FMT)
            - datetime.strptime(start_iso, _TS_FMT)
        ).total_seconds()

    def _new_stats(self) -> dict:
        return {
            "pages_fetched": 0, "bytes_fetched": 0, "inserted": 0,
            "updated": 0, "unchanged": 0, "deleted": 0, "ledger_dedup": 0,
            "cursor_expired": 0, "protocol_switches": 0,
            "protocols_exhausted": 0, "empty_streak": 0,
            "poll_fetches": 0, "similar": 0,
        }
