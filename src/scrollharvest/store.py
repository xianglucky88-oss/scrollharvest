"""单一职责：本地 sqlite3 持久化。

表设计（单库，向后兼容旧 records 表）：
- records        活动记录（墓碑 is_deleted=1 也保留行，可追溯）
- revisions      每次确认入库的修订链（合法更新逐条留痕，含规则版本）
- write_ledger   幂等台账：同一 (run, request, identity) 只允许生效一次
- tombstones     显式删除事件留痕
- harvest_state  每个 (source, stream) 的断点续采状态（恢复位置/在途请求）
- coverage       覆盖窗口三态：covered / pending / unconfirmable
- runs           运行台账：结束原因、预算、统计，回答“本轮为什么结束”

事务约定：引擎在单事务内完成 台账→写记录/修订→推进检查点→覆盖窗口，
随后 commit；commit 确认丢失时重放由 write_ledger 在事务内去重。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager

from .config import DB_PATH


def connect(db_path: str | None = None) -> sqlite3.Connection:
    con = sqlite3.connect(db_path or str(DB_PATH))
    con.execute("PRAGMA foreign_keys=ON")
    _create_schema(con)
    return con


def _create_schema(con: sqlite3.Connection) -> None:
    con.execute(
        "CREATE TABLE IF NOT EXISTS records("
        "key TEXT PRIMARY KEY, data TEXT NOT NULL, "
        "fingerprint TEXT, fp_rule TEXT, is_deleted INTEGER NOT NULL DEFAULT 0, "
        "updated_at TEXT)"
    )
    # 旧库补列（新库 CREATE 已包含）
    for stmt in (
        "ALTER TABLE records ADD COLUMN fingerprint TEXT",
        "ALTER TABLE records ADD COLUMN fp_rule TEXT",
        "ALTER TABLE records ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE records ADD COLUMN updated_at TEXT",
    ):
        try:
            con.execute(stmt)
        except sqlite3.OperationalError:
            pass
    con.execute(
        "CREATE TABLE IF NOT EXISTS revisions("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, identity TEXT NOT NULL, "
        "revision_no INTEGER NOT NULL, fingerprint TEXT NOT NULL, fp_rule TEXT NOT NULL, "
        "identity_rule TEXT NOT NULL, action TEXT NOT NULL, data TEXT NOT NULL, "
        "source TEXT NOT NULL, run_id TEXT, reason TEXT, recorded_at TEXT NOT NULL, "
        "UNIQUE(identity, revision_no))"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS write_ledger("
        "idem_key TEXT PRIMARY KEY, run_id TEXT NOT NULL, identity TEXT NOT NULL, "
        "action TEXT NOT NULL, recorded_at TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS tombstones("
        "identity TEXT PRIMARY KEY, deleted_at TEXT NOT NULL, "
        "source_revision TEXT, run_id TEXT, recorded_at TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS harvest_state("
        "source TEXT NOT NULL, stream TEXT NOT NULL, phase TEXT NOT NULL, "
        "protocol TEXT, resume_request TEXT, inflight_request TEXT, "
        "end_reason TEXT, updated_at TEXT NOT NULL, "
        "PRIMARY KEY(source, stream))"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS coverage("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, stream TEXT NOT NULL, "
        "scope TEXT NOT NULL, protocol TEXT NOT NULL, lower_bound TEXT, upper_bound TEXT, "
        "state TEXT NOT NULL, basis TEXT, note TEXT, updated_at TEXT NOT NULL, "
        "UNIQUE(source, stream, scope))"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS runs("
        "run_id TEXT PRIMARY KEY, source TEXT NOT NULL, stream TEXT NOT NULL, "
        "started_at TEXT NOT NULL, ended_at TEXT, status TEXT NOT NULL, "
        "end_reason TEXT, detail TEXT, stats TEXT, protocols_tried TEXT)"
    )
    con.commit()


# ---- 基础（向后兼容旧流水线）-------------------------------------------
def upsert(con, record):
    con.execute(
        "INSERT INTO records(key,data) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
        (record.key, json.dumps(record.data, ensure_ascii=False)),
    )


def commit(con):
    con.commit()


def count(con):
    return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]


def all_records(con):
    return [(k, json.loads(d)) for k, d in con.execute("SELECT key,data FROM records")]


# ---- 运行台账 -----------------------------------------------------------
def start_run(con, run_id: str, source: str, stream: str, started_at: str) -> None:
    con.execute(
        "INSERT INTO runs(run_id,source,stream,started_at,status) VALUES(?,?,?,?,?)",
        (run_id, source, stream, started_at, "running"),
    )


def finish_run(con, run_id: str, ended_at: str, status: str, end_reason: str,
               detail: str, stats: dict, protocols_tried: list[str]) -> None:
    con.execute(
        "UPDATE runs SET ended_at=?,status=?,end_reason=?,detail=?,stats=?,"
        "protocols_tried=? WHERE run_id=?",
        (ended_at, status, end_reason, detail,
         json.dumps(stats, ensure_ascii=False), json.dumps(protocols_tried), run_id),
    )


def get_run(con, run_id: str) -> dict | None:
    row = con.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if not row:
        return None
    cols = [d[0] for d in con.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).description]
    out = dict(zip(cols, row))
    out["stats"] = json.loads(out["stats"]) if out["stats"] else {}
    out["protocols_tried"] = json.loads(out["protocols_tried"]) if out["protocols_tried"] else []
    return out


def latest_run(con, source: str, stream: str) -> dict | None:
    row = con.execute(
        "SELECT run_id FROM runs WHERE source=? AND stream=? "
        "ORDER BY started_at DESC LIMIT 1", (source, stream)
    ).fetchone()
    return get_run(con, row[0]) if row else None


# ---- 检查点状态 ---------------------------------------------------------
def save_state(con, source: str, stream: str, phase: str, protocol: str | None,
               resume_request: str | None, inflight_request: str | None,
               end_reason: str | None, updated_at: str) -> None:
    con.execute(
        "INSERT INTO harvest_state(source,stream,phase,protocol,resume_request,"
        "inflight_request,end_reason,updated_at) VALUES(?,?,?,?,?,?,?,?) "
        "ON CONFLICT(source,stream) DO UPDATE SET phase=excluded.phase,"
        "protocol=excluded.protocol,resume_request=excluded.resume_request,"
        "inflight_request=excluded.inflight_request,end_reason=excluded.end_reason,"
        "updated_at=excluded.updated_at",
        (source, stream, phase, protocol, resume_request, inflight_request,
         end_reason, updated_at),
    )


def get_state(con, source: str, stream: str) -> dict | None:
    row = con.execute(
        "SELECT phase,protocol,resume_request,inflight_request,end_reason,updated_at "
        "FROM harvest_state WHERE source=? AND stream=?", (source, stream)
    ).fetchone()
    if not row:
        return None
    return dict(zip(("phase", "protocol", "resume_request", "inflight_request",
                     "end_reason", "updated_at"), row))


# ---- 幂等台账与写入 -----------------------------------------------------
def ledger_seen(con, idem_key: str) -> bool:
    return con.execute(
        "SELECT 1 FROM write_ledger WHERE idem_key=?", (idem_key,)
    ).fetchone() is not None


def ledger_add(con, idem_key: str, run_id: str, identity: str,
               action: str, recorded_at: str) -> None:
    con.execute(
        "INSERT OR IGNORE INTO write_ledger(idem_key,run_id,identity,action,recorded_at)"
        " VALUES(?,?,?,?,?)", (idem_key, run_id, identity, action, recorded_at)
    )


def ledger_count(con) -> int:
    return con.execute("SELECT COUNT(*) FROM write_ledger").fetchone()[0]


def get_active_record(con, identity: str) -> dict | None:
    row = con.execute(
        "SELECT data,fingerprint,fp_rule,is_deleted FROM records WHERE key=?",
        (identity,),
    ).fetchone()
    if not row:
        return None
    data = json.loads(row[0])
    data["_fp"] = row[1]
    data["_fp_rule"] = row[2]
    data["_is_deleted"] = bool(row[3])
    return data


def put_record(con, identity: str, record: dict, fingerprint: str, fp_rule: str,
               updated_at: str | None, is_deleted: bool = False) -> None:
    con.execute(
        "INSERT INTO records(key,data,fingerprint,fp_rule,is_deleted,updated_at)"
        " VALUES(?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data,"
        "fingerprint=excluded.fingerprint,fp_rule=excluded.fp_rule,"
        "is_deleted=excluded.is_deleted,updated_at=excluded.updated_at",
        (identity, json.dumps(record, ensure_ascii=False), fingerprint, fp_rule,
         1 if is_deleted else 0, updated_at),
    )


def add_revision(con, identity: str, revision_no: int, fingerprint: str, fp_rule: str,
                 identity_rule: str, action: str, record: dict, source: str,
                 run_id: str, reason: str, recorded_at: str) -> None:
    con.execute(
        "INSERT INTO revisions(identity,revision_no,fingerprint,fp_rule,identity_rule,"
        "action,data,source,run_id,reason,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (identity, revision_no, fingerprint, fp_rule, identity_rule, action,
         json.dumps(record, ensure_ascii=False), source, run_id, reason, recorded_at),
    )


def next_revision_no(con, identity: str) -> int:
    row = con.execute(
        "SELECT MAX(revision_no) FROM revisions WHERE identity=?", (identity,)
    ).fetchone()
    return (row[0] or 0) + 1


def revisions_of(con, identity: str) -> list[dict]:
    rows = con.execute(
        "SELECT revision_no,action,fp_rule,identity_rule,fingerprint,reason,"
        "recorded_at,source,run_id,data FROM revisions WHERE identity=? "
        "ORDER BY revision_no", (identity,)
    ).fetchall()
    out = []
    for r in rows:
        out.append({
            "revision_no": r[0], "action": r[1], "fp_rule": r[2],
            "identity_rule": r[3], "fingerprint": r[4], "reason": r[5],
            "recorded_at": r[6], "source": r[7], "run_id": r[8],
            "data": json.loads(r[9]),
        })
    return out


def add_tombstone(con, identity: str, deleted_at: str, source_revision: str | None,
                  run_id: str, recorded_at: str) -> None:
    con.execute(
        "INSERT INTO tombstones(identity,deleted_at,source_revision,run_id,recorded_at)"
        " VALUES(?,?,?,?,?) ON CONFLICT(identity) DO UPDATE SET "
        "deleted_at=excluded.deleted_at,source_revision=excluded.source_revision,"
        "run_id=excluded.run_id,recorded_at=excluded.recorded_at",
        (identity, deleted_at, source_revision, run_id, recorded_at),
    )


def is_deleted(con, identity: str) -> bool:
    row = con.execute(
        "SELECT is_deleted FROM records WHERE key=?", (identity,)
    ).fetchone()
    return bool(row and row[0])


# ---- 覆盖范围 -----------------------------------------------------------
def upsert_coverage(con, source: str, stream: str, scope: str, protocol: str,
                    lower_bound: str | None, upper_bound: str | None,
                    state: str, basis: str, note: str, updated_at: str) -> None:
    con.execute(
        "INSERT INTO coverage(source,stream,scope,protocol,lower_bound,upper_bound,"
        "state,basis,note,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(source,stream,scope) DO UPDATE SET protocol=excluded.protocol,"
        "lower_bound=excluded.lower_bound,upper_bound=excluded.upper_bound,"
        "state=excluded.state,basis=excluded.basis,note=excluded.note,"
        "updated_at=excluded.updated_at",
        (source, stream, scope, protocol, lower_bound, upper_bound, state,
         basis, note, updated_at),
    )


def coverage_summary(con, source: str, stream: str) -> dict[str, int]:
    rows = con.execute(
        "SELECT state,COUNT(*) FROM coverage WHERE source=? AND stream=? GROUP BY state",
        (source, stream),
    ).fetchall()
    summary = {"covered": 0, "pending": 0, "unconfirmable": 0}
    for state, n in rows:
        summary[state] = n
    return summary


def pending_scopes(con, source: str, stream: str) -> list[str]:
    return [r[0] for r in con.execute(
        "SELECT scope FROM coverage WHERE source=? AND stream=? AND state='pending'",
        (source, stream),
    ).fetchall()]


def resolve_pending(con, source: str, stream: str, scope: str, state: str,
                    note: str, updated_at: str) -> None:
    con.execute(
        "UPDATE coverage SET state=?,note=?,updated_at=? WHERE source=? AND stream=? "
        "AND scope=?", (state, note, updated_at, source, stream, scope)
    )


# ---- 查询面：有效记录 / 修订来源 / 结束原因 ------------------------------
def valid_records(con) -> list[tuple[str, dict]]:
    """当前有效（未删除）记录。"""
    return [
        (k, json.loads(d))
        for k, d in con.execute(
            "SELECT key,data FROM records WHERE is_deleted=0 ORDER BY key"
        )
    ]


def run_history(con, source: str, stream: str) -> list[dict]:
    rows = con.execute(
        "SELECT run_id,started_at,ended_at,status,end_reason,detail,stats "
        "FROM runs WHERE source=? AND stream=? ORDER BY started_at", (source, stream)
    ).fetchall()
    out = []
    for r in rows:
        out.append({
            "run_id": r[0], "started_at": r[1], "ended_at": r[2], "status": r[3],
            "end_reason": r[4], "detail": r[5],
            "stats": json.loads(r[6]) if r[6] else {},
        })
    return out


@contextmanager
def transaction(con):
    """显式事务：提交确认丢失（commit 抛错）时不吞异常，交由引擎重放。"""
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
