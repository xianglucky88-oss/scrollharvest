"""命令行入口。

子命令：
  ingest   兼容旧流程：离线摄取 fixtures/input.json（默认，写 scrollharvest.db）
  harvest  动态列表采集演示：对可变异内存源跑完整引擎（写 demo_harvest.db）
  query    查询有效记录、某身份的修订来源与运行结束原因
"""
import json
import sys

from . import pipeline, store
from .clock import FakeClock
from .config import DB_PATH, PROTO_CURSOR, PROTO_OFFSET
from .convergence import HarvestBudget
from .engine import HarvestConfig, HarvestEngine
from .pagination import PaginationPlan
from .sources import InMemoryMutableSource

DEMO_DB = DB_PATH.with_name("demo_harvest.db")
DEMO_STREAM = "demo"


def _demo_source():
    seed = [
        {"id": "r-001", "url": "https://example.com/a?utm_source=x",
         "name": "Record One", "value": 10, "ts": "2026-11-01T00:00:00Z",
         "updated_at": "2026-11-01T00:00:00Z"},
        {"id": "r-002", "url": "https://example.com/b",
         "name": "Record Two", "value": 20, "ts": "2026-11-02T00:00:00Z",
         "updated_at": "2026-11-02T00:00:00Z"},
        {"id": "r-003", "url": "https://example.com/c",
         "name": "Record Three", "value": 30, "ts": "2026-11-03T00:00:00Z",
         "updated_at": "2026-11-03T00:00:00Z"},
    ]
    clock = FakeClock("2026-11-18T00:00:00Z")
    return InMemoryMutableSource("demo", seed, clock=clock), clock


def cmd_harvest(args):
    source, clock = _demo_source()
    # 演示：采集过程中源站插入与更新
    source.insert({"id": "r-004", "url": "https://example.com/d",
                   "name": "Record Four", "value": 40})
    config = HarvestConfig(
        stream=DEMO_STREAM,
        plan=PaginationPlan(
            preferred_order=(PROTO_CURSOR, PROTO_OFFSET), page_size=2
        ),
        budget=HarvestBudget(max_pages=20, empty_streak_limit=2, quiet_polls=1),
    )
    con = store.connect(str(DEMO_DB))
    try:
        result = HarvestEngine(source, config, con=con, clock=clock).run()
    finally:
        con.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def cmd_query(args):
    con = store.connect(str(DEMO_DB))
    try:
        out = {
            "valid_records": [
                {"identity": k, "data": d} for k, d in store.valid_records(con)
            ],
            "coverage": store.coverage_summary(con, "demo", DEMO_STREAM),
            "runs": store.run_history(con, "demo", DEMO_STREAM),
        }
        if args:
            out["revisions"] = store.revisions_of(con, args[0])
        print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        con.close()
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    cmd = argv[0] if argv else "ingest"
    if cmd == "harvest":
        return cmd_harvest(argv[1:])
    if cmd == "query":
        return cmd_query(argv[1:])
    if cmd == "ingest":
        print(json.dumps(pipeline.run(), ensure_ascii=False))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
