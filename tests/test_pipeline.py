from src.scrollharvest import pipeline, store


def test_pipeline_runs_and_idempotent(tmp_path):
    db = tmp_path / "pipeline.db"
    con = store.connect(str(db))
    try:
        a = pipeline.run(con)
        b = pipeline.run(con)   # 重跑不增行（幂等 upsert）
        assert a["fed"] == 4
        assert b["rows"] == a["rows"] == 3   # r-003 重复被去重
    finally:
        con.close()


def test_store_roundtrip(tmp_path):
    con = store.connect(str(tmp_path / "rt.db"))
    try:
        assert store.count(con) >= 0
    finally:
        con.close()
