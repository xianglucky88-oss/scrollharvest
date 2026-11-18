"""测试共享夹具：临时库 + 可变异内存源 + 引擎工厂。"""
import pytest

from src.scrollharvest import store
from src.scrollharvest.clock import FakeClock
from src.scrollharvest.config import ALL_PROTOCOLS
from src.scrollharvest.convergence import HarvestBudget
from src.scrollharvest.engine import HarvestConfig, HarvestEngine
from src.scrollharvest.pagination import PaginationPlan
from src.scrollharvest.sources import InMemoryMutableSource


@pytest.fixture
def con(tmp_path):
    c = store.connect(str(tmp_path / "test.db"))
    yield c
    c.close()


@pytest.fixture
def clock():
    return FakeClock("2026-11-18T00:00:00Z")


def make_seed(n=6, prefix="r"):
    rows = []
    for i in range(1, n + 1):
        ts = f"2026-11-{i:02d}T00:00:00Z"
        rows.append({
            "id": f"{prefix}-{i:03d}",
            "url": f"https://example.com/{prefix}/{i}?utm_source=mail",
            "name": f"Record {i}",
            "value": i * 10,
            "ts": ts,
            "updated_at": ts,
        })
    return rows


@pytest.fixture
def seed():
    return make_seed()


def make_source(seed, clock, *, protocols=ALL_PROTOCOLS, snapshot=False,
                name="src", ttl=300, expire_on_generation=True):
    return InMemoryMutableSource(
        name, seed, supported_protocols=protocols,
        has_stable_snapshot=snapshot, cursor_ttl_seconds=ttl,
        expire_on_generation=expire_on_generation, clock=clock,
    )


def make_engine(source, con, clock, *, page_size=2, order=ALL_PROTOCOLS,
                fp_version="fp.v1", stream="main", budget=None,
                start_anchor=None, floor_anchor=None):
    config = HarvestConfig(
        stream=stream,
        page_size=page_size,
        fp_version=fp_version,
        plan=PaginationPlan(preferred_order=order, page_size=page_size,
                            start_anchor=start_anchor, floor_anchor=floor_anchor),
        budget=budget or HarvestBudget(
            max_pages=50, max_runtime_seconds=3600, max_bytes=10_000_000,
            empty_streak_limit=3, quiet_polls=2,
        ),
        start_anchor=start_anchor,
        floor_anchor=floor_anchor,
    )
    return HarvestEngine(source, config, con=con, clock=clock)
