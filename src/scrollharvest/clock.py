"""单一职责：可注入的时钟，便于确定性测试。"""
from datetime import datetime, timezone


class Clock:
    def now_iso(self) -> str:  # pragma: no cover - 接口
        raise NotImplementedError


class SystemClock(Clock):
    def now_iso(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class FakeClock(Clock):
    """测试用：手动推进的固定时钟。"""

    def __init__(self, start: str = "2026-11-18T00:00:00Z"):
        self._now = start

    def now_iso(self) -> str:
        return self._now

    def advance(self, seconds: int) -> str:
        dt = datetime.strptime(self._now, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        self._now = dt.fromtimestamp(dt.timestamp() + seconds, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        return self._now
