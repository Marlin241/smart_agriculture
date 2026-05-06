from datetime import datetime, timedelta, timezone
from sensor_health_check import find_recent_objects


class MockObject:
    def __init__(self, last_modified, size=1024):
        self.last_modified = last_modified
        self.size = size


def test_returns_recent_objects():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)
    recent = MockObject(now - timedelta(hours=1))
    old = MockObject(now - timedelta(hours=3))
    result = find_recent_objects([recent, old], cutoff)
    assert result == [recent]


def test_returns_empty_when_all_old():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)
    result = find_recent_objects(
        [MockObject(now - timedelta(hours=5)), MockObject(now - timedelta(hours=10))],
        cutoff,
    )
    assert result == []


def test_returns_empty_for_empty_list():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    assert find_recent_objects([], cutoff) == []


def test_returns_all_when_all_recent():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=2)
    objects = [MockObject(now - timedelta(minutes=30)), MockObject(now - timedelta(minutes=90))]
    result = find_recent_objects(objects, cutoff)
    assert len(result) == 2
