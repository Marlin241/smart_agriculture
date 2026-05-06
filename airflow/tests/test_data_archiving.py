from datetime import datetime, timedelta, timezone
from data_archiving import find_objects_older_than


class MockObject:
    def __init__(self, last_modified, object_name='test.parquet', size=1024):
        self.last_modified = last_modified
        self.object_name = object_name
        self.size = size


def test_returns_objects_older_than_threshold():
    ref = datetime.now(timezone.utc)
    old = MockObject(ref - timedelta(days=8))
    recent = MockObject(ref - timedelta(days=3))
    result = find_objects_older_than([old, recent], 7, ref)
    assert result == [old]


def test_returns_empty_when_all_recent():
    ref = datetime.now(timezone.utc)
    result = find_objects_older_than(
        [MockObject(ref - timedelta(days=2))], 7, ref
    )
    assert result == []


def test_boundary_exactly_7_days_is_not_archived():
    ref = datetime.now(timezone.utc)
    exactly_7 = MockObject(ref - timedelta(days=7))
    result = find_objects_older_than([exactly_7], 7, ref)
    assert result == []


def test_returns_empty_for_empty_list():
    ref = datetime.now(timezone.utc)
    assert find_objects_older_than([], 7, ref) == []


def test_30_day_threshold_for_archive_deletion():
    ref = datetime.now(timezone.utc)
    old = MockObject(ref - timedelta(days=31))
    recent = MockObject(ref - timedelta(days=20))
    result = find_objects_older_than([old, recent], 30, ref)
    assert result == [old]
