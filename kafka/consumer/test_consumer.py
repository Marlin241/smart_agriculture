import io
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from consumer import BUCKETS, build_partition_path, ensure_buckets, flush_batch


def test_build_partition_path_format():
    now = datetime(2026, 5, 6, 14, 35, 12, tzinfo=timezone.utc)
    path = build_partition_path(now)
    assert path == 'year=2026/month=05/day=06/hour=14/batch_143512.parquet'


def test_build_partition_path_zero_padding():
    now = datetime(2026, 1, 3, 9, 5, 7, tzinfo=timezone.utc)
    path = build_partition_path(now)
    assert path == 'year=2026/month=01/day=03/hour=09/batch_090507.parquet'


def test_flush_batch_empty_does_nothing():
    minio_client = MagicMock()
    flush_batch([], minio_client)
    minio_client.put_object.assert_not_called()


def test_flush_batch_uploads_parquet():
    messages = [
        {
            'sensor_id': 'field_1_ble', 'timestamp': '2026-05-06T14:32:00Z',
            'field_id': 1, 'culture': 'Ble', 'temperature_c': 21.4,
            'humidity_pct': 55.2, 'solar_radiation_wm2': 420.0, 'rainfall_mm': 0.0,
            'soil_nitrogen_mg_kg': 62.0, 'soil_phosphorus_mg_kg': 35.0,
            'soil_potassium_mg_kg': 180.0, 'soil_ph': 6.4,
            'plant_growth_pct': 34.5, 'robot_action': 'vide',
            'phase': 'en_croissance', 'water_used_L': 10.0,
            'fertilizer_used_kg': 0.05, 'harvest_count': 0,
        }
    ]
    minio_client = MagicMock()
    flush_batch(messages, minio_client)

    assert minio_client.put_object.call_count == 1
    args = minio_client.put_object.call_args[0]
    assert args[0] == 'smart-farm-raw'
    buffer = args[2]
    buffer.seek(0)
    df = pd.read_parquet(buffer)
    assert len(df) == 1
    assert df.iloc[0]['culture'] == 'Ble'
    assert df.iloc[0]['field_id'] == 1


def test_ensure_buckets_creates_missing():
    client = MagicMock()
    client.bucket_exists.return_value = False
    ensure_buckets(client)
    assert client.make_bucket.call_count == len(BUCKETS)


def test_ensure_buckets_skips_existing():
    client = MagicMock()
    client.bucket_exists.return_value = True
    ensure_buckets(client)
    client.make_bucket.assert_not_called()
