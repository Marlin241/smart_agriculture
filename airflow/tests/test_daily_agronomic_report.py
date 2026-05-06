import pytest
import pandas as pd
from daily_agronomic_report import compute_daily_report


def _make_row(sensor_id, temp, hum, n, p, k, alerts, stage):
    return {
        'sensor_id': sensor_id,
        'temperature_c': temp,
        'humidity_pct': hum,
        'soil_nitrogen_mg_kg': n,
        'soil_phosphorus_mg_kg': p,
        'soil_potassium_mg_kg': k,
        'alerts': alerts,
        'plant_stage': stage,
    }


def test_date_is_preserved():
    df = pd.DataFrame([_make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance')])
    report = compute_daily_report(df, '2026-05-06')
    assert report['date'] == '2026-05-06'


def test_temperature_aggregates():
    df = pd.DataFrame([
        _make_row('s1', 20.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance'),
        _make_row('s1', 30.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance'),
    ])
    report = compute_daily_report(df, '2026-05-06')
    assert report['par_champ']['s1']['temperature_moy'] == pytest.approx(25.0)
    assert report['par_champ']['s1']['temperature_min'] == pytest.approx(20.0)
    assert report['par_champ']['s1']['temperature_max'] == pytest.approx(30.0)


def test_alerte_count_and_detail():
    df = pd.DataFrame([
        _make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, ['sécheresse', 'déficit_azote'], 'croissance'),
        _make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, ['sécheresse'], 'croissance'),
    ])
    report = compute_daily_report(df, '2026-05-06')
    assert report['par_champ']['s1']['nb_alertes'] == 3
    assert report['par_champ']['s1']['alertes_detail']['sécheresse'] == 2
    assert report['par_champ']['s1']['alertes_detail']['déficit_azote'] == 1


def test_cultures_matures():
    df = pd.DataFrame([
        _make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'maturité'),
        _make_row('s2', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance'),
    ])
    report = compute_daily_report(df, '2026-05-06')
    assert 's1' in report['cultures_matures']
    assert 's2' not in report['cultures_matures']


def test_cultures_recolte_imminente():
    df = pd.DataFrame([_make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'récolte_imminente')])
    report = compute_daily_report(df, '2026-05-06')
    assert 's1' in report['cultures_recolte_imminente']
    assert 's1' not in report['cultures_matures']


def test_total_alertes_across_fields():
    df = pd.DataFrame([
        _make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, ['sécheresse'], 'croissance'),
        _make_row('s2', 28.0, 60.0, 45.0, 22.0, 90.0, ['sécheresse', 'déficit_azote'], 'croissance'),
    ])
    report = compute_daily_report(df, '2026-05-06')
    assert report['total_alertes']['sécheresse'] == 2
    assert report['total_alertes']['déficit_azote'] == 1


def test_empty_alerts_list():
    df = pd.DataFrame([_make_row('s1', 28.0, 60.0, 45.0, 22.0, 90.0, [], 'croissance')])
    report = compute_daily_report(df, '2026-05-06')
    assert report['par_champ']['s1']['nb_alertes'] == 0
    assert report['par_champ']['s1']['alertes_detail'] == {}
