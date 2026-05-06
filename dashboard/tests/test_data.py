import pytest
import pandas as pd
from data import humanize_alert, get_stress_label


def test_humanize_alert_secheresse():
    assert humanize_alert('sécheresse') == "Manque d'eau"


def test_humanize_alert_deficit_azote():
    assert humanize_alert('déficit_azote') == "Manque d'engrais azoté"


def test_humanize_alert_unknown_returns_code():
    assert humanize_alert('code_inconnu') == 'code_inconnu'


def test_stress_faible():
    label, color = get_stress_label(0)
    assert label == 'Faible' and color == 'green'


def test_stress_modere():
    label, color = get_stress_label(50)
    assert label == 'Modéré' and color == 'orange'


def test_stress_eleve():
    label, color = get_stress_label(80)
    assert label == 'Élevé' and color == 'red'


def test_stress_borne_30_est_faible():
    label, _ = get_stress_label(30)
    assert label == 'Faible'


def test_stress_borne_31_est_modere():
    label, _ = get_stress_label(31)
    assert label == 'Modéré'


from data import build_field_cards


def _make_df():
    return pd.DataFrame([
        {
            'sensor_id': 'field_1', 'timestamp': '2026-05-06T10:00:00Z',
            'temperature_c': 28.0, 'humidity_pct': 42.0, 'plant_stage': 'croissance',
            'stress_score': 20.0, 'alerts': [],
        },
        {
            'sensor_id': 'field_2', 'timestamp': '2026-05-06T10:00:00Z',
            'temperature_c': 29.0, 'humidity_pct': 38.0, 'plant_stage': 'développement',
            'stress_score': 70.0, 'alerts': ['déficit_azote'],
        },
    ])


def test_build_field_cards_returns_four():
    cards = build_field_cards(_make_df())
    assert len(cards) == 4


def test_build_field_cards_no_data_for_missing_field():
    cards = build_field_cards(_make_df())
    field3 = next(c for c in cards if c['sensor_id'] == 'field_3')
    assert field3['no_data'] is True


def test_build_field_cards_empty_df_all_no_data():
    cards = build_field_cards(pd.DataFrame())
    assert all(c['no_data'] for c in cards)


def test_build_field_cards_alerte_status():
    cards = build_field_cards(_make_df())
    field2 = next(c for c in cards if c['sensor_id'] == 'field_2')
    assert field2['status'] == 'Alerte'


def test_build_field_cards_stage_translated():
    cards = build_field_cards(_make_df())
    field1 = next(c for c in cards if c['sensor_id'] == 'field_1')
    assert field1['stage'] == 'En croissance'
