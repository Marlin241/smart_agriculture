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


from data import build_detail


def _make_df_with_npk():
    return pd.DataFrame([{
        'sensor_id': 'field_1', 'timestamp': '2026-05-06T10:00:00Z',
        'temperature_c': 28.0, 'humidity_pct': 42.0, 'plant_stage': 'croissance',
        'stress_score': 20.0, 'alerts': [],
        'soil_nitrogen_mg_kg': 60.0, 'soil_phosphorus_mg_kg': 25.0, 'soil_potassium_mg_kg': 90.0,
    }])


def test_build_detail_returns_empty_for_unknown_sensor():
    assert build_detail(_make_df_with_npk(), 'field_99') == {}


def test_build_detail_returns_empty_for_empty_df():
    assert build_detail(pd.DataFrame(), 'field_1') == {}


def test_build_detail_returns_npk_values():
    detail = build_detail(_make_df_with_npk(), 'field_1')
    assert detail['nitrogen'] == 60.0
    assert detail['phosphorus'] == 25.0
    assert detail['potassium'] == 90.0


def test_build_detail_stress_label():
    detail = build_detail(_make_df_with_npk(), 'field_1')
    assert detail['stress_label'] == 'Faible'
    assert detail['stress_color'] == 'green'


from data import build_report_table


def _make_today_report():
    return {
        'date': '2026-05-06',
        'par_champ': {
            'field_1': {
                'temperature_moy': 28.0, 'humidity_moy': 42.0,
                'nitrogen_moy': 60.0, 'nb_alertes': 1, 'alertes_detail': {},
            },
        },
        'cultures_matures': [],
        'cultures_recolte_imminente': [],
        'total_alertes': {},
    }


def _make_yesterday_report():
    return {
        'date': '2026-05-05',
        'par_champ': {
            'field_1': {
                'temperature_moy': 26.0, 'humidity_moy': 50.0,
                'nitrogen_moy': 58.0, 'nb_alertes': 0, 'alertes_detail': {},
            },
        },
        'cultures_matures': [],
        'cultures_recolte_imminente': [],
        'total_alertes': {},
    }


def test_build_report_table_returns_four_rows():
    rows = build_report_table(_make_today_report(), _make_yesterday_report())
    assert len(rows) == 4


def test_build_report_table_no_yesterday_sets_flag():
    rows = build_report_table(_make_today_report(), None)
    field1 = next(r for r in rows if r['sensor_id'] == 'field_1')
    assert field1['no_yesterday'] is True


def test_build_report_table_no_yesterday_arrow_is_neutral():
    rows = build_report_table(_make_today_report(), None)
    field1 = next(r for r in rows if r['sensor_id'] == 'field_1')
    assert field1['temp_arrow'] == '~'


def test_build_report_table_temp_up_is_red():
    rows = build_report_table(_make_today_report(), _make_yesterday_report())
    field1 = next(r for r in rows if r['sensor_id'] == 'field_1')
    assert field1['temp_arrow'] == '↑'
    assert field1['temp_color'] == 'red'


def test_build_report_table_humidity_down_is_red():
    rows = build_report_table(_make_today_report(), _make_yesterday_report())
    field1 = next(r for r in rows if r['sensor_id'] == 'field_1')
    assert field1['humidity_arrow'] == '↓'
    assert field1['humidity_color'] == 'red'
