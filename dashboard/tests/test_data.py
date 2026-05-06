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
