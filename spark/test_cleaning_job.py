import pytest

from cleaning_job import (
    compute_alerts,
    compute_plant_stage,
    compute_soil_fertility_score,
    compute_stress_score,
)


# ── compute_stress_score ──────────────────────────────────────────────────────

def test_stress_score_no_stress():
    # humidity=55 (>40), nitrogen=62 (>30) → stress_hum=0, stress_nit=0
    assert compute_stress_score(55.0, 62.0) == pytest.approx(0.0)


def test_stress_score_humidity_only():
    # humidity=20: stress_hum=(40-20)*1.2=24, nitrogen=62: stress_nit=0
    # result = min(100, (24+0)/2) = 12.0
    assert compute_stress_score(20.0, 62.0) == pytest.approx(12.0)


def test_stress_score_both():
    # humidity=10: stress_hum=(40-10)*1.2=36, nitrogen=10: stress_nit=(30-10)*0.8=16
    # result = min(100, (36+16)/2) = 26.0
    assert compute_stress_score(10.0, 10.0) == pytest.approx(26.0)


def test_stress_score_none_input():
    assert compute_stress_score(None, 62.0) is None
    assert compute_stress_score(55.0, None) is None


# ── compute_plant_stage ───────────────────────────────────────────────────────

def test_plant_stage_mature_phase():
    assert compute_plant_stage(50.0, 'mature') == 'récolte_imminente'


def test_plant_stage_maturite():
    assert compute_plant_stage(95.0, 'en_croissance') == 'maturité'


def test_plant_stage_developpement():
    assert compute_plant_stage(70.0, 'en_croissance') == 'développement'


def test_plant_stage_croissance():
    assert compute_plant_stage(40.0, 'en_croissance') == 'croissance'


def test_plant_stage_germination():
    assert compute_plant_stage(10.0, 'en_croissance') == 'germination'


def test_plant_stage_none_growth():
    assert compute_plant_stage(None, 'en_croissance') == 'inconnu'


# ── compute_soil_fertility_score ──────────────────────────────────────────────

def test_soil_fertility_score_max():
    # N=100, P=45, K=200 → (1.0+1.0+1.0)*100/3 = 100.0
    assert compute_soil_fertility_score(100.0, 45.0, 200.0) == pytest.approx(100.0)


def test_soil_fertility_score_half():
    # N=50, P=22.5, K=100 → (0.5+0.5+0.5)*100/3 = 50.0
    assert compute_soil_fertility_score(50.0, 22.5, 100.0) == pytest.approx(50.0)


def test_soil_fertility_score_none():
    assert compute_soil_fertility_score(None, 45.0, 200.0) is None
    assert compute_soil_fertility_score(100.0, None, 200.0) is None
    assert compute_soil_fertility_score(100.0, 45.0, None) is None


# ── compute_alerts ────────────────────────────────────────────────────────────

def test_alerts_empty():
    assert compute_alerts(55.0, 62.0, 5.0, 50.0) == []


def test_alerts_secheresse():
    assert 'sécheresse' in compute_alerts(20.0, 62.0, 5.0, 50.0)


def test_alerts_exces_humidite():
    assert 'excès_humidité' in compute_alerts(90.0, 62.0, 5.0, 50.0)


def test_alerts_deficit_azote():
    assert 'déficit_azote' in compute_alerts(55.0, 15.0, 5.0, 50.0)


def test_alerts_stress_eleve():
    assert 'stress_élevé' in compute_alerts(55.0, 62.0, 70.0, 50.0)


def test_alerts_pret_a_recolter():
    assert 'prêt_à_récolter' in compute_alerts(55.0, 62.0, 5.0, 95.0)


def test_alerts_multiple():
    alerts = compute_alerts(20.0, 15.0, 5.0, 50.0)
    assert 'sécheresse' in alerts
    assert 'déficit_azote' in alerts
