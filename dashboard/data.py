import os
import io
import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd

SENSOR_NAMES = {
    'field_1': ('Blé', '🌾'),
    'field_2': ('Maïs', '🌽'),
    'field_3': ('Tournesol', '🌻'),
    'field_4': ('Soja', '🌱'),
}

ALERT_LABELS = {
    'sécheresse': "Manque d'eau",
    'excès_humidité': "Excès d'humidité",
    'déficit_azote': "Manque d'engrais azoté",
    'stress_élevé': "Stress végétatif élevé",
    'prêt_à_récolter': "Prêt à être récolté",
}

STAGE_LABELS = {
    'germination': 'Germination',
    'croissance': 'En croissance',
    'développement': 'En développement',
    'maturité': 'Mature',
    'récolte_imminente': 'Prêt à être récolté',
    'inconnu': 'Phase inconnue',
}

NPK_THRESHOLDS = {
    'nitrogen': 50,
    'phosphorus': 20,
    'potassium': 80,
}


def humanize_alert(code: str) -> str:
    return ALERT_LABELS.get(code, code)


def get_stress_label(score: float) -> tuple:
    if score <= 30:
        return 'Faible', 'green'
    elif score <= 65:
        return 'Modéré', 'orange'
    return 'Élevé', 'red'
