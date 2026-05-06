# Dashboard Dash — Implementation Plan (Subsystem E)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Créer un service Dash (Python/Plotly) qui affiche en temps quasi-réel l'état des 4 champs agricoles en lisant depuis MinIO, avec rafraîchissement automatique toutes les 5 minutes.

**Architecture:** `data.py` contient toutes les fonctions pures testables (transformations, traductions, agrégations). `pages/overview.py` et `pages/report.py` contiennent les layouts et callbacks Dash. `app.py` est le point d'entrée qui assemble tout avec `dcc.Store` (cache mémoire) et `dcc.Interval` (rafraîchissement 5 min).

**Tech Stack:** dash==2.17.1, plotly==5.22.0, pandas==2.2.2, boto3==1.34.0, pyarrow==16.1.0, pytest==8.2.0

---

## Structure des fichiers

```
dashboard/
├── Dockerfile
├── app.py
├── data.py
├── pages/
│   ├── __init__.py      (vide)
│   ├── overview.py
│   └── report.py
└── tests/
    ├── conftest.py
    └── test_data.py
```

**Modifications :**
- `docker-compose.yml` : ajout service `dashboard`

---

## Task 1 : data.py — humanize_alert + get_stress_label + conftest

**Files:**
- Create: `dashboard/data.py`
- Create: `dashboard/tests/conftest.py`
- Create: `dashboard/tests/test_data.py`

- [ ] **Étape 1 : Créer `dashboard/tests/conftest.py`**

```python
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
```

- [ ] **Étape 2 : Écrire les tests qui vont échouer**

Dans `dashboard/tests/test_data.py` :

```python
import pytest
import pandas as pd
from data import humanize_alert, get_stress_label


def test_humanize_alert_secheresse():
    assert humanize_alert('sécheresse') == 'Manque d\'eau'


def test_humanize_alert_deficit_azote():
    assert humanize_alert('déficit_azote') == 'Manque d\'engrais azoté'


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
```

- [ ] **Étape 3 : Vérifier que les tests échouent**

```
cd dashboard
pytest tests/test_data.py -v
```

Attendu : `ModuleNotFoundError: No module named 'data'` ou `ImportError`

- [ ] **Étape 4 : Créer `dashboard/data.py` avec les constantes et les deux fonctions**

```python
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
```

- [ ] **Étape 5 : Vérifier que les 8 tests passent**

```
cd dashboard
pytest tests/test_data.py -v
```

Attendu : 8 PASSED

- [ ] **Étape 6 : Commit**

```bash
git add dashboard/data.py dashboard/tests/conftest.py dashboard/tests/test_data.py
git commit -m "feat(dashboard): data.py humanize_alert + get_stress_label + 8 tests"
```

---

## Task 2 : data.py — build_field_cards + tests

**Files:**
- Modify: `dashboard/data.py`
- Modify: `dashboard/tests/test_data.py`

- [ ] **Étape 1 : Ajouter les tests de build_field_cards dans `test_data.py`**

Ajouter sous les tests existants :

```python
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
```

- [ ] **Étape 2 : Vérifier que les 5 nouveaux tests échouent**

```
cd dashboard
pytest tests/test_data.py::test_build_field_cards_returns_four -v
```

Attendu : `ImportError` ou `NameError`

- [ ] **Étape 3 : Implémenter `build_field_cards` dans `data.py`**

Ajouter à la suite des fonctions existantes :

```python
def build_field_cards(df: pd.DataFrame) -> list:
    if df.empty:
        return [
            {'sensor_id': sid, 'name': name, 'emoji': emoji, 'no_data': True}
            for sid, (name, emoji) in SENSOR_NAMES.items()
        ]

    cards = []
    for sensor_id, (name, emoji) in SENSOR_NAMES.items():
        field_df = df[df['sensor_id'] == sensor_id]
        if field_df.empty:
            cards.append({'sensor_id': sensor_id, 'name': name, 'emoji': emoji, 'no_data': True})
            continue

        last = field_df.sort_values('timestamp').iloc[-1]
        alerts_flat = [a for row in field_df['alerts'] for a in (row if isinstance(row, list) else [])]

        if alerts_flat:
            status, color = 'Alerte', 'red'
        elif 'stress_score' in field_df.columns and float(last['stress_score']) > 65:
            status, color = 'Attention', 'orange'
        else:
            status, color = 'Sain', 'green'

        cards.append({
            'sensor_id': sensor_id,
            'name': name,
            'emoji': emoji,
            'no_data': False,
            'temp': round(float(field_df['temperature_c'].mean()), 1),
            'humidity': round(float(field_df['humidity_pct'].mean()), 1),
            'stage': STAGE_LABELS.get(str(last.get('plant_stage', 'inconnu')), 'Phase inconnue'),
            'status': status,
            'status_color': color,
            'alerts': list({humanize_alert(a) for a in alerts_flat}),
        })
    return cards
```

- [ ] **Étape 4 : Vérifier que les 13 tests passent**

```
cd dashboard
pytest tests/test_data.py -v
```

Attendu : 13 PASSED

- [ ] **Étape 5 : Commit**

```bash
git add dashboard/data.py dashboard/tests/test_data.py
git commit -m "feat(dashboard): build_field_cards + 5 tests"
```

---

## Task 3 : data.py — build_detail + tests

**Files:**
- Modify: `dashboard/data.py`
- Modify: `dashboard/tests/test_data.py`

- [ ] **Étape 1 : Ajouter les tests de build_detail dans `test_data.py`**

```python
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
```

- [ ] **Étape 2 : Vérifier que les 4 nouveaux tests échouent**

```
cd dashboard
pytest tests/test_data.py::test_build_detail_returns_empty_for_unknown_sensor -v
```

Attendu : `ImportError`

- [ ] **Étape 3 : Implémenter `build_detail` dans `data.py`**

```python
def build_detail(df: pd.DataFrame, sensor_id: str) -> dict:
    if df.empty or sensor_id not in df['sensor_id'].values:
        return {}

    field_df = df[df['sensor_id'] == sensor_id].sort_values('timestamp')
    alerts_flat = [a for row in field_df['alerts'] for a in (row if isinstance(row, list) else [])]
    alert_counts = {}
    for a in alerts_flat:
        label = humanize_alert(a)
        alert_counts[label] = alert_counts.get(label, 0) + 1

    last = field_df.iloc[-1]
    stress = float(last['stress_score']) if 'stress_score' in field_df.columns else 0.0
    stress_label, stress_color = get_stress_label(stress)

    def safe_mean(col):
        if col in field_df.columns:
            return round(float(field_df[col].mean()), 1)
        return 0.0

    return {
        'timestamps': field_df['timestamp'].tolist(),
        'temperatures': field_df['temperature_c'].tolist(),
        'humidities': field_df['humidity_pct'].tolist(),
        'nitrogen': safe_mean('soil_nitrogen_mg_kg'),
        'phosphorus': safe_mean('soil_phosphorus_mg_kg'),
        'potassium': safe_mean('soil_potassium_mg_kg'),
        'alert_counts': alert_counts,
        'stress_score': round(stress, 1),
        'stress_label': stress_label,
        'stress_color': stress_color,
    }
```

- [ ] **Étape 4 : Vérifier que les 17 tests passent**

```
cd dashboard
pytest tests/test_data.py -v
```

Attendu : 17 PASSED

- [ ] **Étape 5 : Commit**

```bash
git add dashboard/data.py dashboard/tests/test_data.py
git commit -m "feat(dashboard): build_detail + 4 tests"
```

---

## Task 4 : data.py — build_report_table + tests

**Files:**
- Modify: `dashboard/data.py`
- Modify: `dashboard/tests/test_data.py`

- [ ] **Étape 1 : Ajouter les tests dans `test_data.py`**

```python
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
    # temp 28 > 26 : hausse = dégradation (higher_is_worse) → rouge ↑
    assert field1['temp_arrow'] == '↑'
    assert field1['temp_color'] == 'red'


def test_build_report_table_humidity_down_is_red():
    rows = build_report_table(_make_today_report(), _make_yesterday_report())
    field1 = next(r for r in rows if r['sensor_id'] == 'field_1')
    # humidity 42 < 50 : baisse = dégradation (higher_is_good) → rouge ↓
    assert field1['humidity_arrow'] == '↓'
    assert field1['humidity_color'] == 'red'
```

- [ ] **Étape 2 : Vérifier que les 5 nouveaux tests échouent**

```
cd dashboard
pytest tests/test_data.py::test_build_report_table_returns_four_rows -v
```

Attendu : `ImportError`

- [ ] **Étape 3 : Implémenter `_trend` et `build_report_table` dans `data.py`**

```python
def _trend(today_val, yesterday_val, higher_is_worse=True) -> tuple:
    if yesterday_val is None or yesterday_val == 0:
        return '~', 'gray'
    pct = (today_val - yesterday_val) / abs(yesterday_val) * 100
    if abs(pct) < 5:
        return '~', 'gray'
    going_up = pct > 0
    if higher_is_worse:
        return ('↑', 'red') if going_up else ('↓', 'green')
    return ('↑', 'green') if going_up else ('↓', 'red')


def build_report_table(today: dict, yesterday: Optional[dict]) -> list:
    rows = []
    for sensor_id, (name, emoji) in SENSOR_NAMES.items():
        t = today.get('par_champ', {}).get(sensor_id, {})
        y = yesterday.get('par_champ', {}).get(sensor_id) if yesterday else None

        def get_trend(key, higher_is_worse=True):
            tv = t.get(key)
            yv = y.get(key) if y else None
            if tv is None:
                return '-', '', ''
            arrow, color = _trend(tv, yv, higher_is_worse)
            return tv, arrow, color

        temp, t_arr, t_col = get_trend('temperature_moy', higher_is_worse=True)
        hum, h_arr, h_col = get_trend('humidity_moy', higher_is_worse=False)
        nit, n_arr, n_col = get_trend('nitrogen_moy', higher_is_worse=False)

        rows.append({
            'sensor_id': sensor_id,
            'name': f"{emoji} {name}",
            'temp': temp, 'temp_arrow': t_arr, 'temp_color': t_col,
            'humidity': hum, 'humidity_arrow': h_arr, 'humidity_color': h_col,
            'nitrogen': nit, 'nitrogen_arrow': n_arr, 'nitrogen_color': n_col,
            'nb_alertes': t.get('nb_alertes', 0),
            'no_yesterday': yesterday is None,
        })
    return rows
```

- [ ] **Étape 4 : Vérifier que les 22 tests passent**

```
cd dashboard
pytest tests/test_data.py -v
```

Attendu : 22 PASSED

- [ ] **Étape 5 : Commit**

```bash
git add dashboard/data.py dashboard/tests/test_data.py
git commit -m "feat(dashboard): build_report_table + _trend + 5 tests"
```

---

## Task 5 : data.py — lecture MinIO (load_parquet_last_24h + load_report)

**Files:**
- Modify: `dashboard/data.py`

Pas de tests unitaires pour ces fonctions (dépendance réseau). Testées lors de l'intégration (Task 9).

- [ ] **Étape 1 : Ajouter les deux fonctions de lecture MinIO dans `data.py`**

```python
def _s3_client():
    endpoint = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
        aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
    )


def load_parquet_last_24h() -> pd.DataFrame:
    try:
        client = _s3_client()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        paginator = client.get_paginator('list_objects_v2')
        keys = []
        for page in paginator.paginate(Bucket='smart-farm-clean'):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet') and obj['LastModified'] >= cutoff:
                    keys.append(obj['Key'])
        if not keys:
            return pd.DataFrame()
        frames = []
        for key in keys:
            resp = client.get_object(Bucket='smart-farm-clean', Key=key)
            frames.append(pd.read_parquet(io.BytesIO(resp['Body'].read())))
        df = pd.concat(frames, ignore_index=True)
        df['alerts'] = df['alerts'].apply(
            lambda x: x.tolist() if hasattr(x, 'tolist') else (x if isinstance(x, list) else [])
        )
        return df
    except Exception:
        return pd.DataFrame()


def load_report(date_str: str) -> Optional[dict]:
    try:
        client = _s3_client()
        resp = client.get_object(Bucket='smart-farm-reports', Key=f'report_{date_str}.json')
        return json.loads(resp['Body'].read())
    except Exception:
        return None
```

- [ ] **Étape 2 : Vérifier que les 22 tests passent toujours**

```
cd dashboard
pytest tests/test_data.py -v
```

Attendu : 22 PASSED (les nouvelles fonctions ne sont pas testées ici)

- [ ] **Étape 3 : Commit**

```bash
git add dashboard/data.py
git commit -m "feat(dashboard): load_parquet_last_24h + load_report (MinIO)"
```

---

## Task 6 : pages/overview.py — Tableau de bord

**Files:**
- Create: `dashboard/pages/__init__.py`
- Create: `dashboard/pages/overview.py`

- [ ] **Étape 1 : Créer `dashboard/pages/__init__.py`** (vide)

- [ ] **Étape 2 : Créer `dashboard/pages/overview.py`**

```python
import json as _json
import dash
from dash import html, dcc, Input, Output, callback
import plotly.graph_objects as go
from data import build_field_cards, build_detail, SENSOR_NAMES


def _card(card: dict, selected: bool) -> html.Div:
    border = '2px solid #2196F3' if selected else '1px solid #ddd'
    bg = '#EBF5FB' if selected else 'white'

    if card.get('no_data'):
        return html.Div([
            html.Div(f"{card['emoji']} {card['name']}", style={'fontWeight': 'bold'}),
            html.Div('Aucune donnée récente', style={'color': '#999', 'fontSize': '13px'}),
        ], id={'type': 'field-card', 'index': card['sensor_id']},
           style={'border': border, 'borderRadius': '8px', 'padding': '12px',
                  'cursor': 'pointer', 'background': '#fafafa'})

    color_map = {'green': '#2e7d32', 'orange': '#e65100', 'red': '#c62828'}
    badge_color = color_map.get(card['status_color'], '#999')

    return html.Div([
        html.Div([
            html.Span(f"{card['emoji']} {card['name']}",
                      style={'fontWeight': 'bold', 'fontSize': '15px'}),
            html.Span(card['status'], style={
                'background': badge_color, 'color': 'white',
                'padding': '2px 8px', 'borderRadius': '12px',
                'fontSize': '11px', 'marginLeft': '8px',
            }),
        ], style={'display': 'flex', 'alignItems': 'center', 'marginBottom': '6px'}),
        html.Div(f"🌡 {card['temp']}°C   💧 {card['humidity']}%",
                 style={'fontSize': '13px', 'color': '#555'}),
        html.Div(card['stage'], style={'fontSize': '12px', 'color': '#777', 'marginTop': '4px'}),
    ], id={'type': 'field-card', 'index': card['sensor_id']},
       style={'border': border, 'borderRadius': '8px', 'padding': '12px',
              'cursor': 'pointer', 'background': bg,
              'boxShadow': '0 1px 3px rgba(0,0,0,0.1)'})


def _detail_panel(detail: dict, sensor_id: str) -> html.Div:
    name, emoji = SENSOR_NAMES.get(sensor_id, (sensor_id, ''))

    if not detail:
        return html.Div('Sélectionne un champ ci-dessus pour voir le détail.',
                        style={'color': '#999', 'padding': '20px', 'textAlign': 'center'})

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=detail['timestamps'], y=detail['temperatures'],
        name='Température (°C)', line={'color': '#e65100'},
    ))
    fig.add_trace(go.Scatter(
        x=detail['timestamps'], y=detail['humidities'],
        name='Humidité (%)', line={'color': '#1565c0'},
    ))
    fig.update_layout(
        margin={'t': 10, 'b': 30, 'l': 40, 'r': 10}, height=200,
        legend={'orientation': 'h', 'y': -0.4},
        plot_bgcolor='white', paper_bgcolor='white',
    )

    npk_items = []
    npk_cfg = [
        ('Azote (N)', detail['nitrogen'], 50),
        ('Phosphore (P)', detail['phosphorus'], 20),
        ('Potassium (K)', detail['potassium'], 80),
    ]
    for label, val, threshold in npk_cfg:
        ok = val >= threshold
        c = '#2e7d32' if ok else '#c62828'
        npk_items.append(html.Div([
            html.Div(label, style={'fontSize': '12px', 'color': '#555'}),
            html.Div(f"{val} mg/kg", style={'fontWeight': 'bold', 'color': c}),
            html.Div(style={'background': '#eee', 'borderRadius': '4px', 'height': '6px', 'margin': '4px 0'},
                     children=[html.Div(style={
                         'background': c,
                         'width': f"{min(100, val / threshold * 100):.0f}%",
                         'height': '100%', 'borderRadius': '4px',
                     })]),
            html.Div('✓ Correct' if ok else f'⚠ Trop bas (min {threshold} mg/kg)',
                     style={'fontSize': '11px', 'color': c}),
        ], style={'flex': '1', 'padding': '0 8px'}))

    stress_colors = {'Faible': '#2e7d32', 'Modéré': '#e65100', 'Élevé': '#c62828'}
    sc = stress_colors.get(detail['stress_label'], '#999')

    alert_list = (
        html.Ul([
            html.Li(f"{alert} (×{count})", style={'color': '#c62828', 'fontSize': '13px'})
            for alert, count in detail['alert_counts'].items()
        ])
        if detail.get('alert_counts')
        else html.Div('Aucune alerte', style={'color': '#2e7d32', 'fontSize': '13px'})
    )

    return html.Div([
        html.H3(f"{emoji} {name} — Dernières 24 heures",
                style={'marginBottom': '12px', 'color': '#333', 'fontSize': '16px'}),
        dcc.Graph(figure=fig, config={'displayModeBar': False}),
        html.Div([
            html.Div('🧪 Azote, Phosphore, Potassium',
                     style={'fontWeight': 'bold', 'marginBottom': '8px', 'fontSize': '13px'}),
            html.Div(npk_items, style={'display': 'flex'}),
        ], style={'marginTop': '16px', 'padding': '12px',
                  'background': '#f9f9f9', 'borderRadius': '8px'}),
        html.Div([
            html.Div([
                html.Div('🚨 Alertes actives',
                         style={'fontWeight': 'bold', 'marginBottom': '8px', 'fontSize': '13px'}),
                alert_list,
            ], style={'flex': '1'}),
            html.Div([
                html.Div('Score de stress', style={'fontSize': '12px', 'color': '#555'}),
                html.Div(str(detail['stress_score']),
                         style={'fontSize': '32px', 'fontWeight': 'bold', 'color': sc}),
                html.Div(f"{detail['stress_label']} / 100",
                         style={'fontSize': '12px', 'color': sc}),
            ], style={'textAlign': 'center', 'padding': '0 16px'}),
        ], style={'display': 'flex', 'marginTop': '12px', 'padding': '12px',
                  'background': '#f9f9f9', 'borderRadius': '8px'}),
    ], style={'padding': '16px', 'background': 'white',
              'borderRadius': '8px', 'boxShadow': '0 1px 3px rgba(0,0,0,0.1)'})


def layout():
    return html.Div(id='overview-content')


@callback(
    Output('overview-content', 'children'),
    Input('data-store', 'data'),
    Input('selected-field', 'data'),
)
def render_overview(store_data, selected_field):
    import pandas as pd

    if not store_data:
        return html.Div(
            'Données indisponibles — connexion au stockage impossible. Nouvelle tentative dans 5 minutes.',
            style={'color': '#c62828', 'padding': '20px', 'textAlign': 'center',
                   'background': '#fdecea', 'borderRadius': '8px'},
        )

    df = pd.DataFrame(store_data.get('parquet', []))
    cards_data = build_field_cards(df)
    selected = selected_field or 'field_1'

    all_alerts = [
        (c['name'], a)
        for c in cards_data
        if not c.get('no_data')
        for a in c.get('alerts', [])
    ]
    banner = None
    if all_alerts:
        alert_text = ' · '.join(f"{a} ({n})" for n, a in all_alerts[:3])
        banner = html.Div(
            f"🚨 {len(all_alerts)} alerte(s) active(s) — {alert_text}",
            style={'background': '#fdecea', 'border': '1px solid #c62828',
                   'color': '#c62828', 'padding': '10px 16px',
                   'borderRadius': '8px', 'marginBottom': '12px', 'fontSize': '13px'},
        )

    cards_grid = html.Div(
        [_card(c, c['sensor_id'] == selected) for c in cards_data],
        style={'display': 'grid', 'gridTemplateColumns': '1fr 1fr',
               'gap': '12px', 'marginBottom': '16px'},
    )

    detail = build_detail(df, selected) if not df.empty else {}
    detail_panel = _detail_panel(detail, selected)

    children = []
    if banner:
        children.append(banner)
    children.extend([
        html.P('Sélectionne un champ pour voir le détail ↓',
               style={'color': '#888', 'fontSize': '12px', 'marginBottom': '8px'}),
        cards_grid,
        detail_panel,
    ])
    return html.Div(children)


@callback(
    Output('selected-field', 'data'),
    Input({'type': 'field-card', 'index': dash.ALL}, 'n_clicks'),
    prevent_initial_call=True,
)
def select_field(_):
    ctx = dash.callback_context
    if not ctx.triggered:
        return 'field_1'
    prop_id = ctx.triggered[0]['prop_id']
    sensor_id = _json.loads(prop_id.split('.')[0])['index']
    return sensor_id
```

- [ ] **Étape 3 : Commit**

```bash
git add dashboard/pages/__init__.py dashboard/pages/overview.py
git commit -m "feat(dashboard): overview page — 4 cartes + panneau détail"
```

---

## Task 7 : pages/report.py — Rapport journalier

**Files:**
- Create: `dashboard/pages/report.py`

- [ ] **Étape 1 : Créer `dashboard/pages/report.py`**

```python
from dash import html, Input, Output, callback
from data import build_report_table, humanize_alert, SENSOR_NAMES

ALERT_BG = {
    'sécheresse': '#fdecea', 'déficit_azote': '#fff3e0',
    'excès_humidité': '#e3f2fd', 'stress_élevé': '#fdecea', 'prêt_à_récolter': '#e8f5e9',
}
ALERT_FG = {
    'sécheresse': '#c62828', 'déficit_azote': '#e65100',
    'excès_humidité': '#1565c0', 'stress_élevé': '#c62828', 'prêt_à_récolter': '#2e7d32',
}


def _arrow_span(arrow, color):
    colors = {'green': '#2e7d32', 'red': '#c62828', 'gray': '#999'}
    return html.Span(arrow, style={'color': colors.get(color, '#999'),
                                   'marginLeft': '4px', 'fontWeight': 'bold'})


def layout():
    return html.Div(id='report-content')


@callback(
    Output('report-content', 'children'),
    Input('data-store', 'data'),
)
def render_report(store_data):
    if not store_data:
        return html.Div('Données indisponibles.',
                        style={'color': '#c62828', 'padding': '20px', 'textAlign': 'center'})

    today = store_data.get('report_today')
    yesterday = store_data.get('report_yesterday')

    if not today:
        return html.Div(
            'Rapport journalier non disponible. Il est généré chaque jour à 01h00.',
            style={'color': '#888', 'padding': '20px', 'textAlign': 'center',
                   'background': '#f5f5f5', 'borderRadius': '8px'},
        )

    date_str = today.get('date', '')
    harvest_ready = today.get('cultures_recolte_imminente', [])

    header_children = [
        html.H2(f"Rapport du {date_str}",
                style={'margin': '0', 'fontSize': '18px', 'color': '#333'}),
        html.P('Comparaison avec la veille' if yesterday else 'Données du jour uniquement',
               style={'color': '#888', 'margin': '4px 0 0 0', 'fontSize': '13px'}),
    ]
    if harvest_ready:
        names = [
            f"{SENSOR_NAMES.get(s, (s, ''))[1]} {SENSOR_NAMES.get(s, (s, ''))[0]}"
            for s in harvest_ready
        ]
        header_children.append(html.Span(
            f"🌻 Prêt à récolter : {', '.join(names)}",
            style={'background': '#e8f5e9', 'color': '#2e7d32', 'padding': '4px 10px',
                   'borderRadius': '12px', 'fontSize': '12px',
                   'display': 'inline-block', 'marginTop': '8px'},
        ))

    rows = build_report_table(today, yesterday)

    header_row = html.Tr([
        html.Th(col, style={'textAlign': al, 'padding': '8px', 'background': '#f5f5f5',
                            'fontWeight': 'bold', 'fontSize': '13px'})
        for col, al in [
            ('Champ', 'left'), ('Température moy.', 'center'),
            ('Humidité moy.', 'center'), ('Azote moyen', 'center'), ('Alertes', 'center'),
        ]
    ])

    data_rows = []
    for r in rows:
        temp_str = f"{r['temp']}°C" if r['temp'] != '-' else '-'
        hum_str = f"{r['humidity']}%" if r['humidity'] != '-' else '-'
        nit_str = f"{r['nitrogen']} mg/kg" if r['nitrogen'] != '-' else '-'
        data_rows.append(html.Tr([
            html.Td(r['name'], style={'padding': '8px', 'fontWeight': 'bold'}),
            html.Td([temp_str, _arrow_span(r['temp_arrow'], r['temp_color'])],
                    style={'textAlign': 'center', 'padding': '8px'}),
            html.Td([hum_str, _arrow_span(r['humidity_arrow'], r['humidity_color'])],
                    style={'textAlign': 'center', 'padding': '8px'}),
            html.Td([nit_str, _arrow_span(r['nitrogen_arrow'], r['nitrogen_color'])],
                    style={'textAlign': 'center', 'padding': '8px'}),
            html.Td(str(r['nb_alertes']),
                    style={'textAlign': 'center', 'padding': '8px',
                           'color': '#c62828' if r['nb_alertes'] > 0 else '#2e7d32',
                           'fontWeight': 'bold'}),
        ], style={'borderBottom': '1px solid #eee'}))

    table = html.Table(
        [header_row] + data_rows,
        style={'width': '100%', 'borderCollapse': 'collapse',
               'background': 'white', 'borderRadius': '8px',
               'boxShadow': '0 1px 3px rgba(0,0,0,0.1)', 'overflow': 'hidden'},
    )

    total = today.get('total_alertes', {})
    alert_cards = [
        html.Div([
            html.Div(str(count), style={
                'fontSize': '28px', 'fontWeight': 'bold',
                'color': ALERT_FG.get(code, '#333'),
            }),
            html.Div(humanize_alert(code), style={
                'fontSize': '11px', 'color': ALERT_FG.get(code, '#333'),
            }),
        ], style={
            'background': ALERT_BG.get(code, '#f5f5f5'),
            'border': f"1px solid {ALERT_FG.get(code, '#ddd')}",
            'borderRadius': '8px', 'padding': '12px',
            'textAlign': 'center', 'minWidth': '110px',
        })
        for code, count in total.items()
    ]

    children = [
        html.Div(header_children, style={'marginBottom': '16px'}),
        html.Div('Moyennes journalières par champ',
                 style={'fontWeight': 'bold', 'color': '#555', 'fontSize': '13px', 'marginBottom': '8px'}),
        table,
    ]
    if not yesterday:
        children.append(html.P('Données du jour précédent non disponibles.',
                               style={'color': '#888', 'fontSize': '12px', 'marginTop': '8px'}))
    if alert_cards:
        children.extend([
            html.Div('Alertes du jour',
                     style={'fontWeight': 'bold', 'color': '#555', 'fontSize': '13px',
                            'marginTop': '20px', 'marginBottom': '8px'}),
            html.Div(alert_cards, style={'display': 'flex', 'gap': '12px', 'flexWrap': 'wrap'}),
        ])

    return html.Div(children)
```

- [ ] **Étape 2 : Commit**

```bash
git add dashboard/pages/report.py
git commit -m "feat(dashboard): rapport journalier J vs J-1 avec flèches de tendance"
```

---

## Task 8 : app.py — Point d'entrée Dash

**Files:**
- Create: `dashboard/app.py`

- [ ] **Étape 1 : Créer `dashboard/app.py`**

```python
import dash
from datetime import datetime, timedelta, timezone
from dash import html, dcc, Input, Output, State

from pages import overview, report
from data import load_parquet_last_24h, load_report

app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

_NAV_ACTIVE = {
    'cursor': 'pointer', 'padding': '8px 16px',
    'borderBottom': '2px solid #2196F3', 'color': '#2196F3',
    'fontWeight': 'bold',
}
_NAV_INACTIVE = {'cursor': 'pointer', 'padding': '8px 16px', 'color': '#888'}

app.layout = html.Div([
    dcc.Store(id='data-store'),
    dcc.Store(id='selected-field', data='field_1'),
    dcc.Store(id='active-tab', data='overview'),
    dcc.Interval(id='refresh-interval', interval=300_000, n_intervals=0),
    dcc.Interval(id='countdown-interval', interval=1_000, n_intervals=0),

    html.Div([
        html.Span('🌾 Ferme intelligente',
                  style={'fontWeight': 'bold', 'fontSize': '18px', 'color': '#333'}),
        html.Div([
            html.Span('Tableau de bord', id='tab-overview', n_clicks=0, style=_NAV_ACTIVE),
            html.Span('Rapport journalier', id='tab-report', n_clicks=0, style=_NAV_INACTIVE),
        ], style={'display': 'flex'}),
        html.Span(id='countdown-display', style={'color': '#888', 'fontSize': '13px'}),
    ], style={
        'display': 'flex', 'alignItems': 'center', 'justifyContent': 'space-between',
        'padding': '12px 20px', 'background': 'white',
        'boxShadow': '0 1px 3px rgba(0,0,0,0.1)', 'marginBottom': '16px',
    }),

    html.Div(id='page-content', style={'padding': '0 20px 20px 20px'}),
], style={'fontFamily': 'Segoe UI, Arial, sans-serif', 'background': '#f0f2f5', 'minHeight': '100vh'})


@app.callback(
    Output('data-store', 'data'),
    Input('refresh-interval', 'n_intervals'),
)
def refresh_data(_):
    df = load_parquet_last_24h()
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    yesterday_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
    return {
        'parquet': df.to_dict('records') if not df.empty else [],
        'report_today': load_report(today_str),
        'report_yesterday': load_report(yesterday_str),
    }


@app.callback(
    Output('active-tab', 'data'),
    Output('tab-overview', 'style'),
    Output('tab-report', 'style'),
    Input('tab-overview', 'n_clicks'),
    Input('tab-report', 'n_clicks'),
    prevent_initial_call=True,
)
def switch_tab(_, __):
    ctx = dash.callback_context
    active = 'report' if ctx.triggered[0]['prop_id'] == 'tab-report.n_clicks' else 'overview'
    if active == 'overview':
        return active, _NAV_ACTIVE, _NAV_INACTIVE
    return active, _NAV_INACTIVE, _NAV_ACTIVE


@app.callback(
    Output('page-content', 'children'),
    Input('active-tab', 'data'),
)
def render_page(tab):
    if tab == 'report':
        return report.layout()
    return overview.layout()


@app.callback(
    Output('countdown-display', 'children'),
    Input('countdown-interval', 'n_intervals'),
)
def update_countdown(n):
    remaining = 300 - (n % 300)
    return f"🔄 Mise à jour dans {remaining // 60}:{remaining % 60:02d}"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=False)
```

- [ ] **Étape 2 : Commit**

```bash
git add dashboard/app.py
git commit -m "feat(dashboard): app.py — layout global, routing onglets, dcc.Interval 5 min"
```

---

## Task 9 : Dockerfile + docker-compose.yml + test d'intégration

**Files:**
- Create: `dashboard/Dockerfile`
- Modify: `docker-compose.yml`

- [ ] **Étape 1 : Créer `dashboard/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir \
    dash==2.17.1 \
    plotly==5.22.0 \
    pandas==2.2.2 \
    boto3==1.34.0 \
    pyarrow==16.1.0
EXPOSE 8050
CMD ["python", "app.py"]
```

- [ ] **Étape 2 : Ajouter le service `dashboard` dans `docker-compose.yml`**

Ajouter à la fin de la section `services:` (avant la section `volumes:`) :

```yaml
  dashboard:
    build: ./dashboard
    ports:
      - "8050:8050"
    environment:
      - MINIO_ENDPOINT=${MINIO_ENDPOINT}
      - MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
      - MINIO_SECRET_KEY=${MINIO_SECRET_KEY}
    networks: [smart-farm-net]
    depends_on:
      - minio
    restart: unless-stopped
```

- [ ] **Étape 3 : Vérifier que les 22 tests passent toujours**

```
cd dashboard
pytest tests/test_data.py -v
```

Attendu : 22 PASSED

- [ ] **Étape 4 : Build et démarrage du dashboard**

```powershell
$env:MSYS_NO_PATHCONV=1
docker compose build dashboard
docker compose up -d dashboard
```

Attendu : `dashboard-1 | Dash is running on http://0.0.0.0:8050/`

Vérifier dans les logs :

```powershell
docker compose logs dashboard --tail=20
```

- [ ] **Étape 5 : Ouvrir http://localhost:8050 et vérifier**

Vérifications manuelles :
- [ ] Les 4 cartes de champs s'affichent (Blé, Maïs, Tournesol, Soja)
- [ ] Cliquer sur une carte met à jour le panneau de détail (courbes, NPK, alertes)
- [ ] Le compteur de rafraîchissement décompte de 5:00 à 0:00
- [ ] Cliquer sur "Rapport journalier" change d'onglet
- [ ] L'onglet rapport affiche soit le tableau J vs J-1, soit le message "Rapport non disponible"
- [ ] Aucune icône générée par IA n'est visible — uniquement des emojis Unicode et du texte

- [ ] **Étape 6 : Commit final**

```bash
git add dashboard/Dockerfile docker-compose.yml
git commit -m "feat(dashboard): Dockerfile + service docker-compose, port 8050"
```
