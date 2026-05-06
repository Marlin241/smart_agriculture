import io
import json
import os
from datetime import datetime

import pandas as pd
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator


def compute_daily_report(df: pd.DataFrame, date_str: str) -> dict:
    report = {
        'date': date_str,
        'par_champ': {},
        'cultures_matures': [],
        'cultures_recolte_imminente': [],
        'total_alertes': {},
    }

    for sensor_id, group in df.groupby('sensor_id'):
        alertes_flat = [a for row in group['alerts'] for a in (row or [])]
        alertes_detail = {}
        for a in alertes_flat:
            alertes_detail[a] = alertes_detail.get(a, 0) + 1

        report['par_champ'][sensor_id] = {
            'temperature_moy': round(float(group['temperature_c'].mean()), 2),
            'temperature_min': round(float(group['temperature_c'].min()), 2),
            'temperature_max': round(float(group['temperature_c'].max()), 2),
            'humidity_moy': round(float(group['humidity_pct'].mean()), 2),
            'nitrogen_moy': round(float(group['soil_nitrogen_mg_kg'].mean()), 2),
            'phosphorus_moy': round(float(group['soil_phosphorus_mg_kg'].mean()), 2),
            'potassium_moy': round(float(group['soil_potassium_mg_kg'].mean()), 2),
            'nb_alertes': len(alertes_flat),
            'alertes_detail': alertes_detail,
        }

        stages = group['plant_stage'].tolist()
        if 'maturité' in stages:
            report['cultures_matures'].append(sensor_id)
        if 'récolte_imminente' in stages:
            report['cultures_recolte_imminente'].append(sensor_id)

    total = {}
    for champ_data in report['par_champ'].values():
        for alerte, count in champ_data['alertes_detail'].items():
            total[alerte] = total.get(alerte, 0) + count
    report['total_alertes'] = total

    return report


def generate_report(**context):
    yesterday = context['data_interval_start'].date()
    date_str = str(yesterday)

    client = Minio(
        os.environ['MINIO_ENDPOINT'].replace('http://', ''),
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False,
    )

    prefix = f"year={yesterday.year}/month={yesterday.month}/day={yesterday.day}/"
    objects = list(client.list_objects('smart-farm-clean', prefix=prefix, recursive=True))

    if not objects:
        print(f"[Report] Aucune donnée pour {date_str}")
        return

    dfs = []
    for obj in objects:
        data = client.get_object('smart-farm-clean', obj.object_name)
        dfs.append(pd.read_parquet(io.BytesIO(data.read())))
    df = pd.concat(dfs, ignore_index=True)

    report = compute_daily_report(df, date_str)
    report_bytes = json.dumps(report, ensure_ascii=False, indent=2).encode('utf-8')
    report_name = f"report_{date_str}.json"
    client.put_object(
        'smart-farm-reports', report_name,
        io.BytesIO(report_bytes), len(report_bytes),
        content_type='application/json',
    )
    print(f"[Report] {report_name} écrit dans smart-farm-reports")


with DAG(
    dag_id='daily_agronomic_report',
    schedule_interval='0 1 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['report', 'agronomic'],
) as dag:
    PythonOperator(
        task_id='generate_daily_report',
        python_callable=generate_report,
    )
