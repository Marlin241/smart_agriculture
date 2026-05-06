import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, dayofmonth, hour, lag, lead, month, to_timestamp, udf, when, year,
)
from pyspark.sql.types import ArrayType, DoubleType, StringType
from pyspark.sql.window import Window

MINIO_ENDPOINT    = os.environ.get('MINIO_ENDPOINT',    'http://minio:9000')
MINIO_ACCESS_KEY  = os.environ.get('MINIO_ACCESS_KEY',  'minioadmin')
MINIO_SECRET_KEY  = os.environ.get('MINIO_SECRET_KEY',  'minioadmin')
BUCKET_RAW        = 'smart-farm-raw'
BUCKET_CLEAN      = 'smart-farm-clean'
BUCKET_QUARANTINE = 'smart-farm-quarantine'

TIMESTAMP_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'


# ── Fonctions pures (testables sans SparkSession) ─────────────────────────────

def compute_stress_score(humidity_pct, soil_nitrogen_mg_kg):
    if humidity_pct is None or soil_nitrogen_mg_kg is None:
        return None
    stress_hum = max(0.0, 40.0 - humidity_pct) * 1.2
    stress_nit = max(0.0, 30.0 - soil_nitrogen_mg_kg) * 0.8
    return min(100.0, (stress_hum + stress_nit) / 2.0)


def compute_plant_stage(plant_growth_pct, phase):
    if phase == 'mature':
        return 'récolte_imminente'
    if plant_growth_pct is None:
        return 'inconnu'
    if plant_growth_pct >= 90:
        return 'maturité'
    if plant_growth_pct >= 60:
        return 'développement'
    if plant_growth_pct >= 25:
        return 'croissance'
    return 'germination'


def compute_soil_fertility_score(n, p, k):
    if n is None or p is None or k is None:
        return None
    return min(100.0, (n / 100.0 + p / 45.0 + k / 200.0) * 100.0 / 3.0)


def compute_alerts(humidity_pct, soil_nitrogen_mg_kg, stress_score, plant_growth_pct):
    alerts = []
    if humidity_pct is not None and humidity_pct < 30:
        alerts.append('sécheresse')
    if humidity_pct is not None and humidity_pct > 85:
        alerts.append('excès_humidité')
    if soil_nitrogen_mg_kg is not None and soil_nitrogen_mg_kg < 20:
        alerts.append('déficit_azote')
    if stress_score is not None and stress_score > 60:
        alerts.append('stress_élevé')
    if plant_growth_pct is not None and plant_growth_pct > 90:
        alerts.append('prêt_à_récolter')
    return alerts


# ── Fonctions Spark ───────────────────────────────────────────────────────────

def build_spark_session():
    return (SparkSession.builder
        .appName('SmartFarmCleaning')
        .master('local[*]')
        .config('spark.hadoop.fs.s3a.endpoint',         MINIO_ENDPOINT)
        .config('spark.hadoop.fs.s3a.access.key',        MINIO_ACCESS_KEY)
        .config('spark.hadoop.fs.s3a.secret.key',        MINIO_SECRET_KEY)
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .config('spark.hadoop.fs.s3a.impl',
                'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .getOrCreate())


def validate(df):
    """Sépare les lignes valides (→ clean) des lignes rejetées (→ quarantine)."""
    invalid = df.filter(
        col('timestamp').isNull() |
        col('sensor_id').isNull() |
        ~col('timestamp').rlike(TIMESTAMP_PATTERN)
    )
    valid = df.filter(
        col('timestamp').isNotNull() &
        col('sensor_id').isNotNull() &
        col('timestamp').rlike(TIMESTAMP_PATTERN)
    )
    return valid, invalid


def clean(df):
    """Corrige les anomalies capteurs et déduplique."""
    w = Window.partitionBy('sensor_id').orderBy('timestamp')
    return (df
        .withColumn('_temp_lag',  lag('temperature_c',  1).over(w))
        .withColumn('_temp_lead', lead('temperature_c', 1).over(w))
        .withColumn('temperature_c', when(
            (col('temperature_c') < -5) | (col('temperature_c') > 60),
            (col('_temp_lag') + col('_temp_lead')) / 2
        ).otherwise(col('temperature_c')))
        .drop('_temp_lag', '_temp_lead')
        .withColumn('humidity_pct',
            when((col('humidity_pct') < 0) | (col('humidity_pct') > 100), None)
            .otherwise(col('humidity_pct')))
        .withColumn('soil_nitrogen_mg_kg',
            when(col('soil_nitrogen_mg_kg') < 0, None)
            .otherwise(col('soil_nitrogen_mg_kg')))
        .withColumn('soil_phosphorus_mg_kg',
            when(col('soil_phosphorus_mg_kg') < 0, None)
            .otherwise(col('soil_phosphorus_mg_kg')))
        .withColumn('soil_potassium_mg_kg',
            when(col('soil_potassium_mg_kg') < 0, None)
            .otherwise(col('soil_potassium_mg_kg')))
        .withColumn('plant_growth_pct',
            when((col('plant_growth_pct') < 0) | (col('plant_growth_pct') > 100), None)
            .otherwise(col('plant_growth_pct')))
        .dropDuplicates(['sensor_id', 'timestamp']))


def enrich(df):
    """Ajoute stress_score, plant_stage, soil_fertility_score, alerts."""
    stress_udf    = udf(compute_stress_score,        DoubleType())
    stage_udf     = udf(compute_plant_stage,          StringType())
    fertility_udf = udf(compute_soil_fertility_score, DoubleType())
    alerts_udf    = udf(compute_alerts,               ArrayType(StringType()))

    return (df
        .withColumn('stress_score',
            stress_udf(col('humidity_pct'), col('soil_nitrogen_mg_kg')))
        .withColumn('plant_stage',
            stage_udf(col('plant_growth_pct'), col('phase')))
        .withColumn('soil_fertility_score',
            fertility_udf(col('soil_nitrogen_mg_kg'),
                          col('soil_phosphorus_mg_kg'),
                          col('soil_potassium_mg_kg')))
        .withColumn('alerts',
            alerts_udf(col('humidity_pct'), col('soil_nitrogen_mg_kg'),
                       col('stress_score'), col('plant_growth_pct'))))


def add_partition_columns(df):
    """Extrait year/month/day/hour depuis timestamp pour le partitionnement Hive."""
    ts = to_timestamp(col('timestamp'), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    return (df
        .withColumn('year',  year(ts))
        .withColumn('month', month(ts))
        .withColumn('day',   dayofmonth(ts))
        .withColumn('hour',  hour(ts)))


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    print('[Spark] Lecture de smart-farm-raw...')
    df = spark.read.parquet(f's3a://{BUCKET_RAW}/')
    print(f'[Spark] {df.count()} lignes lues')

    valid_df, quarantine_df = validate(df)
    q_count = quarantine_df.count()
    if q_count > 0:
        print(f'[Spark] {q_count} lignes → smart-farm-quarantine')
        quarantine_df.write.mode('append').parquet(f's3a://{BUCKET_QUARANTINE}/')

    enriched_df = enrich(clean(valid_df))
    partitioned_df = add_partition_columns(enriched_df)

    print('[Spark] Écriture dans smart-farm-clean...')
    (partitioned_df.write
        .mode('append')
        .partitionBy('year', 'month', 'day', 'hour')
        .parquet(f's3a://{BUCKET_CLEAN}/'))

    print(f'[Spark] {partitioned_df.count()} lignes écrites dans smart-farm-clean')
    spark.stop()


if __name__ == '__main__':
    main()
