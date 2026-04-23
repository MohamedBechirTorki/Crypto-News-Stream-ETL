from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure Airflow can find your modules
sys.path.append('/opt/airflow')

# Import your ETL logic
# Change 'run' to whatever your main function is named in each file
from modules.fetcher import run_fetcher
from modules.cleaner import run_cleaner
from modules.noise_filter import run_filter
from modules.enricher import run_enricher
from modules.window_features_agg import run_aggregator
from modules.storage import save_to_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_rss_etl_hourly',
    default_args=default_args,
    description='Crypto News ETL: Fetch -> Clean -> Filter -> Enrich -> Agg -> Store',
    schedule='@hourly',
    start_date=datetime(2026, 4, 1), # Adjusted to your current project timeline
    catchup=False,
    tags=['crypto', 'etl', 'rss'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_rss_data',
        python_callable=run_fetcher,
    )

    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=run_cleaner,
    )

    filter_task = PythonOperator(
        task_id='noise_filter',
        python_callable=run_filter,
    )

    enrich_task = PythonOperator(
        task_id='enrich_with_llm',
        python_callable=run_enricher,
    )

    agg_task = PythonOperator(
        task_id='window_features_aggregation',
        python_callable=run_aggregator,
    )

    storage_task = PythonOperator(
        task_id='save_to_storage',
        python_callable=save_to_db,
    )

    # The sequence: fetcher -> cleaner -> noise_filter -> enricher -> windows_features_agg -> storage
    fetch_task >> clean_task >> filter_task >> enrich_task >> agg_task >> storage_task