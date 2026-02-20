from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure src is in the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.extract import WeatherExtractor
from src.transform import WeatherTransformer
from src.load import WeatherLoader

# Configuration
LATITUDE = 40.7128 # New York
LONGITUDE = -74.0060
DB_URL = "postgresql+psycopg2://airflow:airflow@postgres/airflow"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_dist_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_etl_dag',
    default_args=default_args,
    description='A simple weather ETL daily pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['weather', 'etl'],
) as dag:

    def run_etl():
        # 1. Extraction
        extractor = WeatherExtractor(latitude=LATITUDE, longitude=LONGITUDE)
        raw_data = extractor.fetch_daily_weather()
        if not raw_data:
            raise Exception("Failed to extract data")

        # 2. Transformation
        transformer = WeatherTransformer()
        clean_df = transformer.transform(raw_data)
        if clean_df is None or clean_df.empty:
            raise Exception("Failed to transform data")

        # 3. Loading
        loader = WeatherLoader(DB_URL)
        loader.load_upsert(clean_df, "weather_metrics")

    etl_task = PythonOperator(
        task_id='execute_weather_etl',
        python_callable=run_etl,
    )

    etl_task
