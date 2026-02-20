import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WeatherLoader:
    """Handles data loading into PostgreSQL with Upsert strategy."""

    def __init__(self, db_url: str):
        self.engine: Engine = create_engine(db_url)

    def load_upsert(self, df: pd.DataFrame, table_name: str):
        """
        Loads data into PostgreSQL using an Upsert strategy.
        Prevents duplicates by checking (date, latitude, longitude) unique constraint.
        """
        if df is None or df.empty:
            logger.warning("No data to load.")
            return

        try:
            # 1. Create temporary table
            temp_table = f"temp_{table_name}"
            df.to_sql(temp_table, self.engine, if_exists="replace", index=False)

            # 2. SQL for Upsert (ON CONFLICT DO UPDATE)
            # Note: Requires a unique constraint on (date, latitude, longitude)
            upsert_query = f"""
            INSERT INTO {table_name} (date, temp_max, temp_min, precipitation, latitude, longitude, processed_at)
            SELECT date, temp_max, temp_min, precipitation, latitude, longitude, processed_at
            FROM {temp_table}
            ON CONFLICT (date, latitude, longitude) 
            DO UPDATE SET
                temp_max = EXCLUDED.temp_max,
                temp_min = EXCLUDED.temp_min,
                precipitation = EXCLUDED.precipitation,
                processed_at = EXCLUDED.processed_at;
            """
            
            with self.engine.begin() as conn:
                # Ensure the target table exists and has the constraint
                conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    date TIMESTAMP,
                    temp_max FLOAT,
                    temp_min FLOAT,
                    precipitation FLOAT,
                    latitude FLOAT,
                    longitude FLOAT,
                    processed_at TIMESTAMP,
                    PRIMARY KEY (date, latitude, longitude)
                );
                """))
                conn.execute(text(upsert_query))
                conn.execute(text(f"DROP TABLE {temp_table};"))

            logger.info(f"Successfully loaded {len(df)} rows into {table_name} (Upsert).")

        except Exception as e:
            logger.error(f"Loading error: {e}")

if __name__ == "__main__":
    # Example connection string (for local testing if DB is running)
    DB_URL = "postgresql+psycopg2://airflow:airflow@localhost/airflow"
    loader = WeatherLoader(DB_URL)
    # Testing logic would go here
