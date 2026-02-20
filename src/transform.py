import pandas as pd
import logging
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WeatherTransformer:
    """Handles data transformation and cleaning using Pandas."""

    def transform(self, raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Processes raw JSON weather data into a cleaned DataFrame.
        """
        try:
            if not raw_data or "daily" not in raw_data:
                logger.warning("No daily data found in raw response.")
                return None

            daily_data = raw_data["daily"]
            df = pd.DataFrame(daily_data)

            # 1. Rename columns for clarity
            df = df.rename(columns={
                "time": "date",
                "temperature_2m_max": "temp_max",
                "temperature_2m_min": "temp_min",
                "precipitation_sum": "precipitation"
            })

            # 2. Type normalization
            df["date"] = pd.to_datetime(df["date"])
            df["temp_max"] = df["temp_max"].astype(float)
            df["temp_min"] = df["temp_min"].astype(float)
            df["precipitation"] = df["precipitation"].astype(float)

            # 3. Handle nulls (optional based on requirements)
            # In weather data, we might want to fill with 0 or drop
            df = df.dropna(subset=["date", "temp_max", "temp_min"])

            # 4. Add metadata
            df["latitude"] = raw_data.get("latitude")
            df["longitude"] = raw_data.get("longitude")
            df["processed_at"] = pd.Timestamp.now(tz="UTC")

            logger.info(f"Transformation complete. Rows processed: {len(df)}")
            return df

        except Exception as e:
            logger.error(f"Transformation error: {e}")
            return None

if __name__ == "__main__":
    # Mock data for testing
    mock_data = {
        "latitude": 40.71,
        "longitude": -74.0,
        "daily": {
            "time": ["2024-01-01", "2024-01-02"],
            "temperature_2m_max": [5.0, 6.2],
            "temperature_2m_min": [-1.0, 0.5],
            "precipitation_sum": [0.0, 1.2]
        }
    }
    transformer = WeatherTransformer()
    df_clean = transformer.transform(mock_data)
    if df_clean is not None:
        print(df_clean.head())
