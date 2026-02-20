import requests
import logging
from typing import Dict, Any, Optional
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WeatherExtractor:
    """Handles data extraction from Open-Meteo API."""
    
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, latitude: float, longitude: float):
        self.latitude = latitude
        self.longitude = longitude

    def fetch_daily_weather(self) -> Optional[Dict[str, Any]]:
        """
        Fetches daily weather forecast.
        Includes error handling and basic retry logic.
        """
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
            "timezone": "UTC"
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching weather data for lat={self.latitude}, lon={self.longitude} (Attempt {attempt+1})")
                response = requests.get(self.BASE_URL, params=params, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.critical("Failed to fetch data after several attempts.")
                    return None
        return None

if __name__ == "__main__":
    # Example usage
    extractor = WeatherExtractor(latitude=40.7128, longitude=-74.0060) # NYC
    data = extractor.fetch_daily_weather()
    if data:
        print(f"Success! Data received for: {data.get('latitude')}, {data.get('longitude')}")
