from src.extract import WeatherExtractor
from src.transform import WeatherTransformer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_local_flow():
    """
    Runs a test of the Extraction and Transformation logic 
    without needing a Database or Airflow.
    """
    print("\n[START] Iniciando Prueba Local del Pipeline ETL...")
    
    # 1. Prueba de Extracción
    print("\n--- [1/2] Extraccion ---")
    extractor = WeatherExtractor(latitude=40.7128, longitude=-74.0060)
    raw_data = extractor.fetch_daily_weather()
    
    if raw_data:
        print("OK: Datos extraidos correctamente.")
        
        # 2. Prueba de Transformación
        print("\n--- [2/2] Transformacion ---")
        transformer = WeatherTransformer()
        df = transformer.transform(raw_data)
        
        if df is not None and not df.empty:
            print("OK: Datos transformados a Pandas DataFrame:")
            print(df.head())
            print(f"\nTotal de filas procesadas: {len(df)}")
            print("\n[SUCCESS] ¡La logica de Extraccion y Transformacion funciona perfectamente!")
        else:
            print("ERR: La transformacion fallo.")
    else:
        print("ERR: La extraccion fallo. Revisa tu conexion a internet.")

if __name__ == "__main__":
    test_local_flow()
