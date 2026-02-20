import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import os

# Configuración de la página
st.set_page_config(page_title="Weather ETL Dashboard", layout="wide")

st.title("🌦️ Weather ETL Data Dashboard")
st.markdown("""
Esta aplicación visualiza los datos extraídos de la API de Open-Meteo y almacenados 
en la base de datos PostgreSQL.
""")

# Configuración de conexión (Ajustar según entorno)
# En Docker, el host sería 'postgres'. Para correr local, usamos el puerto 5433.
DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@127.0.0.1:5433/airflow?client_encoding=utf8")

@st.cache_data(ttl=600)
def load_data():
    conn = None
    try:
        engine = create_engine(DB_URL)
        query = "SELECT * FROM weather_metrics ORDER BY date DESC"
        # Obtenemos la conexión de forma manual para evitar fallos de protocolo de contexto
        conn = engine.raw_connection()
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        # Mostramos el error de forma que no rompa por temas de codificación
        st.error("⚠️ Error de conexión detectado.")
        st.code(repr(e))
        return None
    finally:
        if conn:
            conn.close()

# Cargar datos
df = load_data()

if df is not None:
    if not df.empty:
        # Métricas principales
        st.subheader("Métricas en Tiempo Real")
        col1, col2, col3 = st.columns(3)
        
        latest = df.iloc[0]
        col1.metric("Temp Max (Hoy)", f"{latest['temp_max']} °C")
        col2.metric("Temp Min (Hoy)", f"{latest['temp_min']} °C")
        col3.metric("Precipitación", f"{latest['precipitation']} mm")

        # Gráfico de Tendencias
        st.subheader("Tendencia de Temperaturas")
        fig = px.line(df, x="date", y=["temp_max", "temp_min"], 
                      labels={"value": "Temperatura (°C)", "date": "Fecha"},
                      title="Evolución de Temperatura Máxima y Mínima")
        st.plotly_chart(fig, use_container_width=True)

        # Tabla de Datos
        st.subheader("Datos Raw del ETL")
        st.dataframe(df, use_container_width=True)
        
    else:
        st.info("La tabla 'weather_metrics' está vacía. Ejecuta el DAG en Airflow para cargar datos.")
else:
    st.warning("No se pudo cargar la información. Asegúrate de que el contenedor de Postgres esté corriendo.")

st.sidebar.info("""
**Stack del Proyecto:**
- Python (Pandas + Requests)
- Airflow (Orquestación)
- PostgreSQL (Almacenamiento)
- Streamlit (Visualización)
""")
