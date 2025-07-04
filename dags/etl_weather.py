from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils import timezone

from datetime import datetime
import pandas as pd

# Constants
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': timezone.datetime(2025, 6, 30),
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['weather', 'ETL'],
) as dag:

    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')

        ## Build the API endpoint
        ## https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        ## Make the request via the HTTP Hook
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted data into a structured format."""
        current_weather = weather_data['current_weather']


        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'weather_code': current_weather['weathercode'],
            'wind_speed': current_weather['windspeed'],
            'wind_direction': current_weather['winddirection'],
                       
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        """Load the data into PostgreSQL."""
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                
                latitude VARCHAR(20),
                longitude VARCHAR(20),
                temperature FLOAT,
                weather_code INT,                
                wind_speed FLOAT,
                wind_direction FLOAT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                       
                  );
        """)

        cursor.execute("""
            INSERT INTO weather_data (
                latitude, longitude, temperature, weather_code, 
                wind_speed, wind_direction
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['weather_code'],
            transformed_data['wind_speed'],
            transformed_data['wind_direction'],
        ))

        conn.commit()
        cursor.close()

    # Define DAG flow
    raw_data = extract_weather_data()
    clean_data = transform_weather_data(raw_data)
    load_weather_data(clean_data)
