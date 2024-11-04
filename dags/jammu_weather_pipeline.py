from decouple import config
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta

## Location params
JAMMU = {# 'timezone':'IST',
            'timezone':'auto',
            'latitude':32.7357,
            'longitude':74.8691
            }


## Connection Names

# to set API HOST for HttpHook
# API_CONN_ID = 'open_meteo_api'
API_CONN_ID = config('API_CONN')

# for test/dev using postgres on docker
POSTGRES_CONN_ID = config('POSTGRES_CONN')

# to be used to dump data into AWS RDS PostgreSQL
AWS_RDS_CONN_ID = config('AWS_RDS_CONN')

# to be used to dump data into Azure Database for PostgreSQL
AZ_POSTGRES_CONN_ID = config('AZ_POSTGRES_CONN')


## DAG default args
dag_default_args = {
    'owner':'airflow',
    'start_date':days_ago(n=1),
    'retries':3, # will try for upto 3 retries
    'retry_delay':timedelta(minutes=5) # will retry after 5min after failure/retry
}

# Directed Acyclic Graph (DAG)

with DAG(dag_id='jammu_weather',
            default_args=dag_default_args,
            schedule_interval='@hourly',
            catchup=False,) as dags:
    
    @task()
    def extract_jammu_weather():
        # Use HttpHook from airflow providers Http to get Http Connection
        http_hook = HttpHook(method='GET', http_conn_id=API_CONN_ID)
        
        # Weather API endpoint
        # API_HOST = 'https://api.open-meteo.com/'
        api_endpoint = f'/v1/forecast?latitude={JAMMU["latitude"]}&longitude={JAMMU["longitude"]}&timezone={JAMMU["timezone"]}&current_weather=true'
        
        # Perform request using HttpHook
        api_response = http_hook.run(endpoint=api_endpoint)
        
        if api_response.status_code == 200:
            return api_response.json()
        else:
            raise Exception(f'Unsuccessful extraction from weather data :\
                {api_response.status_code}')
    
    @task()
    def transform_jammu_weather(extracted):
        
        current_weather = extracted['current_weather']
        transformed = {
            'longitude': extracted['longitude'],
            'latitude': extracted['latitude'],
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
            'timezone': extracted['timezone_abbreviation'],
            'timestamp': current_weather['time']
        }
        return transformed
        
    
    @task()
    def load_jammu_weather(transform):

        # connections = [POSTGRES_CONN_ID, AWS_RDS_CONN_ID, AZ_POSTGRES_CONN_ID]
        # connections = [POSTGRES_CONN_ID, AZ_POSTGRES_CONN_ID]
        # connections = [POSTGRES_CONN_ID, AWS_RDS_CONN_ID]
        connections = [POSTGRES_CONN_ID]
        for conn_id in connections:
            # Use PostgreSQL hook from Airflow providers PostgresHook
            # to get PostgreSQL connection
            pg_hook = PostgresHook(postgres_conn_id=conn_id)
            
            # Create DB connection and cursor
            cnx = pg_hook.get_conn()
            crsr = cnx.cursor()
            
            # Create table if not exists
            query_str = """CREATE TABLE IF NOT EXISTS weather_jammu_hourly (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timezone VARCHAR(50),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );"""
            crsr.execute(query=query_str)
            
            # Insert transformed data into PostgreSQL table
            query_str = """INSERT INTO weather_jammu_hourly
                            (latitude, longitude, temperature, windspeed,
                            winddirection, weathercode, timezone, timestamp)
                        VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s)"""
            query_params = (transform['latitude'],
                            transform['longitude'],
                            transform['temperature'],
                            transform['windspeed'],
                            transform['winddirection'],
                            transform['weathercode'],
                            transform['timezone'],
                            transform['timestamp'])
            crsr.execute(query=query_str, vars=query_params)
            
            # Close DB connection and cursor
            cnx.commit()
            cnx.close()
        
    
    # DAG Workflow
    extracted_jammu_weather_data = extract_jammu_weather()
    transformed_jammu_weather_data = transform_jammu_weather(extracted=extracted_jammu_weather_data)
    load_jammu_weather(transform=transformed_jammu_weather_data)
    