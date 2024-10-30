from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.decorators import task

## Location params
JAMMU = {# 'timezone':'IST',
            'timezone':'auto',
            'latitude':32.7357,
            'longitude':74.8691
            }


## Connection Names

# to set API HOST for HttpHook
API_CONN_ID = 'open_meteo_api'

# for test/dev using postgres on docker
POSTGRES_CONN_ID = 'postgres_docker'

# to be used to dump data into AWS RDS PostgreSQL
AWS_RDS_CONN_ID = 'aws_rds_default'

# to be used to dump data into Azure Database for PostgreSQL
AZ_POSTGRES_CONN_ID = 'az_postgresql_default'


## DAG default args
dag_default_args = {
    'owner':'airflow',
    'start_date':days_ago(n=1)
}


## API endpoint
# API_HOST = 'https://api.open-meteo.com/'
api_endpoint = f'/v1/forecast?latitude={JAMMU["latitude"]}&longitude={JAMMU["longitude"]}&current_weather=true'

# Directed Acyclic Graph (DAG)

with DAG(dag_id='jammu_weather',
            default_args=dag_default_args,
            schedule_interval='@hourly',
            catchup=False) as dags:
    
    @task()
    def extract_jammu_weather():
        pass
    
    @task()
    def transform_jammu_weather(extracted_jammu_weather_data):
        pass
    
    @task()
    def load_jammu_weather(transformed_jammu_weather_data):
        pass
    
    # DAG Workflow
    extracted_jammu_weather_data = extract_jammu_weather()
    transformed_jammu_weather_data = transform_jammu_weather(extracted_jammu_weather_data=extracted_jammu_weather_data)
    load_jammu_weather(transformed_jammu_weather_data=transformed_jammu_weather_data)
    