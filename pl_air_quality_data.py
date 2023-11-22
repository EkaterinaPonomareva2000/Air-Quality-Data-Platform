import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from datetime import datetime 
import requests
import json
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
import requests

################ Function to load data ################
def get_data():
    
    url = "https://api.openaq.org/v2/latest?limit=100&page=1&offset=0&sort=desc&radius=1000&order_by=lastUpdated&dump_raw=false"

    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.get(url, headers=headers)

    #parse data
    response_json = json.loads(response.text)
    unparsed_df = pd.json_normalize(response_json['results'])
    measurements = pd.json_normalize(response_json['results'],
                                    record_path='measurements',
                                    meta='location',
                                    record_prefix='measurements.',
                                    errors='ignore'
                                    )
    unparsed_df.drop(['measurements'], axis='columns', inplace=True)
    parsed_df = unparsed_df.merge(measurements, how='outer')

    #rename columns
    parsed_df.rename(columns={'location': 'location'
                            , 'city': 'city'
                            , 'country': 'country'
                            , 'coordinates.latitude': 'latitude'
                            , 'coordinates.longitude': 'longtitude'
                            , 'measurements.parameter': 'parameter'
                            , 'measurements.value': 'value'
                            , 'measurements.lastUpdated': 'lastUpdated'
                            , 'measurements.unit': 'unit'}, inplace=True)
    parsed_df['lastUpdated'] = parsed_df['lastUpdated'].apply(lambda x: datetime.strptime(x[:x.find('+')], '%Y-%m-%dT%H:%M:%S'))

    csv_buffer = StringIO()
    parsed_df.to_csv(csv_buffer, sep=',', mode='w', header=False, index=False)

    #get access to S3
    bucket_name = 'data-for-analytics-dev-project'

    session = boto3.session.Session()

    ENDPOINT = "https://storage.yandexcloud.net"

    session = boto3.Session(
        aws_access_key_id='YCAJEOEnDaD9P22_YnDHqIpT-',
        aws_secret_access_key='YCOA9zIJuNdNGr_nPM6lVLi4x9zT3z1Ujr5k35Kb',
        region_name="ru-central1",
    )

    s3 = session.client(
        "s3", endpoint_url=ENDPOINT)

    load_dttm = datetime.today().strftime('%Y%m%d%H%M%S')
    s3.put_object(Bucket=bucket_name,
                Key=f'air_quality_data/{load_dttm}/{load_dttm}.csv',
                Body=csv_buffer.getvalue(),
                StorageClass='COLD')

################ Function to load data ################

with DAG(
    dag_id='pl_air_quality_data',
    description='Extract data from API and load it to S3',
    schedule_interval='0 7 * * *',
    start_date=datetime(2023,11,21),
    catchup=False,
    max_active_runs=1,
    tags=['Air Quality']) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    get_air_quality_data = PythonOperator(
        task_id='get_air_quality_data',
        python_callable=get_data
    )

    send_good_message = TelegramOperator(
        task_id="send_good_message",
        telegram_conn_id='telegram_conn_id',
        chat_id='908288360',
        text="Data is ready for you!",
        trigger_rule='all_success',
        dag=dag
    )

    send_bad_message = TelegramOperator(
        task_id="send_bad_message",
        telegram_conn_id='telegram_conn_id',
        chat_id='908288360',
        text="Something went wrong!",
        trigger_rule='all_failed',
        dag=dag
    )

    start >> get_air_quality_data >> [send_good_message, send_bad_message]