from airflow.sdk import dag, task, chain
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

@dag(
    dag_id = 'check_dag',
    schedule = '@daily',
    start_date = datetime(2025, 1, 1),
    description = 'DAG to check data',
    tags = ['data_engineering']
)
def check_dag():

    @task.bash
    def create_file():
        return 'echo "Hi there!" >/tmp/dummy'

    @task.bash
    def check_file():
        return 'test -f /tmp/dummy'

    @task
    def verify_file():
        print(open('/tmp/dummy', 'rb').read())

    create_file() >> check_file() >> verify_file()

check_dag()