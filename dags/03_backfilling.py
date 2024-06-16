from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="03_backfilling",
    start_date=datetime(2023, 12, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task1 = EmptyOperator(task_id="first_task")
    task2 = EmptyOperator(task_id="second_task")
    task3 = EmptyOperator(task_id="third_task")

    task1 >> [task2, task3]
