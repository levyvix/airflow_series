from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="first_dag", schedule=None, start_date=datetime(2024, 5, 1)) as dag:
    e = EmptyOperator(task_id="extract")

    t = EmptyOperator(task_id="transform")

    l = EmptyOperator(task_id="load")

    e >> t >> l
