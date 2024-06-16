from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _primeira_funcao(posicao: str):
    print(f"hello {posicao} world")


with DAG(
    dag_id="02_python_operator", start_date=datetime(2024, 5, 1), schedule=None
) as dag:
    task1 = PythonOperator(
        task_id="primeira_funcao",
        python_callable=_primeira_funcao,
        # op_kwargs={"x", "levi"},
        op_args=["levi"],
    )

    task2 = PythonOperator(
        task_id="segunda_funcao",
        python_callable=_primeira_funcao,
        op_kwargs={"posicao": "levy"},
    )

    [task1, task2]
