from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _ingest(ti):
    import pandas as pd

    url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vendas-gasolina-c-m3-2020.csv"

    df = pd.read_csv(url, delimiter=";", encoding="utf-8")
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()

    df = df.assign(
        jan=lambda df: df["jan"].astype(str).str.replace(".", "").astype(float),
        fev=lambda df: df["fev"].astype(str).str.replace(".", "").astype(float),
        mar=lambda df: df["mar"].astype(str).str.replace(".", "").astype(float),
        abr=lambda df: df["abr"].astype(str).str.replace(".", "").astype(float),
        mai=lambda df: df["mai"].astype(str).str.replace(".", "").astype(float),
        jun=lambda df: df["jun"].astype(str).str.replace(".", "").astype(float),
        jul=lambda df: df["jul"].astype(str).str.replace(".", "").astype(float),
        ago=lambda df: df["ago"].astype(str).str.replace(".", "").astype(float),
        set=lambda df: df["set"].astype(str).str.replace(".", "").astype(float),
        out=lambda df: df["out"].astype(str).str.replace(".", "").astype(float),
        nov=lambda df: df["nov"].astype(str).str.replace(".", "").astype(float),
        dez=lambda df: df["dez"].astype(str).str.replace(".", "").astype(float),
        total=lambda df: df["total"].astype(str).str.replace(".", "").astype(float),
    )

    df_melted = df.melt(
        id_vars=["combustível", "ano", "região", "estado", "unidade", "total"],
        value_name="mes",
    ).assign(
        contribution=lambda df: df["mes"] / df["total"],
        contribution_pct=lambda df: (df["contribution"] / df["total"]) * 100,
    )

    max_contribution = df_melted["contribution_pct"].max()

    # min contribution without nan

    min_contribution = df_melted[df_melted["contribution_pct"].notna()][
        "contribution_pct"
    ].min()

    return {
        "max_contribution": max_contribution,
        "min_contribution": min_contribution,
    }


def _transform(ti):
    return_value = ti.xcom_pull(key="return_value", task_ids="task1")
    # min_contribution = ti.xcom_pull(key="min_contribution", task_ids="task1")

    max_contribution = return_value["max_contribution"]
    min_contribution = return_value["min_contribution"]

    print(f"Max contribution: {max_contribution}")
    print(f"Min contribution: {min_contribution}")


def _load():
    pass


with DAG(dag_id="04_xcom", start_date=datetime(2024, 5, 1), schedule=None) as dag:
    task1 = PythonOperator(task_id="task1", python_callable=_ingest)

    task2 = PythonOperator(task_id="task2", python_callable=_transform)

    # task3 = PythonOperator(task_id="task3", python_callable=_load)

    task1 >> task2
