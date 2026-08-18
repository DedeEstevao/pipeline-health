from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def success_task():
    print(">>> SUCCESS TASK")


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 004")


with DAG(
    dag_id="spike_final_states",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=success_task,
    )

    failure = PythonOperator(
        task_id="failure",
        python_callable=failing_task,
    )

    downstream_1 = PythonOperator(
        task_id="downstream_1",
        python_callable=success_task,
    )

    downstream_2 = PythonOperator(
        task_id="downstream_2",
        python_callable=success_task,
    )

    start >> failure >> downstream_1 >> downstream_2
    