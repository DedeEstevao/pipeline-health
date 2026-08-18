from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def failing_task():
    print(">>> TASK EXECUTANDO")
    raise ValueError("ERRO INTENCIONAL DA SPIKE")


def on_failure(context):
    print("\n========== FAILURE CALLBACK ==========")

    dag = context.get("dag")
    task_instance = context.get("task_instance")
    exception = context.get("exception")

    print("DAG:", dag.dag_id if dag else None)
    print("TASK:", task_instance.task_id if task_instance else None)
    print("STATE:", task_instance.state if task_instance else None)
    print("TRY NUMBER:", task_instance.try_number if task_instance else None)
    print("EXCEPTION:", repr(exception))

    print("======================================\n")


def on_retry(context):
    print("\n========== RETRY CALLBACK ==========")

    task_instance = context.get("task_instance")
    exception = context.get("exception")

    print("DAG:", task_instance.dag_id)
    print("TASK:", task_instance.task_id)
    print("STATE:", task_instance.state)
    print("TRY NUMBER:", task_instance.try_number)
    print("EXCEPTION:", repr(exception))

    print("====================================\n")


with DAG(
    dag_id="spike_callback_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    on_failure_callback=on_failure,
) as dag:

    test_failure = PythonOperator(
        task_id="test_failure",
        python_callable=failing_task,
        retries=2,
        on_retry_callback=on_retry,
        on_failure_callback=on_failure,
    )
    