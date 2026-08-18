from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def failing_task():
    print(">>> TASK EXECUTANDO")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 002")


def inspect_context(context):
    print("\n========== CONTEXT EXPLORATION ==========")

    print("\n--- CONTEXT KEYS ---")

    for key in sorted(context.keys()):
        print(f"{key}: {type(context[key]).__name__}")

    print("\n--- TASK INSTANCE ---")

    ti = context.get("task_instance")

    if ti:
        print("dag_id:", ti.dag_id)
        print("task_id:", ti.task_id)
        print("state:", ti.state)
        print("try_number:", ti.try_number)
        print("run_id:", ti.run_id)
        print("execution_date:", ti.execution_date)
        print("start_date:", ti.start_date)
        print("end_date:", ti.end_date)

    print("\n--- DAG ---")

    dag = context.get("dag")

    if dag:
        print("dag_id:", dag.dag_id)
        print("start_date:", dag.start_date)

    print("\n--- EXCEPTION ---")

    print("exception:", repr(context.get("exception")))

    print("\n==========================================\n")


with DAG(
    dag_id="spike_context_exploration",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_failure = PythonOperator(
        task_id="test_failure",
        python_callable=failing_task,
        on_failure_callback=inspect_context,
    )
