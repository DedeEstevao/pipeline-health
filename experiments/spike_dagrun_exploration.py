from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def success_task():
    print(">>> SUCCESS TASK")


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 003")


def inspect_dagrun(context):
    print("\n========== DAGRUN EXPLORATION ==========")

    dag_run = context.get("dag_run")

    print("\n--- DAGRUN ---")

    if dag_run:
        print("dag_id:", dag_run.dag_id)
        print("run_id:", dag_run.run_id)
        print("state:", dag_run.state)
        print("logical_date:", dag_run.logical_date)
        print("start_date:", dag_run.start_date)
        print("end_date:", dag_run.end_date)

    print("\n--- TASK INSTANCES ---")

    if dag_run:
        task_instances = dag_run.get_task_instances()

        for ti in task_instances:
            print(
                "task_id:",
                ti.task_id,
                "| state:",
                ti.state,
                "| try_number:",
                ti.try_number,
            )

    print("\n========================================\n")


with DAG(
    dag_id="spike_dagrun_exploration",
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
        on_failure_callback=inspect_dagrun,
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
    