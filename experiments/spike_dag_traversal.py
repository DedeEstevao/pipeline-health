from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def success_task():
    print(">>> SUCCESS TASK")


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 006")


def inspect_traversal(context):
    dag = context["dag"]
    failed_task = context["task"]

    print("\n========== DAG TRAVERSAL ==========")

    print("\n--- STARTING TASK ---")
    print(f"task_id: {failed_task.task_id}")

    print("\n--- DIRECT DOWNSTREAM ---")
    print(failed_task.downstream_task_ids)

    print("\n--- ALL DOWNSTREAM ---")

    downstream_tasks = dag.get_task_instances(
        state=None
    )

    for task_instance in downstream_tasks:
        print(
            f"task_id: {task_instance.task_id}"
        )

    print("\n--- DAG TASK IDS ---")
    print(dag.task_ids)

    print("\n===================================")


with DAG(
    dag_id="spike_dag_traversal",
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
        on_failure_callback=inspect_traversal,
    )

    branch_a = PythonOperator(
        task_id="branch_a",
        python_callable=success_task,
    )

    branch_a_2 = PythonOperator(
        task_id="branch_a_2",
        python_callable=success_task,
    )

    branch_b = PythonOperator(
        task_id="branch_b",
        python_callable=success_task,
    )

    branch_b_2 = PythonOperator(
        task_id="branch_b_2",
        python_callable=success_task,
    )

    final = PythonOperator(
        task_id="final",
        python_callable=success_task,
    )

    start >> failure

    failure >> branch_a >> branch_a_2
    failure >> branch_b >> branch_b_2

    [branch_a_2, branch_b_2] >> final
    