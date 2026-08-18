from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def success_task():
    print(">>> SUCCESS TASK")


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 005")


def inspect_graph(context):
    dag = context["dag"]
    task = context["task"]

    print("\n========== DAG GRAPH EXPLORATION ==========")

    print("\n--- DAG TASKS ---")
    print(f"task_ids: {dag.task_ids}")

    print("\n--- FAILED TASK ---")
    print(f"task_id: {task.task_id}")

    print("\n--- UPSTREAM ---")
    print(f"upstream_task_ids: {task.upstream_task_ids}")

    print("\n--- DOWNSTREAM ---")
    print(f"downstream_task_ids: {task.downstream_task_ids}")

    print("\n--- DIRECT TASK OBJECTS ---")

    for task_id in task.upstream_task_ids:
        upstream_task = dag.get_task(task_id)
        print(
            f"UPSTREAM: {upstream_task.task_id} "
            f"| type: {type(upstream_task).__name__}"
        )

    for task_id in task.downstream_task_ids:
        downstream_task = dag.get_task(task_id)
        print(
            f"DOWNSTREAM: {downstream_task.task_id} "
            f"| type: {type(downstream_task).__name__}"
        )

    print("\n============================================")


with DAG(
    dag_id="spike_dag_graph",
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
        on_failure_callback=inspect_graph,
    )

    downstream_1 = PythonOperator(
        task_id="downstream_1",
        python_callable=success_task,
    )

    downstream_2 = PythonOperator(
        task_id="downstream_2",
        python_callable=success_task,
    )

    start >> failure >> [downstream_1, downstream_2]
    