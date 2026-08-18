from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def success_task():
    print(">>> SUCCESS TASK")


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 007")


def traverse_downstream(task, visited=None):
    """
    Percorre recursivamente todos os downstreams
    a partir de uma task.
    """

    if visited is None:
        visited = set()

    if task.task_id in visited:
        return

    visited.add(task.task_id)

    print(f"VISITANDO: {task.task_id}")

    for downstream_task_id in task.downstream_task_ids:
        downstream_task = task.dag.get_task(
            downstream_task_id
        )

        traverse_downstream(
            downstream_task,
            visited
        )


def inspect_traversal(context):
    failed_task = context["task"]

    print("\n========== REAL DAG TRAVERSAL ==========")

    print("\n--- STARTING TASK ---")
    print(failed_task.task_id)

    print("\n--- TRAVERSAL ---")

    visited = set()

    traverse_downstream(
        failed_task,
        visited
    )

    print("\n--- VISITED TASKS ---")
    print(visited)

    print("\n--- COUNT ---")
    print(len(visited))

    print("\n========================================")


with DAG(
    dag_id="spike_dag_real_traversal",
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
    