from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 012")


def analyze_combined(context):
    print("\n========== COMBINED ANALYSIS ==========")

    ti = context.get("task_instance")
    dag_run = context.get("dag_run")

    if not ti or not dag_run:
        print("TaskInstance ou DagRun não encontrado")
        return

    failed_task = ti.task

    print("\n--- FAILED TASK ---")
    print(f"task_id: {failed_task.task_id}")
    print(f"state: {ti.state}")
    print(f"trigger_rule: {failed_task.trigger_rule}")

    print("\n--- DOWNSTREAM ANALYSIS ---")

    downstream_tasks = failed_task.get_flat_relatives(upstream=False)

    for task in downstream_tasks:

        task_instance = dag_run.get_task_instance(
            task_id=task.task_id
        )

        print("\n--------------------------------")
        print(f"task_id: {task.task_id}")

        print(f"trigger_rule: {task.trigger_rule}")

        print(
            f"upstream_task_ids: "
            f"{task.upstream_task_ids}"
        )

        if task_instance:
            print(
                f"real_state: "
                f"{task_instance.state}"
            )
        else:
            print("real_state: TaskInstance não encontrada")

    print("\n========================================")


with DAG(
    dag_id="spike_combined_analysis",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print(">>> START"),
    )

    failure = PythonOperator(
        task_id="failure",
        python_callable=failing_task,
        on_failure_callback=analyze_combined,
    )

    branch_a = PythonOperator(
        task_id="branch_a",
        python_callable=lambda: print(">>> BRANCH A"),
    )

    branch_a_2 = PythonOperator(
        task_id="branch_a_2",
        python_callable=lambda: print(">>> BRANCH A 2"),
    )

    branch_b = PythonOperator(
        task_id="branch_b",
        python_callable=lambda: print(">>> BRANCH B"),
    )

    branch_b_2 = PythonOperator(
        task_id="branch_b_2",
        python_callable=lambda: print(">>> BRANCH B 2"),
    )

    final = PythonOperator(
        task_id="final",
        python_callable=lambda: print(">>> FINAL"),
    )

    audit = PythonOperator(
        task_id="audit",
        python_callable=lambda: print(">>> AUDIT"),
    )

    start >> failure

    failure >> branch_a >> branch_a_2
    failure >> branch_b >> branch_b_2

    [branch_a_2, branch_b_2] >> final

    final >> audit

