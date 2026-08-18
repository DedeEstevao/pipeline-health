from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 013")


def analyze_impact(**context):

    dag_run = context.get("dag_run")

    if not dag_run:
        print("DagRun não encontrado")
        return

    print("\n========== AIRFLOW IMPACT ANALYZER ==========")

    task_instances = dag_run.get_task_instances()

    print("\n--- DAG STATE ---")

    for ti in task_instances:
        print(
            f"{ti.task_id:15} | "
            f"state={ti.state}"
        )

    print("\n--- ANALYSIS ---")

    for ti in task_instances:

        task = dag_run.dag.get_task(ti.task_id)

        print("\n--------------------------------")
        print(f"TASK: {ti.task_id}")
        print(f"STATE: {ti.state}")
        print(f"TRIGGER RULE: {task.trigger_rule}")
        print(f"UPSTREAM: {task.upstream_task_ids}")
        print(f"DOWNSTREAM: {task.downstream_task_ids}")

    print("\n=============================================")


with DAG(
    dag_id="spike_impact_analyzer",
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
        trigger_rule="all_done",
    )

    analyze = PythonOperator(
        task_id="analyze",
        python_callable=analyze_impact,
        trigger_rule="all_done",
    )

    start >> failure

    failure >> branch_a >> branch_a_2
    failure >> branch_b >> branch_b_2

    [branch_a_2, branch_b_2] >> final

    final >> analyze

