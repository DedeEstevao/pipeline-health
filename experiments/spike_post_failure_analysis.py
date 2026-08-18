
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 013")


def analyze_combined(**context):

    dag_run = context.get("dag_run")

    if not dag_run:
        print("DagRun não encontrado")
        return

    print("\n========== DAG STATE ANALYSIS ==========")

    for ti in dag_run.get_task_instances():

        print(
            f"{ti.task_id} | "
            f"state={ti.state}"
        )

    print("========================================")


with DAG(
    dag_id="spike_post_failure_analysis",
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
    )

    analyze = PythonOperator(
        task_id="analyze",
        python_callable=analyze_combined,
        trigger_rule="all_done",
    )

    start >> failure

    failure >> branch_a >> branch_a_2
    failure >> branch_b >> branch_b_2

    [branch_a_2, branch_b_2] >> final

    final >> analyze


