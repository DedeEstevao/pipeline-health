from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline_health.impact_analyzer import analyze_impact


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA INTEGRATION DAG")


def run_impact_analysis(**context):
    dag_run = context["dag_run"]

    result = analyze_impact(dag_run)

    print("\n========== PIPELINE HEALTH RESULT ==========")

    print(f"Root failures: {result['root_failures']}")
    print(f"Direct impact: {result['direct_impact']}")
    print(f"Propagated impact: {result['propagated_impact']}")
    print(
        "Executed despite failure: "
        f"{result['executed_despite_failure']}"
    )

    print("============================================")


with DAG(
    dag_id="integration_impact_analyzer",
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
        python_callable=run_impact_analysis,
        trigger_rule="all_done",
    )

    start >> failure

    failure >> branch_a >> branch_a_2
    failure >> branch_b >> branch_b_2

    [branch_a_2, branch_b_2] >> final

    final >> analyze

