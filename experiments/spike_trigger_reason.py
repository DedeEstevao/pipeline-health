from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


def start_task():
    print(">>> START")


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 010")


def downstream_task():
    print(">>> DOWNSTREAM EXECUTANDO")


def analyze_impact(context):
    print("\n========== TRIGGER REASON ANALYSIS ==========")

    ti = context["ti"]
    dag = context["dag"]

    failed_task_id = ti.task_id

    print(f"\nFAILED TASK: {failed_task_id}")

    failed_task = dag.get_task(failed_task_id)

    print("\n--- DOWNSTREAM TASKS ---")

    for task_id in failed_task.downstream_task_ids:

        task = dag.get_task(task_id)

        print(f"\nTASK: {task.task_id}")
        print(f"trigger_rule: {task.trigger_rule}")

        print("upstream_task_ids:")
        print(task.upstream_task_ids)

        print("================================")


with DAG(
    dag_id="spike_trigger_reason",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_task,
    )

    failure = PythonOperator(
        task_id="failure",
        python_callable=failing_task,
        on_failure_callback=analyze_impact,
    )

    branch_a = PythonOperator(
        task_id="branch_a",
        python_callable=downstream_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    branch_b = PythonOperator(
        task_id="branch_b",
        python_callable=downstream_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    branch_a_2 = PythonOperator(
        task_id="branch_a_2",
        python_callable=downstream_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    branch_b_2 = PythonOperator(
        task_id="branch_b_2",
        python_callable=downstream_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    final = PythonOperator(
        task_id="final",
        python_callable=downstream_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    start >> failure

    failure >> [branch_a, branch_b]

    branch_a >> branch_a_2
    branch_b >> branch_b_2

    [branch_a_2, branch_b_2] >> final

