from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


def start_task():
    print(">>> START")


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 011")


def downstream_task():
    print(">>> DOWNSTREAM EXECUTANDO")


def audit_dag(context):
    """
    Executa depois que todas as upstreams terminarem.
    Observa o estado real das TaskInstances no DAG Run.
    """

    print("\n========== FINAL STATE AUDIT ==========")

    dag_run = context["dag_run"]
    dag = context["dag"]

    print(f"\nDAG RUN: {dag_run.run_id}")
    print(f"STATE: {dag_run.state}")

    print("\n--- TASK INSTANCE STATES ---")

    task_instances = dag_run.get_task_instances()

    for ti in task_instances:
        task = dag.get_task(ti.task_id)

        print(
            f"{ti.task_id} | "
            f"state={ti.state} | "
            f"trigger_rule={task.trigger_rule}"
        )

    print("\n=======================================")


with DAG(
    dag_id="spike_real_states",
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

    audit = PythonOperator(
        task_id="audit",
        python_callable=audit_dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> failure

    failure >> [branch_a, branch_b]

    branch_a >> branch_a_2
    branch_b >> branch_b_2

    [branch_a_2, branch_b_2] >> final

    [
        failure,
        branch_a,
        branch_b,
        branch_a_2,
        branch_b_2,
        final,
    ] >> audit



