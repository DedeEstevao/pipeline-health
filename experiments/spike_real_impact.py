from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def start_task():
    print(">>> START")


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 009")


def downstream_task(name):
    print(f">>> EXECUTANDO {name}")


def analyze_impact(context):
    print("\n========== REAL IMPACT ANALYSIS ==========")

    ti = context["ti"]
    dag_run = context["dag_run"]
    dag = context["dag"]

    failed_task_id = ti.task_id

    print("\n--- FAILED TASK ---")
    print(f"task_id: {failed_task_id}")

    # --------------------------------------------------
    # 1. Descendentes diretos
    # --------------------------------------------------

    failed_task = dag.get_task(failed_task_id)

    direct_impact = failed_task.downstream_task_ids

    print("\n--- DIRECT IMPACT ---")
    print(direct_impact)

    # --------------------------------------------------
    # 2. Traversal para encontrar todos os descendentes
    # --------------------------------------------------

    visited = set()
    stack = list(direct_impact)

    while stack:
        task_id = stack.pop()

        if task_id in visited:
            continue

        visited.add(task_id)

        task = dag.get_task(task_id)

        for downstream_id in task.downstream_task_ids:
            if downstream_id not in visited:
                stack.append(downstream_id)

    print("\n--- POTENTIAL IMPACT ---")
    print(visited)

    # --------------------------------------------------
    # 3. Consultar estado REAL de cada task
    # --------------------------------------------------

    print("\n--- REAL EXECUTION STATE ---")

    real_impact = []

    for task_id in visited:

        task_instance = dag_run.get_task_instance(
            task_id=task_id
        )

        state = task_instance.state if task_instance else None

        task = dag.get_task(task_id)

        print(
            f"{task_id} | "
            f"state={state} | "
            f"trigger_rule={task.trigger_rule}"
        )

        if state in ["upstream_failed", "failed", "skipped"]:
            real_impact.append(task_id)

    # --------------------------------------------------
    # 4. Resultado
    # --------------------------------------------------

    print("\n--- IMPACT SUMMARY ---")

    print(f"Potential impact: {len(visited)}")
    print(f"Real impact:      {len(real_impact)}")

    print(f"\nReal impacted tasks: {real_impact}")

    print("============================================")


with DAG(
    dag_id="spike_real_impact",
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
        python_callable=lambda: downstream_task("BRANCH A"),
    )

    branch_b = PythonOperator(
        task_id="branch_b",
        python_callable=lambda: downstream_task("BRANCH B"),
    )

    branch_a_2 = PythonOperator(
        task_id="branch_a_2",
        python_callable=lambda: downstream_task("BRANCH A 2"),
    )

    branch_b_2 = PythonOperator(
        task_id="branch_b_2",
        python_callable=lambda: downstream_task("BRANCH B 2"),
    )

    final = PythonOperator(
        task_id="final",
        python_callable=lambda: downstream_task("FINAL"),
    )

    start >> failure

    failure >> branch_a >> branch_a_2 >> final
    failure >> branch_b >> branch_b_2 >> final

