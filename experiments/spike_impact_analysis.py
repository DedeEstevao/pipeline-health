from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def failing_task():
    print(">>> FAILURE TASK")
    raise ValueError("ERRO INTENCIONAL DA SPIKE 008")


def analyze_impact(context):
    print("\n========== IMPACT ANALYSIS ==========")

    ti = context.get("task_instance")

    if not ti:
        print("TaskInstance não encontrada no context")
        return

    failed_task = ti.task

    print("\n--- FAILED TASK ---")
    print(f"task_id: {failed_task.task_id}")

    # --------------------------------------------------
    # 1. IMPACTO DIRETO
    # --------------------------------------------------

    direct_downstream = failed_task.downstream_task_ids

    print("\n--- DIRECT IMPACT ---")
    print(direct_downstream)

    # --------------------------------------------------
    # 2. TRAVERSAL DO GRAFO
    # --------------------------------------------------

    visited = set()
    queue = list(direct_downstream)
    dag = context.get("dag")

    while queue:

       task_id = queue.pop(0)

       if task_id in visited:
           continue

       visited.add(task_id)

       task = dag.get_task(task_id)

       for downstream_id in task.downstream_task_ids:
           if downstream_id not in visited:
               queue.append(downstream_id)

    # --------------------------------------------------
    # 3. IMPACTO INDIRETO
    # --------------------------------------------------

    indirect_impact = visited - direct_downstream

    print("\n--- INDIRECT IMPACT ---")
    print(indirect_impact)

    # --------------------------------------------------
    # 4. RESULTADO FINAL
    # --------------------------------------------------

    impact_analysis = {
        "failed_task": failed_task.task_id,
        "direct_impact": list(direct_downstream),
        "indirect_impact": list(indirect_impact),
        "total_impact": len(visited),
    }

    print("\n--- IMPACT ANALYSIS RESULT ---")
    print(impact_analysis)

    print("\n====================================")


with DAG(
    dag_id="spike_impact_analysis",
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
        on_failure_callback=analyze_impact,
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

    start >> failure

    failure >> branch_a >> branch_a_2 >> final
    failure >> branch_b >> branch_b_2 >> final

