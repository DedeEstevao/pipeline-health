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

    # -------------------------------------------------
    # 1. Recupera todas as TaskInstances da execução
    # -------------------------------------------------

    task_instances = dag_run.get_task_instances()

    # Cria um acesso rápido:
    # task_id -> TaskInstance
    task_instance_map = {
        ti.task_id: ti
        for ti in task_instances
    }

    # -------------------------------------------------
    # 2. Identifica as tasks que realmente falharam
    # -------------------------------------------------

    failed_tasks = [
        ti for ti in task_instances
        if ti.state == "failed"
    ]

    print("\n--- ROOT FAILURES ---")

    for ti in failed_tasks:
        print(
            f"{ti.task_id} | "
            f"state={ti.state}"
        )

    # -------------------------------------------------
    # 3. Analisa cada falha
    # -------------------------------------------------

    for failed_ti in failed_tasks:

        failed_task = dag_run.dag.get_task(
            failed_ti.task_id
        )

        print("\n========================================")
        print(
            f"ROOT FAILURE: "
            f"{failed_task.task_id}"
        )
        print("========================================")

        direct_impact = []
        propagated_impact = []
        executed_despite_failure = []

        # -------------------------------------------------
        # 4. Percorre todos os downstream
        # -------------------------------------------------

        downstream_tasks = (
            failed_task.get_flat_relatives(
                upstream=False
            )
        )

        for task in downstream_tasks:

            # O próprio analyzer não entra na análise
            if task.task_id == "analyze":
                continue

            ti = task_instance_map.get(
                task.task_id
            )

            if not ti:
                continue

            # -------------------------------------------------
            # 5. Classifica pelo estado real
            # -------------------------------------------------

            if ti.state == "upstream_failed":

                # Diretamente dependente da falha
                if failed_task.task_id in task.upstream_task_ids:

                    direct_impact.append(task.task_id)

                # Mais distante da falha
                else:

                    propagated_impact.append(task.task_id)

            elif ti.state == "success":

                executed_despite_failure.append(
                    task.task_id
                )

        # -------------------------------------------------
        # 6. Resultado
        # -------------------------------------------------

        print("\n--- DIRECT IMPACT ---")

        for task_id in direct_impact:
            print(
                f"{task_id} → upstream_failed"
            )

        print("\n--- PROPAGATED IMPACT ---")

        for task_id in propagated_impact:
            print(
                f"{task_id} → upstream_failed"
            )

        print(
            "\n--- EXECUTED DESPITE FAILURE ---"
        )

        for task_id in executed_despite_failure:
            print(
                f"{task_id} → success"
            )

        print("\n--- SUMMARY ---")

        print(
            f"Root failure: "
            f"{failed_task.task_id}"
        )

        print(
            f"Direct impact: "
            f"{len(direct_impact)}"
        )

        print(
            f"Propagated impact: "
            f"{len(propagated_impact)}"
        )

        print(
            f"Executed despite failure: "
            f"{len(executed_despite_failure)}"
        )

        print(
            f"Real impact: "
            f"{len(direct_impact) + len(propagated_impact)}"
        )

    print(
        "\n============================================="
    )

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

