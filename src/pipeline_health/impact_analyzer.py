from pipeline_health.models import ImpactAnalysisResult, TaskImpact


def analyze_impact(dag_run):
    """
    Analisa o impacto de falhas em uma execução de DAG.
    """

    task_instances = dag_run.get_task_instances()

    task_instance_map = {
        ti.task_id: ti
        for ti in task_instances
    }

    failed_tasks = [
        ti
        for ti in task_instances
        if ti.state == "failed"
    ]

    result = ImpactAnalysisResult()

    for failed_ti in failed_tasks:

        failed_task = dag_run.dag.get_task(
            failed_ti.task_id
        )

        result.root_failures.append(
            failed_task.task_id
        )

        downstream_tasks = (
            failed_task.get_flat_relatives(
                upstream=False
            )
        )

        for task in downstream_tasks:

            ti = task_instance_map.get(
                task.task_id
            )

            if not ti:
                continue

            if ti.state == "upstream_failed":

                if failed_task.task_id in task.upstream_task_ids:

                    result.impacted_tasks.append(
                        TaskImpact(
                            task_id=task.task_id,
                            state=ti.state,
                            impact_type="direct",
                            cause=failed_task.task_id,
                            trigger_rule=task.trigger_rule,
                        )
                    )

                else:

                    result.impacted_tasks.append(
                        TaskImpact(
                            task_id=task.task_id,
                            state=ti.state,
                            impact_type="propagated",
                            cause=failed_task.task_id,
                            trigger_rule=task.trigger_rule,
                        )
                    )

            elif ti.state == "success":

                result.impacted_tasks.append(
                    TaskImpact(
                        task_id=task.task_id,
                        state=ti.state,
                        impact_type="executed_despite_failure",
                        cause=failed_task.task_id,
                        trigger_rule=task.trigger_rule,
                    )
                )

    return result

