from dataclasses import dataclass, field


@dataclass
class TaskImpact:
    task_id: str
    state: str
    impact_type: str
    cause: str | None = None
    trigger_rule: str | None = None

@dataclass
class TaskDiagnosis:
    task_id: str
    message: str
    severity: str

@dataclass
class ImpactAnalysisResult:
    root_failures: list[str] = field(default_factory=list)
    impacted_tasks: list[TaskImpact] = field(default_factory=list)
    diagnoses: list[TaskDiagnosis] = field(default_factory=list)

