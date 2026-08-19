from dataclasses import dataclass, field


@dataclass
class ImpactAnalysisResult:
    root_failures: list[str] = field(default_factory=list)
    direct_impact: list[str] = field(default_factory=list)
    propagated_impact: list[str] = field(default_factory=list)
    executed_despite_failure: list[str] = field(default_factory=list)

