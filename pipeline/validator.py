"""
pipeline/validator.py
=====================
Post-extraction validation: verifies that the extracted OCPNModel is
structurally sound and warns about potential quality issues before metrics
are computed.

Validation checks (in order)
-----------------------------
V1  Non-empty model (at least 1 object, 1 activity, 1 relation)
V2  Relation integrity (every (a, o) in R has a ∈ A and o ∈ O)
V3  Isolated activities (activities linked to 0 object types)
V4  Isolated object types (objects linked to 0 activities)
V5  Single-node interaction graph (no inter-object connections possible)
V6  Fully disconnected interaction graph (OID = 0)
V7  Relation density sanity (AORC / (|A|·|O|) should be plausible)
V8  Duplicate relation check (should never occur given OCPNModel.relations is a set)

Each check produces a ValidationFinding with:
  level   : "ERROR" | "WARNING" | "INFO"
  code    : short identifier (V1, V2, …)
  message : human-readable explanation

A ValidationResult collects all findings and exposes:
  is_valid : bool — False if any ERROR-level finding exists
  summary  : concise printable text
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from core.model import OCPNModel


Level = Literal["ERROR", "WARNING", "INFO"]


@dataclass
class ValidationFinding:
    """One finding from a validation check."""
    level:   Level
    code:    str
    message: str

    def __str__(self) -> str:
        icon = {"ERROR": "✗", "WARNING": "⚠", "INFO": "ℹ"}[self.level]
        return f"  [{self.code}] {icon}  {self.message}"


@dataclass
class ValidationResult:
    """
    Collection of all findings for one OCPNModel.

    Attributes
    ----------
    model_name : str
    findings   : list[ValidationFinding]
    is_valid   : bool  — True iff no ERROR-level findings exist
    """
    model_name: str
    findings:   list[ValidationFinding] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not any(f.level == "ERROR" for f in self.findings)

    @property
    def errors(self) -> list[ValidationFinding]:
        return [f for f in self.findings if f.level == "ERROR"]

    @property
    def warnings(self) -> list[ValidationFinding]:
        return [f for f in self.findings if f.level == "WARNING"]

    def print_report(self, indent: str = "  ") -> None:
        status = "VALID ✓" if self.is_valid else "INVALID ✗"
        print(f"\n{indent}┌─ Validation Report [{status}] ─────────────────────────┐")
        if not self.findings:
            print(f"{indent}│  No issues found.")
        else:
            for f in self.findings:
                print(f"{indent}│ {f}")
        print(f"{indent}└───────────────────────────────────────────────────────┘")


class ModelValidator:
    """
    Stateless validator.  Call validate(model) to run all checks.

    Parameters
    ----------
    max_density_warning : float
        If AORC / (|A|·|O|) exceeds this value, emit a WARNING about
        potentially over-specified relations (default: 0.9).
    """

    def __init__(self, max_density_warning: float = 0.9) -> None:
        self.max_density_warning = max_density_warning

    def validate(self, model: OCPNModel) -> ValidationResult:
        """
        Run all validation checks on *model* and return a ValidationResult.
        """
        result = ValidationResult(model_name=model.name)
        add    = result.findings.append  # shorthand

        # V1 — Non-empty model
        if len(model.objects) == 0:
            add(ValidationFinding("ERROR", "V1",
                "Model has no object types (|O| = 0). Cannot compute meaningful metrics."))
        if len(model.activities) == 0:
            add(ValidationFinding("ERROR", "V1",
                "Model has no activities (|A| = 0). Cannot compute meaningful metrics."))
        if len(model.relations) == 0:
            add(ValidationFinding("ERROR", "V1",
                "Model has no relations (|R| = 0). All interaction metrics will be zero."))

        # V2 — Relation integrity (should be guaranteed by OCPNModel, but double-check)
        for act, obj in model.relations:
            if act not in model.activities:
                add(ValidationFinding("ERROR", "V2",
                    f"Relation references unknown activity '{act}'."))
            if obj not in model.objects:
                add(ValidationFinding("ERROR", "V2",
                    f"Relation references unknown object type '{obj}'."))

        # V3 — Isolated activities
        isolated_acts = [
            a for a in model.activities
            if len(model.activity_to_objects.get(a, set())) == 0
        ]
        if isolated_acts:
            add(ValidationFinding("WARNING", "V3",
                f"Activities with no relations (isolated): {isolated_acts[:5]}"
                + ("…" if len(isolated_acts) > 5 else "")))

        # V4 — Isolated object types
        isolated_objs = [
            o for o in model.objects
            if len(model.object_to_activities.get(o, set())) == 0
        ]
        if isolated_objs:
            add(ValidationFinding("WARNING", "V4",
                f"Object types with no relations (isolated): {isolated_objs[:5]}"
                + ("…" if len(isolated_objs) > 5 else "")))

        # V5 — Single object type: interaction graph metrics meaningless
        if len(model.objects) == 1:
            add(ValidationFinding("WARNING", "V5",
                "Only 1 object type — OID, OC, and DIAM will all be 0. "
                "Consider whether the model is complete."))

        # V6 — Fully disconnected interaction graph
        if (len(model.objects) >= 2
                and model.interaction_graph.number_of_edges() == 0):
            add(ValidationFinding("WARNING", "V6",
                "Object interaction graph has no edges (OID = 0). "
                "No two object types share any activity — "
                "the model may be too coarse or the data too sparse."))

        # V7 — Relation density sanity
        if len(model.activities) > 0 and len(model.objects) > 0:
            density = len(model.relations) / (len(model.activities) * len(model.objects))
            if density > self.max_density_warning:
                add(ValidationFinding("WARNING", "V7",
                    f"Relation density = {density:.2f} (AORC / (|A|·|O|) > "
                    f"{self.max_density_warning}). Every activity touches almost "
                    "every object type — check whether object types are too coarsely "
                    "defined."))
            else:
                add(ValidationFinding("INFO", "V7",
                    f"Relation density = {density:.2f} — within normal range."))

        # V8 — Single-activity model
        if len(model.activities) == 1:
            add(ValidationFinding("WARNING", "V8",
                "Model contains only 1 activity. SAR and SD will both be 0 or 1 "
                "with no gradation — complexity discrimination is limited."))

        # All clear
        if not result.findings:
            add(ValidationFinding("INFO", "V0",
                "Model passed all validation checks."))

        return result
