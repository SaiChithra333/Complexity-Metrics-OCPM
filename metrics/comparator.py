"""
metrics/comparator.py
=====================
Utility for comparing multiple OCPNModel instances side-by-side.

Produces:
  - a comparison table printed to stdout
  - a ranked list by composite complexity
  - optional: pass results to plotter for visual comparison
"""

from __future__ import annotations
from typing import Optional

from core.model import OCPNModel
from metrics.complexity import ComplexityMetrics, MetricResult


class ModelComparator:
    """
    Compare complexity metrics across a collection of OCPNModels.

    Parameters
    ----------
    models : list[OCPNModel]
        Models to compare.
    weights : dict, optional
        Shared weight dictionary forwarded to each ComplexityMetrics instance.
    """

    def __init__(
        self,
        models: list[OCPNModel],
        weights: Optional[dict] = None,
    ) -> None:
        self.models  = models
        self.weights = weights
        self.results: list[MetricResult] = []

    def run(self) -> list[MetricResult]:
        """Compute metrics for every model and cache the results."""
        self.results = [
            ComplexityMetrics(m, weights=self.weights).compute_all()
            for m in self.models
        ]
        return self.results

    def print_comparison_table(self) -> None:
        """Print a side-by-side ASCII comparison table."""
        if not self.results:
            self.run()

        # Core metrics: structural, interaction, distribution, composite
        metrics = [
            # ── Structural ────────────────────────────────────
            ("OTC",        "otc"),
            ("AORC",       "aorc"),
            ("OID",        "oid"),
            # ── Interaction ────────────────────────────────────
            ("SAR",        "sar"),
            ("AFO",        "afo"),
            # ── Distribution ───────────────────────────────────
            ("RE",         "re"),
            ("OC",         "oc"),
            # ── Composite ──────────────────────────────────────
            ("Composite",  "composite"),
        ]

        col_w   = 14
        label_w = 12
        names   = [r.model_name[:col_w] for r in self.results]
        sep     = "─" * (label_w + col_w * len(self.results) + 3)

        print(f"\n{'═' * len(sep)}")
        print("  MODEL COMPARISON TABLE")
        print(f"{'═' * len(sep)}")
        header = f"  {'Metric':<{label_w}}" + "".join(f"{n:>{col_w}}" for n in names)
        print(header)
        print(f"  {sep}")

        divider_after = {"oc", "composite"}  # visual section breaks
        for label, attr in metrics:
            row = f"  {label:<{label_w}}"
            for r in self.results:
                val = getattr(r, attr)
                row += f"{val:>{col_w}.4f}" if isinstance(val, float) else f"{val:>{col_w}}"
            print(row)
            if attr in divider_after:
                print(f"  {'· ' * (len(sep) // 2)}")

        print(f"  {sep}")

        # Ranking by composite
        ranked = sorted(self.results, key=lambda r: r.composite, reverse=True)
        print("\n  Ranking by Composite Complexity (highest → most complex):")
        for i, r in enumerate(ranked, 1):
            print(f"    {i}. {r.model_name}  →  C(G) = {r.composite:.4f}")
        print()
