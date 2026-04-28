"""
pipeline/pipeline.py
====================
The OCPNPipeline class orchestrates the full end-to-end flow:

    Raw data
      │
      ▼
    EventParser.parse()          ← accepts list[dict], CSV, JSON, DataFrame
      │
      ▼
    DataCleaner.clean()          ← handles noise, nulls, duplicates, rare values
      │
      ▼
    ModelExtractor.extract()     ← derives O, A, R with support filtering
      │
      ▼
    ModelValidator.validate()    ← structural integrity checks
      │
      ▼
    ComplexityMetrics.compute_all()   ← all 11 metrics
      │
      ▼
    PipelineResult               ← unified result container

Usage (single dataset)
-----------------------
    pipeline = OCPNPipeline()
    result   = pipeline.run(raw_data, name="My Dataset")
    result.print_full_report()

Usage (multi-dataset comparison)
---------------------------------
    results = OCPNPipeline.run_many([
        (data_a, "Dataset A"),
        (data_b, "Dataset B"),
        (data_c, "Dataset C"),
    ])
    OCPNPipeline.compare(results)
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from pipeline.schema     import RawEventLog
from pipeline.parser     import EventParser
from pipeline.cleaner    import DataCleaner, CleaningReport
from pipeline.extractor  import ModelExtractor, ExtractionReport
from pipeline.validator  import ModelValidator, ValidationResult
from core.model          import OCPNModel
from metrics.complexity  import ComplexityMetrics, MetricResult
from metrics.comparator  import ModelComparator

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Unified result container
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class PipelineResult:
    """
    Holds every artefact produced by one pipeline run.

    Attributes
    ----------
    name             : dataset / model label
    raw_log          : parsed but uncleaned RawEventLog
    cleaned_log      : RawEventLog after cleaning
    model            : extracted OCPNModel
    cleaning_report  : CleaningReport from DataCleaner
    extraction_report: ExtractionReport from ModelExtractor
    validation_result: ValidationResult from ModelValidator
    metrics          : MetricResult from ComplexityMetrics
    elapsed_seconds  : wall-clock time for the whole pipeline run
    """
    name:               str
    raw_log:            RawEventLog
    cleaned_log:        RawEventLog
    model:              OCPNModel
    cleaning_report:    CleaningReport
    extraction_report:  ExtractionReport
    validation_result:  ValidationResult
    metrics:            MetricResult
    elapsed_seconds:    float = 0.0

    # ── Report helpers ────────────────────────────────────────────────────

    def print_full_report(self) -> None:
        """Print the complete pipeline report to stdout."""
        sep = "═" * 64
        print(f"\n{sep}")
        print(f"  PIPELINE REPORT  ▸  {self.name}")
        print(f"  Completed in {self.elapsed_seconds:.2f}s")
        print(sep)

        # Raw log stats
        print(f"\n  Raw log summary:")
        print(f"    {self.raw_log.summary().replace(chr(10), chr(10)+'    ')}")

        # Cleaning
        self.cleaning_report.print_report()

        # Extraction
        self.extraction_report.print_report()

        # Validation
        self.validation_result.print_report()

        # Model summary
        print(f"\n  Extracted OCPN Model:")
        for line in self.model.summary().splitlines():
            print(f"    {line}")

        # Metrics
        ComplexityMetrics.print_report(self.metrics)

    def is_valid(self) -> bool:
        """True if the validation step found no ERROR-level issues."""
        return self.validation_result.is_valid


# ═══════════════════════════════════════════════════════════════════════════
# Pipeline
# ═══════════════════════════════════════════════════════════════════════════

class OCPNPipeline:
    """
    End-to-end pipeline: raw event data → OCPN complexity metrics.

    Parameters
    ----------
    cleaner_kwargs   : dict forwarded to DataCleaner
    extractor_kwargs : dict forwarded to ModelExtractor
    validator_kwargs : dict forwarded to ModelValidator
    metric_weights   : dict forwarded to ComplexityMetrics
    verbose          : if True, print progress messages during the run
    """

    def __init__(
        self,
        cleaner_kwargs:   Optional[dict] = None,
        extractor_kwargs: Optional[dict] = None,
        validator_kwargs: Optional[dict] = None,
        metric_weights:   Optional[dict] = None,
        verbose:          bool = True,
    ) -> None:
        self.cleaner   = DataCleaner(**(cleaner_kwargs   or {}))
        self.extractor = ModelExtractor(**(extractor_kwargs or {}))
        self.validator = ModelValidator(**(validator_kwargs or {}))
        self.metric_weights = metric_weights
        self.verbose   = verbose

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"  [pipeline] {msg}")

    # ── Single-dataset run ────────────────────────────────────────────────

    def run(
        self,
        source: Any,
        name:   str = "Dataset",
    ) -> PipelineResult:
        """
        Execute the full pipeline on one raw data source.

        Parameters
        ----------
        source : list[dict] | str | Path | pd.DataFrame
            Raw event data in any supported format.
        name   : str
            Human-readable label for reports and comparisons.

        Returns
        -------
        PipelineResult
        """
        t0 = time.perf_counter()

        # ── 1. Parse ──────────────────────────────────────────────────────
        self._log(f"Parsing '{name}'…")
        # Accept a pre-parsed RawEventLog directly (e.g. from OCEL parsers)
        if isinstance(source, RawEventLog):
            raw_log = source
            raw_log.source_name = name  # apply caller's name
        else:
            raw_log = EventParser.parse(source, source_name=name)
        self._log(f"  → {len(raw_log)} records from {raw_log.total_raw_rows} raw rows")

        # ── 2. Clean ──────────────────────────────────────────────────────
        self._log("Cleaning…")
        cleaned_log, c_report = self.cleaner.clean(raw_log)
        self._log(
            f"  → {len(cleaned_log)} records kept "
            f"({c_report.total_dropped} dropped)"
        )

        # ── 3. Extract ────────────────────────────────────────────────────
        self._log("Extracting OCPN model…")
        model, e_report = self.extractor.extract(cleaned_log, model_name=name)
        self._log(
            f"  → |O|={len(model.objects)} "
            f"|A|={len(model.activities)} "
            f"|R|={len(model.relations)}"
        )

        # ── 4. Validate ───────────────────────────────────────────────────
        self._log("Validating…")
        v_result = self.validator.validate(model)
        status   = "✓ valid" if v_result.is_valid else f"✗ {len(v_result.errors)} error(s)"
        self._log(f"  → {status}")

        # ── 5. Compute metrics ────────────────────────────────────────────
        self._log("Computing complexity metrics…")
        cm      = ComplexityMetrics(model, weights=self.metric_weights)
        metrics = cm.compute_all()
        self._log(f"  → C(G) = {metrics.composite:.4f}")

        elapsed = time.perf_counter() - t0

        return PipelineResult(
            name=name,
            raw_log=raw_log,
            cleaned_log=cleaned_log,
            model=model,
            cleaning_report=c_report,
            extraction_report=e_report,
            validation_result=v_result,
            metrics=metrics,
            elapsed_seconds=round(elapsed, 3),
        )

    # ── Multi-dataset batch run ───────────────────────────────────────────

    def run_many(
        self,
        datasets: list[tuple[Any, str]],
    ) -> list[PipelineResult]:
        """
        Run the pipeline on multiple datasets sequentially.

        Parameters
        ----------
        datasets : list of (source, name) tuples

        Returns
        -------
        list[PipelineResult]  — one per dataset, in input order
        """
        results: list[PipelineResult] = []
        for source, name in datasets:
            self._log(f"\n{'─'*50}")
            result = self.run(source, name=name)
            results.append(result)
        return results

    # ── Comparison helper ─────────────────────────────────────────────────

    @staticmethod
    def compare(results: list[PipelineResult]) -> None:
        """
        Print a side-by-side comparison table for a list of PipelineResults.

        Uses the existing ModelComparator from the metrics package.
        """
        models      = [r.model   for r in results]
        metric_list = [r.metrics for r in results]

        comparator = ModelComparator(models)
        comparator.results = metric_list  # inject pre-computed results
        comparator.print_comparison_table()

    # ── Visualisation helper ──────────────────────────────────────────────

    @staticmethod
    def visualise(
        results: list[PipelineResult],
        save_path: Optional[str] = None,
        show: bool = False,
        output_dir: Optional[str] = None,
    ) -> None:
        """
        For each PipelineResult, save THREE separate images in a dedicated
        subfolder under output_dir:

            <output_dir>/<dataset_slug>/
                01_object_interaction_graph.png
                02_activity_object_relations.png
                03_metric_summary.png

        Also saves a cross-model comparison bar chart as
            <output_dir>/summary_comparison.png

        Parameters
        ----------
        results    : list of PipelineResult
        save_path  : legacy param — if output_dir is None, its dirname is used
        show       : call plt.show() for each figure (default False)
        output_dir : root directory for all output images
        """
        from visualization.plotter import save_dataset_images, plot_model_comparison

        # Resolve output directory
        if output_dir is None:
            if save_path:
                output_dir = os.path.dirname(os.path.abspath(save_path)) or "output"
            else:
                output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)

        # Per-dataset: 3 separate images in subfolder
        for r in results:
            print(f"\n  Saving images for: {r.name}")
            folder = save_dataset_images(
                model=r.model,
                result=r.metrics,
                output_dir=output_dir,
                show=show,
            )
            print(f"    → folder: {folder}")

        # Cross-model comparison bar chart
        if len(results) > 1:
            import matplotlib.pyplot as plt
            metric_list = [r.metrics for r in results]
            fig, ax = plt.subplots(figsize=(11, 5))
            fig.patch.set_facecolor("#F8F9FA")
            plot_model_comparison(metric_list, ax=ax, show=False)
            comp_path = os.path.join(output_dir, "summary_comparison.png")
            fig.savefig(comp_path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            print(f"\n  ✓ Cross-model comparison → {comp_path}")