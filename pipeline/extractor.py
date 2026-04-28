"""
pipeline/extractor.py
=====================
Extracts activities, object types, and relations from a cleaned RawEventLog
and constructs a valid OCPNModel.

Extraction strategy
-------------------
The extractor treats each EventRecord as evidence of a relation:
  (record.activity, record.object_type)  ∈  R

It then:
  1. Collects the universe of activities (A) and object types (O).
  2. Builds the relation set R ⊆ A × O.
  3. Optionally applies frequency thresholds — a relation must appear in
     at least *min_relation_support* distinct events to be included.
  4. Validates that the resulting (O, A, R) is coherent before handing
     off to OCPNModel.

Frequency-based filtering (min_relation_support)
-------------------------------------------------
In real logs, spurious one-off relations arise from data-entry errors.
Setting min_relation_support=2 (or higher) suppresses these without
discarding the objects or activities themselves.

ExtractionReport
----------------
A structured summary of what was discovered vs. suppressed, printed as
part of the pipeline output.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Optional

from pipeline.schema import EventRecord, RawEventLog
from core.model import OCPNModel

logger = logging.getLogger(__name__)


@dataclass
class ExtractionReport:
    """
    Describes the model extraction outcome.

    Attributes
    ----------
    total_records          : records entering extraction
    unique_activities      : distinct activity names found
    unique_object_types    : distinct object type names found
    unique_relations       : distinct (activity, object_type) pairs found
    suppressed_relations   : pairs dropped due to low support frequency
    orphan_activities      : activities with no surviving relation
    orphan_objects         : object types with no surviving relation
    relation_support       : Counter mapping each relation → event count
    warnings               : list of human-readable messages
    """
    total_records:        int                              = 0
    unique_activities:    int                              = 0
    unique_object_types:  int                              = 0
    unique_relations:     int                              = 0
    suppressed_relations: int                              = 0
    orphan_activities:    list[str]                        = field(default_factory=list)
    orphan_objects:       list[str]                        = field(default_factory=list)
    relation_support:     Counter                          = field(default_factory=Counter)
    warnings:             list[str]                        = field(default_factory=list)

    def print_report(self, indent: str = "  ") -> None:
        print(f"\n{indent}┌─ Extraction Report ────────────────────────────────────┐")
        print(f"{indent}│  Records processed        : {self.total_records:>7}")
        print(f"{indent}│  Unique activities (|A|)  : {self.unique_activities:>7}")
        print(f"{indent}│  Unique object types (|O|): {self.unique_object_types:>7}")
        print(f"{indent}│  Unique relations (|R|)   : {self.unique_relations:>7}")
        if self.suppressed_relations:
            print(f"{indent}│  Suppressed (low support) : {self.suppressed_relations:>7}")
        if self.orphan_activities:
            print(f"{indent}│  Orphan activities        : {', '.join(self.orphan_activities[:5])}")
        if self.orphan_objects:
            print(f"{indent}│  Orphan object types      : {', '.join(self.orphan_objects[:5])}")
        if self.warnings:
            print(f"{indent}│  ── Warnings ({len(self.warnings)}) ────────────────────────────────")
            for w in self.warnings[:8]:
                print(f"{indent}│    ⚠  {w}")
        print(f"{indent}└───────────────────────────────────────────────────────┘")


class ModelExtractor:
    """
    Builds an OCPNModel from a cleaned RawEventLog.

    Parameters
    ----------
    min_relation_support : int
        A (activity, object_type) pair must appear in at least this many
        distinct EventRecord instances to be included in R.
        Default: 1 (all observed pairs included).
    include_orphan_nodes : bool
        If True, activities or objects with no surviving relations are still
        added to A / O (but will be flagged in the report).
        If False (default), they are silently removed to keep the model clean.
    """

    def __init__(
        self,
        min_relation_support: int = 1,
        include_orphan_nodes: bool = False,
    ) -> None:
        self.min_relation_support = min_relation_support
        self.include_orphan_nodes = include_orphan_nodes

    def extract(
        self,
        log: RawEventLog,
        model_name: Optional[str] = None,
    ) -> tuple[OCPNModel, ExtractionReport]:
        """
        Extract an OCPNModel from the cleaned event log.

        Parameters
        ----------
        log        : RawEventLog (after cleaning)
        model_name : label for the produced OCPNModel; defaults to log.source_name

        Returns
        -------
        (model, report) : tuple[OCPNModel, ExtractionReport]
        """
        name   = model_name or log.source_name
        report = ExtractionReport(total_records=len(log.records))

        if not log.records:
            report.warnings.append("Empty log — cannot extract a model.")
            return (
                OCPNModel(objects=[], activities=[], relations=[], name=name),
                report,
            )

        # ── Step 1: Count relation occurrences ────────────────────────────
        relation_counts: Counter = Counter(
            (rec.activity, rec.object_type) for rec in log.records
        )
        report.relation_support = relation_counts

        # ── Step 2: Collect all A, O sets from the raw log ────────────────
        all_activities    = {rec.activity    for rec in log.records}
        all_object_types  = {rec.object_type for rec in log.records}

        # ── Step 3: Apply min support threshold ───────────────────────────
        valid_relations: set[tuple[str, str]] = set()
        suppressed: list[tuple[str, str, int]] = []

        for (act, obj), count in relation_counts.items():
            if count >= self.min_relation_support:
                valid_relations.add((act, obj))
            else:
                suppressed.append((act, obj, count))
                report.suppressed_relations += 1

        if suppressed:
            examples = [f"({a},{o})×{c}" for a, o, c in suppressed[:5]]
            report.warnings.append(
                f"Suppressed {len(suppressed)} low-support relation(s) "
                f"[min={self.min_relation_support}]: "
                + ", ".join(examples)
                + ("…" if len(suppressed) > 5 else "")
            )

        # ── Step 4: Derive surviving A, O from valid relations ─────────────
        if valid_relations:
            active_activities   = {a for a, _ in valid_relations}
            active_object_types = {o for _, o in valid_relations}
        else:
            active_activities   = set()
            active_object_types = set()

        # ── Step 5: Detect orphan nodes ───────────────────────────────────
        orphan_acts = all_activities - active_activities
        orphan_objs = all_object_types - active_object_types

        if orphan_acts:
            report.orphan_activities = sorted(orphan_acts)
            report.warnings.append(
                f"Activities with no surviving relation: {sorted(orphan_acts)}"
            )
        if orphan_objs:
            report.orphan_objects = sorted(orphan_objs)
            report.warnings.append(
                f"Object types with no surviving relation: {sorted(orphan_objs)}"
            )

        # ── Step 6: Decide final O and A sets ─────────────────────────────
        if self.include_orphan_nodes:
            final_activities   = all_activities
            final_object_types = all_object_types
        else:
            final_activities   = active_activities
            final_object_types = active_object_types

        # ── Step 7: Populate report counts ────────────────────────────────
        report.unique_activities   = len(final_activities)
        report.unique_object_types = len(final_object_types)
        report.unique_relations    = len(valid_relations)

        # ── Step 8: Sanity check ─────────────────────────────────────────
        if not final_activities:
            report.warnings.append(
                "No activities survived extraction. The model will be empty."
            )
        if not final_object_types:
            report.warnings.append(
                "No object types survived extraction. The model will be empty."
            )

        # ── Step 9: Build OCPNModel ───────────────────────────────────────
        model = OCPNModel(
            objects=sorted(final_object_types),
            activities=sorted(final_activities),
            relations=sorted(valid_relations),
            name=name,
        )

        logger.info(
            "Extracted model '%s': |O|=%d |A|=%d |R|=%d",
            name, len(model.objects), len(model.activities), len(model.relations),
        )
        return model, report
