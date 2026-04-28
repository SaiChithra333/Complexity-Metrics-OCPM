"""
pipeline/cleaner.py
===================
Cleans and normalises a RawEventLog before model extraction.

Problems handled
----------------
1.  Null / empty activity or object_type             → drop record
2.  Whitespace-only strings                          → drop record
3.  Case normalisation                               → configurable
4.  Activity / object_type standardisation           → strip, collapse
    internal whitespace, remove control characters
5.  Rare activities (frequency below min_activity_freq threshold)
                                                     → drop or flag
6.  Rare object types (frequency below threshold)    → drop or flag
7.  Duplicate (activity, object_type, event_id) triples
                                                     → deduplicate
8.  Generic / placeholder names (e.g. "N/A", "None", "Unknown")
                                                     → configurable
9.  Inconsistent casing of the same concept
    (e.g. "create order" vs "Create Order")          → normalise to
                                                       title-case by default

Outputs
-------
CleanedLog  : a new RawEventLog with cleaned records + a CleaningReport
              describing what was changed and why.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional

from pipeline.schema import EventRecord, RawEventLog

logger = logging.getLogger(__name__)

# ── Placeholder / null-like strings to reject ─────────────────────────────
_NULL_STRINGS: set[str] = {
    "", "n/a", "na", "nan", "null", "none", "unknown", "undefined",
    "-", "--", "?", "??", "tbd", "tbc", "nil", "missing",
}

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_MULTI_SPACE   = re.compile(r"\s+")


@dataclass
class CleaningReport:
    """
    Summary of all cleaning decisions made on a dataset.

    Attributes
    ----------
    original_count      : records entering the cleaner
    final_count         : records surviving all filters
    dropped_null        : removed due to null/empty required fields
    dropped_placeholder : removed due to placeholder values (N/A, None…)
    dropped_rare_activity : removed because activity appeared < min_freq times
    dropped_rare_object : removed because object type appeared < min_freq times
    dropped_duplicate   : removed duplicate (activity, object_type, event_id)
    normalised_activity : number of activity strings that were altered
    normalised_object   : number of object_type strings that were altered
    warnings            : list of human-readable warning messages
    """
    original_count:        int        = 0
    final_count:           int        = 0
    dropped_null:          int        = 0
    dropped_placeholder:   int        = 0
    dropped_rare_activity: int        = 0
    dropped_rare_object:   int        = 0
    dropped_duplicate:     int        = 0
    normalised_activity:   int        = 0
    normalised_object:     int        = 0
    warnings:              list[str]  = field(default_factory=list)

    @property
    def total_dropped(self) -> int:
        return (
            self.dropped_null
            + self.dropped_placeholder
            + self.dropped_rare_activity
            + self.dropped_rare_object
            + self.dropped_duplicate
        )

    def print_report(self, indent: str = "  ") -> None:
        print(f"\n{indent}┌─ Cleaning Report ─────────────────────────────────────┐")
        print(f"{indent}│  Input records            : {self.original_count:>7}")
        print(f"{indent}│  Output records           : {self.final_count:>7}")
        print(f"{indent}│  ── Dropped ──────────────────────────────────────────")
        print(f"{indent}│  Null / empty fields      : {self.dropped_null:>7}")
        print(f"{indent}│  Placeholder values       : {self.dropped_placeholder:>7}")
        print(f"{indent}│  Rare activities          : {self.dropped_rare_activity:>7}")
        print(f"{indent}│  Rare object types        : {self.dropped_rare_object:>7}")
        print(f"{indent}│  Duplicates               : {self.dropped_duplicate:>7}")
        print(f"{indent}│  ── Normalised ───────────────────────────────────────")
        print(f"{indent}│  Activity names altered   : {self.normalised_activity:>7}")
        print(f"{indent}│  Object type names altered: {self.normalised_object:>7}")
        if self.warnings:
            print(f"{indent}│  ── Warnings ({len(self.warnings)}) ─────────────────────────────")
            for w in self.warnings[:10]:
                print(f"{indent}│    ⚠  {w}")
            if len(self.warnings) > 10:
                print(f"{indent}│    … and {len(self.warnings)-10} more")
        print(f"{indent}└───────────────────────────────────────────────────────┘")


def _clean_string(s: str, title_case: bool = True) -> str:
    """
    Normalise a single string:
      - Strip leading/trailing whitespace
      - Remove control characters
      - Collapse internal whitespace to single spaces
      - Optionally convert to Title Case
    """
    s = _CONTROL_CHARS.sub(" ", s)
    s = _MULTI_SPACE.sub(" ", s).strip()
    if title_case and s:
        s = s.title()
    return s


def _is_placeholder(s: str) -> bool:
    """Return True if *s* looks like a null/placeholder value."""
    return s.strip().lower() in _NULL_STRINGS


class DataCleaner:
    """
    Applies a configurable sequence of cleaning steps to a RawEventLog.

    Parameters
    ----------
    min_activity_freq : int
        Activities appearing fewer than this many times across all records
        are considered noise and their records dropped.  Default: 1 (keep all).
    min_object_freq : int
        Same threshold for object types.  Default: 1 (keep all).
    title_case : bool
        If True, normalise activity and object_type strings to Title Case.
        Helps merge "create order", "Create Order", "CREATE ORDER" into one.
    deduplicate : bool
        If True, collapse records with identical (activity, object_type, event_id).
    reject_placeholders : bool
        If True, drop records whose activity or object_type is a placeholder.
    """

    def __init__(
        self,
        min_activity_freq: int = 1,
        min_object_freq:   int = 1,
        title_case:        bool = True,
        deduplicate:       bool = True,
        reject_placeholders: bool = True,
    ) -> None:
        self.min_activity_freq    = min_activity_freq
        self.min_object_freq      = min_object_freq
        self.title_case           = title_case
        self.deduplicate          = deduplicate
        self.reject_placeholders  = reject_placeholders

    def clean(self, log: RawEventLog) -> tuple[RawEventLog, CleaningReport]:
        """
        Run all cleaning steps in sequence.

        Returns
        -------
        (cleaned_log, report) : tuple
            cleaned_log : RawEventLog with validated, normalised records
            report      : CleaningReport describing every decision made
        """
        report = CleaningReport(original_count=len(log.records))
        records = list(log.records)

        # ── Step 1: Drop null / empty required fields ─────────────────────
        kept: list[EventRecord] = []
        for rec in records:
            if not rec.activity or not rec.object_type:
                report.dropped_null += 1
                logger.debug("Dropped null-field row %d", rec.source_row)
            else:
                kept.append(rec)
        records = kept

        # ── Step 2: Reject placeholder strings ───────────────────────────
        if self.reject_placeholders:
            kept = []
            for rec in records:
                if _is_placeholder(rec.activity) or _is_placeholder(rec.object_type):
                    report.dropped_placeholder += 1
                    logger.debug(
                        "Dropped placeholder row %d: act=%r obj=%r",
                        rec.source_row, rec.activity, rec.object_type,
                    )
                else:
                    kept.append(rec)
            records = kept

        # ── Step 3: Normalise strings ─────────────────────────────────────
        for rec in records:
            clean_act = _clean_string(rec.activity, title_case=self.title_case)
            clean_obj = _clean_string(rec.object_type, title_case=self.title_case)
            if clean_act != rec.activity:
                report.normalised_activity += 1
            if clean_obj != rec.object_type:
                report.normalised_object += 1
            rec.activity    = clean_act
            rec.object_type = clean_obj

        # ── Step 4: Filter rare activities ───────────────────────────────
        if self.min_activity_freq > 1:
            act_counts = Counter(r.activity for r in records)
            rare_acts  = {a for a, c in act_counts.items() if c < self.min_activity_freq}
            if rare_acts:
                report.warnings.append(
                    f"Rare activities removed (freq<{self.min_activity_freq}): "
                    + ", ".join(sorted(rare_acts)[:5])
                    + ("…" if len(rare_acts) > 5 else "")
                )
            kept = []
            for rec in records:
                if rec.activity in rare_acts:
                    report.dropped_rare_activity += 1
                else:
                    kept.append(rec)
            records = kept

        # ── Step 5: Filter rare object types ─────────────────────────────
        if self.min_object_freq > 1:
            obj_counts = Counter(r.object_type for r in records)
            rare_objs  = {o for o, c in obj_counts.items() if c < self.min_object_freq}
            if rare_objs:
                report.warnings.append(
                    f"Rare object types removed (freq<{self.min_object_freq}): "
                    + ", ".join(sorted(rare_objs)[:5])
                    + ("…" if len(rare_objs) > 5 else "")
                )
            kept = []
            for rec in records:
                if rec.object_type in rare_objs:
                    report.dropped_rare_object += 1
                else:
                    kept.append(rec)
            records = kept

        # ── Step 6: Deduplicate ───────────────────────────────────────────
        if self.deduplicate:
            seen: set[tuple[str, str, str]] = set()
            kept = []
            for rec in records:
                key = (rec.activity, rec.object_type, rec.event_id)
                if key in seen:
                    report.dropped_duplicate += 1
                else:
                    seen.add(key)
                    kept.append(rec)
            records = kept

        # ── Final validation: warn if very few records remain ─────────────
        if len(records) == 0:
            report.warnings.append(
                "WARNING: All records were dropped. Check column names and "
                "min_frequency thresholds."
            )
        elif len(records) < 5:
            report.warnings.append(
                f"Only {len(records)} record(s) remain after cleaning — "
                "model quality may be low."
            )

        report.final_count = len(records)

        cleaned_log = RawEventLog(
            records=records,
            source_name=log.source_name,
            total_raw_rows=log.total_raw_rows,
        )
        return cleaned_log, report
