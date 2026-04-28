"""
pipeline/schema.py
==================
Defines the canonical in-memory schema for raw event data consumed by the
pipeline.  Every input format (CSV, JSON, dict-list) is normalised into
these structures before any extraction or validation occurs.

Schema design goals
-------------------
* Generic enough to represent any process domain.
* Rich enough to carry provenance (source row/id) for error reporting.
* Tolerant of optional fields — the pipeline degrades gracefully when
  columns like 'timestamp' or 'case_id' are absent.

Core concepts
-------------
EventRecord   : one observed event in the log (a single row after parsing).
RawEventLog   : an ordered collection of EventRecord instances with
                dataset-level metadata.

Canonical field names (case-insensitive aliases resolved at parse time)
-----------------------------------------------------------------------
  activity    → the name of the process step that occurred        [REQUIRED]
  object_type → the category of the business object involved      [REQUIRED]
  object_id   → a specific instance identifier                    [optional]
  case_id     → groups events belonging to the same process trace [optional]
  timestamp   → ISO-8601 string or datetime                       [optional]
  event_id    → unique row identifier                             [optional]

Multiple objects per event
--------------------------
A single raw row may carry several objects, expressed either as:
  (a) separate rows with the same event_id / case_id
  (b) a JSON array in the object_type column, e.g. '["Order","Item"]'
  (c) pipe/comma-separated string, e.g. "Order|Item"
The parser normalises all three into individual EventRecord instances.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


# ── Canonical column aliases ───────────────────────────────────────────────
# Maps known variants → canonical name.  Resolved once during parsing.
COLUMN_ALIASES: dict[str, str] = {
    # activity
    "activity":        "activity",
    "activity_name":   "activity",
    "action":          "activity",
    "event":           "activity",
    "event_name":      "activity",
    "task":            "activity",
    "task_name":       "activity",
    "concept:name":    "activity",      # XES standard
    # object_type
    "object_type":     "object_type",
    "objecttype":      "object_type",
    "object":          "object_type",
    "entity":          "object_type",
    "entity_type":     "object_type",
    "resource_type":   "object_type",
    "type":            "object_type",
    # object_id
    "object_id":       "object_id",
    "objectid":        "object_id",
    "instance_id":     "object_id",
    "entity_id":       "object_id",
    "resource_id":     "object_id",
    # case_id
    "case_id":         "case_id",
    "caseid":          "case_id",
    "case:concept:name": "case_id",     # XES standard
    "trace_id":        "case_id",
    "process_id":      "case_id",
    "order_id":        "case_id",
    # timestamp
    "timestamp":       "timestamp",
    "time":            "timestamp",
    "time:timestamp":  "timestamp",     # XES standard
    "date":            "timestamp",
    "datetime":        "timestamp",
    "event_time":      "timestamp",
    # event_id
    "event_id":        "event_id",
    "eventid":         "event_id",
    "id":              "event_id",
    "row_id":          "event_id",
}

# Separators used when an object_type cell contains multiple types
_MULTI_OBJECT_SEPARATORS = re.compile(r"[|;,\n]+")


@dataclass
class EventRecord:
    """
    One normalised event observation.

    Fields
    ------
    activity    : str   — name of the activity (required, non-empty after cleaning)
    object_type : str   — category of object type (required, non-empty after cleaning)
    object_id   : str   — specific instance; empty string if unknown
    case_id     : str   — trace/case grouping; empty string if unknown
    timestamp   : Optional[datetime]
    event_id    : str   — original row identifier or auto-assigned index
    source_row  : int   — 0-based index in the original raw dataset (for error tracing)
    extra       : dict  — any columns not mapped to canonical names
    """
    activity:    str
    object_type: str
    object_id:   str               = ""
    case_id:     str               = ""
    timestamp:   Optional[datetime] = None
    event_id:    str               = ""
    source_row:  int               = -1
    extra:       dict              = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Strip whitespace from string fields
        self.activity    = self.activity.strip()
        self.object_type = self.object_type.strip()
        self.object_id   = str(self.object_id).strip() if self.object_id else ""
        self.case_id     = str(self.case_id).strip()   if self.case_id   else ""
        self.event_id    = str(self.event_id).strip()  if self.event_id  else ""


@dataclass
class RawEventLog:
    """
    Ordered collection of EventRecord instances with dataset metadata.

    Parameters
    ----------
    records     : list of EventRecord — the normalised event observations.
    source_name : human-readable label for the originating dataset.
    total_raw_rows : total number of rows in the source before any filtering,
                     used to compute the discard rate in validation reports.
    """
    records:        list[EventRecord]
    source_name:    str  = "Unnamed Dataset"
    total_raw_rows: int  = 0

    # ── Convenience accessors ──────────────────────────────────────────────

    def __len__(self) -> int:
        return len(self.records)

    def unique_activities(self) -> set[str]:
        return {r.activity for r in self.records}

    def unique_object_types(self) -> set[str]:
        return {r.object_type for r in self.records}

    def unique_relations(self) -> set[tuple[str, str]]:
        return {(r.activity, r.object_type) for r in self.records}

    def discard_rate(self) -> float:
        """Fraction of raw rows that were dropped during parsing/cleaning."""
        if self.total_raw_rows == 0:
            return 0.0
        kept = len(self.records)
        return round(1.0 - kept / self.total_raw_rows, 4)

    def summary(self) -> str:
        lines = [
            f"Dataset     : {self.source_name}",
            f"Raw rows    : {self.total_raw_rows}",
            f"Valid events: {len(self.records)}",
            f"Discard rate: {self.discard_rate():.1%}",
            f"Activities  : {len(self.unique_activities())}",
            f"Object types: {len(self.unique_object_types())}",
            f"Relations   : {len(self.unique_relations())}",
        ]
        return "\n".join(lines)
