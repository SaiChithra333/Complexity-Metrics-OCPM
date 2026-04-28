"""
pipeline/parser.py
==================
Accepts raw event data in any of the following formats and normalises it
into a RawEventLog of EventRecord instances:

  • Python list-of-dicts   (most direct; no I/O involved)
  • CSV file / CSV string
  • JSON file / JSON string (array of objects OR OCEL-style)
  • pandas DataFrame        (if pandas is available)

All parsing is done through a single public entry-point:

    log = EventParser.parse(source, source_name="My Dataset")

The parser:
  1. Detects the input type automatically.
  2. Resolves column aliases → canonical field names.
  3. Expands multi-object rows (pipe/comma-separated or JSON arrays).
  4. Assigns auto-incrementing event_ids where missing.
  5. Parses timestamps leniently (falls back to None on failure).
  6. Forwards unrecognised columns into EventRecord.extra for auditability.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

from pipeline.schema import (
    COLUMN_ALIASES,
    _MULTI_OBJECT_SEPARATORS,
    EventRecord,
    RawEventLog,
)

logger = logging.getLogger(__name__)

# ── Timestamp format candidates (tried in order) ──────────────────────────
_TS_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%d-%m-%Y",
]


def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Try to parse *value* as a datetime; return None on failure."""
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    logger.debug("Could not parse timestamp: %r", value)
    return None


def _resolve_columns(raw_keys: list[str]) -> dict[str, str]:
    """
    Map raw column names → canonical names using COLUMN_ALIASES.

    Returns a dict {raw_key: canonical_key} for every raw_key that has
    a known alias.  Comparison is case-insensitive and ignores surrounding
    whitespace.
    """
    mapping: dict[str, str] = {}
    for key in raw_keys:
        normalised = key.strip().lower()
        if normalised in COLUMN_ALIASES:
            mapping[key] = COLUMN_ALIASES[normalised]
    return mapping


def _expand_object_types(raw_value: Any) -> list[str]:
    """
    Split a cell that may contain multiple object types into a list.

    Handles:
      • JSON array strings : '["Order", "Item"]'  → ["Order", "Item"]
      • Pipe/semicolon/comma-separated : "Order|Item" → ["Order", "Item"]
      • Plain string       : "Order"              → ["Order"]
      • Already a list     : ["Order", "Item"]    → ["Order", "Item"]
    """
    if isinstance(raw_value, list):
        return [str(v).strip() for v in raw_value if str(v).strip()]

    s = str(raw_value).strip()
    if not s:
        return []

    # Try JSON array first
    if s.startswith("["):
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(v).strip() for v in parsed if str(v).strip()]
        except json.JSONDecodeError:
            pass

    # Fall back to delimiter splitting
    parts = [p.strip() for p in _MULTI_OBJECT_SEPARATORS.split(s) if p.strip()]
    return parts


def _row_to_records(
    row: dict[str, Any],
    col_map: dict[str, str],
    source_row: int,
    auto_event_id: str,
) -> list[EventRecord]:
    """
    Convert one raw dict row into one or more EventRecord instances.

    One row may produce multiple records if the object_type cell contains
    several types (multi-object event).

    Returns an empty list if the row is missing a required field (activity
    or object_type) — the caller logs a warning.
    """
    # Remap keys to canonical names
    canonical: dict[str, Any] = {}
    extra: dict[str, Any] = {}
    for raw_key, value in row.items():
        can = col_map.get(raw_key)
        if can:
            canonical[can] = value
        else:
            extra[raw_key] = value

    activity = str(canonical.get("activity", "")).strip()
    if not activity:
        return []  # required field missing

    raw_obj = canonical.get("object_type", "")
    obj_types = _expand_object_types(raw_obj)
    if not obj_types:
        return []  # required field missing

    ts        = _parse_timestamp(canonical.get("timestamp"))
    case_id   = str(canonical.get("case_id",   "")).strip()
    object_id = str(canonical.get("object_id", "")).strip()
    event_id  = str(canonical.get("event_id",  auto_event_id)).strip()

    records = []
    for obj_type in obj_types:
        records.append(
            EventRecord(
                activity=activity,
                object_type=obj_type,
                object_id=object_id,
                case_id=case_id,
                timestamp=ts,
                event_id=event_id,
                source_row=source_row,
                extra=dict(extra),
            )
        )
    return records


class EventParser:
    """
    Stateless parser class.  All methods are class-level; instantiation
    is optional.

    Entry point
    -----------
    EventParser.parse(source, source_name="...")
      source may be:
        • list[dict]          — Python list of row dicts
        • str                 — file path (*.csv / *.json) or raw CSV/JSON text
        • pathlib.Path        — file path
        • pd.DataFrame        — pandas DataFrame (if pandas is installed)
    """

    @classmethod
    def parse(
        cls,
        source: Any,
        source_name: str = "Dataset",
    ) -> RawEventLog:
        """
        Auto-detect input type and return a normalised RawEventLog.

        Parameters
        ----------
        source      : list[dict] | str | Path | pd.DataFrame
        source_name : label for reports and comparison tables

        Returns
        -------
        RawEventLog
        """
        # ── pandas DataFrame ──────────────────────────────────────────────
        try:
            import pandas as pd  # type: ignore
            if isinstance(source, pd.DataFrame):
                return cls._from_rows(
                    source.to_dict(orient="records"),
                    source_name=source_name,
                )
        except ImportError:
            pass

        # ── list of dicts ─────────────────────────────────────────────────
        if isinstance(source, list):
            return cls._from_rows(source, source_name=source_name)

        # ── Path object or string file path ──────────────────────────────
        path: Optional[Path] = None
        if isinstance(source, Path):
            path = source
        elif isinstance(source, str) and os.path.exists(source):
            path = Path(source)

        if path is not None:
            suffix = path.suffix.lower()
            text   = path.read_text(encoding="utf-8")
            if suffix == ".json":
                return cls._from_json_text(text, source_name=source_name)
            else:  # default: CSV
                return cls._from_csv_text(text, source_name=source_name)

        # ── Raw text (CSV or JSON) ────────────────────────────────────────
        if isinstance(source, str):
            stripped = source.strip()
            if stripped.startswith(("[", "{")):
                return cls._from_json_text(stripped, source_name=source_name)
            else:
                return cls._from_csv_text(stripped, source_name=source_name)

        raise TypeError(
            f"EventParser.parse() does not support source type: {type(source).__name__}"
        )

    # ── Private format handlers ────────────────────────────────────────────

    @classmethod
    def _from_rows(
        cls,
        rows: list[dict[str, Any]],
        source_name: str,
    ) -> RawEventLog:
        """Core normalisation loop shared by all format handlers."""
        if not rows:
            return RawEventLog(records=[], source_name=source_name, total_raw_rows=0)

        # Union keys across ALL rows — not just row[0] — so that heterogeneous
        # datasets (rows with different column schemas) resolve aliases correctly.
        all_keys = list(dict.fromkeys(k for row in rows for k in row.keys()))
        col_map  = _resolve_columns(all_keys)
        records: list[EventRecord] = []
        skipped = 0

        for i, row in enumerate(rows):
            new_recs = _row_to_records(row, col_map, source_row=i, auto_event_id=f"evt_{i:06d}")
            if new_recs:
                records.extend(new_recs)
            else:
                skipped += 1
                logger.warning("Row %d skipped — missing activity or object_type: %s", i, row)

        if skipped:
            logger.info("%d / %d raw rows skipped during parsing.", skipped, len(rows))

        return RawEventLog(
            records=records,
            source_name=source_name,
            total_raw_rows=len(rows),
        )

    @classmethod
    def _from_csv_text(cls, text: str, source_name: str) -> RawEventLog:
        reader = csv.DictReader(io.StringIO(text))
        rows   = list(reader)
        return cls._from_rows(rows, source_name=source_name)

    @classmethod
    def _from_json_text(cls, text: str, source_name: str) -> RawEventLog:
        data = json.loads(text)

        # OCEL 2.0 style: {"events": [...], "objects": [...]}
        if isinstance(data, dict):
            if "events" in data:
                data = cls._flatten_ocel(data)
            else:
                # Single event object wrapped in dict — treat as one row
                data = [data]

        if not isinstance(data, list):
            raise ValueError("JSON source must be an array of objects or OCEL format.")

        return cls._from_rows(data, source_name=source_name)

    @staticmethod
    def _flatten_ocel(ocel: dict) -> list[dict]:
        """
        Flatten a simplified OCEL (Object-Centric Event Log) dict into rows.

        Expected shape:
        {
          "events": [
            {"id": "e1", "activity": "...", "timestamp": "...",
             "omap": ["o1", "o2"]},
            ...
          ],
          "objects": [
            {"id": "o1", "type": "Order"},
            {"id": "o2", "type": "Item"},
            ...
          ]
        }
        """
        obj_map: dict[str, str] = {}
        for obj in ocel.get("objects", []):
            obj_id   = str(obj.get("id", "")).strip()
            obj_type = str(obj.get("type", obj.get("object_type", ""))).strip()
            if obj_id and obj_type:
                obj_map[obj_id] = obj_type

        rows: list[dict] = []
        for evt in ocel.get("events", []):
            activity  = evt.get("activity", evt.get("activity_name", ""))
            timestamp = evt.get("timestamp", evt.get("time", ""))
            event_id  = evt.get("id", evt.get("event_id", ""))
            omap      = evt.get("omap", evt.get("objects", []))

            if not omap:
                # No object mapping — emit without object type (will be filtered)
                rows.append({
                    "activity": activity,
                    "object_type": "",
                    "timestamp": timestamp,
                    "event_id": event_id,
                })
                continue

            for obj_ref in omap:
                obj_type = obj_map.get(str(obj_ref), str(obj_ref))
                rows.append({
                    "activity":    activity,
                    "object_type": obj_type,
                    "object_id":   str(obj_ref),
                    "timestamp":   timestamp,
                    "event_id":    event_id,
                })

        return rows
