"""
pipeline/real_world/ocel2_parser.py
====================================
Parses OCEL 2.0 JSON files from ocel-standard.org / Zenodo into our
pipeline's RawEventLog format.

OCEL 2.0 JSON structure (confirmed from live Zenodo files):
{
  "objectTypes": [{"name": "purchase_requisition", "attributes": [...]}],
  "eventTypes":  [{"name": "Create Purchase Requisition", "attributes": [...]}],
  "objects":     [{"id": "purchase_requisition:1", "type": "purchase_requisition",
                   "attributes": [...], "relationships": [...]}],
  "events":      [{"id": "event_1", "type": "Create Purchase Requisition",
                   "time": "2022-04-05T08:00:00.000Z",
                   "attributes": [...],
                   "relationships": [{"objectId": "purchase_requisition:1",
                                      "qualifier": "..."}]}]
}

OCEL 1.0 JSON (legacy logs, e.g. Order Management):
{
  "ocel:global-event": {...},
  "ocel:global-object": {...},
  "ocel:events": {
    "event_id": {
      "ocel:activity": "...",
      "ocel:timestamp": "...",
      "ocel:omap": ["obj1", "obj2"],
      "ocel:vmap": {}
    }
  },
  "ocel:objects": {
    "obj_id": {"ocel:type": "order", "ocel:ovmap": {}}
  }
}

OCEL 2.0 CSV (Angular GitHub): columns include activity, timestamp,
  ocel:type:file, ocel:type:branch columns with object IDs.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from collections import defaultdict
from typing import Any

from pipeline.schema import EventRecord, RawEventLog

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# OCEL 2.0 JSON  (P2P, Logistics, Order Management new format)
# ─────────────────────────────────────────────────────────────────────────────

def parse_ocel2_json(raw: str | dict, source_name: str = "OCEL 2.0") -> RawEventLog:
    """
    Parse an OCEL 2.0 JSON string or pre-loaded dict into a RawEventLog.

    Handles both:
      - Standard OCEL 2.0  (keys: objectTypes, eventTypes, objects, events)
      - Legacy OCEL 1.0    (keys: ocel:events, ocel:objects)
    """
    data = json.loads(raw) if isinstance(raw, str) else raw

    # Detect format
    if "events" in data and "objects" in data:
        return _parse_ocel2_standard(data, source_name)
    elif "ocel:events" in data:
        return _parse_ocel1_legacy(data, source_name)
    else:
        raise ValueError(
            f"Unrecognised OCEL JSON structure. "
            f"Top-level keys: {list(data.keys())[:8]}"
        )


def _parse_ocel2_standard(data: dict, source_name: str) -> RawEventLog:
    """Parse OCEL 2.0 standard format (P2P, Logistics)."""
    # Build object-id → type lookup
    obj_type_map: dict[str, str] = {}
    for obj in data.get("objects", []):
        obj_id   = str(obj.get("id", "")).strip()
        obj_type = str(obj.get("type", "")).strip()
        if obj_id and obj_type:
            obj_type_map[obj_id] = obj_type

    records: list[EventRecord] = []
    raw_event_count = 0

    for evt in data.get("events", []):
        raw_event_count += 1
        event_id  = str(evt.get("id", f"evt_{raw_event_count}")).strip()
        activity  = str(evt.get("type", evt.get("activity", ""))).strip()
        timestamp_str = str(evt.get("time", evt.get("timestamp", ""))).strip()
        ts = _parse_ts(timestamp_str)

        relationships = evt.get("relationships", [])
        if not relationships:
            # Event with no object links — skip (will be logged)
            logger.debug("Event %s has no relationships, skipping", event_id)
            continue

        for rel in relationships:
            obj_id   = str(rel.get("objectId", "")).strip()
            obj_type = obj_type_map.get(obj_id, "")
            if not obj_type:
                # object ID not in master list — use the ID prefix as type
                obj_type = obj_id.split(":")[0] if ":" in obj_id else obj_id

            if activity and obj_type:
                records.append(EventRecord(
                    activity=activity,
                    object_type=obj_type,
                    object_id=obj_id,
                    timestamp=ts,
                    event_id=event_id,
                    source_row=raw_event_count - 1,
                ))

    return RawEventLog(
        records=records,
        source_name=source_name,
        total_raw_rows=raw_event_count,
    )


def _parse_ocel1_legacy(data: dict, source_name: str) -> RawEventLog:
    """Parse OCEL 1.0 legacy format (Order Management, SAP logs)."""
    # Build object-id → type lookup
    obj_type_map: dict[str, str] = {}
    for obj_id, obj_data in data.get("ocel:objects", {}).items():
        obj_type = str(obj_data.get("ocel:type", "")).strip()
        if obj_type:
            obj_type_map[str(obj_id)] = obj_type

    records: list[EventRecord] = []
    raw_event_count = 0

    for event_id, evt_data in data.get("ocel:events", {}).items():
        raw_event_count += 1
        activity  = str(evt_data.get("ocel:activity", "")).strip()
        ts        = _parse_ts(str(evt_data.get("ocel:timestamp", "")))
        omap      = evt_data.get("ocel:omap", [])

        for obj_ref in omap:
            obj_id   = str(obj_ref).strip()
            obj_type = obj_type_map.get(obj_id, "")
            if not obj_type:
                obj_type = obj_id.split(":")[0] if ":" in obj_id else "unknown"
            if activity and obj_type:
                records.append(EventRecord(
                    activity=activity,
                    object_type=obj_type,
                    object_id=obj_id,
                    timestamp=ts,
                    event_id=str(event_id),
                    source_row=raw_event_count - 1,
                ))

    return RawEventLog(
        records=records,
        source_name=source_name,
        total_raw_rows=raw_event_count,
    )


# ─────────────────────────────────────────────────────────────────────────────
# OCEL 2.0 CSV  (Angular GitHub Commits)
# ─────────────────────────────────────────────────────────────────────────────

def parse_ocel2_csv(
    raw: str,
    source_name: str = "OCEL 2.0 CSV",
    max_rows: int | None = None,
) -> RawEventLog:
    """
    Parse OCEL 2.0 CSV format (Angular GitHub commits).

    The Angular CSV has columns like:
      ocel:eid, ocel:activity, ocel:timestamp, ocel:type:file, ocel:type:branch,
      ocel:oid:file:..., ocel:oid:branch:...

    Strategy: for each row, emit one EventRecord per object type referenced.
    Columns starting with 'ocel:type:' identify object-type columns where
    the cell value is a list of object IDs for that type.
    """
    reader  = csv.DictReader(io.StringIO(raw))
    records : list[EventRecord] = []
    row_num = 0

    # Detect object type columns  → those starting with "ocel:type:"
    # We need to read the header first
    fieldnames = reader.fieldnames or []

    # In OCEL 2.0 CSV: object columns are named "ocel:type:<typename>"
    # and contain semicolon/comma-separated object IDs
    obj_type_cols = {
        col: col.replace("ocel:type:", "").strip()
        for col in fieldnames
        if col.startswith("ocel:type:")
    }

    # Fallback: look for columns named after known object types
    if not obj_type_cols:
        # Try "@@classifier" style (OCEL 1.0 CSV)
        for col in fieldnames:
            if col not in ("ocel:eid", "ocel:activity", "ocel:timestamp",
                           "ocel:eid:complete", "time:timestamp", "concept:name"):
                obj_type_cols[col] = col

    for row in reader:
        if max_rows and row_num >= max_rows:
            break
        row_num += 1

        activity = str(row.get("ocel:activity", row.get("concept:name", ""))).strip()
        ts_str   = str(row.get("ocel:timestamp", row.get("time:timestamp", ""))).strip()
        event_id = str(row.get("ocel:eid", f"row_{row_num}")).strip()
        ts = _parse_ts(ts_str)

        if not activity:
            continue

        emitted = False
        for col, obj_type in obj_type_cols.items():
            cell = str(row.get(col, "")).strip()
            if not cell or cell.lower() in ("", "nan", "none", "[]"):
                continue
            # Cell may be "id1,id2" or "[id1, id2]" or just "id1"
            cell = cell.strip("[]")
            obj_ids = [x.strip().strip("'\"") for x in cell.split(",") if x.strip()]
            for obj_id in obj_ids:
                if obj_id:
                    records.append(EventRecord(
                        activity=activity,
                        object_type=obj_type,
                        object_id=obj_id,
                        timestamp=ts,
                        event_id=event_id,
                        source_row=row_num - 1,
                    ))
                    emitted = True

        # If no object columns matched, emit one record with empty object_type
        # (will be filtered downstream)
        if not emitted and activity:
            records.append(EventRecord(
                activity=activity,
                object_type="",
                timestamp=ts,
                event_id=event_id,
                source_row=row_num - 1,
            ))

    return RawEventLog(
        records=records,
        source_name=source_name,
        total_raw_rows=row_num,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

from datetime import datetime
from typing import Optional

_TS_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
]

def _parse_ts(s: str) -> Optional[datetime]:
    """Leniently parse a timestamp string."""
    s = s.strip()
    if not s:
        return None
    # Handle timezone offset like +02:00 by stripping it
    for suffix_len in (6, 5):
        if len(s) > suffix_len and s[-suffix_len] in ("+", "-"):
            s = s[:-suffix_len]
            break
    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None
