"""
data/synthetic_datasets.py
==========================
Generates three realistic synthetic event log datasets to demonstrate
the full pipeline.

Each dataset deliberately includes real-world data quality issues:
  - missing fields
  - inconsistent casing
  - placeholder/null values
  - duplicate rows
  - multi-object events (pipe-separated or list)
  - extra columns unrelated to the OCPN schema

DATASET A — E-Commerce Order Fulfilment (300 events, clean-ish)
DATASET B — Hospital Patient Management (250 events, noisy)
DATASET C — Logistics & Supply Chain (200 events, OCEL-style JSON)
"""

from __future__ import annotations
import json
import random
import string
from datetime import datetime, timedelta

random.seed(42)  # reproducible


def _random_ts(base: datetime, max_hours: int = 72) -> str:
    delta = timedelta(hours=random.randint(0, max_hours),
                      minutes=random.randint(0, 59))
    return (base + delta).strftime("%Y-%m-%dT%H:%M:%S")


def _random_id(prefix: str = "", length: int = 6) -> str:
    return prefix + "".join(random.choices(string.digits, k=length))


# ═══════════════════════════════════════════════════════════════════════════
# DATASET A — E-Commerce (list of dicts, some noise)
# ═══════════════════════════════════════════════════════════════════════════

def generate_ecommerce_log() -> list[dict]:
    """
    E-Commerce order fulfilment log.
    Columns: activity, object_type, object_id, case_id, timestamp, region
    Noise injected:
      - ~5% rows with null activity
      - ~5% rows with object_type = 'N/A'
      - ~3% duplicated rows
      - multi-object events (Order + Payment on same event)
      - mixed case in activity names
    """
    base = datetime(2024, 1, 1)
    activities = [
        "Place Order", "place order", "PLACE ORDER",   # casing variants
        "Verify Payment",
        "Pick Items",
        "Pack Order",
        "Ship Order",
        "Deliver Order",
        "Process Return",
        "Issue Refund",
    ]
    object_types = ["Order", "Payment", "Item", "Customer", "Shipment", "Return"]

    # Define realistic (activity, object_type) pairs
    act_obj_pairs = [
        ("Place Order",      "Order"),
        ("Place Order",      "Customer"),
        ("Verify Payment",   "Payment"),
        ("Verify Payment",   "Order"),
        ("Pick Items",       "Item"),
        ("Pick Items",       "Order"),
        ("Pack Order",       "Item"),
        ("Pack Order",       "Order"),
        ("Ship Order",       "Shipment"),
        ("Ship Order",       "Order"),
        ("Deliver Order",    "Shipment"),
        ("Deliver Order",    "Customer"),
        ("Process Return",   "Return"),
        ("Process Return",   "Order"),
        ("Issue Refund",     "Payment"),
        ("Issue Refund",     "Customer"),
    ]

    rows: list[dict] = []
    n_cases = 40

    for case_num in range(n_cases):
        case_id = f"CASE-EC-{case_num:04d}"
        for act, obj in random.sample(act_obj_pairs, k=random.randint(4, 8)):
            rows.append({
                "activity":    act,
                "object_type": obj,
                "object_id":   _random_id(obj[:3].upper() + "-"),
                "case_id":     case_id,
                "timestamp":   _random_ts(base),
                "region":      random.choice(["North", "South", "East", "West"]),
            })

    # ── Noise ─────────────────────────────────────────────────────────────
    n = len(rows)

    # Null activities (~5%)
    for i in random.sample(range(n), k=max(1, n // 20)):
        rows[i]["activity"] = None

    # Placeholder object types (~5%)
    for i in random.sample(range(n), k=max(1, n // 20)):
        rows[i]["object_type"] = random.choice(["N/A", "Unknown", "null", ""])

    # Duplicates (~3%)
    dupe_indices = random.sample(range(n), k=max(1, n // 33))
    for i in dupe_indices:
        rows.append(dict(rows[i]))

    # Multi-object row (pipe-separated)
    rows.append({
        "activity":    "Verify Payment",
        "object_type": "Payment|Order",   # multi-object
        "object_id":   "MULTI-001",
        "case_id":     "CASE-EC-MULTI",
        "timestamp":   _random_ts(base),
        "region":      "West",
    })

    random.shuffle(rows)
    return rows


# ═══════════════════════════════════════════════════════════════════════════
# DATASET B — Hospital (CSV text, heavy noise)
# ═══════════════════════════════════════════════════════════════════════════

def generate_hospital_csv() -> str:
    """
    Hospital patient management log as a CSV string.
    Uses non-standard column names → tests alias resolution.
    Columns: event_name, entity_type, entity_id, trace_id, event_time, ward
    Noise:
      - 8% null event_name
      - 6% placeholder entity_type
      - Rare activity injected (appears only once)
      - Missing timestamps
    """
    import io, csv

    activities = [
        "Admit Patient",
        "Assign Room",
        "Prescribe Medication",
        "Administer Medication",
        "Run Lab Test",
        "Review Lab Results",
        "Discharge Patient",
        "Transfer Patient",
        "Emergency Procedure",
        "Consultation",
    ]
    obj_types = ["Patient", "Doctor", "Nurse", "Medication", "Room", "Lab", "Equipment"]

    act_obj_pairs = [
        ("Admit Patient",          ["Patient", "Doctor", "Nurse"]),
        ("Assign Room",            ["Patient", "Room"]),
        ("Prescribe Medication",   ["Doctor", "Medication", "Patient"]),
        ("Administer Medication",  ["Nurse", "Medication", "Patient"]),
        ("Run Lab Test",           ["Lab", "Patient"]),
        ("Review Lab Results",     ["Doctor", "Lab"]),
        ("Discharge Patient",      ["Patient", "Doctor"]),
        ("Transfer Patient",       ["Patient", "Room", "Nurse"]),
        ("Emergency Procedure",    ["Patient", "Doctor", "Equipment"]),
        ("Consultation",           ["Patient", "Doctor"]),
    ]

    base = datetime(2024, 2, 1)
    rows: list[dict] = []
    n_traces = 30

    for t in range(n_traces):
        trace_id = f"HOSP-{t:04d}"
        for act, objs in random.sample(act_obj_pairs, k=random.randint(3, 7)):
            for obj in objs:
                rows.append({
                    "event_name":  act,
                    "entity_type": obj,
                    "entity_id":   _random_id(obj[:2].upper()),
                    "trace_id":    trace_id,
                    "event_time":  _random_ts(base, max_hours=120) if random.random() > 0.08 else "",
                    "ward":        random.choice(["A", "B", "C", "ICU"]),
                })

    # Rare activity (appears only once — should be filtered with min_activity_freq=2)
    rows.append({
        "event_name":  "Experimental Surgery",
        "entity_type": "Patient",
        "entity_id":   "PAT-RARE",
        "trace_id":    "HOSP-RARE",
        "event_time":  "",
        "ward":        "OR",
    })

    # Noise
    n = len(rows)
    for i in random.sample(range(n), k=max(1, n // 13)):
        rows[i]["event_name"] = ""
    for i in random.sample(range(n), k=max(1, n // 17)):
        rows[i]["entity_type"] = random.choice(["Unknown", "N/A", "??"])

    # Write to CSV text
    buf = io.StringIO()
    fieldnames = ["event_name", "entity_type", "entity_id", "trace_id", "event_time", "ward"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════════════
# DATASET C — Logistics (OCEL-style JSON)
# ═══════════════════════════════════════════════════════════════════════════

def generate_logistics_ocel() -> str:
    """
    Logistics & supply chain log as an OCEL-style JSON string.

    Format:
    {
      "events":  [...],
      "objects": [...]
    }

    Noise: some events with empty omap (no object mapping)
    """
    activities = [
        "Receive Order",
        "Allocate Stock",
        "Load Cargo",
        "Dispatch Vehicle",
        "Track Shipment",
        "Customs Clearance",
        "Unload Cargo",
        "Deliver to Warehouse",
        "Final Delivery",
        "Generate Invoice",
        "Reconcile Accounts",
    ]
    obj_type_map = {
        "OR": "Order",
        "IT": "Item",
        "VH": "Vehicle",
        "DR": "Driver",
        "WH": "Warehouse",
        "SH": "Shipment",
        "IV": "Invoice",
    }

    act_obj_pairs = [
        ("Receive Order",        ["OR", "IT"]),
        ("Allocate Stock",       ["IT", "WH"]),
        ("Load Cargo",           ["IT", "VH", "DR"]),
        ("Dispatch Vehicle",     ["VH", "DR", "SH"]),
        ("Track Shipment",       ["SH", "VH"]),
        ("Customs Clearance",    ["SH", "OR"]),
        ("Unload Cargo",         ["IT", "WH"]),
        ("Deliver to Warehouse", ["SH", "WH"]),
        ("Final Delivery",       ["SH", "OR"]),
        ("Generate Invoice",     ["IV", "OR"]),
        ("Reconcile Accounts",   ["IV", "WH"]),
    ]

    objects: list[dict] = []
    obj_id_counter: dict[str, int] = {k: 0 for k in obj_type_map}

    def new_obj(prefix: str) -> str:
        obj_id_counter[prefix] += 1
        oid = f"{prefix}-{obj_id_counter[prefix]:04d}"
        objects.append({"id": oid, "type": obj_type_map[prefix]})
        return oid

    # Pre-create some shared objects
    shared_vehicles  = [new_obj("VH") for _ in range(5)]
    shared_warehouses = [new_obj("WH") for _ in range(3)]

    base   = datetime(2024, 3, 1)
    events: list[dict] = []
    n_cases = 35

    for c in range(n_cases):
        for act, prefixes in random.sample(act_obj_pairs, k=random.randint(3, 7)):
            omap = []
            for pfx in prefixes:
                if pfx == "VH":
                    omap.append(random.choice(shared_vehicles))
                elif pfx == "WH":
                    omap.append(random.choice(shared_warehouses))
                else:
                    omap.append(new_obj(pfx))

            # Occasionally add a noise event with no omap
            if random.random() < 0.04:
                events.append({
                    "id":        f"evt_{len(events):05d}",
                    "activity":  act,
                    "timestamp": _random_ts(base, max_hours=200),
                    "omap":      [],   # empty — will be filtered
                })
            else:
                events.append({
                    "id":        f"evt_{len(events):05d}",
                    "activity":  act,
                    "timestamp": _random_ts(base, max_hours=200),
                    "omap":      omap,
                })

    return json.dumps({"events": events, "objects": objects}, indent=2)


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def get_all_datasets() -> list[tuple[object, str]]:
    """
    Return all three datasets as (source, name) tuples ready for the pipeline.
    """
    return [
        (generate_ecommerce_log(),  "E-Commerce Fulfilment"),
        (generate_hospital_csv(),   "Hospital Management"),
        (generate_logistics_ocel(), "Logistics Supply Chain"),
    ]
