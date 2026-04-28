"""
tests/test_pipeline.py
======================
Unit + integration tests for the full pipeline.

Run with:
    python tests/test_pipeline.py
Or:
    python -m pytest tests/test_pipeline.py -v
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.schema    import EventRecord, RawEventLog
from pipeline.parser    import EventParser
from pipeline.cleaner   import DataCleaner
from pipeline.extractor import ModelExtractor
from pipeline.validator import ModelValidator
from pipeline.pipeline  import OCPNPipeline


# ═══════════════════════════════════════════════════════════════════════════
# Parser tests
# ═══════════════════════════════════════════════════════════════════════════

def test_parser_list_of_dicts():
    rows = [
        {"activity": "Create Order", "object_type": "Order"},
        {"activity": "Ship Item",    "object_type": "Item"},
    ]
    log = EventParser.parse(rows, source_name="T1")
    assert len(log) == 2
    assert log.unique_activities() == {"Create Order", "Ship Item"}

def test_parser_alias_resolution():
    """Non-standard column names should resolve via COLUMN_ALIASES."""
    rows = [
        {"event_name": "Act A", "entity_type": "ObjX"},
        {"action":     "Act B", "object":      "ObjY"},
    ]
    log = EventParser.parse(rows)
    acts = log.unique_activities()
    objs = log.unique_object_types()
    assert "Act A" in acts and "Act B" in acts
    assert "ObjX" in objs and "ObjY" in objs

def test_parser_multi_object_pipe():
    """Pipe-separated object_type should produce two EventRecords."""
    rows = [{"activity": "Multi Act", "object_type": "Order|Item"}]
    log  = EventParser.parse(rows)
    assert len(log) == 2
    assert log.unique_object_types() == {"Order", "Item"}

def test_parser_multi_object_json_array():
    """JSON-array object_type should produce multiple EventRecords."""
    rows = [{"activity": "Multi Act", "object_type": '["A", "B", "C"]'}]
    log  = EventParser.parse(rows)
    assert len(log) == 3

def test_parser_csv_text():
    csv_text = "activity,object_type,case_id\nCreate,Order,C1\nShip,Item,C1\n"
    log = EventParser.parse(csv_text)
    assert len(log) == 2

def test_parser_json_text():
    import json
    data = [
        {"activity": "A1", "object_type": "Obj1"},
        {"activity": "A2", "object_type": "Obj2"},
    ]
    log = EventParser.parse(json.dumps(data))
    assert len(log) == 2

def test_parser_ocel_json():
    import json
    ocel = {
        "events": [
            {"id": "e1", "activity": "Load", "timestamp": "2024-01-01", "omap": ["o1", "o2"]},
        ],
        "objects": [
            {"id": "o1", "type": "Item"},
            {"id": "o2", "type": "Vehicle"},
        ]
    }
    log = EventParser.parse(json.dumps(ocel))
    assert len(log) == 2
    assert log.unique_object_types() == {"Item", "Vehicle"}

def test_parser_skips_missing_required_fields():
    rows = [
        {"activity": "",          "object_type": "Order"},  # empty activity
        {"activity": "Good Act",  "object_type": ""},       # empty object_type
        {"activity": "Good Act2", "object_type": "Item"},   # valid
    ]
    log = EventParser.parse(rows)
    assert len(log) == 1


# ═══════════════════════════════════════════════════════════════════════════
# Cleaner tests
# ═══════════════════════════════════════════════════════════════════════════

def test_cleaner_drops_nulls():
    rows = [
        {"activity": None,        "object_type": "Order"},
        {"activity": "Good Act",  "object_type": "Item"},
    ]
    log     = EventParser.parse(rows)
    cleaner = DataCleaner()
    cleaned, report = cleaner.clean(log)
    assert len(cleaned) == 1
    assert report.dropped_null == 0  # null activity filtered at parser level

def test_cleaner_drops_placeholders():
    rows = [
        {"activity": "N/A",      "object_type": "Order"},
        {"activity": "Good Act", "object_type": "Unknown"},
        {"activity": "Good Act", "object_type": "Item"},
    ]
    log     = EventParser.parse(rows)
    cleaner = DataCleaner(reject_placeholders=True)
    cleaned, report = cleaner.clean(log)
    assert len(cleaned) == 1
    assert report.dropped_placeholder == 2

def test_cleaner_title_case():
    rows = [{"activity": "create order", "object_type": "order"}]
    log     = EventParser.parse(rows)
    cleaner = DataCleaner(title_case=True)
    cleaned, _ = cleaner.clean(log)
    assert cleaned.records[0].activity    == "Create Order"
    assert cleaned.records[0].object_type == "Order"

def test_cleaner_deduplication():
    rows = [
        {"activity": "Act", "object_type": "Obj", "event_id": "E1"},
        {"activity": "Act", "object_type": "Obj", "event_id": "E1"},  # duplicate
        {"activity": "Act", "object_type": "Obj", "event_id": "E2"},  # different id
    ]
    log     = EventParser.parse(rows)
    cleaner = DataCleaner(deduplicate=True)
    cleaned, report = cleaner.clean(log)
    assert len(cleaned) == 2
    assert report.dropped_duplicate == 1

def test_cleaner_min_activity_freq():
    rows = (
        [{"activity": "Rare Act", "object_type": "Obj"}]
        + [{"activity": "Common Act", "object_type": "Obj"}] * 5
    )
    log     = EventParser.parse(rows)
    cleaner = DataCleaner(min_activity_freq=2)
    cleaned, report = cleaner.clean(log)
    acts = cleaned.unique_activities()
    assert "Rare Act" not in acts
    assert "Common Act" in acts


# ═══════════════════════════════════════════════════════════════════════════
# Extractor tests
# ═══════════════════════════════════════════════════════════════════════════

def test_extractor_basic():
    rows = [
        {"activity": "A1", "object_type": "O1"},
        {"activity": "A1", "object_type": "O2"},
        {"activity": "A2", "object_type": "O2"},
    ]
    log   = EventParser.parse(rows)
    ex    = ModelExtractor()
    model, report = ex.extract(log, model_name="Test")
    assert model.objects    == {"O1", "O2"}
    assert model.activities == {"A1", "A2"}
    assert ("A1", "O1") in model.relations
    assert ("A1", "O2") in model.relations

def test_extractor_min_support():
    rows = (
        [{"activity": "A1", "object_type": "O1"}]          # support=1 (should be filtered)
        + [{"activity": "A2", "object_type": "O2"}] * 3   # support=3 (kept)
    )
    log   = EventParser.parse(rows)
    ex    = ModelExtractor(min_relation_support=2)
    model, report = ex.extract(log)
    assert ("A1", "O1") not in model.relations
    assert ("A2", "O2") in model.relations
    assert report.suppressed_relations == 1

def test_extractor_empty_log():
    log   = RawEventLog(records=[], source_name="Empty", total_raw_rows=0)
    ex    = ModelExtractor()
    model, report = ex.extract(log)
    assert len(model.objects) == 0
    assert len(report.warnings) > 0


# ═══════════════════════════════════════════════════════════════════════════
# Validator tests
# ═══════════════════════════════════════════════════════════════════════════

def test_validator_valid_model():
    from core.model import OCPNModel
    model  = OCPNModel(["A","B"],["Act1"],[("Act1","A"),("Act1","B")])
    val    = ModelValidator()
    result = val.validate(model)
    assert result.is_valid

def test_validator_empty_model():
    from core.model import OCPNModel
    model  = OCPNModel([],[],[])
    val    = ModelValidator()
    result = val.validate(model)
    assert not result.is_valid
    codes  = [f.code for f in result.errors]
    assert "V1" in codes


# ═══════════════════════════════════════════════════════════════════════════
# Full pipeline integration tests
# ═══════════════════════════════════════════════════════════════════════════

def test_pipeline_end_to_end_dict():
    data = [
        {"activity": "Create Order",  "object_type": "Order"},
        {"activity": "Create Order",  "object_type": "Customer"},
        {"activity": "Ship Order",    "object_type": "Order"},
        {"activity": "Ship Order",    "object_type": "Shipment"},
        {"activity": "Deliver Order", "object_type": "Shipment"},
        {"activity": "Deliver Order", "object_type": "Customer"},
    ]
    result = OCPNPipeline(verbose=False).run(data, name="Integration Test")
    assert result.is_valid()
    assert result.metrics.otc  == 3
    assert result.metrics.aorc == 6
    assert result.metrics.oid  == 1.0   # fully connected

def test_pipeline_handles_noise_gracefully():
    data = [
        {"activity": None,            "object_type": "Order"},
        {"activity": "Good Act",      "object_type": "N/A"},
        {"activity": "Good Act",      "object_type": "Item"},
        {"activity": "Good Act",      "object_type": "Item"},  # duplicate
        {"activity": "Another Act",   "object_type": "Item"},
    ]
    result = OCPNPipeline(verbose=False).run(data)
    # Should survive cleaning and produce a model
    assert len(result.model.objects) >= 1

def test_pipeline_csv_input():
    csv_text = (
        "concept:name,object_type,case:concept:name\n"
        "Place Order,Order,C1\n"
        "Place Order,Customer,C1\n"
        "Ship Item,Item,C1\n"
        "Ship Item,Order,C1\n"
    )
    result = OCPNPipeline(verbose=False).run(csv_text, name="CSV Test")
    assert "Order" in result.model.objects
    assert "Customer" in result.model.objects

def test_pipeline_run_many():
    datasets = [
        ([{"activity": "A", "object_type": "X"}, {"activity": "A", "object_type": "Y"}], "DS1"),
        ([{"activity": "B", "object_type": "P"}, {"activity": "B", "object_type": "Q"}], "DS2"),
    ]
    results = OCPNPipeline(verbose=False).run_many(datasets)
    assert len(results) == 2
    assert results[0].name == "DS1"
    assert results[1].name == "DS2"

def test_pipeline_compare_does_not_crash():
    datasets = [
        ([{"activity": "A", "object_type": "X"}, {"activity": "A", "object_type": "Y"}], "M1"),
        ([{"activity": "B", "object_type": "P"}, {"activity": "B", "object_type": "Q"},
          {"activity": "C", "object_type": "Q"}], "M2"),
    ]
    results = OCPNPipeline(verbose=False).run_many(datasets)
    OCPNPipeline.compare(results)  # should not raise


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    tests = [(k, v) for k, v in globals().items() if k.startswith("test_")]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ✓  {name}")
            passed += 1
        except Exception as e:
            print(f"  ✗  {name}  →  {e}")
            import traceback; traceback.print_exc()
            failed += 1
    print(f"\n  {passed} passed, {failed} failed out of {passed+failed} tests.")
