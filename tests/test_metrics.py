"""
tests/test_metrics.py
=====================
Unit tests for OCPNModel construction and ComplexityMetrics computation.
Run with:  python -m pytest tests/ -v
Or simply: python tests/test_metrics.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import math
from core.model import OCPNModel
from metrics.complexity import ComplexityMetrics


# ── Helpers ────────────────────────────────────────────────────────────────

def make_simple_model() -> OCPNModel:
    return OCPNModel(
        objects    = ["Order", "Item", "Vehicle"],
        activities = ["Create Order", "Load Cargo", "Start Route"],
        relations  = [
            ("Create Order", "Order"),
            ("Load Cargo",   "Item"),
            ("Load Cargo",   "Vehicle"),
            ("Start Route",  "Vehicle"),
            ("Start Route",  "Item"),
        ],
        name = "Test Simple",
    )

def make_isolated_model() -> OCPNModel:
    """Model where one object type has no relations."""
    return OCPNModel(
        objects    = ["A", "B", "Orphan"],
        activities = ["Act1"],
        relations  = [("Act1", "A"), ("Act1", "B")],
        name       = "Isolation Test",
    )

def make_star_model() -> OCPNModel:
    """One activity connects all objects → fully connected interaction graph."""
    return OCPNModel(
        objects    = ["X", "Y", "Z"],
        activities = ["BigAct"],
        relations  = [("BigAct", "X"), ("BigAct", "Y"), ("BigAct", "Z")],
        name       = "Star Model",
    )


# ── Tests ──────────────────────────────────────────────────────────────────

def test_otc():
    cm = ComplexityMetrics(make_simple_model())
    assert cm.object_type_count() == 3

def test_aorc():
    cm = ComplexityMetrics(make_simple_model())
    assert cm.activity_object_relation_count() == 5

def test_oid_simple():
    cm = ComplexityMetrics(make_simple_model())
    oid = cm.object_interaction_density()
    # Order only appears in "Create Order" → shares no activity with Item or Vehicle
    # Item & Vehicle both appear in "Load Cargo" + "Start Route" → 1 edge
    # 1 edge out of 3 possible → 1/3
    assert abs(oid - round(1/3, 4)) < 1e-6

def test_oid_star():
    """Star model: all 3 objects share the same activity → fully connected."""
    cm = ComplexityMetrics(make_star_model())
    assert cm.object_interaction_density() == 1.0

def test_sar_simple():
    cm = ComplexityMetrics(make_simple_model())
    # Load Cargo + Start Route are multi-object; Create Order is not
    sar = cm.shared_activity_ratio()
    assert abs(sar - round(2/3, 4)) < 1e-6

def test_sar_simple_verified():
    """Verify SAR computation for simple model."""
    cm  = ComplexityMetrics(make_simple_model())
    # SAR = activities with > 1 object / total activities
    # Load Cargo and Start Route have > 1 object → 2/3
    sar = cm.shared_activity_ratio()
    assert abs(sar - round(2/3, 4)) < 1e-6

def test_oii_isolated():
    cm  = ComplexityMetrics(make_isolated_model())
    oii = cm.object_isolation_index()
    # 1 orphan out of 3
    assert abs(oii - round(1/3, 4)) < 1e-6

def test_oii_no_isolation():
    cm  = ComplexityMetrics(make_star_model())
    assert cm.object_isolation_index() == 0.0

def test_afo():
    cm  = ComplexityMetrics(make_simple_model())
    # Create Order:1, Load Cargo:2, Start Route:2 → avg = 5/3
    afo = cm.activity_fan_out()
    assert abs(afo - round(5/3, 4)) < 1e-6

def test_relation_entropy_uniform():
    """If all objects appear equally → max entropy."""
    cm   = ComplexityMetrics(make_star_model())
    re   = cm.relation_entropy()
    # Each object appears in 1 activity, |R|=3, p=1/3 for each
    expected = -3 * (1/3) * math.log2(1/3)
    assert abs(re - round(expected, 4)) < 1e-4

def test_invalid_relation_raises():
    try:
        OCPNModel(
            objects    = ["A"],
            activities = ["Act1"],
            relations  = [("Act1", "UNKNOWN")],
        )
        assert False, "Should have raised ValueError"
    except ValueError:
        pass

def test_from_dict():
    model = OCPNModel.from_dict({
        "objects":    ["P", "Q"],
        "activities": ["Do"],
        "relations":  [("Do", "P"), ("Do", "Q")],
    }, name="Dict Test")
    assert model.name == "Dict Test"
    assert len(model.objects) == 2

def test_composite_default_weights():
    """Verify composite uses 7 core metrics with equal weights (1/7 each)."""
    cm     = ComplexityMetrics(make_simple_model())
    result = cm.compute_all()
    # Weights: 1/7 ≈ 0.142857 for each of 7 core metrics
    w = 1 / 7
    manual = (
        w * result.otc
        + w * result.aorc
        + w * result.oid
        + w * result.sar
        + w * result.afo
        + w * result.re
        + w * result.oc
    )
    assert abs(result.composite - round(manual, 4)) < 1e-4


# ── Runner ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✓  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ✗  {t.__name__}  →  {e}")
            failed += 1
    print(f"\n  {passed} passed, {failed} failed out of {passed+failed} tests.")
