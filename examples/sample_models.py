"""
examples/sample_models.py
=========================
Three sample OCPNModels of increasing complexity used for testing and
demonstration.

  MODEL 1 — Simple Logistics Process
    3 object types, 3 activities, 5 relations
    Baseline / low complexity

  MODEL 2 — E-Commerce Order Fulfilment
    5 object types, 6 activities, 12 relations
    Medium complexity — your original example, extended

  MODEL 3 — Hospital Patient Management
    6 object types, 8 activities, 16 relations
    High complexity — many cross-object activities
"""

from core.model import OCPNModel

# ── Model 1: Simple Logistics ─────────────────────────────────────────────
MODEL_SIMPLE = OCPNModel.from_dict(
    {
        "objects":    ["Order", "Item", "Vehicle"],
        "activities": ["Create Order", "Load Cargo", "Start Route"],
        "relations": [
            ("Create Order", "Order"),
            ("Load Cargo",   "Item"),
            ("Load Cargo",   "Vehicle"),
            ("Start Route",  "Vehicle"),
            ("Start Route",  "Item"),
        ],
    },
    name="Simple Logistics",
)

# ── Model 2: E-Commerce Order Fulfilment ──────────────────────────────────
MODEL_ECOMMERCE = OCPNModel.from_dict(
    {
        "objects": ["Order", "Item", "Customer", "Payment", "Shipment"],
        "activities": [
            "Place Order",
            "Verify Payment",
            "Pick Items",
            "Pack Order",
            "Ship Order",
            "Deliver Order",
        ],
        "relations": [
            ("Place Order",    "Order"),
            ("Place Order",    "Customer"),
            ("Verify Payment", "Payment"),
            ("Verify Payment", "Order"),
            ("Pick Items",     "Item"),
            ("Pick Items",     "Order"),
            ("Pack Order",     "Item"),
            ("Pack Order",     "Order"),
            ("Ship Order",     "Shipment"),
            ("Ship Order",     "Order"),
            ("Deliver Order",  "Shipment"),
            ("Deliver Order",  "Customer"),
        ],
    },
    name="E-Commerce Fulfilment",
)

# ── Model 3: Hospital Patient Management ──────────────────────────────────
MODEL_HOSPITAL = OCPNModel.from_dict(
    {
        "objects": ["Patient", "Doctor", "Nurse", "Medication", "Room", "Lab"],
        "activities": [
            "Admit Patient",
            "Assign Room",
            "Prescribe Medication",
            "Administer Medication",
            "Run Lab Test",
            "Review Results",
            "Discharge Patient",
            "Transfer Patient",
        ],
        "relations": [
            ("Admit Patient",          "Patient"),
            ("Admit Patient",          "Doctor"),
            ("Admit Patient",          "Nurse"),
            ("Assign Room",            "Patient"),
            ("Assign Room",            "Room"),
            ("Prescribe Medication",   "Doctor"),
            ("Prescribe Medication",   "Medication"),
            ("Prescribe Medication",   "Patient"),
            ("Administer Medication",  "Nurse"),
            ("Administer Medication",  "Medication"),
            ("Administer Medication",  "Patient"),
            ("Run Lab Test",           "Lab"),
            ("Run Lab Test",           "Patient"),
            ("Review Results",         "Doctor"),
            ("Review Results",         "Lab"),
            ("Discharge Patient",      "Patient"),
            ("Discharge Patient",      "Doctor"),
            ("Transfer Patient",       "Patient"),
            ("Transfer Patient",       "Room"),
            ("Transfer Patient",       "Nurse"),
        ],
    },
    name="Hospital Patient Management",
)

ALL_MODELS = [MODEL_SIMPLE, MODEL_ECOMMERCE, MODEL_HOSPITAL]
