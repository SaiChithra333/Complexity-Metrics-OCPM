"""
pipeline_demo.py
================
Full end-to-end demonstration of the OCPN event-data pipeline.

Runs four demos:
  1. Minimal hand-crafted dict → instant OCPN model
  2. Noisy realistic dict log (E-Commerce) with full report
  3. Hospital log from CSV string with min_activity_freq filter
  4. Logistics OCEL JSON, multi-dataset comparison + dashboard
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import logging
logging.basicConfig(level=logging.WARNING)   # suppress debug noise in demo

from pipeline.pipeline  import OCPNPipeline
from data.synthetic_datasets import (
    generate_ecommerce_log,
    generate_hospital_csv,
    generate_logistics_ocel,
    get_all_datasets,
)


# ═══════════════════════════════════════════════════════════════════════════
# DEMO 1 — Minimal dict input
# ═══════════════════════════════════════════════════════════════════════════

def demo_1_minimal():
    print("\n" + "━"*66)
    print("  DEMO 1 — Minimal dict input (from specification format)")
    print("━"*66)

    raw_data = [
        {"activity": "Create Order",  "object_type": "Order"},
        {"activity": "Load Cargo",    "object_type": "Item"},
        {"activity": "Load Cargo",    "object_type": "Vehicle"},
        {"activity": "Start Route",   "object_type": "Vehicle"},
        {"activity": "Start Route",   "object_type": "Item"},
    ]

    pipeline = OCPNPipeline(verbose=True)
    result   = pipeline.run(raw_data, name="Spec Example (via Pipeline)")
    result.print_full_report()


# ═══════════════════════════════════════════════════════════════════════════
# DEMO 2 — Noisy E-Commerce log
# ═══════════════════════════════════════════════════════════════════════════

def demo_2_ecommerce():
    print("\n" + "━"*66)
    print("  DEMO 2 — E-Commerce Log (list[dict], noisy real-world data)")
    print("━"*66)

    raw_data = generate_ecommerce_log()
    print(f"\n  Generated {len(raw_data)} raw rows (with injected noise)")

    pipeline = OCPNPipeline(
        cleaner_kwargs   = {"min_activity_freq": 2, "min_object_freq": 2},
        extractor_kwargs = {"min_relation_support": 2},
        verbose          = True,
    )
    result = pipeline.run(raw_data, name="E-Commerce Fulfilment")
    result.print_full_report()


# ═══════════════════════════════════════════════════════════════════════════
# DEMO 3 — Hospital CSV
# ═══════════════════════════════════════════════════════════════════════════

def demo_3_hospital():
    print("\n" + "━"*66)
    print("  DEMO 3 — Hospital Management (CSV text, non-standard columns)")
    print("━"*66)

    csv_text = generate_hospital_csv()
    line_count = csv_text.count("\n")
    print(f"\n  CSV has {line_count} lines (including header)")

    pipeline = OCPNPipeline(
        cleaner_kwargs   = {"min_activity_freq": 3},
        extractor_kwargs = {"min_relation_support": 2},
        verbose          = True,
    )
    result = pipeline.run(csv_text, name="Hospital Patient Management")
    result.print_full_report()


# ═══════════════════════════════════════════════════════════════════════════
# DEMO 4 — Multi-dataset: all three + comparison + dashboard
# ═══════════════════════════════════════════════════════════════════════════

def demo_4_comparison():
    print("\n" + "━"*66)
    print("  DEMO 4 — Multi-dataset comparison + visualisation dashboard")
    print("━"*66)

    datasets = get_all_datasets()

    pipeline = OCPNPipeline(
        cleaner_kwargs   = {"min_activity_freq": 2},
        extractor_kwargs = {"min_relation_support": 2},
        verbose          = True,
    )

    results = pipeline.run_many(datasets)

    print("\n\n" + "═"*66)
    print("  CROSS-DATASET COMPARISON")
    print("═"*66)
    OCPNPipeline.compare(results)

    print("\n  Generating visualisation dashboard…")
    OCPNPipeline.visualise(
        results,
        save_path="pipeline_dashboard.png",
        show=False,
    )
    print("  Dashboard saved to: pipeline_dashboard.png")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    demo_1_minimal()
    demo_2_ecommerce()
    demo_3_hospital()
    demo_4_comparison()
    print("\n  ✓ All pipeline demos complete.\n")
