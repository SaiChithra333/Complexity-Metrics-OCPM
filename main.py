"""
main.py
=======
Entry point for the OCPN Complexity Metrics system.

Demonstrates:
  1. Defining an OCPN model from a plain dict
  2. Computing all metrics via ComplexityMetrics
  3. Printing a formatted report
  4. Comparing multiple models side-by-side
  5. Per-dataset dashboard images  ← each dataset gets its own PNG
  6. Cross-model summary dashboard

Run:
    python main.py
"""

import sys
import os
import re

sys.path.insert(0, os.path.dirname(__file__))

from core.model import OCPNModel
from metrics.complexity import ComplexityMetrics
from metrics.comparator import ModelComparator
from visualization.plotter import save_dataset_images, plot_full_dashboard
from examples.sample_models import MODEL_SIMPLE, MODEL_ECOMMERCE, MODEL_HOSPITAL, ALL_MODELS

# Output directory for generated images
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def _safe_filename(name: str) -> str:
    """Convert a model name to a safe filename stem."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 — Single model report
# ═══════════════════════════════════════════════════════════════════════════

def demo_single_model() -> None:
    """Compute and display metrics for the specification's example model."""
    print("\n" + "━" * 64)
    print("  DEMO 1 — Single Model (specification example)")
    print("━" * 64)

    model_data = {
        "objects":    ["Order", "Item", "Vehicle"],
        "activities": ["Create Order", "Load Cargo", "Start Route"],
        "relations": [
            ("Create Order", "Order"),
            ("Load Cargo",   "Item"),
            ("Load Cargo",   "Vehicle"),
            ("Start Route",  "Vehicle"),
            ("Start Route",  "Item"),
        ],
    }

    model = OCPNModel.from_dict(model_data, name="Specification Example")
    print("\n" + model.summary())

    cm     = ComplexityMetrics(model)
    result = cm.compute_all()
    cm.print_report(result)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 — Custom weights (7-metric set)
# ═══════════════════════════════════════════════════════════════════════════

def demo_custom_weights() -> None:
    """Show how changing weights alters the composite complexity score."""
    print("\n" + "━" * 64)
    print("  DEMO 2 — Custom Weights on E-Commerce Model")
    print("━" * 64)

    # Emphasise interaction and distribution metrics
    custom_weights = {
        "w_otc":  0.05,
        "w_aorc": 0.05,
        "w_oid":  0.20,
        "w_sar":  0.20,
        "w_afo":  0.20,
        "w_re":   0.15,
        "w_oc":   0.15,
    }

    cm_default = ComplexityMetrics(MODEL_ECOMMERCE)
    cm_custom  = ComplexityMetrics(MODEL_ECOMMERCE, weights=custom_weights)

    r_default = cm_default.compute_all()
    r_custom  = cm_custom.compute_all()

    print(f"\n  Default weights  → C(G) = {r_default.composite:.4f}")
    print(f"  Custom weights   → C(G) = {r_custom.composite:.4f}")
    print("\n  (Custom weights emphasise interaction and distribution metrics)")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — Multi-model comparison table
# ═══════════════════════════════════════════════════════════════════════════

def demo_comparison() -> None:
    """Compare all sample models in a side-by-side table."""
    print("\n" + "━" * 64)
    print("  DEMO 3 — Multi-Model Comparison")
    print("━" * 64)

    comparator = ModelComparator(ALL_MODELS)
    comparator.run()
    comparator.print_comparison_table()


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 — Per-dataset individual dashboard images
# ═══════════════════════════════════════════════════════════════════════════

def demo_per_dataset_dashboards() -> None:
    """
    Generate three separate images per dataset, each in its own subfolder:
        output/<dataset>/01_object_interaction_graph.png
        output/<dataset>/02_activity_object_relations.png
        output/<dataset>/03_metric_summary.png
    """
    print("\n" + "━" * 64)
    print("  DEMO 4 — Per-Dataset Individual Images")
    print("━" * 64)

    for model in ALL_MODELS:
        result = ComplexityMetrics(model).compute_all()
        print(f"\n  Saving images for: {model.name}")
        folder = save_dataset_images(
            model=model,
            result=result,
            output_dir=OUTPUT_DIR,
            show=False,
        )
        print(f"    → {folder}")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 — Cross-model summary dashboard
# ═══════════════════════════════════════════════════════════════════════════

def demo_summary_dashboard() -> None:
    """Render the cross-model summary dashboard (all models in one image)."""
    print("\n" + "━" * 64)
    print("  DEMO 5 — Cross-Model Summary Dashboard")
    print("━" * 64)

    results   = [ComplexityMetrics(m).compute_all() for m in ALL_MODELS]
    save_path = os.path.join(OUTPUT_DIR, "dashboard_summary_all_models.png")

    plot_full_dashboard(
        models    = ALL_MODELS,
        results   = results,
        save_path = save_path,
        show      = False,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    demo_single_model()
    demo_custom_weights()
    demo_comparison()
    demo_per_dataset_dashboards()
    demo_summary_dashboard()
    print("\n  ✓ All demos complete.\n")