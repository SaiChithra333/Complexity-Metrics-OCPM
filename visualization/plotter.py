"""
visualization/plotter.py
========================
Plotting utilities for OCPN complexity analysis.

Composite C(G) is now a fully normalised [0, 1] score derived from
4 core metrics: OID, SAR, AFO_norm, RE_norm.

The complexity percentage displayed in the gauge is simply:
    pct = result.composite * 100

This keeps the gauge, the table composite value, and the level band
perfectly consistent — all three come from the same number.

Public API
----------
  plot_interaction_graph(model, ...)
  plot_relation_bipartite(model, ...)
  plot_metric_radar(results, ...)        — 4-metric radar
  plot_model_comparison(results, ...)    — 4-metric bar chart
  plot_model_dashboard(model, result, ...) — backward-compat wrapper
  plot_full_dashboard(models, results, ...) — cross-model overview
  save_dataset_images(model, result, ...) — 3 PNGs per dataset
"""

from __future__ import annotations
from typing import Optional
import math
import os

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx
import numpy as np

from core.model import OCPNModel
from metrics.complexity import MetricResult

# ── Aesthetic constants ────────────────────────────────────────────────────
PALETTE = [
    "#4C72B0", "#DD8452", "#55A868", "#C44E52",
    "#8172B3", "#937860", "#DA8BC3", "#8C8C8C",
]
EDGE_COLOR = "#AAAACC"
NODE_COLOR = "#4C72B0"
BG_COLOR   = "#F8F9FA"
ACCENT     = "#C44E52"

# 4 core metric labels used across radar and bar charts
_CORE_RADAR_LABELS = ["OID", "SAR", "AFO (norm)", "RE (norm)"]
_CORE_BAR_KEYS     = ["oid", "sar", "afo_norm", "re_norm", "composite"]
_CORE_BAR_LABELS   = ["OID", "SAR", "AFO\n(norm)", "RE\n(norm)", "C(G)\nComposite"]


# ═══════════════════════════════════════════════════════════════════════════
# 1. Object Interaction Graph  (unchanged)
# ═══════════════════════════════════════════════════════════════════════════

def plot_interaction_graph(
    model: OCPNModel,
    ax: Optional[plt.Axes] = None,
    show: bool = True,
    save_path: Optional[str] = None,
) -> plt.Figure:
    """Draw the object interaction graph for a single OCPNModel."""
    G = model.interaction_graph
    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(8, 6))
        fig.patch.set_facecolor(BG_COLOR)
    else:
        fig = ax.get_figure()

    ax.set_facecolor(BG_COLOR)

    if G.number_of_nodes() == 0:
        ax.text(0.5, 0.5, "No objects in model", ha="center", va="center",
                transform=ax.transAxes, fontsize=14, color="grey")
        ax.axis("off")
        return fig

    pos = nx.spring_layout(G, seed=42, k=2.5)

    node_sizes = [
        300 + 200 * len(model.object_to_activities[node])
        for node in G.nodes()
    ]
    degrees    = dict(G.degree())
    max_degree = max(degrees.values()) if degrees else 1
    cmap       = plt.cm.Blues
    node_colors = [
        cmap(0.35 + 0.55 * degrees[n] / max(max_degree, 1))
        for n in G.nodes()
    ]

    nx.draw_networkx_nodes(G, pos, ax=ax, node_size=node_sizes,
                           node_color=node_colors, edgecolors="#2C3E50", linewidths=1.5)
    nx.draw_networkx_labels(G, pos, ax=ax, font_size=10,
                            font_weight="bold", font_color="#1A1A2E")

    edges  = list(G.edges(data=True))
    widths = [1 + 1.5 * d.get("weight", 1) for _, _, d in edges]
    nx.draw_networkx_edges(G, pos, ax=ax, width=widths,
                           edge_color=EDGE_COLOR, alpha=0.8)
    edge_labels = {(u, v): f"×{d['weight']}" for u, v, d in edges}
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, ax=ax,
                                 font_size=8, font_color="#555555")

    ax.set_title(
        f"Object Interaction Graph — {model.name}\n"
        f"Objects: {G.number_of_nodes()}  |  Interaction edges: {G.number_of_edges()}",
        fontsize=12, fontweight="bold", pad=12,
    )
    ax.axis("off")

    if save_path and standalone:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  ✓ Saved → {save_path}")
    if show and standalone:
        plt.tight_layout()
        plt.show()
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# 2. Bipartite Activity–Object Relation Graph  (unchanged)
# ═══════════════════════════════════════════════════════════════════════════

def plot_relation_bipartite(
    model: OCPNModel,
    ax: Optional[plt.Axes] = None,
    show: bool = True,
    save_path: Optional[str] = None,
) -> plt.Figure:
    """Draw the bipartite activity–object relation graph for a single model."""
    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(9, max(6, len(model.activities) * 0.55 + 2)))
        fig.patch.set_facecolor(BG_COLOR)
    else:
        fig = ax.get_figure()

    ax.set_facecolor(BG_COLOR)
    acts = sorted(model.activities)
    objs = sorted(model.objects)

    if not acts or not objs:
        ax.text(0.5, 0.5, "No activities or objects to display",
                ha="center", va="center", transform=ax.transAxes,
                fontsize=13, color="grey")
        ax.axis("off")
        return fig

    pos: dict[str, tuple] = {}
    n_acts, n_objs = len(acts), len(objs)
    for i, a in enumerate(acts):
        pos[a] = (0.0, 1 - i / max(n_acts - 1, 1))
    for j, o in enumerate(objs):
        pos[o] = (1.0, 1 - j / max(n_objs - 1, 1))

    B = nx.Graph()
    B.add_nodes_from(acts, bipartite=0)
    B.add_nodes_from(objs, bipartite=1)
    for act, obj in model.relations:
        B.add_edge(act, obj)

    nx.draw_networkx_edges(B, pos, ax=ax, edge_color=EDGE_COLOR, alpha=0.6, width=1.2)

    act_xs = [pos[a][0] for a in acts]
    act_ys = [pos[a][1] for a in acts]
    ax.scatter(act_xs, act_ys, s=500, c="#DD8452", marker="s",
               zorder=3, edgecolors="#7B3F10", linewidths=1.2, label="Activity")

    obj_xs = [pos[o][0] for o in objs]
    obj_ys = [pos[o][1] for o in objs]
    ax.scatter(obj_xs, obj_ys, s=500, c="#4C72B0", marker="o",
               zorder=3, edgecolors="#1A2C5B", linewidths=1.2, label="Object type")

    for a in acts:
        ax.text(pos[a][0] - 0.04, pos[a][1], a,
                ha="right", va="center", fontsize=9, fontweight="bold", color="#1A1A2E")
    for o in objs:
        ax.text(pos[o][0] + 0.04, pos[o][1], o,
                ha="left", va="center", fontsize=9, fontweight="bold", color="#1A1A2E")

    ax.text(0.0, 1.06, "Activities", ha="center", va="bottom",
            fontsize=10, fontweight="bold", color="#DD8452", transform=ax.transAxes)
    ax.text(1.0, 1.06, "Object Types", ha="center", va="bottom",
            fontsize=10, fontweight="bold", color="#4C72B0", transform=ax.transAxes)

    ax.set_xlim(-0.45, 1.45)
    ax.set_ylim(-0.12, 1.12)
    ax.axis("off")
    ax.legend(loc="lower center", ncol=2, fontsize=9, framealpha=0.8,
              bbox_to_anchor=(0.5, -0.08))
    ax.set_title(
        f"Activity–Object Relations — {model.name}\n"
        f"Activities: {len(acts)}  |  Object types: {len(objs)}  |  Relations: {len(model.relations)}",
        fontsize=12, fontweight="bold", pad=14,
    )

    if save_path and standalone:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  ✓ Saved → {save_path}")
    if show and standalone:
        plt.tight_layout()
        plt.show()
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# 3. Radar chart  — 4 core normalised metrics
# ═══════════════════════════════════════════════════════════════════════════

def plot_metric_radar(
    results: list[MetricResult],
    ax: Optional[plt.Axes] = None,
    show: bool = True,
    save_path: Optional[str] = None,
) -> plt.Figure:
    """
    Radar chart comparing the 4 normalised core metrics across models.

    Axes: OID, SAR, AFO_norm, RE_norm  — all already [0,1].
    OTC and AORC are omitted (raw counts, not comparable).
    OC is omitted (redundant with OID).
    """
    labels = _CORE_RADAR_LABELS
    N      = len(labels)
    angles = [n / float(N) * 2 * math.pi for n in range(N)]
    angles += angles[:1]

    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(7, 7), subplot_kw={"polar": True})
        fig.patch.set_facecolor(BG_COLOR)
    else:
        fig = ax.get_figure()

    ax.set_facecolor(BG_COLOR)
    ax.set_theta_offset(math.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_ylim(0, 1)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, size=11, fontweight="bold")
    ax.yaxis.set_tick_params(labelsize=7)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.grid(color="grey", linestyle="--", alpha=0.4)

    for i, result in enumerate(results):
        # All 4 values are already normalised [0,1]
        values = [result.oid, result.sar, result.afo_norm, result.re_norm]
        values += values[:1]
        color = PALETTE[i % len(PALETTE)]
        ax.plot(angles, values, linewidth=2, linestyle="solid",
                color=color, label=result.model_name)
        ax.fill(angles, values, alpha=0.15, color=color)

    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.15),
              fontsize=9, framealpha=0.8)
    ax.set_title("Core Metric Radar (4 Normalised Metrics)",
                 fontsize=13, fontweight="bold", pad=20)

    if save_path and standalone:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  ✓ Saved → {save_path}")
    if show and standalone:
        plt.tight_layout()
        plt.show()
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# 4. Bar comparison  — 4 core metrics + composite
# ═══════════════════════════════════════════════════════════════════════════

def plot_model_comparison(
    results: list[MetricResult],
    ax: Optional[plt.Axes] = None,
    show: bool = True,
    save_path: Optional[str] = None,
) -> plt.Figure:
    """
    Grouped bar chart: OID, SAR, AFO_norm, RE_norm, C(G) — all [0,1].
    No additional normalisation needed; values are directly comparable.
    """
    metric_keys   = _CORE_BAR_KEYS
    metric_labels = _CORE_BAR_LABELS

    n_models  = len(results)
    n_metrics = len(metric_keys)
    x         = np.arange(n_metrics)
    width     = 0.8 / n_models

    standalone = ax is None
    if standalone:
        fig, ax = plt.subplots(figsize=(11, 5))
        fig.patch.set_facecolor(BG_COLOR)
    else:
        fig = ax.get_figure()

    ax.set_facecolor(BG_COLOR)

    for i, result in enumerate(results):
        values = [getattr(result, k) for k in metric_keys]
        offset = (i - n_models / 2 + 0.5) * width
        bars = ax.bar(
            x + offset, values, width,
            label=result.model_name,
            color=PALETTE[i % len(PALETTE)],
            alpha=0.85,
            edgecolor="white",
            linewidth=0.6,
        )
        for bar, val in zip(bars, values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.01,
                f"{val:.2f}",
                ha="center", va="bottom", fontsize=7.5, color="#333",
            )

    # Highlight the composite column with a subtle background band
    ax.axvspan(n_metrics - 1 - 0.45, n_metrics - 1 + 0.45,
               color="#FFF3CD", alpha=0.5, zorder=0)

    ax.set_xticks(x)
    ax.set_xticklabels(metric_labels, fontsize=10)
    ax.set_ylim(0, 1.18)
    ax.set_ylabel("Metric Value  [0 – 1]", fontsize=10)
    ax.set_title("Model Complexity Comparison (All Metrics Normalised [0,1])",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=9, framealpha=0.8)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.spines[["top", "right"]].set_visible(False)

    if save_path and standalone:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  ✓ Saved → {save_path}")
    if show and standalone:
        plt.tight_layout()
        plt.show()
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# 5. Complexity score helpers
# ═══════════════════════════════════════════════════════════════════════════

def _complexity_score(result: MetricResult) -> float:
    """
    Return the complexity percentage (0.0 – 100.0).

    Directly derived from result.composite which is already [0, 1]:
        pct = composite × 100

    This guarantees that the gauge percentage, the table composite value,
    and the level band are all consistent — one source of truth.
    """
    return round(min(result.composite * 100, 100.0), 1)


def _complexity_level(pct: float) -> tuple[str, str, str]:
    """
    Map percentage to (label, hex_colour, description).

    Bands:
        0–25   → Low        green
        25–50  → Medium     amber
        50–75  → High       orange
        75–100 → Very High  red
    """
    if pct < 25:
        return "Low",       "#2ECC71", "Few cross-object dependencies"
    elif pct < 50:
        return "Medium",    "#F39C12", "Moderate inter-object coordination"
    elif pct < 75:
        return "High",      "#E67E22", "Dense cross-object interactions"
    else:
        return "Very High", "#E74C3C", "Highly coupled, many shared activities"


def _draw_complexity_gauge(ax: plt.Axes, result: MetricResult) -> None:
    """
    Horizontal progress-bar gauge showing the complexity percentage.
    The percentage is result.composite × 100 (single source of truth).
    """
    ax.set_facecolor(BG_COLOR)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 1)
    ax.axis("off")

    pct   = _complexity_score(result)
    level, color, desc = _complexity_level(pct)

    bar_y = 0.30
    bar_h = 0.28

    # Background track
    ax.barh(bar_y, 100, height=bar_h, left=0,
            color="#E0E0E0", edgecolor="#CCCCCC", linewidth=0.8, align="center")

    # Zone fills
    zones = [
        (0,  25,  "#2ECC71", "Low"),
        (25, 50,  "#F39C12", "Medium"),
        (50, 75,  "#E67E22", "High"),
        (75, 100, "#E74C3C", "Very High"),
    ]
    for x0, x1, zcolor, zlabel in zones:
        ax.barh(bar_y, x1 - x0, height=bar_h, left=x0,
                color=zcolor, alpha=0.18, align="center", edgecolor="none")
        ax.text((x0 + x1) / 2, bar_y - bar_h / 2 - 0.06,
                zlabel, ha="center", va="top", fontsize=8,
                color=zcolor, fontweight="bold")
        if x0 > 0:
            ax.plot([x0, x0], [bar_y - bar_h / 2, bar_y + bar_h / 2],
                    color="#BBBBBB", lw=0.8, zorder=2)

    # Filled portion
    ax.barh(bar_y, pct, height=bar_h, left=0,
            color=color, alpha=0.85, align="center", edgecolor="none", zorder=3)

    # Needle
    ax.plot([pct, pct], [bar_y - bar_h / 2 - 0.04, bar_y + bar_h / 2 + 0.04],
            color="#2C3E50", lw=2.5, zorder=5)
    ax.plot(pct, bar_y + bar_h / 2 + 0.05,
            marker="v", color="#2C3E50", markersize=8, zorder=6)

    # Percentage text
    txt_x = min(pct + 1.5, 97)
    ax.text(txt_x, bar_y, f"{pct:.1f}%",
            va="center", ha="left", fontsize=14,
            fontweight="bold", color="#2C3E50", zorder=7)

    # Level badge
    ax.text(50, bar_y + bar_h / 2 + 0.26,
            f"Complexity Level:  {level}",
            ha="center", va="bottom", fontsize=15, fontweight="bold", color=color)

    # Description
    ax.text(50, bar_y + bar_h / 2 + 0.10,
            desc, ha="center", va="bottom", fontsize=9,
            color="#555555", style="italic")

    # Scale ticks
    for tick in [0, 25, 50, 75, 100]:
        ax.text(tick, bar_y - bar_h / 2 - 0.18,
                f"{tick}%", ha="center", va="top", fontsize=7.5, color="#888888")

    ax.set_title(
        f"Metric Summary — {result.model_name}",
        fontsize=14, fontweight="bold", pad=10, color="#1A1A2E",
    )


def _draw_metric_table(ax: plt.Axes, result: MetricResult) -> None:
    """
    Render the metric table (3 columns: Category, Metric, Value).

    Row colour scheme — two colours only:
      CORE  (used in composite)  → soft teal  #D6EEE8
      OTHER (not in composite)   → soft grey  #EBEBEB
      COMPOSITE row              → level colour (green/amber/orange/red)

    Core rows  : OID, SAR, AFO (norm), RE (norm)
    Other rows : OTC, AORC, AFO (raw), RE (raw bits), OC
    """
    ax.set_facecolor(BG_COLOR)
    ax.axis("off")

    pct   = _complexity_score(result)
    level, color, _ = _complexity_level(pct)

    CORE_COLOR  = "#D6EEE8"   # soft teal  — metrics that feed C(G)
    OTHER_COLOR = "#EBEBEB"   # soft grey  — informational / raw only

    # (kind, category_label, metric_label, value)
    # kind: "core" | "other" | "composite"
    rows_data = [
        ("other",     "Informational", "OTC  — Object-Type Count",            result.otc),
        ("other",     "Informational", "AORC — Relation Count",               result.aorc),
        ("core",      "Structural",    "OID  — Interaction Density",          result.oid),
        ("core",      "Interaction",   "SAR  — Shared Activity Ratio",        result.sar),
        ("other",     "Interaction",   "AFO  — Activity Fan-Out (raw)",       result.afo),
        ("core",      "Interaction",   "AFO  — Activity Fan-Out (norm ÷ OTC)",result.afo_norm),
        ("other",     "Distribution",  "RE   — Relation Entropy (bits)",      result.re),
        ("core",      "Distribution",  "RE   — Relation Entropy (norm)",      result.re_norm),
        ("other",     "Informational", "OC   — Object Coupling",              result.oc),
        ("composite", "Composite",     f"C(G) — Composite  [{level}  {pct:.1f}%]",
                                                                               result.composite),
    ]

    col_labels = ["Category", "Metric", "Value"]
    table_data = []
    row_colors = []
    composite_row_idx = None

    for idx, (kind, cat, metric, value) in enumerate(rows_data):
        val_str = f"{value:.4f}" if isinstance(value, float) else str(value)
        table_data.append([cat, metric, val_str])
        if kind == "core":
            row_colors.append([CORE_COLOR] * 3)
        elif kind == "composite":
            row_colors.append([CORE_COLOR] * 3)   # overwritten below
            composite_row_idx = idx
        else:
            row_colors.append([OTHER_COLOR] * 3)

    tbl = ax.table(
        cellText=table_data,
        colLabels=col_labels,
        cellColours=row_colors,
        loc="center",
        cellLoc="left",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1.0, 1.72)

    # Header row
    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor("#2C3E50")
        tbl[(0, j)].get_text().set_color("white")
        tbl[(0, j)].get_text().set_fontweight("bold")

    # Composite row — level colour with white text
    if composite_row_idx is not None:
        for j in range(len(col_labels)):
            cell = tbl[(composite_row_idx + 1, j)]
            cell.set_facecolor(color)
            cell.get_text().set_color("white")
            cell.get_text().set_fontweight("bold")

    # Add a small legend below the table explaining the two colours
    ax.text(
        0.01, 0.01,
        "■  Teal = used in composite C(G)     ■  Grey = informational only",
        transform=ax.transAxes,
        fontsize=8.5,
        color="#444444",
        va="bottom",
    )


# ═══════════════════════════════════════════════════════════════════════════
# 6. Per-dataset: three separate images
# ═══════════════════════════════════════════════════════════════════════════

def _slug(name: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def save_dataset_images(
    model: OCPNModel,
    result: MetricResult,
    output_dir: str = "output",
    show: bool = False,
) -> str:
    """
    Save three PNGs for a single dataset into its own subfolder:
        01_object_interaction_graph.png
        02_activity_object_relations.png
        03_metric_summary.png
    """
    folder = os.path.join(output_dir, _slug(model.name))
    os.makedirs(folder, exist_ok=True)

    # 1. Interaction graph
    p1 = os.path.join(folder, "01_object_interaction_graph.png")
    fig1, ax1 = plt.subplots(figsize=(10, 7))
    fig1.patch.set_facecolor(BG_COLOR)
    plot_interaction_graph(model, ax=ax1, show=False)
    plt.tight_layout()
    fig1.savefig(p1, dpi=150, bbox_inches="tight")
    plt.close(fig1)
    print(f"    ✓ {p1}")

    # 2. Bipartite relation graph
    p2 = os.path.join(folder, "02_activity_object_relations.png")
    fig_h = max(7, len(model.activities) * 0.55 + 2)
    fig2, ax2 = plt.subplots(figsize=(11, fig_h))
    fig2.patch.set_facecolor(BG_COLOR)
    plot_relation_bipartite(model, ax=ax2, show=False)
    plt.tight_layout()
    fig2.savefig(p2, dpi=150, bbox_inches="tight")
    plt.close(fig2)
    print(f"    ✓ {p2}")

    # 3. Metric summary (gauge + table)
    p3 = os.path.join(folder, "03_metric_summary.png")
    fig3 = plt.figure(figsize=(12, 9), facecolor=BG_COLOR)
    gs3 = fig3.add_gridspec(2, 1, height_ratios=[1, 2.8], hspace=0.08)
    ax_gauge = fig3.add_subplot(gs3[0])
    ax_table = fig3.add_subplot(gs3[1])
    _draw_complexity_gauge(ax_gauge, result)
    _draw_metric_table(ax_table, result)
    fig3.savefig(p3, dpi=150, bbox_inches="tight")
    plt.close(fig3)
    print(f"    ✓ {p3}")

    return folder


# backward-compat wrapper
def plot_model_dashboard(
    model: OCPNModel,
    result: MetricResult,
    save_path: Optional[str] = None,
    show: bool = True,
) -> None:
    out_dir = os.path.dirname(save_path) if save_path else "output"
    save_dataset_images(model, result, output_dir=out_dir, show=show)


# ═══════════════════════════════════════════════════════════════════════════
# 7. Multi-model summary dashboard
# ═══════════════════════════════════════════════════════════════════════════

def plot_full_dashboard(
    models: list[OCPNModel],
    results: list[MetricResult],
    save_path: Optional[str] = None,
    show: bool = True,
) -> plt.Figure:
    """
    Cross-model summary dashboard:
      [0,0] Interaction graph — models[0]
      [0,1] Interaction graph — models[1]  (blank if < 2)
      [1,0] Radar — 4 normalised metrics, all models
      [1,1] Bar   — 4 metrics + composite, all models
    """
    fig = plt.figure(figsize=(16, 12), facecolor=BG_COLOR)
    fig.suptitle(
        "OCPN Complexity Analysis — Multi-Model Overview",
        fontsize=16, fontweight="bold", y=0.98, color="#1A1A2E",
    )

    gs = fig.add_gridspec(2, 2, hspace=0.38, wspace=0.3)
    ax_g1    = fig.add_subplot(gs[0, 0])
    ax_g2    = fig.add_subplot(gs[0, 1])
    ax_radar = fig.add_subplot(gs[1, 0], polar=True)
    ax_bar   = fig.add_subplot(gs[1, 1])

    plot_interaction_graph(models[0], ax=ax_g1, show=False)

    if len(models) >= 2:
        plot_interaction_graph(models[1], ax=ax_g2, show=False)
    else:
        ax_g2.set_facecolor(BG_COLOR)
        ax_g2.text(0.5, 0.5, "Only one model\nprovided",
                   ha="center", va="center", transform=ax_g2.transAxes,
                   fontsize=13, color="grey")
        ax_g2.axis("off")

    plot_metric_radar(results, ax=ax_radar, show=False)
    plot_model_comparison(results, ax=ax_bar, show=False)

    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"  ✓ Multi-model dashboard saved → {save_path}")
    if show:
        plt.show()
    return fig