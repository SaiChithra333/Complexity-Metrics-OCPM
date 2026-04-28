"""
metrics/complexity.py
=====================
Implements all complexity metrics for an OCPNModel.

Core metrics (4 — used in composite C(G))
------------------------------------------
All 4 are normalised to [0, 1] before compositing.

A) Structural
   OID  — Object Interaction Density

B) Interaction-based
   SAR  — Shared Activity Ratio
   AFO  — Activity Fan-Out  (normalised by OTC)

C) Distribution-aware
   RE   — Relation Entropy  (normalised by log2(OTC))

D) Composite
   C(G) — equal-weight average of the 4 normalised core metrics → [0, 1]

Informational metrics (displayed in table, NOT part of composite)
-----------------------------------------------------------------
   OTC  — Object-Type Count       (raw count, context only)
   AORC — Activity–Object Relation Count  (raw count, context only)
   OC   — Object Coupling         (redundant with OID, kept for reference)

Diagnostic / optional metrics (computed but NOT part of composite)
-------------------------------------------------------------------
   OII  — Object Isolation Index
   DIAM — Interaction Graph Diameter

Rationale for dropping OTC / AORC / OC from composite
------------------------------------------------------
  OTC  : After self-normalisation it always equals 1.0 — zero discriminative power.
  AORC : Derivable from AFO × |A|; strongly correlated, adds no independent signal.
  OC   : Summarises the same interaction graph as OID from a different angle;
         introduces redundancy without new information.
"""

from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import Optional

import networkx as nx

from core.model import OCPNModel


# ═══════════════════════════════════════════════════════════════════════════
# Result container
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class MetricResult:
    """
    Holds all computed metric values for one OCPNModel.

    Core metrics (4 — contribute to composite C(G), all normalised [0,1]):
        oid, sar, afo_norm, re_norm

    Informational metrics (displayed only, not in composite):
        otc, aorc, afo (raw), re (raw), oc

    Diagnostic metrics (informational only):
        oii, diam
    """
    model_name: str

    # ── Informational raw metrics (display only) ──────────────────────────
    otc:  float = 0.0   # Object-Type Count          (raw)
    aorc: float = 0.0   # Activity–Object Relation Count (raw)
    oc:   float = 0.0   # Object Coupling            (raw, informational)

    # ── Core metrics — raw values (for display in table) ──────────────────
    oid:  float = 0.0   # Object Interaction Density  [0,1]
    sar:  float = 0.0   # Shared Activity Ratio       [0,1]
    afo:  float = 0.0   # Activity Fan-Out            (raw)
    re:   float = 0.0   # Relation Entropy            (raw, bits)

    # ── Core metrics — normalised values (used in composite) ──────────────
    afo_norm: float = 0.0   # AFO / OTC               [0,1]
    re_norm:  float = 0.0   # RE  / log2(OTC)         [0,1]

    # ── Composite (normalised, always [0,1]) ──────────────────────────────
    composite: float = 0.0  # C(G) = mean(oid, sar, afo_norm, re_norm)

    # ── Diagnostic metrics ─────────────────────────────────────────────────
    oii:  float = 0.0   # Object Isolation Index
    diam: float = 0.0   # Interaction Graph Diameter

    # ── Weights used for composite ────────────────────────────────────────
    weights: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        """Return all metrics as a plain dictionary (useful for DataFrames)."""
        return {
            "Model":                              self.model_name,
            # Informational
            "OTC  – Object-Type Count":           self.otc,
            "AORC – Relation Count":              self.aorc,
            # Core (raw)
            "OID  – Interaction Density":         self.oid,
            "SAR  – Shared Activity Ratio":       self.sar,
            "AFO  – Activity Fan-Out (raw)":      self.afo,
            "RE   – Relation Entropy (bits)":     self.re,
            # Core (normalised)
            "AFO_norm – Activity Fan-Out [0,1]":  self.afo_norm,
            "RE_norm  – Relation Entropy [0,1]":  self.re_norm,
            # Informational
            "OC   – Object Coupling":             self.oc,
            # Composite
            "C(G) – Composite Complexity [0,1]":  self.composite,
            # Diagnostic
            "OII  – Object Isolation Index":      self.oii,
            "DIAM – Graph Diameter":              self.diam,
        }


# ═══════════════════════════════════════════════════════════════════════════
# Metric computation class
# ═══════════════════════════════════════════════════════════════════════════

class ComplexityMetrics:
    """
    Computes structural, interaction-aware, and distribution-aware complexity
    metrics for an OCPNModel.

    Composite C(G) is the equal-weight mean of 4 normalised core metrics:
        C(G) = (OID + SAR + AFO_norm + RE_norm) / 4    ∈ [0, 1]

    Usage
    -----
    >>> cm = ComplexityMetrics(model)
    >>> result = cm.compute_all()
    >>> cm.print_report(result)

    Parameters
    ----------
    model : OCPNModel
    weights : dict, optional
        Custom weights for composite complexity.
        Keys: 'w_oid', 'w_sar', 'w_afo', 'w_re'
        All default to 0.25 (equal weighting across 4 core metrics).
    """

    DEFAULT_WEIGHTS: dict = {
        "w_oid": 0.25,
        "w_sar": 0.25,
        "w_afo": 0.25,
        "w_re":  0.25,
    }

    def __init__(
        self,
        model: OCPNModel,
        weights: Optional[dict] = None,
    ) -> None:
        self.model = model
        self.weights = {**self.DEFAULT_WEIGHTS, **(weights or {})}

    # ─────────────────────────────────────────────────────────────────────
    # Informational metrics (raw counts — display only, not in composite)
    # ─────────────────────────────────────────────────────────────────────

    def object_type_count(self) -> int:
        """
        OTC — Object-Type Count  [informational]
        =========================================
        Formula : OTC = |O|
        Displayed in the table for context.
        NOT included in composite (self-normalises to 1.0 always).
        """
        return len(self.model.objects)

    def activity_object_relation_count(self) -> int:
        """
        AORC — Activity–Object Relation Count  [informational]
        ========================================================
        Formula : AORC = |R|
        Displayed in the table for context.
        NOT included in composite (strongly correlated with AFO × |A|).
        """
        return len(self.model.relations)

    def object_coupling(self) -> float:
        """
        OC — Object Coupling  [informational]
        =======================================
        Formula : OC = (2 · |E|) / |O|   (= average degree in interaction graph)
        Displayed in the table for context.
        NOT included in composite (redundant with OID — same graph).
        """
        G = self.model.interaction_graph
        if len(G.nodes) == 0:
            return 0.0
        degrees = [d for _, d in G.degree()]
        return round(sum(degrees) / len(degrees), 4)

    # ─────────────────────────────────────────────────────────────────────
    # A) Core metric 1 — Structural
    # ─────────────────────────────────────────────────────────────────────

    def object_interaction_density(self) -> float:
        """
        OID — Object Interaction Density  [core, naturally [0,1]]
        ===========================================================
        Formula : OID = |E_interaction| / (|O|×(|O|−1)/2)

        Meaning : Proportion of possible object-type pairs that actually
                  co-occur in at least one activity.
                  0 → no pairs share any activity.
                  1 → every pair of object types shares an activity.

        Already bounded [0,1] — no further normalisation needed.
        Returns 0.0 for models with fewer than 2 object types.
        """
        n = len(self.model.objects)
        if n < 2:
            return 0.0
        max_edges    = n * (n - 1) / 2
        actual_edges = self.model.interaction_graph.number_of_edges()
        return round(actual_edges / max_edges, 4)

    # ─────────────────────────────────────────────────────────────────────
    # B) Core metrics 2 & 3 — Interaction-based
    # ─────────────────────────────────────────────────────────────────────

    def shared_activity_ratio(self) -> float:
        """
        SAR — Shared Activity Ratio  [core, naturally [0,1]]
        ======================================================
        Formula : SAR = |{a ∈ A : |objects(a)| > 1}| / |A|

        Meaning : Fraction of activities that touch more than one object type.
                  Already bounded [0,1] — no further normalisation needed.
        Returns 0.0 if no activities exist.
        """
        if not self.model.activities:
            return 0.0
        shared = sum(
            1 for objs in self.model.activity_to_objects.values()
            if len(objs) > 1
        )
        return round(shared / len(self.model.activities), 4)

    def activity_fan_out(self) -> tuple[float, float]:
        """
        AFO — Activity Fan-Out  [core, normalised by OTC]
        ===================================================
        Raw formula    : AFO_raw  = (Σ_{a∈A} |objects(a)|) / |A|
        Normalised     : AFO_norm = AFO_raw / OTC   ∈ [0, 1]

        Normalisation rationale: the maximum possible fan-out per activity
        equals OTC (every activity touches every object type), so dividing
        by OTC maps the raw value to [0, 1].

        Returns (afo_raw, afo_norm).
        Returns (0.0, 0.0) if no activities exist.
        """
        if not self.model.activities:
            return 0.0, 0.0
        total    = sum(len(objs) for objs in self.model.activity_to_objects.values())
        afo_raw  = round(total / len(self.model.activities), 4)
        otc_safe = max(len(self.model.objects), 1)
        afo_norm = round(min(afo_raw / otc_safe, 1.0), 4)
        return afo_raw, afo_norm

    # ─────────────────────────────────────────────────────────────────────
    # C) Core metric 4 — Distribution-aware
    # ─────────────────────────────────────────────────────────────────────

    def relation_entropy(self) -> tuple[float, float]:
        """
        RE — Relation Entropy  [core, normalised by log2(OTC)]
        ========================================================
        Raw formula    : RE_raw  = −Σ p(o)·log2(p(o))
                         where p(o) = |activities involving o| / |R|

        Normalised     : RE_norm = RE_raw / log2(OTC)   ∈ [0, 1]

        Normalisation rationale: maximum Shannon entropy over OTC outcomes
        equals log2(OTC) (uniform distribution), so dividing maps RE to [0,1].

        Meaning : 0 → all relations concentrate on one object type (maximum skew).
                  1 → relations distributed perfectly uniformly across all types.

        Returns (re_raw, re_norm).
        Returns (0.0, 0.0) if no relations exist.
        """
        if not self.model.relations:
            return 0.0, 0.0

        otc_safe = max(len(self.model.objects), 1)
        total    = len(self.model.relations)
        entropy  = 0.0
        for obj in self.model.objects:
            count = len(self.model.object_to_activities[obj])
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)

        re_raw  = round(entropy, 4)
        re_max  = math.log2(otc_safe) if otc_safe > 1 else 1.0
        re_norm = round(min(re_raw / re_max, 1.0), 4) if re_max > 0 else 0.0
        return re_raw, re_norm

    # ─────────────────────────────────────────────────────────────────────
    # D) Composite complexity  (4-metric, fully normalised, [0,1])
    # ─────────────────────────────────────────────────────────────────────

    def composite_complexity(
        self,
        oid:      float,
        sar:      float,
        afo_norm: float,
        re_norm:  float,
    ) -> float:
        """
        C(G) — Composite Complexity Score  [0, 1]
        ==========================================
        Formula : C(G) = w_oid·OID + w_sar·SAR + w_afo·AFO_norm + w_re·RE_norm

        All four inputs are normalised to [0, 1], so C(G) ∈ [0, 1].
        Default weights are equal (0.25 each).

        Metric roles
        ------------
          OID      — structural density  (graph-level: how connected?)
          SAR      — interaction breadth (activity-level: how shared?)
          AFO_norm — interaction depth   (per-activity load: how heavy?)
          RE_norm  — distribution balance(entropy-level: how balanced?)

        Parameters
        ----------
        oid, sar, afo_norm, re_norm : float  — pre-normalised [0,1] values
        """
        w = self.weights
        score = (
            w["w_oid"] * oid
            + w["w_sar"] * sar
            + w["w_afo"] * afo_norm
            + w["w_re"]  * re_norm
        )
        return round(min(score, 1.0), 4)

    # ─────────────────────────────────────────────────────────────────────
    # E) Diagnostic / optional metrics
    # ─────────────────────────────────────────────────────────────────────

    def object_isolation_index(self) -> float:
        """
        OII — Object Isolation Index  [diagnostic]
        ============================================
        Formula : OII = |{o ∈ O : o appears in no relation}| / |O|
        Proportion of declared object types never referenced by any activity.
        Returns 0.0 if no objects are declared.
        """
        if not self.model.objects:
            return 0.0
        isolated = sum(
            1 for o in self.model.objects
            if len(self.model.object_to_activities[o]) == 0
        )
        return round(isolated / len(self.model.objects), 4)

    def interaction_graph_diameter(self) -> float:
        """
        DIAM — Interaction Graph Diameter  [diagnostic]
        =================================================
        Longest shortest path in the object interaction graph.
        Computed on the largest connected component when disconnected.
        Returns 0.0 if fewer than 2 nodes.
        """
        G = self.model.interaction_graph
        if G.number_of_nodes() < 2:
            return 0.0
        if not nx.is_connected(G):
            largest = max(nx.connected_components(G), key=len)
            G_sub   = G.subgraph(largest)
            if G_sub.number_of_nodes() < 2:
                return 0.0
            return float(nx.diameter(G_sub))
        return float(nx.diameter(G))

    # ─────────────────────────────────────────────────────────────────────
    # Master compute method
    # ─────────────────────────────────────────────────────────────────────

    def compute_all(self) -> MetricResult:
        """
        Compute every metric and return a MetricResult instance.

        Composite C(G) = mean(OID, SAR, AFO_norm, RE_norm)  ∈ [0, 1].
        """
        # Informational
        otc  = self.object_type_count()
        aorc = self.activity_object_relation_count()
        oc   = self.object_coupling()

        # Core
        oid              = self.object_interaction_density()
        sar              = self.shared_activity_ratio()
        afo_raw, afo_norm = self.activity_fan_out()
        re_raw,  re_norm  = self.relation_entropy()

        # Composite — all inputs already [0,1]
        comp = self.composite_complexity(oid, sar, afo_norm, re_norm)

        # Diagnostic
        oii  = self.object_isolation_index()
        diam = self.interaction_graph_diameter()

        return MetricResult(
            model_name = self.model.name,
            # informational
            otc=otc, aorc=aorc, oc=oc,
            # core raw
            oid=oid, sar=sar, afo=afo_raw, re=re_raw,
            # core normalised
            afo_norm=afo_norm, re_norm=re_norm,
            # composite
            composite=comp,
            # diagnostic
            oii=oii, diam=diam,
            weights=self.weights.copy(),
        )

    # ─────────────────────────────────────────────────────────────────────
    # Pretty-print report
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def print_report(result: MetricResult) -> None:
        """Print a formatted complexity report to stdout."""
        sep = "═" * 66
        print(f"\n{sep}")
        print(f"  OCPN COMPLEXITY REPORT — {result.model_name}")
        print(sep)

        print("\n  ┌─ Informational Metrics (not in composite) ─────────────────┐")
        print(f"  │  OTC  – Object-Type Count          : {result.otc:>8}")
        print(f"  │  AORC – Activity–Object Relations  : {result.aorc:>8}")
        print(f"  │  OC   – Object Coupling            : {result.oc:>8.4f}")
        print("  └────────────────────────────────────────────────────────────┘")

        print("\n  ┌─ Core Metrics (composite inputs, all normalised [0,1]) ─────┐")
        print(f"  │  OID  – Interaction Density        : {result.oid:>8.4f}  [0,1]")
        print(f"  │  SAR  – Shared Activity Ratio      : {result.sar:>8.4f}  [0,1]")
        print(f"  │  AFO  – Activity Fan-Out (raw)     : {result.afo:>8.4f}")
        print(f"  │  AFO  – Activity Fan-Out (norm)    : {result.afo_norm:>8.4f}  [0,1]")
        print(f"  │  RE   – Relation Entropy (bits)    : {result.re:>8.4f}")
        print(f"  │  RE   – Relation Entropy (norm)    : {result.re_norm:>8.4f}  [0,1]")
        print("  └────────────────────────────────────────────────────────────┘")

        print("\n  ┌─ Composite Complexity ──────────────────────────────────────┐")
        w = result.weights
        print(f"  │  Weights → OID:{w['w_oid']:.2f}  SAR:{w['w_sar']:.2f}"
              f"  AFO:{w['w_afo']:.2f}  RE:{w['w_re']:.2f}")
        print(f"  │  C(G) = (OID + SAR + AFO_norm + RE_norm) / 4")
        print(f"  │  C(G) – Composite Score [0,1]      : {result.composite:>8.4f}")
        print(f"  │  Complexity %                      : {result.composite*100:>7.1f}%")
        print("  └────────────────────────────────────────────────────────────┘")

        print("\n  ┌─ Diagnostic Metrics (not in composite) ─────────────────────┐")
        print(f"  │  OII  – Object Isolation Index     : {result.oii:>8.4f}")
        print(f"  │  DIAM – Interaction Graph Diameter : {result.diam:>8.1f}")
        print("  └────────────────────────────────────────────────────────────┘")
        print(f"\n{sep}\n")