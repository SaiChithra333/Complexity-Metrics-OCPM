"""
core/model.py
=============
Defines the OCPNModel class — the central data structure representing
an Object-Centric Petri Net as G = (O, A, R).

An OCPN is represented by:
  O  — set of object types (e.g., "Order", "Item", "Vehicle")
  A  — set of activities  (e.g., "Create Order", "Load Cargo")
  R  — relations          (e.g., {("Create Order", "Order"), ("Load Cargo", "Item")})

The model also builds derived structures used by the metrics:
  - activity_to_objects  : dict mapping each activity → set of object types it involves
  - object_to_activities : dict mapping each object type → set of activities it appears in
  - interaction_graph    : NetworkX graph where nodes = object types,
                           edges = shared at least one activity
"""

from __future__ import annotations
from typing import Iterable
import networkx as nx


class OCPNModel:
    """
    Represents an Object-Centric Petri Net (OCPN).

    Parameters
    ----------
    objects : list[str]
        Names of all object types in the model.
    activities : list[str]
        Names of all activities in the model.
    relations : list[tuple[str, str]]
        Each tuple is (activity_name, object_type_name), indicating that
        the activity operates on that object type.
    name : str, optional
        A human-readable label for this model (used in comparisons/reports).

    Raises
    ------
    ValueError
        If any relation references an activity or object not declared in
        the objects / activities lists.
    """

    def __init__(
        self,
        objects: list[str],
        activities: list[str],
        relations: list[tuple[str, str]],
        name: str = "OCPN Model",
    ) -> None:
        self.name = name
        self.objects: set[str] = set(objects)
        self.activities: set[str] = set(activities)
        self.relations: set[tuple[str, str]] = set()

        # Validate and store relations
        for act, obj in relations:
            if act not in self.activities:
                raise ValueError(
                    f"Relation references unknown activity '{act}'. "
                    f"Add it to the activities list first."
                )
            if obj not in self.objects:
                raise ValueError(
                    f"Relation references unknown object type '{obj}'. "
                    f"Add it to the objects list first."
                )
            self.relations.add((act, obj))

        # ── Derived lookup structures ─────────────────────────────────────
        self.activity_to_objects: dict[str, set[str]] = {a: set() for a in self.activities}
        self.object_to_activities: dict[str, set[str]] = {o: set() for o in self.objects}

        for act, obj in self.relations:
            self.activity_to_objects[act].add(obj)
            self.object_to_activities[obj].add(act)

        # ── Object Interaction Graph ──────────────────────────────────────
        self.interaction_graph: nx.Graph = self._build_interaction_graph()

    # ─────────────────────────────────────────────────────────────────────
    # Private helpers
    # ─────────────────────────────────────────────────────────────────────

    def _build_interaction_graph(self) -> nx.Graph:
        """
        Build the object interaction graph.

        Nodes  = object types (O)
        Edges  = two object types share at least one common activity

        The edge attribute 'shared_activities' stores the set of activities
        that caused the two object types to be connected — useful for
        richer analysis and visualisation.
        """
        G = nx.Graph()
        G.add_nodes_from(self.objects)

        obj_list = sorted(self.objects)  # deterministic iteration order
        for i, o1 in enumerate(obj_list):
            for o2 in obj_list[i + 1 :]:
                shared = self.object_to_activities[o1] & self.object_to_activities[o2]
                if shared:
                    G.add_edge(o1, o2, shared_activities=shared, weight=len(shared))

        return G

    # ─────────────────────────────────────────────────────────────────────
    # Convenience / introspection
    # ─────────────────────────────────────────────────────────────────────

    @classmethod
    def from_dict(cls, data: dict, name: str = "OCPN Model") -> "OCPNModel":
        """
        Construct an OCPNModel from a plain dictionary.

        Expected keys: 'objects', 'activities', 'relations'.

        Example
        -------
        >>> model = OCPNModel.from_dict({
        ...     "objects":    ["Order", "Item"],
        ...     "activities": ["Create Order", "Pick Item"],
        ...     "relations":  [("Create Order", "Order"), ("Pick Item", "Item")],
        ... })
        """
        return cls(
            objects=data["objects"],
            activities=data["activities"],
            relations=data["relations"],
            name=name,
        )

    def summary(self) -> str:
        """Return a human-readable summary of the model's dimensions."""
        lines = [
            f"Model      : {self.name}",
            f"Objects    : {len(self.objects)}  → {sorted(self.objects)}",
            f"Activities : {len(self.activities)}  → {sorted(self.activities)}",
            f"Relations  : {len(self.relations)}",
        ]
        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"OCPNModel(name={self.name!r}, "
            f"|O|={len(self.objects)}, "
            f"|A|={len(self.activities)}, "
            f"|R|={len(self.relations)})"
        )
