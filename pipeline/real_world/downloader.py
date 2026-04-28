"""
pipeline/real_world/downloader.py
===================================
Downloads real OCEL 2.0 datasets from Zenodo and caches them locally.

Datasets
--------
1. P2P (Procure-to-Pay)      — DOI 10.5281/zenodo.8412920
   14,671 events | 7 object types (SAP-based)
   URL: ocel2-p2p.json

2. Order Management          — Legacy OCEL 1.0 (converted to OCEL 2.0)
   22,367 events | 6 object types
   URL: from legacy-logs page JSON

3. Angular GitHub Commits    — DOI 10.5281/zenodo.8430332
   27,847 events | 2 object types (file, branch)
   URL: angular_github_commits_ocel.csv  (90 MB JSON too large; CSV used)
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Cache directory — sibling of this file's package
CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "real_world_data"
CACHE_DIR.mkdir(exist_ok=True)

# ── Dataset registry ──────────────────────────────────────────────────────────
DATASETS = {
    "p2p": {
        "url": "https://zenodo.org/records/8412920/files/ocel2-p2p.json?download=1",
        "filename": "ocel2-p2p.json",
        "format": "ocel2_json",
        "label": "P2P (Procure-to-Pay) — SAP ERP Simulation",
        "events": 14671, "object_types": 7,
        "doi": "10.5281/zenodo.8412920",
        "credit": "Park & Tacke genannt Unterberg, RWTH Aachen, 2023 (CC BY 4.0)",
    },
    "order_management": {
        "url": "https://ocel-standard.org/1.0/running-example.jsonocel",
        "filename": "order-management.jsonocel",
        "format": "ocel1_json",
        "label": "Order Management — OCEL Running Example",
        "events": 22, "object_types": 3,   # running example is small
        "doi": "ocel-standard.org",
        "credit": "van der Aalst et al., RWTH Aachen (CC BY 4.0)",
    },
    "angular": {
        "url": "https://zenodo.org/records/8430332/files/angular_github_commits_ocel.csv?download=1",
        "filename": "angular_github_commits_ocel.csv",
        "format": "ocel2_csv",
        "label": "Angular GitHub Commits — Real-World DevOps",
        "events": 27847, "object_types": 2,
        "doi": "10.5281/zenodo.8430332",
        "credit": "Pegoraro & van der Aalst, RWTH Aachen, 2023 (CC BY 4.0)",
    },
    "email": {
        "url": "file:///D:/avita/Desktop/SEM-6/Subjects/PMA/ocpn_complexity_v4/ocpn_complexity/real_world_data/enron_top_10000.json",
        "filename": "enron_top_10000.json",
        "format": "ocel2_json",
        "label": "enron_top_10000 Email Dataset",
        "doi":"10.5281/zenodo.18334093",
        "events": 27847, "object_types": 2,
        "credit": "Pegoraro & van der Aalst, RWTH Aachen, 2023 (CC BY 4.0)",
    },
}


def _cached_path(key: str) -> Path:
    return CACHE_DIR / DATASETS[key]["filename"]


def is_cached(key: str) -> bool:
    return _cached_path(key).exists() and _cached_path(key).stat().st_size > 0


def download_dataset(key: str, timeout: int = 30) -> Optional[Path]:
    """
    Download dataset *key* from Zenodo/ocel-standard.org and cache it locally.

    Returns the cached Path on success, or None if the download fails.
    The function prints progress and honours a timeout.
    """
    meta  = DATASETS[key]
    dest  = _cached_path(key)
    label = meta["label"]

    if is_cached(key):
        size_mb = dest.stat().st_size / 1_048_576
        print(f"  ✓ Already cached: {dest.name} ({size_mb:.1f} MB)")
        return dest

    print(f"  ↓ Downloading: {label}")
    print(f"    URL  : {meta['url']}")
    print(f"    Cache: {dest}")

    try:
        req = urllib.request.Request(
            meta["url"],
            headers={"User-Agent": "OCPN-Complexity-Research/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content = resp.read()
        dest.write_bytes(content)
        size_mb = len(content) / 1_048_576
        print(f"    ✓ Saved {size_mb:.1f} MB → {dest.name}")
        return dest
    except Exception as e:
        print(f"    ✗ Download failed: {e}")
        logger.error("Download failed for %s: %s", key, e)
        return None


def load_dataset_text(key: str) -> Optional[str]:
    """
    Load a cached dataset as text (UTF-8).
    Returns None if not available.
    """
    path = _cached_path(key)
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        logger.error("Could not read %s: %s", path, e)
        return None


def dataset_info(key: str) -> dict:
    """Return metadata dict for *key*."""
    return DATASETS.get(key, {})
