#!/usr/bin/env python3
"""
Self-contained wrapper for deterministic HR Score Generator.
Prefers reading/writing to hr_app/data; falls back to ../data transparently.

It reuses the implementation from the top-level generate_hr_scores.py by
delegating to its generate(date) while setting DATA_DIR search order.
"""
from __future__ import annotations

import os, json, sys
from typing import Optional

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(APP_DIR)
LOCAL_DATA = os.path.join(APP_DIR, 'data')
ROOT_DATA = os.path.join(ROOT_DIR, 'data')

# Import the actual core generator implementation
import generate_hr_scores_core as core  # type: ignore


def _ensure_local_dir():
    os.makedirs(LOCAL_DATA, exist_ok=True)


def generate(date_str: Optional[str] = None) -> dict:
    """
    Call the core generator, then save a copy under hr_app/data as well.
    The core module already writes to ROOT_DATA; we mirror into LOCAL_DATA.
    """
    data = core.generate(date_str, save=True)
    _ensure_local_dir()
    out_path = os.path.join(LOCAL_DATA, f"hr-scores-{data['date']}.json")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"Mirrored HR scores to {out_path}")
    return data


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate HR scores (self-contained)')
    parser.add_argument('--date', help='Target date YYYY-MM-DD (optional)')
    args = parser.parse_args()
    generate(args.date)
