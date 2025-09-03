#!/usr/bin/env python3
from __future__ import annotations
import os, json
from datetime import datetime, timedelta
from typing import Any, Dict

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(APP_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def save_json(obj: Any, path: str):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2)
    print(f"Saved {path}")


def fetch_hr_hitters_for_date(date: str) -> Dict[str, Any]:
    """Return a map of batter MLBAM id -> { name, hr } for HRs hit on the date."""
    try:
        from pybaseball import statcast
    except Exception:
        return {'date': date, 'hitters': {}}
    try:
        df = statcast(start_dt=date, end_dt=date)
    except Exception:
        df = None
    if df is None or df.empty:
        return {'date': date, 'hitters': {}}
    # Statcast event column typically 'events' === 'home_run'
    mask = (df['events'].astype(str).str.lower() == 'home_run') if 'events' in df.columns else None
    if mask is None:
        return {'date': date, 'hitters': {}}
    sub = df[mask]
    hitters: Dict[str, Dict[str, Any]] = {}
    pid_col = 'batter' if 'batter' in sub.columns else None
    name_col = 'player_name' if 'player_name' in sub.columns else None
    for _, row in sub.iterrows():
        try:
            pid = int(row.get(pid_col)) if pid_col else None
        except Exception:
            pid = None
        if not pid:
            # fallback: skip if no id
            continue
        nm = str(row.get(name_col) or '').strip()
        rec = hitters.get(str(pid))
        if not rec:
            hitters[str(pid)] = {'name': nm or None, 'hr': 1}
        else:
            rec['hr'] = int(rec.get('hr') or 0) + 1
    return {'date': date, 'hitters': hitters}


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fetch list of HR hitters for a specific date')
    default_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    parser.add_argument('--date', default=default_date, help='YYYY-MM-DD (defaults to yesterday)')
    args = parser.parse_args()
    date = args.date

    out = fetch_hr_hitters_for_date(date)
    save_json(out, os.path.join(DATA_DIR, f'hr-hitters-{date}.json'))


if __name__ == '__main__':
    main()
