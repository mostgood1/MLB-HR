#!/usr/bin/env python3
from __future__ import annotations
import os, json, csv
from datetime import datetime

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(APP_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# Output CSV path (append)
CSV_PATH = os.path.join(DATA_DIR, 'historical-hr-events.csv')

FIELDS = [
    'date','name','team','hr_score','model_prob','model_prob_raw','calibration_method','homered','hr_count'
]

def load_json(path: str):
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path,'r',encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def log_outcomes(date: str):
    scores_path = os.path.join(DATA_DIR, f'hr-scores-{date}.json')
    hitters_path = os.path.join(DATA_DIR, f'hr-hitters-{date}.json')
    scores = load_json(scores_path) or {}
    hitters = (load_json(hitters_path) or {}).get('hitters') or {}
    # Build map of player -> hr count (by id) not always available: rely on name match fallback
    # The scores file might not include MLBAM id; so we match by name case-insensitive
    name_hr = {}
    for pid, rec in hitters.items():
        nm = (rec.get('name') or '').strip().lower()
        if nm:
            name_hr[nm] = int(rec.get('hr') or 1)
    rows = []
    for p in (scores.get('players') or []):
        nm = (p.get('name') or '').strip()
        key = nm.lower()
        hr_cnt = name_hr.get(key, 0)
        rows.append({
            'date': date,
            'name': nm,
            'team': p.get('team'),
            'hr_score': p.get('hr_score'),
            'model_prob': p.get('model_prob'),
            'model_prob_raw': p.get('model_prob_raw'),
            'calibration_method': p.get('calibration_method'),
            'homered': 1 if hr_cnt > 0 else 0,
            'hr_count': hr_cnt
        })
    # Append to CSV
    file_exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, 'a', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Logged {len(rows)} rows to {CSV_PATH}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Append daily outcomes to historical CSV for calibration.')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    log_outcomes(args.date)

if __name__ == '__main__':
    main()
