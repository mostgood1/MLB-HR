"""Example calibration fitter.

Reads historical CSV rows (glob via CALIBRATION_CSV_GLOB env) with columns:
  model_prob, homered
Fits Platt or Isotonic and writes model_calibration.json into data directory.
"""
from __future__ import annotations
import os, glob, csv
from calibration import fit_and_save

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, 'data')

def load_examples(pattern: str):
    rows = []
    for path in glob.glob(pattern):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                r = csv.DictReader(f)
                for row in r:
                    try:
                        p = float(row.get('model_prob'))
                        y = int(row.get('homered'))
                        if 0 <= p <= 1 and y in (0,1):
                            rows.append({'p': p, 'y': y})
                    except Exception:
                        continue
        except Exception:
            continue
    return rows

def main():
    pattern = os.environ.get('CALIBRATION_CSV_GLOB', os.path.join(DATA_DIR, 'historical-hr-events-*.csv'))
    method = os.environ.get('CALIBRATION_METHOD', 'platt').lower().strip()
    out_path = os.environ.get('CALIBRATION_FILE', os.path.join(DATA_DIR, 'model_calibration.json'))
    examples = load_examples(pattern)
    if not examples:
        print('No examples found; aborting calibration.')
        return
    model = fit_and_save(examples, method, out_path)
    print(f"Saved calibration {model.get('method')} with {model.get('n_samples')} samples -> {out_path}")

if __name__ == '__main__':
    main()
