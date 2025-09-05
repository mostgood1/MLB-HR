#!/usr/bin/env python3
"""
Simple backtester for HR score model.

Uses existing data files in ./data for given dates and calls
generate_hr_scores_core.generate(date, save=False) under different
environment parameter settings to evaluate predictive quality.

Metrics:
- ROC-AUC (rank-based)
- Brier score (after min-max scaling scores to [0,1])
- Top-K Precision/Recall (K=10,20,30)
- Hit rate by decile

Ground truth is taken from data/hr-hitters-YYYY-MM-DD.json where
"hitters" is a mapping of MLBAM batter id -> { name, hr }.
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Dict, List, Tuple

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, 'data')


def _load_json(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _get_player_id_map(date: str) -> Dict[str, int]:
    """Map player name -> MLBAM id for a given date using player-stats file."""
    path = os.path.join(DATA_DIR, f'player-stats-{date}.json')
    if not os.path.exists(path):
        return {}
    data = _load_json(path)
    out: Dict[str, int] = {}
    for p in data.get('players', []):
        n = p.get('name')
        try:
            pid = int(p.get('mlbam_id')) if p.get('mlbam_id') is not None else None
        except Exception:
            pid = None
        if n and pid:
            out[n] = pid
    return out


def _get_ground_truth_ids(date: str) -> Dict[int, int]:
    """Return batter_id -> hr_count for the date; empty if file missing."""
    path = os.path.join(DATA_DIR, f'hr-hitters-{date}.json')
    if not os.path.exists(path):
        return {}
    data = _load_json(path)
    hitters = data.get('hitters', {}) or {}
    out: Dict[int, int] = {}
    for sid, rec in hitters.items():
        try:
            bid = int(sid)
            out[bid] = int(rec.get('hr') or 1)
        except Exception:
            continue
    return out


def _generate_predictions(date: str, env_overrides: Dict[str, str]) -> List[Tuple[int, float, str]]:
    """Return list of (batter_id, score, name) for the date under env overrides."""
    # Apply env overrides for this run
    prev_vals = {}
    for k, v in env_overrides.items():
        prev_vals[k] = os.environ.get(k)
        os.environ[k] = str(v)
    try:
        # Import locally to ensure it reads current env
        import importlib
        core = importlib.import_module('generate_hr_scores_core')
        res = core.generate(date, save=False)
    finally:
        # restore env
        for k, old in prev_vals.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old
    id_map = _get_player_id_map(date)
    preds: List[Tuple[int, float, str]] = []
    for p in res.get('players', []):
        n = p.get('name')
        try:
            bid = int(id_map.get(n)) if id_map.get(n) is not None else None
        except Exception:
            bid = None
        if bid is None:
            continue
        s = float(p.get('hr_score') or 0.0)
        preds.append((bid, s, n))
    return preds


def _auc_roc(pairs: List[Tuple[float, int]]) -> float:
    """Compute ROC-AUC via pairwise comparisons: (score, label)."""
    pos = [s for s, y in pairs if y == 1]
    neg = [s for s, y in pairs if y == 0]
    n_pos = len(pos)
    n_neg = len(neg)
    if n_pos == 0 or n_neg == 0:
        return float('nan')
    wins = ties = 0.0
    for sp in pos:
        for sn in neg:
            if sp > sn:
                wins += 1.0
            elif sp == sn:
                ties += 1.0
    return (wins + 0.5 * ties) / (n_pos * n_neg)


def _brier(pairs: List[Tuple[float, int]]) -> float:
    """Brier score after min-max scaling scores to [0,1]."""
    if not pairs:
        return float('nan')
    scores = [s for s, _ in pairs]
    lo, hi = min(scores), max(scores)
    if hi - lo < 1e-9:
        probs = [0.5 for _ in scores]
    else:
        probs = [(s - lo) / (hi - lo) for s in scores]
    ys = [y for _, y in pairs]
    return sum((p - y) ** 2 for p, y in zip(probs, ys)) / len(ys)


def _topk_metrics(pairs: List[Tuple[float, int]], ks=(10, 20, 30)) -> Dict[str, float]:
    out: Dict[str, float] = {}
    pairs_sorted = sorted(pairs, key=lambda x: x[0], reverse=True)
    total_pos = sum(1 for _, y in pairs_sorted if y == 1)
    for k in ks:
        top = pairs_sorted[:k]
        hit = sum(1 for _, y in top if y == 1)
        out[f'prec@{k}'] = hit / k if k > 0 else float('nan')
        out[f'recall@{k}'] = hit / total_pos if total_pos > 0 else float('nan')
    return out


def _decile_rates(pairs: List[Tuple[float, int]]) -> List[float]:
    if not pairs:
        return []
    n = len(pairs)
    pairs_sorted = sorted(pairs, key=lambda x: x[0], reverse=True)
    deciles: List[float] = []
    for i in range(10):
        start = int(i * n / 10)
        end = int((i + 1) * n / 10)
        bucket = pairs_sorted[start:end]
        if not bucket:
            deciles.append(float('nan'))
        else:
            rate = sum(1 for _, y in bucket if y == 1) / len(bucket)
            deciles.append(rate)
    return deciles


def eval_one(date: str, env_overrides: Dict[str, str]) -> Dict:
    gt = _get_ground_truth_ids(date)
    preds = _generate_predictions(date, env_overrides)
    # Build (score, label) pairs
    label_by_id = {bid: 1 for bid in gt.keys()}
    pairs: List[Tuple[float, int]] = [(s, 1 if label_by_id.get(bid, 0) == 1 else 0) for (bid, s, _) in preds]
    auc = _auc_roc(pairs)
    brier = _brier(pairs)
    topk = _topk_metrics(pairs)
    deciles = _decile_rates(pairs)
    return {
        'date': date,
        'n_players': len(pairs),
        'n_hr': sum(1 for _ in gt.keys()),
        'auc': auc,
        'brier': brier,
        'topk': topk,
        'deciles': deciles,
    }


def aggregate(results: List[Dict]) -> Dict:
    if not results:
        return {}
    k_keys = sorted(next(iter(results)).get('topk', {}).keys())
    agg = {
        'dates': [r['date'] for r in results],
        'auc': sum(r['auc'] for r in results if r['auc'] == r['auc']) / max(1, sum(1 for r in results if r['auc'] == r['auc'])),
        'brier': sum(r['brier'] for r in results if r['brier'] == r['brier']) / max(1, sum(1 for r in results if r['brier'] == r['brier'])),
        'topk': {k: sum(r['topk'].get(k, 0.0) for r in results) / len(results) for k in k_keys},
        'n_players_total': sum(r['n_players'] for r in results),
        'n_hr_total': sum(r['n_hr'] for r in results),
    }
    return agg


def main():
    parser = argparse.ArgumentParser(description='Backtest HR score model over given dates')
    parser.add_argument('--dates', required=True, help='Comma-separated dates YYYY-MM-DD')
    parser.add_argument('--out', help='Optional output JSON path for results')
    args = parser.parse_args()

    dates = [d.strip() for d in args.dates.split(',') if d.strip()]
    # Define parameter sweeps (env overrides)
    settings = [
        {'name': 'base', 'env': {}},
        {'name': 'park_1.05', 'env': {'PARK_EXPONENT': '1.05'}},
        {'name': 'park_1.15', 'env': {'PARK_EXPONENT': '1.15'}},
        # Market scaling variations
        {'name': 'market_off', 'env': {'MARKET_SCALE_MIN': '1.0', 'MARKET_SCALE_MAX': '1.0'}},
        {'name': 'market_wide', 'env': {'MARKET_SCALE_MIN': '0.97', 'MARKET_SCALE_MAX': '1.05'}},
        # Park clamp range variations
        {'name': 'park_wide_clamp', 'env': {'PARK_CLAMP_MIN': '0.85', 'PARK_CLAMP_MAX': '1.15'}},
    ]

    all_results = []
    for setting in settings:
        env_over = setting['env']
        per_date = [eval_one(d, env_over) for d in dates]
        agg = aggregate(per_date)
        all_results.append({'setting': setting['name'], 'env': env_over, 'aggregate': agg, 'per_date': per_date})

    # Print concise report
    print('\nBacktest Summary:')
    for r in all_results:
        agg = r['aggregate']
        topk = agg.get('topk', {})
        print(f"- {r['setting']}: AUC={agg.get('auc'):.3f} | Brier={agg.get('brier'):.3f} | "
              f"prec@10={topk.get('prec@10', float('nan')):.3f} | recall@10={topk.get('recall@10', float('nan')):.3f} | "
              f"prec@20={topk.get('prec@20', float('nan')):.3f} | recall@20={topk.get('recall@20', float('nan')):.3f}")

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump({'results': all_results}, f, indent=2)
        print(f"Saved results to {args.out}")


if __name__ == '__main__':
    main()
