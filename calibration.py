"""Calibration utilities for mapping raw model probabilities to calibrated probabilities.

Supports:
  - Platt (logistic) scaling
  - Isotonic regression (simple PAV implementation)

No external deps to keep deployment light.
"""
from __future__ import annotations

import json
import math
import os
from datetime import datetime
from typing import Dict, List, Optional


def _sigmoid(z: float) -> float:
	if z < -60:
		return 0.0
	if z > 60:
		return 1.0
	return 1.0 / (1.0 + math.exp(-z))


def _logit(p: float) -> float:
	p = min(1 - 1e-12, max(1e-12, p))
	return math.log(p / (1 - p))


def load_calibrator(path: str) -> Optional[Dict]:
	if not path or not os.path.exists(path):
		return None
	try:
		with open(path, 'r', encoding='utf-8') as f:
			data = json.load(f)
		if data.get('method') in ('platt', 'isotonic'):
			return data
	except Exception:
		return None
	return None


def apply_calibration(raw_p: float, calibrator: Optional[Dict]) -> float:
	if calibrator is None:
		return raw_p
	method = calibrator.get('method')
	if method == 'platt':
		params = calibrator.get('params') or {}
		alpha = float(params.get('alpha', 1.0))
		beta = float(params.get('beta', 0.0))
		z = alpha * _logit(raw_p) + beta
		return min(1.0, max(0.0, _sigmoid(z)))
	if method == 'isotonic':
		params = calibrator.get('params') or {}
		xs = params.get('x') or []
		ys = params.get('y') or []
		if not xs or not ys or len(xs) != len(ys):
			return raw_p
		if raw_p <= xs[0]:
			return float(ys[0])
		if raw_p >= xs[-1]:
			return float(ys[-1])
		lo, hi = 0, len(xs) - 1
		while lo <= hi:
			mid = (lo + hi) // 2
			xv = xs[mid]
			if abs(xv - raw_p) < 1e-12:
				return float(ys[mid])
			if xv < raw_p:
				lo = mid + 1
			else:
				hi = mid - 1
		i2 = lo
		i1 = max(0, i2 - 1)
		x1, x2 = xs[i1], xs[i2]
		y1, y2 = ys[i1], ys[i2]
		if x2 == x1:
			return float(y1)
		t = (raw_p - x1) / (x2 - x1)
		return float(y1 + t * (y2 - y1))
	return raw_p


def fit_platt(examples: List[Dict[str, float]], max_iter: int = 250, lr: float = 0.1) -> Dict:
	alpha = 1.0
	beta = 0.0
	n = len(examples)
	if n == 0:
		raise ValueError('No examples for calibration')
	for _ in range(max_iter):
		grad_a = 0.0
		grad_b = 0.0
		for ex in examples:
			p_raw = min(1 - 1e-9, max(1e-9, float(ex['p'])))
			y = float(ex['y'])
			z = alpha * _logit(p_raw) + beta
			p_hat = _sigmoid(z)
			diff = p_hat - y
			x = _logit(p_raw)
			grad_a += diff * x
			grad_b += diff
		grad_a /= n
		grad_b /= n
		grad_a += 1e-4 * alpha
		grad_b += 1e-4 * beta
		alpha -= lr * grad_a
		beta -= lr * grad_b
		if abs(grad_a) < 1e-6 and abs(grad_b) < 1e-6:
			break
	return {
		'method': 'platt',
		'fitted_at': datetime.utcnow().isoformat(),
		'n_samples': n,
		'params': {'alpha': alpha, 'beta': beta}
	}


def fit_isotonic(examples: List[Dict[str, float]]) -> Dict:
	ex_sorted = sorted(examples, key=lambda e: e['p'])
	blocks = []
	for ex in ex_sorted:
		p = float(ex['p'])
		y = float(ex['y'])
		blocks.append({'weight': 1.0, 'sum': y, 'p_min': p, 'p_max': p})
		while len(blocks) >= 2 and (blocks[-2]['sum'] / blocks[-2]['weight']) > (blocks[-1]['sum'] / blocks[-1]['weight']):
			b2 = blocks.pop()
			b1 = blocks.pop()
			merged = {
				'weight': b1['weight'] + b2['weight'],
				'sum': b1['sum'] + b2['sum'],
				'p_min': b1['p_min'],
				'p_max': b2['p_max']
			}
			blocks.append(merged)
	xs = []
	ys = []
	for b in blocks:
		avg_y = b['sum'] / b['weight']
		xs.append(b['p_min'])
		ys.append(avg_y)
		if b['p_max'] != b['p_min']:
			xs.append(b['p_max'])
			ys.append(avg_y)
	uniq = []
	last_x = None
	for x, y in zip(xs, ys):
		if last_x is None or x > last_x:
			uniq.append((x, y))
			last_x = x
		else:
			uniq[-1] = (x, y)
	xs_final = [u[0] for u in uniq]
	ys_final = [u[1] for u in uniq]
	return {
		'method': 'isotonic',
		'fitted_at': datetime.utcnow().isoformat(),
		'n_samples': len(examples),
		'params': {'x': xs_final, 'y': ys_final}
	}


def save_calibrator(model: Dict, path: str):
	tmp = path + '.tmp'
	with open(tmp, 'w', encoding='utf-8') as f:
		json.dump(model, f, indent=2)
	os.replace(tmp, path)


def fit_and_save(examples: List[Dict[str, float]], method: str, path: str) -> Dict:
	if method == 'platt':
		model = fit_platt(examples)
	elif method == 'isotonic':
		model = fit_isotonic(examples)
	else:
		raise ValueError(f'Unsupported method: {method}')
	save_calibrator(model, path)
	return model
