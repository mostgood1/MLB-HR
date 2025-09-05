#!/usr/bin/env python3
from __future__ import annotations
"""
Fetch player HR prop odds (anytime HR) using The Odds API (if ODDS_API_KEY is set).

Output: data/player-hr-odds-YYYY-MM-DD.json
Structure:
{
  "date": "YYYY-MM-DD",
  "source": "the-odds-api",
  "players": {
     "Aaron Judge": {
        "best_american": 250,
        "best_prob": 0.2857,
        "offers": [ {"book": "DraftKings", "american": 250, "prob": 0.2857, "market": "player_home_runs"} ]
     },
     ...
  }
}

Notes:
- The Odds API plan must include player props. Market keys for HR can vary by book.
- We try a set of candidate market keys; override via env PLAYER_HR_MARKETS (comma-separated).
"""
import os, json
from datetime import datetime
from typing import Any, Dict, List
import requests

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(APP_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def save_json(obj: Any, path: str):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2)
    print(f"Saved {path}")


def american_to_prob(odds) -> float | None:
    try:
        o = float(odds)
    except Exception:
        return None
    if o < 0:
        return (-o) / ((-o) + 100.0)
    return 100.0 / (o + 100.0)


def norm_player_name(n: str) -> str:
    import unicodedata, re
    s = unicodedata.normalize('NFKD', str(n or '')).encode('ascii', 'ignore').decode('ascii')
    s = re.sub(r"\s+", ' ', s).strip()
    return s


def fetch_player_hr_odds(date: str):
    api_key = os.getenv('ODDS_API_KEY') or os.getenv('THE_ODDS_API_KEY')
    out_path = os.path.join(DATA_DIR, f'player-hr-odds-{date}.json')
    if not api_key:
        print('[player-hr] No ODDS_API_KEY found; skipping player HR odds')
        save_json({'date': date, 'source': None, 'players': {}}, out_path)
        return
    # Candidate market keys; override via env
    default_markets = [
        'player_home_runs',
        'player_homeruns',
        'player_to_hit_a_home_run',
        'player_to_hit_home_run',
        'player_hr',
        'player_anytime_home_run',
        'player_to_hit_hr'
    ]
    env_markets = os.getenv('PLAYER_HR_MARKETS')
    markets = [m.strip() for m in env_markets.split(',')] if env_markets else default_markets
    # Build request; request multiple markets comma-separated (The Odds API supports multiple)
    mk = ','.join(dict.fromkeys(markets))
    base = 'https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/'
    url = f"{base}?regions=us,us2,eu,uk,au&markets={mk}&oddsFormat=american&dateFormat=iso&apiKey={api_key}"
    try:
        r = requests.get(url, timeout=45)
        if r.status_code != 200:
            try:
                dbg = {'date': date, 'status': r.status_code, 'text': r.text[:800], 'url': url}
                save_json(dbg, os.path.join(DATA_DIR, f'odds-player-debug-{date}.json'))
            except Exception:
                pass
            save_json({'date': date, 'source': 'the-odds-api', 'players': {}}, out_path)
            return
        games = r.json()
        try:
            save_json({'date': date, 'markets': markets, 'count': (len(games) if isinstance(games, list) else None), 'raw': games}, os.path.join(DATA_DIR, f'odds-player-raw-{date}.json'))
        except Exception:
            pass
    except Exception as e:
        print('[player-hr] request failed:', e)
        save_json({'date': date, 'source': 'the-odds-api', 'players': {}}, out_path)
        return

    # Parse outcomes across books/markets
    players: Dict[str, Dict[str, Any]] = {}
    if isinstance(games, list):
        for g in games:
            for bk in g.get('bookmakers', []) or []:
                book_name = bk.get('title') or bk.get('key')
                for mk in bk.get('markets', []) or []:
                    mkey = mk.get('key') or ''
                    # Only consider candidate markets
                    if mkey not in markets:
                        # heuristic: allow keys that contain 'player' and 'home' in case of variants
                        lk = (mkey or '').lower()
                        if not ('player' in lk and 'home' in lk):
                            continue
                    outs = mk.get('outcomes', []) or []
                    for out in outs:
                        # Expected fields: name (player), price (american odds) or odds
                        pname = norm_player_name(out.get('name') or out.get('description') or out.get('player') or '')
                        if not pname:
                            continue
                        american = out.get('price') if out.get('price') is not None else out.get('odds')
                        try:
                            american_i = int(float(american))
                        except Exception:
                            continue
                        prob = american_to_prob(american_i)
                        if prob is None:
                            continue
                        rec = players.get(pname)
                        offer = {'book': book_name, 'american': american_i, 'prob': round(prob, 5), 'market': mkey}
                        if not rec:
                            players[pname] = {
                                'best_american': american_i,
                                'best_prob': round(prob, 5),
                                'offers': [offer]
                            }
                        else:
                            rec['offers'].append(offer)
                            # Keep "best" as the highest probability (lowest payout) for conservative baseline
                            if prob > rec.get('best_prob', 0.0):
                                rec['best_prob'] = round(prob, 5)
                                rec['best_american'] = american_i

    save_json({'date': date, 'source': 'the-odds-api', 'players': players}, out_path)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fetch player HR prop odds (anytime HR) if available')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    fetch_player_hr_odds(args.date)


if __name__ == '__main__':
    main()
