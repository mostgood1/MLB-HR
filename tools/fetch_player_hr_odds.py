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
    # Remove trailing team abbreviation in parentheses e.g., "Aaron Judge (NYY)"
    s = re.sub(r"\s*\([A-Z]{2,4}\)$", '', s)
    return s


def build_browser_session() -> requests.Session:
    s = requests.Session()
    # Desktop Chrome UA; some sportsbooks block default Python UA
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://sportsbook.draftkings.com',
        'Referer': 'https://sportsbook.draftkings.com/leagues/baseball/mlb'
    })
    # Optional proxy support
    proxy = os.getenv('HTTP_PROXY') or os.getenv('http_proxy')
    if proxy:
        s.proxies.update({'http': proxy, 'https': proxy})
    return s


def fetch_player_hr_odds(date: str):
    # Load .env if present so standalone runs pick up the key
    try:
        env_path = os.path.join(APP_DIR, '.env')
        if os.path.exists(env_path):
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith('#'):
                        continue
                    if '=' in s:
                        k, v = s.split('=', 1)
                        k = k.strip(); v = v.strip().strip('"').strip("'")
                        if k and v and k not in os.environ:
                            os.environ[k] = v
    except Exception:
        pass
    api_key = os.getenv('ODDS_API_KEY') or os.getenv('THE_ODDS_API_KEY')
    out_path = os.path.join(DATA_DIR, f'player-hr-odds-{date}.json')
    if not api_key:
        print('[player-hr] No ODDS_API_KEY found; will try DraftKings fallback')
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
    games = []
    if api_key:
        try:
            r = requests.get(url, timeout=45)
            if r.status_code != 200:
                try:
                    dbg = {'date': date, 'status': r.status_code, 'text': r.text[:800], 'url': url}
                    save_json(dbg, os.path.join(DATA_DIR, f'odds-player-debug-{date}.json'))
                except Exception:
                    pass
                # Try discover valid markets for this account and retry once
                try:
                    mk_url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds-markets/?apiKey={api_key}"
                    mr = requests.get(mk_url, timeout=30)
                    if mr.status_code == 200:
                        ml = mr.json() if isinstance(mr.json(), list) else []
                        cand = [m for m in ml if isinstance(m, str) and ('player' in m.lower()) and (('home' in m.lower()) or ('hr' in m.lower()) or ('homer' in m.lower()))]
                        cand = list(dict.fromkeys(cand))[:5]
                        if cand:
                            url2 = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/?regions=us,us2,eu,uk,au&markets={','.join(cand)}&oddsFormat=american&dateFormat=iso&apiKey={api_key}"
                            r2 = requests.get(url2, timeout=45)
                            if r2.status_code == 200:
                                games = r2.json()
                                try:
                                    save_json({'date': date, 'markets': cand, 'count': (len(games) if isinstance(games, list) else None), 'raw': games}, os.path.join(DATA_DIR, f'odds-player-raw-{date}.json'))
                                except Exception:
                                    pass
                            else:
                                try:
                                    save_json({'date': date, 'status': r2.status_code, 'text': r2.text[:800], 'url': url2}, os.path.join(DATA_DIR, f'odds-player-debug-2-{date}.json'))
                                except Exception:
                                    pass
                except Exception:
                    pass
            else:
                games = r.json()
                try:
                    save_json({'date': date, 'markets': markets, 'count': (len(games) if isinstance(games, list) else None), 'raw': games}, os.path.join(DATA_DIR, f'odds-player-raw-{date}.json'))
                except Exception:
                    pass
        except Exception as e:
            print('[player-hr] the-odds-api request failed:', e)

    # Parse outcomes across books/markets
    players: Dict[str, Dict[str, Any]] = {}
    if isinstance(games, list) and games:
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

    # If empty, try DraftKings fallback (public JSON used by the sportsbook site)
    if not players:
        try:
            dk_url = 'https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/84240?format=json'
            sess = build_browser_session()
            dr = sess.get(dk_url, timeout=45)
            if dr.status_code == 200:
                dj = dr.json() or {}
                # Build eventId -> startDate map to filter by target date
                eg = (dj.get('eventGroup') or {})
                events = eg.get('events') or []
                date_events = set()
                for ev in events:
                    try:
                        eid = str(ev.get('eventId') or ev.get('eventId'.lower()) or ev.get('id'))
                        sd = ev.get('startDate') or ''
                        if eid and isinstance(sd, str) and sd.startswith(date):
                            date_events.add(eid)
                    except Exception:
                        continue
                # Traverse offer categories to find player HR subcategories
                cats = eg.get('offerCategories') or []
                def is_hr_subcat(name: str) -> bool:
                    s = (name or '').lower()
                    return ('home run' in s) or ('to hit a home run' in s) or ('home-run' in s) or ('hr' == s.strip())
                for cat in cats:
                    try:
                        cname = (cat.get('name') or '').lower()
                        if 'player' not in cname:
                            # Some sites embed the subcat name fully; still scan
                            pass
                        descs = cat.get('offerSubcategoryDescriptors') or []
                        for d in descs:
                            sub_name = d.get('name') or d.get('label') or ''
                            sub = d.get('offerSubcategory') or {}
                            if not is_hr_subcat(sub_name):
                                # Also check descriptor children names for HR hint
                                if not any(is_hr_subcat((c.get('name') or '')) for c in (d.get('children') or [])):
                                    continue
                            offers = sub.get('offers') or []
                            # offers is list of lists per event
                            for group in offers:
                                for offer in (group or []):
                                    eid = str(offer.get('eventId') or '')
                                    if date_events and eid and eid not in date_events:
                                        continue
                                    for out in (offer.get('outcomes') or []):
                                        pname = norm_player_name(out.get('label') or out.get('participant') or out.get('name') or '')
                                        if not pname:
                                            continue
                                        odds_amer = out.get('oddsAmerican') or out.get('americanOdds') or out.get('odds')
                                        try:
                                            american_i = int(str(odds_amer).replace('+','').replace('−','-'))
                                        except Exception:
                                            continue
                                        prob = american_to_prob(american_i)
                                        if prob is None:
                                            continue
                                        offer_rec = {'book': 'DraftKings', 'american': american_i, 'prob': round(prob, 5), 'market': sub_name}
                                        rec = players.get(pname)
                                        if not rec:
                                            players[pname] = {'best_american': american_i, 'best_prob': round(prob,5), 'offers': [offer_rec]}
                                        else:
                                            rec['offers'].append(offer_rec)
                                            if prob > rec.get('best_prob', 0.0):
                                                rec['best_prob'] = round(prob, 5)
                                                rec['best_american'] = american_i
                    except Exception:
                        continue
            else:
                try:
                    save_json({'date': date, 'status': dr.status_code, 'text': dr.text[:800], 'url': dk_url}, os.path.join(DATA_DIR, f'odds-dk-debug-{date}.json'))
                except Exception:
                    pass
        except Exception as e:
            print('[player-hr][dk] fallback failed:', e)

    # If still empty, try Bovada public JSON
    if not players:
        try:
            sess = build_browser_session()
            # Bovada MLB events with descriptions; market names vary by locale
            bv_url = 'https://www.bovada.lv/services/sports/event/v2/events/A/description/baseball/mlb'
            br = sess.get(bv_url, timeout=45)
            if br.status_code == 200:
                bj = br.json() or []
                # bj is typically a list with one or more sport trees
                def walk_events(node):
                    for item in (node or []):
                        events = item.get('events') or []
                        for ev in events:
                            yield ev
                        # some nesting under 'children'
                        for ch in (item.get('children') or []):
                            yield from walk_events([ch])
                def is_hr_market(name: str) -> bool:
                    s = (name or '').lower()
                    return ('to hit a home run' in s) or ('home run' in s and 'to hit' in s) or ('home runs' in s and 'player' in s)
                # Build date filter similar to DK by startTime
                for ev in walk_events(bj):
                    try:
                        st = ev.get('startTime')  # epoch ms
                        # optional: filter by date string match
                        # Bovada uses ms epoch; we won't filter strictly to avoid TZ pitfalls
                        dgs = ev.get('displayGroups') or []
                        for dg in dgs:
                            for mk in (dg.get('markets') or []):
                                mname = mk.get('description') or mk.get('name') or ''
                                if not is_hr_market(mname):
                                    continue
                                for out in (mk.get('outcomes') or []):
                                    try:
                                        pname = norm_player_name(out.get('description') or out.get('name') or '')
                                        if not pname:
                                            continue
                                        price = (out.get('price') or {})
                                        amer = price.get('american') or price.get('americanDisplay') or price.get('a')
                                        if amer is None:
                                            continue
                                        american_i = int(str(amer).replace('+','').replace('−','-'))
                                        prob = american_to_prob(american_i)
                                        if prob is None:
                                            continue
                                        offer_rec = {'book': 'Bovada', 'american': american_i, 'prob': round(prob, 5), 'market': mname}
                                        rec = players.get(pname)
                                        if not rec:
                                            players[pname] = {'best_american': american_i, 'best_prob': round(prob,5), 'offers': [offer_rec]}
                                        else:
                                            rec['offers'].append(offer_rec)
                                            if prob > rec.get('best_prob', 0.0):
                                                rec['best_prob'] = round(prob, 5)
                                                rec['best_american'] = american_i
                                    except Exception:
                                        continue
                    except Exception:
                        continue
            else:
                try:
                    save_json({'date': date, 'status': br.status_code, 'text': br.text[:800], 'url': bv_url}, os.path.join(DATA_DIR, f'odds-bovada-debug-{date}.json'))
                except Exception:
                    pass
        except Exception as e:
            print('[player-hr][bovada] fallback failed:', e)

    # Manual override: if a manual odds file exists, merge/replace
    try:
        man_path = os.path.join(DATA_DIR, f'player-hr-odds-{date}.manual.json')
        if os.path.exists(man_path):
            with open(man_path, 'r', encoding='utf-8') as f:
                manual = json.load(f) or {}
            mplayers = manual.get('players') if isinstance(manual, dict) else manual
            if isinstance(mplayers, dict) and mplayers:
                # Replace existing with manual where provided; keep others
                base_players = players.copy()
                base_players.update(mplayers)
                players = base_players
    except Exception:
        pass

    src = None
    if games:
        src = 'the-odds-api'
    elif players:
        # Determine which fallback populated first by checking any offer's book
        any_offers = next(iter(players.values()), {}).get('offers') or []
        src = (any_offers[0].get('book') if any_offers else None)
    save_json({'date': date, 'source': src, 'players': players}, out_path)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fetch player HR prop odds (anytime HR) if available')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    fetch_player_hr_odds(args.date)


if __name__ == '__main__':
    main()
