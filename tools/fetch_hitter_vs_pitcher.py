#!/usr/bin/env python3
"""
Dedicated H2H (batter vs pitcher) fetcher.
- Builds team->opposing probable pitcher map from hydrated schedule
- Picks batters from lineups (preferred), else from saved player-stats, else from active roster
- Queries MLB people/{batter}/stats?stats=vsPlayer&group=hitting&opposingPlayerId=PID
- Saves:
  - data/hitter-vs-pitcher.js (for app)
  - data/hitter-vs-pitcher-YYYY-MM-DD.json (for history/debug)
Also logs simple counts for debugging.
"""
from __future__ import annotations
import os, json
from datetime import datetime
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(APP_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def http_json(url: str, timeout: int = 25) -> dict:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


def load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def ensure_schedule(date: str) -> dict:
    p = os.path.join(DATA_DIR, f'fresh-schedule-{date}.json')
    if os.path.exists(p):
        return load_json(p)
    # fallback to todays-schedule
    p2 = os.path.join(DATA_DIR, f'todays-schedule-{date}.json')
    if os.path.exists(p2):
        return load_json(p2)
    # fetch hydrated schedule directly
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}&hydrate=probablePitcher(note)"
    data = http_json(url)
    # save for reuse
    with open(os.path.join(DATA_DIR, f'fresh-schedule-{date}.json'), 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    return data


def team_id_to_abbr(team_id: int) -> str | None:
    meta = http_json(f"https://statsapi.mlb.com/api/v1/teams/{int(team_id)}")
    try:
        t0 = (meta.get('teams') or [{}])[0]
        return t0.get('abbreviation') or t0.get('teamCode')
    except Exception:
        return None


def map_teams_to_opp_pitcher(schedule: dict) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for d in (schedule.get('dates') or []):
        for g in d.get('games', []):
            teams = g.get('teams') or {}
            home = (teams.get('home') or {}).get('team') or {}
            away = (teams.get('away') or {}).get('team') or {}
            hp = (teams.get('home') or {}).get('probablePitcher') or {}
            ap = (teams.get('away') or {}).get('probablePitcher') or {}
            ha = home.get('abbreviation') or team_id_to_abbr(home.get('id'))
            aa = away.get('abbreviation') or team_id_to_abbr(away.get('id'))
            if ha and ap.get('id') and ap.get('fullName'):
                out[ha] = {'id': int(ap['id']), 'name': ap['fullName']}
            if aa and hp.get('id') and hp.get('fullName'):
                out[aa] = {'id': int(hp['id']), 'name': hp['fullName']}
    return out


def lineup_batters_for_team(date: str, team_abbr: str) -> List[Dict[str, Any]]:
    # prefer lineups file
    lp = os.path.join(DATA_DIR, f'lineups-{date}.json')
    if os.path.exists(lp):
        try:
            ldata = load_json(lp)
            for abbr, entries in (ldata.get('lineups') or {}).items():
                pass
            entries = (ldata.get('lineups') or {}).get(team_abbr) or []
            if entries:
                # entries are {name, slot}; we need batter id resolutions though
                # join to player-stats to get mlbam_id
                ps = load_json(os.path.join(DATA_DIR, f'player-stats-{date}.json')).get('players', [])
                idx = {p.get('name'): p for p in ps}
                enriched = []
                for e in entries:
                    p = idx.get(e.get('name'))
                    if p and p.get('mlbam_id'):
                        enriched.append({'mlbam_id': int(p['mlbam_id']), 'name': p['name']})
                if enriched:
                    return enriched
        except Exception:
            pass
    # fallback to players list
    ps = load_json(os.path.join(DATA_DIR, f'player-stats-{date}.json')).get('players', [])
    bat = [p for p in ps if p.get('team') == team_abbr and p.get('mlbam_id')]
    # heuristic: keep top 12 hitters (exclude pitchers)
    bat = [
        {'mlbam_id': int(p['mlbam_id']), 'name': p.get('name')}
        for p in bat if (p.get('position') not in ('Pitcher', 'Unknown'))
    ][:12]
    if bat:
        return bat
    # final fallback: active roster minus pitchers
    sched = ensure_schedule(date)
    # find team id for abbr
    team_ids = []
    for d in (sched.get('dates') or []):
        for g in d.get('games', []):
            for side in ('home', 'away'):
                t = (g.get('teams') or {}).get(side, {}).get('team') or {}
                ab = t.get('abbreviation') or team_id_to_abbr(t.get('id'))
                if ab == team_abbr:
                    if t.get('id'):
                        team_ids.append(int(t['id']))
    team_ids = list(dict.fromkeys(team_ids))
    for tid in team_ids:
        ro = http_json(f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster?rosterType=active")
        out = []
        for e in ro.get('roster', []) or []:
            if (e.get('position') or {}).get('code') == 'P':
                continue
            pid = (e.get('person') or {}).get('id')
            name = (e.get('person') or {}).get('fullName')
            if pid and name:
                out.append({'mlbam_id': int(pid), 'name': name})
        if out:
            return out[:12]
    return []


def fetch_bvp(bid: int, pid: int, year: int) -> Dict[str, Any]:
    # Use people/{id}/stats endpoint directly (more stable than hydrate variants)
    url = (
        f"https://statsapi.mlb.com/api/v1/people/{int(bid)}/stats?stats=vsPlayer&group=hitting&"
        f"opposingPlayerId={int(pid)}&season={year}&gameType=R"
    )
    data = http_json(url)
    # shape: { stats: [{ splits: [ { stat: {...}, opponent: {...}} ] }] }
    try:
        splits = []
        for blk in (data.get('stats') or []):
            splits.extend(blk.get('splits') or [])
        # choose the split with max PA (usually only one)
        best = None
        for sp in splits:
            st = sp.get('stat') or {}
            pa = int(st.get('plateAppearances') or 0)
            if best is None or pa > best[0]:
                best = (pa, st)
        if not best or best[0] == 0:
            return {}
        st = best[1]
        return {
            'pa': int(st.get('plateAppearances') or 0),
            'hr': int(st.get('homeRuns') or 0),
            'avg': float(st.get('avg') or 0),
            'slg': float(st.get('slg') or 0),
        }
    except Exception:
        return {}


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fetch batter-vs-pitcher H2H for a date')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    date = args.date
    year = datetime.strptime(date, '%Y-%m-%d').year

    schedule = ensure_schedule(date)
    opp = map_teams_to_opp_pitcher(schedule)
    teams = sorted(opp.keys())
    print(f"Opposing pitchers map for {len(teams)} teams")

    # Build batter list by team
    batters_by_team: Dict[str, List[Dict[str, Any]]] = {t: lineup_batters_for_team(date, t) for t in teams}
    total_batters = sum(len(v) for v in batters_by_team.values())
    print(f"Collected {total_batters} batter entries across teams")

    h2h: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {}
        for team, batters in batters_by_team.items():
            pitcher = opp.get(team)
            if not pitcher:
                continue
            pid = int(pitcher['id'])
            for b in batters:
                bid = int(b['mlbam_id'])
                bname = b.get('name')
                futures[ex.submit(fetch_bvp, bid, pid, year)] = (bname, pitcher['name'])
        for f in as_completed(futures):
            bname, pname = futures[f]
            stats = f.result() or {}
            if stats:
                h2h.setdefault(bname, {})[pname] = stats

    # Save files
    js_path = os.path.join(DATA_DIR, 'hitter-vs-pitcher.js')
    with open(js_path, 'w', encoding='utf-8') as f:
        f.write('const hitterVsPitcherData = ')
        json.dump(h2h, f, indent=2)
        f.write(';' + "\n")
    json_path = os.path.join(DATA_DIR, f'hitter-vs-pitcher-{date}.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({'date': date, 'h2h': h2h}, f, indent=2)
    print(f"Saved {js_path}")
    print(f"Saved {json_path}")
    print(f"H2H pairs written: {sum(len(v) for v in h2h.values())}")


if __name__ == '__main__':
    main()
