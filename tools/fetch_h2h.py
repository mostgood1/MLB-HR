#!/usr/bin/env python3
"""
Fetch H2H batter-vs-pitcher data for today's matchups and write data/hitter-vs-pitcher.js
in the expected JS variable format used by generate_hr_scores_core.py.

Structure expected in JS:
const hitterVsPitcherData = {
  "Batter Name": {
     "Pitcher Name": { "pa": 10, "hr": 1, "avg": 0.300, "slg": 0.600 }
  },
  ...
};

We use MLB StatsAPI to get batter vs pitcher splits by querying for each batter
against his opposing probable pitcher.
"""
from __future__ import annotations
import os, json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(APP_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def http_json(url: str, timeout: int = 20) -> dict:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def load_schedule(date: str) -> dict:
    path = os.path.join(DATA_DIR, f'fresh-schedule-{date}.json')
    if not os.path.exists(path):
        path = os.path.join(DATA_DIR, 'todays-schedule.json')
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def players_list_for_teams(date: str, teams: set[str]) -> list[dict]:
    # Use players file saved earlier
    pf = os.path.join(DATA_DIR, f'player-stats-{date}.json')
    players = []
    if os.path.exists(pf):
        with open(pf, 'r', encoding='utf-8') as f:
            players = json.load(f).get('players', [])
    return [p for p in players if (p.get('team') in teams and p.get('mlbam_id'))]


def team_id_to_abbr(team_id: int) -> str | None:
    try:
        url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}"
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        t0 = (data.get('teams') or [{}])[0]
        return t0.get('abbreviation') or t0.get('teamCode')
    except Exception:
        return None


def fetch_bvps_for_pair(bid: int, pid: int) -> dict:
    """Fetch batter vs pitcher splits via StatsAPI for a batter/pitcher pair."""
    try:
        url = f"https://statsapi.mlb.com/api/v1/people/{bid}?hydrate=stats(group=hitting,stats=vsPlayer,opposingPlayerId={pid})"
        data = http_json(url)
        splits = (((data.get('people') or [{}])[0].get('stats') or [{}])[0].get('splits') or [])
        if not splits:
            return {}
        s = splits[0].get('stat') or {}
        pa = int(s.get('plateAppearances') or 0)
        hr = int(s.get('homeRuns') or 0)
        avg = float(s.get('avg') or 0)
        slg = float(s.get('slg') or 0)
        return { 'pa': pa, 'hr': hr, 'avg': avg, 'slg': slg }
    except Exception:
        return {}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    date = args.date

    sched = load_schedule(date)
    dates = sched.get('dates') or []
    games = (dates[0].get('games') if dates else []) or sched.get('games') or []

    # Map teams (by abbreviation) to opposing probable pitcher
    opp_pitcher_by_team: dict[str, dict] = {}
    for g in games:
        teams = g.get('teams') or {}
        home = (teams.get('home') or {}).get('team') or {}
        away = (teams.get('away') or {}).get('team') or {}
        hp = (teams.get('home') or {}).get('probablePitcher') or {}
        ap = (teams.get('away') or {}).get('probablePitcher') or {}
        # Derive abbreviations (schedule often omits them)
        ha = home.get('abbreviation') or team_id_to_abbr(home.get('id'))
        aa = away.get('abbreviation') or team_id_to_abbr(away.get('id'))
        if ha and ap.get('id') and ap.get('fullName'):
            opp_pitcher_by_team[ha] = {'id': int(ap['id']), 'name': ap['fullName']}
        if aa and hp.get('id') and hp.get('fullName'):
            opp_pitcher_by_team[aa] = {'id': int(hp['id']), 'name': hp['fullName']}

    teams_today = set(opp_pitcher_by_team.keys())
    batters = players_list_for_teams(date, teams_today)

    # For each batter, query vs his opposing pitcher
    tasks = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = {}
        for b in batters:
            team = b.get('team')
            opp = opp_pitcher_by_team.get(team)
            if not opp:
                continue
            futures[ex.submit(fetch_bvps_for_pair, int(b['mlbam_id']), int(opp['id']))] = (b.get('name'), opp['name'])

        h2h: dict[str, dict[str, dict]] = {}
        for f in as_completed(futures):
            batter, pitcher = futures[f]
            stats = f.result() or {}
            if stats:
                h2h.setdefault(batter, {})[pitcher] = stats

    # Write JS (for app) and date-stamped JSON (for history)
    out_js = os.path.join(DATA_DIR, 'hitter-vs-pitcher.js')
    with open(out_js, 'w', encoding='utf-8') as f:
        f.write('const hitterVsPitcherData = ')
        json.dump(h2h, f, indent=2)
        f.write(';' + "\n")
    out_json = os.path.join(DATA_DIR, f'hitter-vs-pitcher-{date}.json')
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump({'date': date, 'h2h': h2h}, f, indent=2)
    print(f"Saved {out_js}")
    print(f"Saved {out_json}")


if __name__ == '__main__':
    main()
