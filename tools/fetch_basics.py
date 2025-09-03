#!/usr/bin/env python3
"""
Self-contained fetchers to populate hr_app/data with the minimal inputs needed:
- todays-schedule.json (MLB StatsAPI)
- fresh-schedule-YYYY-MM-DD.json
- fetch_daily_player_stats -> player-stats-YYYY-MM-DD.json
- fetch_todays_pitchers -> pitcher-stats-YYYY-MM-DD.json
- fetch_recent_performance -> recent-performance-YYYY-MM-DD.json
"""
from __future__ import annotations
import os, json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Iterable
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(APP_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def http_json(url: str, tries: int = 3, timeout: int = 20) -> dict:
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            last = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last = e
    raise last or RuntimeError('request failed')


def save(obj: Any, path: str):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2)
    print(f"Saved {path}")


def fetch_schedule(date: str) -> dict:
    # Use hydrate to include probable starters directly in schedule
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}&hydrate=probablePitcher(note)"
    return http_json(url)


def fetch_players_simple(date: str) -> dict:
    """
    Minimal roster+season stats per team via MLB StatsAPI; simplified to name/team/avg/slg/HR/position.
    """
    sched = fetch_schedule(date)
    teams = set()
    team_abbr_by_id: dict[int, str] = {}
    for d in sched.get('dates', []):
        for g in d.get('games', []):
            for side in ('home', 'away'):
                t = (g.get('teams') or {}).get(side, {}).get('team') or {}
                if t.get('id'):
                    tid = int(t['id'])
                    teams.add(tid)
                    abbr = t.get('abbreviation')
                    if abbr:
                        team_abbr_by_id[tid] = abbr

    # Ensure we have abbreviations for all team ids
    for tid in list(teams):
        if tid in team_abbr_by_id:
            continue
        try:
            team_meta = http_json(f"https://statsapi.mlb.com/api/v1/teams/{tid}")
            t0 = (team_meta.get('teams') or [{}])[0]
            abbr = t0.get('abbreviation') or t0.get('teamCode')
            if abbr:
                team_abbr_by_id[tid] = abbr
        except Exception:
            pass

    season_year = datetime.strptime(date, '%Y-%m-%d').year
    players = []
    player_tasks = []
    # For deduping duplicate roster appearances, keep best per player id
    best_by_pid: Dict[int, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=16) as executor:
        for tid in teams:
            ro = http_json(f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster?rosterType=active")
            for e in ro.get('roster', []):
                pid = e.get('person', {}).get('id')
                name = e.get('person', {}).get('fullName')
                pos = e.get('position', {}).get('name') or 'Unknown'
                if not pid:
                    continue
                team_abbr = team_abbr_by_id.get(tid) or e.get('parentTeamAbbreviation') or ''
                # Submit fetching stats in parallel
                # Use dedicated stats endpoint for reliability
                player_tasks.append(executor.submit(
                    lambda pid=pid, name=name, team_abbr=team_abbr, pos=pos, season_year=season_year: (
                        pid,
                        http_json(
                            f"https://statsapi.mlb.com/api/v1/people/{pid}/stats?stats=season&group=hitting&season={season_year}&gameType=R"
                        ),
                        name,
                        team_abbr,
                        pos,
                    )
                ))
        for future in as_completed(player_tasks):
            pid, stats, name, team_abbr, pos = future.result()
            avg = slg = 0.0
            hr = 0
            try:
                # people/{id}/stats returns { stats: [{group, type, splits: [...] }] }
                entries = []
                for blk in (stats.get('stats') or []):
                    grp = blk.get('group')
                    if isinstance(grp, dict):
                        grp_name = str(grp.get('displayName') or grp.get('code') or grp.get('name') or '').lower()
                    else:
                        grp_name = str(grp or '').lower()
                    if grp_name == 'hitting':
                        entries.extend(blk.get('splits') or [])
                best = None
                for sp in entries:
                    stat = sp.get('stat') or {}
                    pa = float(stat.get('plateAppearances') or 0)
                    if best is None or pa > float((best.get('stat') or {}).get('plateAppearances') or 0):
                        best = sp
                if best:
                    st = best.get('stat') or {}
                    # Some APIs return strings like ".256"; float handles that
                    try:
                        avg = float(st.get('avg') or 0)
                    except Exception:
                        avg = 0.0
                    try:
                        slg = float(st.get('slg') or 0)
                    except Exception:
                        slg = 0.0
                    hr = int(st.get('homeRuns') or 0)
            except Exception:
                pass
            # Keep the best record per player by higher PA proxy (avg of PA and HR)
            cur = best_by_pid.get(pid)
            new_row = {
                'name': name,
                'team': team_abbr,
                'mlbam_id': pid,
                'battingAvg': avg,
                'sluggingPerc': slg,
                'homeRuns': hr,
                'position': pos
            }
            if not cur:
                best_by_pid[pid] = new_row
            else:
                # Prefer row with non-zero PA/HR implied via slg or hr
                score_cur = (cur.get('homeRuns') or 0) + (cur.get('sluggingPerc') or 0)
                score_new = (new_row.get('homeRuns') or 0) + (new_row.get('sluggingPerc') or 0)
                if score_new > score_cur:
                    best_by_pid[pid] = new_row
    players = list(best_by_pid.values())
    return {'date': date, 'players': players}


def fetch_pitchers_simple(date: str) -> dict:
    sched = fetch_schedule(date)
    pitchers: List[dict] = []
    want: Dict[int, str] = {}
    team_ids_today: List[int] = []
    # First try probablePitcher
    for d in sched.get('dates', []):
        for g in d.get('games', []):
            teams = g.get('teams') or {}
            # Track team ids playing today for potential fallback
            try:
                home_team = teams.get('home', {}).get('team', {})
                away_team = teams.get('away', {}).get('team', {})
                if home_team.get('id'):
                    team_ids_today.append(int(home_team['id']))
                if away_team.get('id'):
                    team_ids_today.append(int(away_team['id']))
            except Exception:
                pass
            for side in ('home', 'away'):
                pp = teams.get(side, {}).get('probablePitcher') or {}
                pid = pp.get('id')
                name = pp.get('fullName') or pp.get('name')
                if pid and name:
                    want[int(pid)] = name
    # For games without probable, infer starters from boxscore
    def infer_starters_from_box(g: dict):
        pk = g.get('gamePk')
        if not pk:
            return []
        try:
            data = http_json(f"https://statsapi.mlb.com/api/v1/game/{int(pk)}/boxscore")
        except Exception:
            return []
        out = []
        for side in ('home', 'away'):
            td = (data.get('teams') or {}).get(side) or {}
            players = td.get('players') or {}
            # Heuristic: pitcher with gamesStarted >= 1
            starter_pid = None
            starter_name = None
            for k, p in players.items():
                pos = (p.get('position') or {}).get('code')
                if pos != '1':
                    continue
                stats = (p.get('stats') or {}).get('pitching') or {}
                gs = int(stats.get('gamesStarted') or 0)
                if gs >= 1:
                    starter_pid = (p.get('person') or {}).get('id')
                    starter_name = (p.get('person') or {}).get('fullName')
                    break
            # Fallback: first pitcher listed for the team
            if not starter_pid:
                plist = td.get('pitchers') or []
                if plist:
                    try:
                        starter_pid = int(plist[0])
                    except Exception:
                        starter_pid = None
                    if starter_pid:
                        # find name in players map (keys like ID_12345)
                        key = f"ID_{starter_pid}"
                        ppd = players.get(key) or {}
                        starter_name = (ppd.get('person') or {}).get('fullName')
            if starter_pid and starter_name:
                out.append((int(starter_pid), starter_name))
        return out

    for d in sched.get('dates', []):
        for g in d.get('games', []):
            for pid, nm in infer_starters_from_box(g):
                want.setdefault(pid, nm)

    # Fallback: if no probables could be determined (or very few),
    # pick a likely starter per team based on max gamesStarted among active pitchers
    def _pick_likely_starter_for_team(tid: int):
        try:
            ro = http_json(f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster?rosterType=active")
        except Exception:
            return None
        best = None  # (gamesStarted, pid, name)
        for e in ro.get('roster', []) or []:
            pos = (e.get('position') or {}).get('code') or ''
            if pos != 'P':
                continue
            pid = (e.get('person') or {}).get('id')
            name = (e.get('person') or {}).get('fullName')
            if not pid or not name:
                continue
            try:
                stats = http_json(
                    f"https://statsapi.mlb.com/api/v1/people/{int(pid)}/stats?stats=season&group=pitching&season={datetime.strptime(date, '%Y-%m-%d').year}&gameType=R"
                )
                entries = []
                for blk in (stats.get('stats') or []):
                    grp = blk.get('group')
                    if isinstance(grp, dict):
                        grp_name = str(grp.get('displayName') or grp.get('code') or grp.get('name') or '').lower()
                    else:
                        grp_name = str(grp or '').lower()
                    if grp_name == 'pitching':
                        entries.extend(blk.get('splits') or [])
                gs_max = 0
                st_best = None
                for sp in entries:
                    st = sp.get('stat') or {}
                    gs = int(st.get('gamesStarted') or 0)
                    if gs >= gs_max:
                        gs_max = gs
                        st_best = st
                if st_best is not None:
                    # prefer pitchers with at least some GS; but keep the max even if 0 so we produce something
                    score = gs_max
                    if best is None or score > best[0]:
                        best = (score, int(pid), name, st_best)
            except Exception:
                continue
        return best

    # If we have fewer than half the teams covered, attempt fallback selection for missing teams
    try:
        unique_team_ids = sorted(set(team_ids_today))
    except Exception:
        unique_team_ids = []
    team_has_probable = set()
    if want:
        # Map probables to teams by roster lookup once to avoid duplicates
        try:
            for tid in unique_team_ids:
                ro = http_json(f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster?rosterType=active")
                pids = {int((e.get('person') or {}).get('id')) for e in (ro.get('roster') or []) if (e.get('person') or {}).get('id')}
                if any(pid in want for pid in pids):
                    team_has_probable.add(tid)
        except Exception:
            pass
    need_fallback = len(team_has_probable) < max(1, len(unique_team_ids) // 2)
    if need_fallback:
        for tid in unique_team_ids:
            # Skip teams that already have a probable in 'want'
            try:
                ro = http_json(f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster?rosterType=active")
                roster_pids = {int((e.get('person') or {}).get('id')) for e in (ro.get('roster') or []) if (e.get('person') or {}).get('id')}
            except Exception:
                roster_pids = set()
            if any(pid in want for pid in roster_pids):
                continue
            picked = _pick_likely_starter_for_team(tid)
            if picked:
                _, pid, name, _ = picked
                want.setdefault(pid, name)

    # Fetch season pitching stats for all wanted pitchers
    season_year = datetime.strptime(date, '%Y-%m-%d').year
    pitcher_tasks = []
    best_by_pid: Dict[int, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=12) as executor:
        for pid, name in want.items():
            pitcher_tasks.append(executor.submit(
                lambda pid=pid, name=name, season_year=season_year: (
                    pid,
                    http_json(
                        f"https://statsapi.mlb.com/api/v1/people/{pid}/stats?stats=season&group=pitching&season={season_year}&gameType=R"
                    ),
                    name,
                )
            ))
        for future in as_completed(pitcher_tasks):
            pid, stats, name = future.result()
            era = 0.0
            hr = 0
            try:
                # Direct stats endpoint shape
                entries = []
                for blk in (stats.get('stats') or []):
                    grp = blk.get('group')
                    if isinstance(grp, dict):
                        grp_name = str(grp.get('displayName') or grp.get('code') or grp.get('name') or '').lower()
                    else:
                        grp_name = str(grp or '').lower()
                    if grp_name == 'pitching':
                        entries.extend(blk.get('splits') or [])
                best = None
                for sp in entries:
                    stat = sp.get('stat') or {}
                    ip = stat.get('inningsPitched') or '0.0'
                    # convert to true innings to rank by workload
                    try:
                        whole = int(str(ip).split('.')[0])
                        frac = int(str(ip).split('.')[1]) if '.' in str(ip) and str(ip).split('.')[1].isdigit() else 0
                        ip_val = whole + (frac / 3.0)
                    except Exception:
                        ip_val = 0.0
                    if best is None or ip_val > best[0]:
                        best = (ip_val, sp)
                if best:
                    st = (best[1].get('stat') or {})
                    era = float(st.get('era') or 0)
                    hr = int(st.get('homeRuns') or 0)
            except Exception:
                pass
            row = {'name': name, 'mlbam_id': pid, 'era': era, 'homeRunsAllowed': hr}
            prev = best_by_pid.get(pid)
            if not prev:
                best_by_pid[pid] = row
            else:
                # prefer non-zero innings proxy via hr or lower ERA if both present
                score_prev = (0 if prev.get('era') is None else -prev.get('era')) + (prev.get('homeRunsAllowed') or 0)
                score_new = (0 if row.get('era') is None else -row.get('era')) + (row.get('homeRunsAllowed') or 0)
                if score_new > score_prev:
                    best_by_pid[pid] = row
    pitchers = list(best_by_pid.values())
    return {'date': date, 'pitchers': pitchers}


def fetch_recent_simple(date: str) -> dict:
    """Recent HR form: last 14 days HR count via Statcast (by batter), with safe fallbacks."""
    end_d = datetime.strptime(date, '%Y-%m-%d').date()
    start_d = end_d - timedelta(days=14)
    try:
        from pybaseball import statcast
    except Exception:
        return {'date': date, 'players': []}

    def chunks(it: Iterable[int], size: int = 50):
        buf = []
        for x in it:
            buf.append(int(x))
            if len(buf) >= size:
                yield buf
                buf = []
        if buf:
            yield buf

    # Helper: resolve batter ids to names via MLB API
    def lookup_names(ids: List[int]) -> Dict[int, str]:
        out: Dict[int, str] = {}
        for chunk in chunks(ids, 50):
            try:
                url = f"https://statsapi.mlb.com/api/v1/people?personIds={','.join(str(i) for i in chunk)}"
                data = http_json(url)
                for p in data.get('people', []) or []:
                    pid = p.get('id')
                    nm = p.get('fullName')
                    if pid and nm:
                        out[int(pid)] = str(nm)
            except Exception:
                continue
        return out

    try:
        df = statcast(start_dt=start_d.strftime('%Y-%m-%d'), end_dt=end_d.strftime('%Y-%m-%d'))
        if df is None or df.empty:
            return {'date': date, 'players': []}
        # Only count batter home runs
        if 'events' not in df.columns or 'batter' not in df.columns:
            return {'date': date, 'players': []}
        hr_df = df[df['events'] == 'home_run']
        if hr_df.empty:
            return {'date': date, 'players': []}
        counts = hr_df.groupby('batter').size().sort_values(ascending=False)
        ids = [int(i) for i in counts.index.tolist() if pd.notna(i)]
        id_to_name = lookup_names(ids) if ids else {}
        players = []
        for pid, cnt in counts.items():
            try:
                pid_int = int(pid)
            except Exception:
                continue
            players.append({'name': id_to_name.get(pid_int), 'mlbam_id': pid_int, 'last_14_day_hr': int(cnt)})
        return {'date': date, 'players': players}
    except Exception:
        return {'date': date, 'players': []}

def fetch_ballpark_weather(date: str) -> dict:
    """
    Fetch venues from today's schedule and create stub park/weather factors.
    Extend with real weather API as needed.
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime as _dt
    sched = fetch_schedule(date)
    ballpark_factors = {}
    weather_conditions = {}
    # Prefer env key; fallback to provided if present
    api_key = os.getenv('OPENWEATHER_API_KEY') or os.getenv('OWM_API_KEY') or "487d8b3060df1751a73e0f242629f0ca"
    def get_weather(city):
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=imperial"
        try:
            if not city or not str(city).strip():
                return 75, 0, 'none'
            data = http_json(url)
            temp = data.get('main', {}).get('temp', 75)
            wind = data.get('wind', {})
            wind_speed = wind.get('speed', 0)
            wind_deg = wind.get('deg', 0)
            dirs = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
            ix = int((wind_deg + 22.5) // 45) % 8
            wind_dir = dirs[ix]
            return temp, wind_speed, wind_dir
        except Exception as e:
            print(f"Weather fetch failed for {city}: {e}")
            return 75, 0, 'none'

    # Pull Park Factors (HR) for season from FanGraphs, with robust column detection,
    # and fallback to Baseball-Reference if needed.
    year = _dt.strptime(date, '%Y-%m-%d').year
    hr_pf_by_abbr: dict[str, float] = {}
    # Common team name variants to help matching
    TEAM_VARIANTS = {
        'ARI': ['Diamondbacks', 'D-backs', 'Arizona'],
        'ATL': ['Braves', 'Atlanta'],
        'BAL': ['Orioles', 'Baltimore'],
        'BOS': ['Red Sox', 'Boston'],
        'CHC': ['Cubs', 'Chicago Cubs'],
        'CWS': ['White Sox', 'Chicago White Sox'],
        'CIN': ['Reds', 'Cincinnati'],
        'CLE': ['Guardians', 'Cleveland'],
        'COL': ['Rockies', 'Colorado'],
        'DET': ['Tigers', 'Detroit'],
        'HOU': ['Astros', 'Houston'],
        'KC':  ['Royals', 'Kansas City'],
        'LAA': ['Angels', 'Los Angeles Angels', 'LA Angels', 'Anaheim'],
        'LAD': ['Dodgers', 'Los Angeles Dodgers', 'LA Dodgers'],
        'MIA': ['Marlins', 'Miami'],
        'MIL': ['Brewers', 'Milwaukee'],
        'MIN': ['Twins', 'Minnesota'],
        'NYM': ['Mets', 'New York Mets'],
        'NYY': ['Yankees', 'New York Yankees'],
        'OAK': ['Athletics', 'Oakland'],
        'PHI': ['Phillies', 'Philadelphia'],
        'PIT': ['Pirates', 'Pittsburgh'],
        'SD':  ['Padres', 'San Diego'],
        'SEA': ['Mariners', 'Seattle'],
        'SF':  ['Giants', 'San Francisco'],
        'STL': ['Cardinals', 'St. Louis', 'Saint Louis'],
        'TB':  ['Rays', 'Tampa Bay'],
        'TEX': ['Rangers', 'Texas'],
        'TOR': ['Blue Jays', 'Toronto'],
        'WSH': ['Nationals', 'Washington']
    }

    def _match_abbr(team_str: str) -> str | None:
        t = (team_str or '').strip()
        for abbr, variants in TEAM_VARIANTS.items():
            for v in variants:
                if v.lower() in t.lower():
                    return abbr
        return None

    try:
        url_pf = f"https://www.fangraphs.com/guts.aspx?type=pf&teamid=0&season={year}"
        tables = pd.read_html(url_pf)
        for df in tables:
            cols = [str(c) for c in df.columns]
            if not any('team' in str(c).lower() for c in cols):
                continue
            # Identify team column
            team_col = None
            for c in df.columns:
                if 'team' in str(c).lower():
                    team_col = c
                    break
            if team_col is None:
                team_col = df.columns[0]
            # Identify HR column flexibly
            hr_col = None
            for c in df.columns:
                cl = str(c).lower()
                if 'hr' in cl and all(x not in cl for x in ['rhb', 'lhb', 'home/away', 'park']) and cl.strip() in ['hr', 'hr factor', 'hr pf', 'hr (100=avg)', 'hr%','hr% (100=avg)'] or ('hr' in cl and '%' not in cl):
                    hr_col = c
                    break
            # If still not found, try columns that look numeric and named like 'HR'
            if hr_col is None:
                for c in df.columns:
                    if str(c).strip().upper() == 'HR':
                        hr_col = c
                        break
            if hr_col is None:
                continue
            for _, row in df.iterrows():
                ab = _match_abbr(str(row.get(team_col)))
                if not ab:
                    continue
                try:
                    val = row.get(hr_col)
                    hr_pf = float(val)
                except Exception:
                    continue
                if hr_pf and hr_pf > 0:
                    # Convert from 100=avg to multiplier
                    hr_pf_by_abbr[ab] = hr_pf / 100.0
        # If we got at least most teams, we're good
    except Exception as e:
        print(f"FG park factors fetch failed: {e}")
        hr_pf_by_abbr = {}

    if len(hr_pf_by_abbr) < 24:  # try Baseball-Reference as a fallback
        try:
            br_url = f"https://www.baseball-reference.com/leagues/majors/{year}-park-factors.shtml"
            tables = pd.read_html(br_url)
            for df in tables:
                cols = [str(c).lower() for c in df.columns]
                if not any('team' in c for c in cols) or not any(c.strip() == 'hr' for c in [str(x).lower().strip() for x in df.columns]):
                    continue
                team_col = None
                for c in df.columns:
                    if 'team' in str(c).lower():
                        team_col = c
                        break
                hr_col = None
                for c in df.columns:
                    if str(c).lower().strip() == 'hr':
                        hr_col = c
                        break
                if team_col is None or hr_col is None:
                    continue
                for _, row in df.iterrows():
                    ab = _match_abbr(str(row.get(team_col)))
                    if not ab:
                        continue
                    try:
                        hr_pf = float(row.get(hr_col))
                    except Exception:
                        continue
                    if hr_pf and hr_pf > 0:
                        hr_pf_by_abbr[ab] = hr_pf / 100.0
        except Exception as e:
            print(f"BR park factors fetch failed: {e}")

    # Prepare requests
    tasks = []
    meta_by_key = {}
    venue_ids = {}
    NAME_TO_ABBR = {
        'Arizona Diamondbacks': 'ARI', 'Atlanta Braves': 'ATL', 'Baltimore Orioles': 'BAL', 'Boston Red Sox': 'BOS',
        'Chicago Cubs': 'CHC', 'Chicago White Sox': 'CWS', 'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE',
        'Colorado Rockies': 'COL', 'Detroit Tigers': 'DET', 'Houston Astros': 'HOU', 'Kansas City Royals': 'KC',
        'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD', 'Miami Marlins': 'MIA', 'Milwaukee Brewers': 'MIL',
        'Minnesota Twins': 'MIN', 'New York Mets': 'NYM', 'New York Yankees': 'NYY', 'Oakland Athletics': 'OAK',
        'Philadelphia Phillies': 'PHI', 'Pittsburgh Pirates': 'PIT', 'San Diego Padres': 'SD', 'Seattle Mariners': 'SEA',
        'San Francisco Giants': 'SF', 'St. Louis Cardinals': 'STL', 'Tampa Bay Rays': 'TB', 'Texas Rangers': 'TEX',
        'Toronto Blue Jays': 'TOR', 'Washington Nationals': 'WSH'
    }

    def resolve_abbr(team_obj: dict) -> str:
        if not team_obj:
            return ''
        abbr = team_obj.get('abbreviation') or team_obj.get('teamCode') or ''
        if abbr:
            return abbr
        name = team_obj.get('name') or ''
        return NAME_TO_ABBR.get(name, '')

    for d in sched.get('dates', []):
        for g in d.get('games', []):
            venue = g.get('venue', {})
            name = venue.get('name', '')
            # Use team home city; schedule's venue rarely includes city
            home_team = (g.get('teams', {}).get('home', {}).get('team', {}) or {})
            city = home_team.get('locationName', '') or venue.get('city', '')
            abbr = resolve_abbr(home_team)
            key = f"{abbr}_park" if abbr else name.replace(' ', '_')
            game_utc = g.get('gameDate')  # ISO UTC string
            vlink = venue.get('link') or ''
            vid = None
            try:
                # Extract ID from /api/v1/venues/<id>
                if vlink and vlink.strip('/').split('/')[-2] == 'venues':
                    vid = int(vlink.strip('/').split('/')[-1])
            except Exception:
                vid = venue.get('id')
            if isinstance(vid, str) and vid.isdigit():
                vid = int(vid)
            venue_ids[key] = vid
            meta_by_key[key] = {'venue_name': name, 'city': city, 'venue_id': vid, 'abbr': abbr, 'game_utc': game_utc}
    # Prefer static lat/lon per park to avoid API schema issues
    MLB_PARK_COORDS = {
        'ARI': (33.4455, -112.0667),  # Chase Field
        'ATL': (33.8907, -84.4677),   # Truist Park
        'BAL': (39.2839, -76.6217),   # Camden Yards
        'BOS': (42.3467, -71.0972),   # Fenway Park
        'CHC': (41.9484, -87.6553),   # Wrigley Field
        'CWS': (41.8299, -87.6338),   # Guaranteed Rate Field
        'CIN': (39.0975, -84.5073),   # Great American Ball Park
        'CLE': (41.4962, -81.6880),   # Progressive Field
        'COL': (39.7559, -104.9942),  # Coors Field
        'DET': (42.3390, -83.0485),   # Comerica Park
        'HOU': (29.7570, -95.3550),   # Minute Maid Park
        'KC':  (39.0517, -94.4803),   # Kauffman Stadium
        'LAA': (33.8003, -117.8827),  # Angel Stadium
        'LAD': (34.0739, -118.2400),  # Dodger Stadium
        'MIA': (25.7781, -80.2197),   # loanDepot park
        'MIL': (43.0280, -87.9710),   # American Family Field
        'MIN': (44.9817, -93.2776),   # Target Field
        'NYM': (40.7571, -73.8458),   # Citi Field
        'NYY': (40.8296, -73.9262),   # Yankee Stadium
        'OAK': (37.7516, -122.2005),  # Oakland Coliseum
        'PHI': (39.9050, -75.1665),   # Citizens Bank Park
        'PIT': (40.4469, -80.0057),   # PNC Park
        'SD':  (32.7073, -117.1566),  # Petco Park
        'SEA': (47.5914, -122.3325),  # T-Mobile Park
        'SF':  (37.7786, -122.3893),  # Oracle Park
        'STL': (38.6226, -90.1928),   # Busch Stadium
        'TB':  (27.7682, -82.6534),   # Tropicana Field
        'TEX': (32.7473, -97.0827),   # Globe Life Field
        'TOR': (43.6414, -79.3894),   # Rogers Centre
        'WSH': (38.8730, -77.0074),   # Nationals Park
        # Also include COL already above; ensure all 30 present
        'BAL': (39.2839, -76.6217),
        'BOS': (42.3467, -71.0972),
        'CHC': (41.9484, -87.6553),
        'CWS': (41.8299, -87.6338),
        'CIN': (39.0975, -84.5073),
        'CLE': (41.4962, -81.6880),
        'COL': (39.7559, -104.9942),
        'DET': (42.3390, -83.0485),
        'HOU': (29.7570, -95.3550),
        'KC':  (39.0517, -94.4803),
        'LAA': (33.8003, -117.8827),
        'LAD': (34.0739, -118.2400),
        'MIA': (25.7781, -80.2197),
        'MIL': (43.0280, -87.9710),
        'MIN': (44.9817, -93.2776),
        'NYM': (40.7571, -73.8458),
        'NYY': (40.8296, -73.9262),
        'OAK': (37.7516, -122.2005),
        'PHI': (39.9050, -75.1665),
        'PIT': (40.4469, -80.0057),
        'SD':  (32.7073, -117.1566),
        'SEA': (47.5914, -122.3325),
        'SF':  (37.7786, -122.3893),
        'STL': (38.6226, -90.1928),
        'TB':  (27.7682, -82.6534),
        'TEX': (32.7473, -97.0827),
        'TOR': (43.6414, -79.3894),
        'WSH': (38.8730, -77.0074),
    }

    # Canonical venue names by team abbr
    ABBR_TO_VENUE = {
        'ARI': 'Chase Field',
        'ATL': 'Truist Park',
        'BAL': 'Oriole Park at Camden Yards',
        'BOS': 'Fenway Park',
        'CHC': 'Wrigley Field',
        'CWS': 'Guaranteed Rate Field',
        'CIN': 'Great American Ball Park',
        'CLE': 'Progressive Field',
        'COL': 'Coors Field',
        'DET': 'Comerica Park',
        'HOU': 'Minute Maid Park',
        'KC':  'Kauffman Stadium',
        'LAA': 'Angel Stadium of Anaheim',
        'LAD': 'Dodger Stadium',
        'MIA': 'loanDepot park',
        'MIL': 'American Family Field',
        'MIN': 'Target Field',
        'NYM': 'Citi Field',
        'NYY': 'Yankee Stadium',
        'OAK': 'Oakland Coliseum',
        'PHI': 'Citizens Bank Park',
        'PIT': 'PNC Park',
        'SD':  'Petco Park',
        'SEA': 'T-Mobile Park',
        'SF':  'Oracle Park',
        'STL': 'Busch Stadium',
        'TB':  'Tropicana Field',
        'TEX': 'Globe Life Field',
        'TOR': 'Rogers Centre',
        'WSH': 'Nationals Park',
    }

    with ThreadPoolExecutor(max_workers=8) as ex:
        def weather_task(k, meta):
            from datetime import datetime, timezone
            ab = meta.get('abbr')
            coords = MLB_PARK_COORDS.get(ab)
            game_iso = meta.get('game_utc')
            game_ts = None
            if game_iso:
                try:
                    iso = str(game_iso).replace('Z', '+00:00')
                    game_dt = datetime.fromisoformat(iso)
                    if game_dt.tzinfo is None:
                        game_dt = game_dt.replace(tzinfo=timezone.utc)
                    else:
                        game_dt = game_dt.astimezone(timezone.utc)
                    game_ts = int(game_dt.timestamp())
                except Exception:
                    game_ts = None
            if coords and game_ts:
                lat, lon = coords
                url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}&units=imperial"
                try:
                    data = http_json(url)
                    entries = data.get('list', []) or []
                    closest = None
                    best_diff = 10**12
                    for it in entries:
                        dt = int(it.get('dt') or 0)
                        diff = abs(dt - game_ts)
                        if diff < best_diff:
                            best_diff = diff
                            closest = it
                    if closest:
                        temp = ((closest.get('main') or {}).get('temp')) or 75
                        wind = closest.get('wind') or {}
                        wind_speed = wind.get('speed', 0)
                        wind_deg = wind.get('deg', 0)
                        dirs = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
                        ix = int((wind_deg + 22.5) // 45) % 8
                        wind_dir = dirs[ix]
                        # Determine local period using city.timezone offset
                        tz_off = int((data.get('city') or {}).get('timezone') or 0)
                        local_hour = int(((game_ts + tz_off) % 86400) // 3600)
                        period = 'night' if local_hour >= 17 else 'day'
                        game_local = None
                        try:
                            game_local = datetime.fromtimestamp(game_ts + tz_off, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
                        except Exception:
                            pass
                        return k, temp, wind_speed, wind_dir, period, (game_iso or ''), (game_local or '')
                except Exception as e:
                    print(f"Forecast fetch failed for {k} ({ab}): {e}")
            # fallbacks: current weather or city
            if coords:
                lat, lon = coords
                url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=imperial"
                try:
                    data = http_json(url)
                    temp = data.get('main', {}).get('temp', 75)
                    wind = data.get('wind', {})
                    wind_speed = wind.get('speed', 0)
                    wind_deg = wind.get('deg', 0)
                    dirs = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
                    ix = int((wind_deg + 22.5) // 45) % 8
                    wind_dir = dirs[ix]
                    return k, temp, wind_speed, wind_dir, None, (game_iso or ''), ''
                except Exception:
                    pass
            # final fallback: team location/city name
            city_fallback = meta.get('city')
            temp, ws, wd = get_weather(city_fallback)
            return k, temp, ws, wd, None, (game_iso or ''), ''

        weather_futures = {ex.submit(weather_task, k, m): k for k, m in meta_by_key.items()}
        for fut in as_completed(weather_futures):
            key, temp, wind_speed, wind_dir, period, game_utc, game_local = fut.result()
            # Derive team abbr from key when possible
            team_abbr = meta_by_key.get(key, {}).get('abbr') or (key.replace('_park', '') if key.endswith('_park') else None)
            hr_factor = hr_pf_by_abbr.get(team_abbr, 1.0)
            # Normalize venue name by team if available
            venue_name = ABBR_TO_VENUE.get(team_abbr) or meta_by_key[key]['venue_name']
            ballpark_factors[key] = {'hr_factor': hr_factor, 'venue_name': venue_name}
            entry = {'temperature': temp, 'wind_speed': wind_speed, 'wind_direction': wind_dir}
            if period:
                entry['game_period'] = period
            if game_utc:
                entry['game_time_utc'] = game_utc
            if game_local:
                entry['game_time_local'] = game_local
            weather_conditions[key] = entry
    return {
        'date': date,
        'ballpark_factors': ballpark_factors,
        'weather_conditions': weather_conditions
    }


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    date = args.date

    sched = fetch_schedule(date)
    # Save current and a date-stamped copy for history
    save(sched, os.path.join(DATA_DIR, 'todays-schedule.json'))
    save(sched, os.path.join(DATA_DIR, f'todays-schedule-{date}.json'))
    save(sched, os.path.join(DATA_DIR, f'fresh-schedule-{date}.json'))
    save(fetch_players_simple(date), os.path.join(DATA_DIR, f'player-stats-{date}.json'))
    save(fetch_pitchers_simple(date), os.path.join(DATA_DIR, f'pitcher-stats-{date}.json'))
    save(fetch_recent_simple(date), os.path.join(DATA_DIR, f'recent-performance-{date}.json'))
    save(fetch_ballpark_weather(date), os.path.join(DATA_DIR, f'ballpark-weather-{date}.json'))

if __name__ == '__main__':
    main()
