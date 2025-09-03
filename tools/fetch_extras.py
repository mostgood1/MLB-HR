#!/usr/bin/env python3
"""
Generate advanced inputs referenced by generate_hr_scores_core.py.
This creates dated JSONs with safe default structures so the core can run even
when upstream data sources aren't available. You can enhance each fetcher later.

Files created:
- statcast-metrics-YYYY-MM-DD.json
- pitcher-advanced-YYYY-MM-DD.json
- pitch-type-metrics-YYYY-MM-DD.json
- bullpen-metrics-YYYY-MM-DD.json
- implied-totals-YYYY-MM-DD.json
- lineups-YYYY-MM-DD.json
"""
from __future__ import annotations
import os, json, math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
from pybaseball import statcast_batter_exitvelo_barrels
from pybaseball import statcast_pitcher_exitvelo_barrels
from pybaseball import pitching_stats

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(APP_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(obj: Any, path: str):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2)
    print(f"Saved {path}")


def http_json(url: str, timeout: int = 25) -> dict:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


def _extract_team_ids(schedule: dict) -> List[int]:
    ids: List[int] = []
    for d in (schedule.get('dates') or []):
        for g in d.get('games', []):
            for side in ('home', 'away'):
                t = (g.get('teams') or {}).get(side, {}).get('team', {}) or {}
                tid = t.get('id')
                if isinstance(tid, int):
                    ids.append(tid)
    # de-dup preserve order
    seen = set()
    out = []
    for i in ids:
        if i in seen:
            continue
        seen.add(i)
        out.append(i)
    return out


def _team_id_to_abbr_map(team_ids: List[int]) -> tuple[dict[int, str], dict[str, int]]:
    id_to_abbr: dict[int, str] = {}
    abbr_to_id: dict[str, int] = {}
    for tid in team_ids:
        meta = http_json(f"https://statsapi.mlb.com/api/v1/teams/{tid}")
        try:
            t0 = (meta.get('teams') or [{}])[0]
            abbr = t0.get('abbreviation') or t0.get('teamCode')
            if abbr:
                id_to_abbr[tid] = abbr
                abbr_to_id[abbr] = tid
        except Exception:
            continue
    return id_to_abbr, abbr_to_id


def teams_from_schedule(schedule: dict) -> List[str]:
    ids = _extract_team_ids(schedule)
    id_to_abbr, _ = _team_id_to_abbr_map(ids)
    return [abbr for abbr in id_to_abbr.values()]


def _season_dates(date: str) -> Tuple[str, str]:
    d = datetime.strptime(date, '%Y-%m-%d').date()
    season_start = datetime(d.year, 3, 1).date()
    return season_start.strftime('%Y-%m-%d'), date


def fetch_statcast_metrics(date: str):
    """
    Compute average EV and barrel rate per hitter for season-to-date using raw Statcast events.
    - Avg EV: mean launch_speed for BBE per batter
    - Barrel rate: barrels / plate appearances (PA) per batter
    Uses a robust barrel heuristic based on EV and launch angle and chunks the date range to avoid large queries.
    Output: { date, metrics: { playerName: { exit_velocity, barrel_rate } } }
    """
    players = load_json(os.path.join(DATA_DIR, f'player-stats-{date}.json')).get('players', [])
    if not players:
        save_json({'date': date, 'metrics': {}}, os.path.join(DATA_DIR, f'statcast-metrics-{date}.json'))
        return
    ids = {int(p.get('mlbam_id')): p.get('name') for p in players if p.get('mlbam_id')}
    start_s, end_s = _season_dates(date)
    try:
        from pybaseball import statcast
    except Exception:
        metrics = {p.get('name'): {'exit_velocity': None, 'barrel_rate': None} for p in players if p.get('name')}
        save_json({'date': date, 'metrics': metrics}, os.path.join(DATA_DIR, f'statcast-metrics-{date}.json'))
        return

    # Helper: generator of [start, end] windows of ~28 days
    def windows(start: str, end: str):
        sd = datetime.strptime(start, '%Y-%m-%d').date()
        ed = datetime.strptime(end, '%Y-%m-%d').date()
        cur = sd
        while cur <= ed:
            nxt = min(cur + timedelta(days=27), ed)
            yield cur.strftime('%Y-%m-%d'), nxt.strftime('%Y-%m-%d')
            cur = nxt + timedelta(days=1)

    # Approximate barrel classification
    def is_barrel(ev: float, la: float) -> bool:
        try:
            evf = float(ev)
            laf = float(la)
        except Exception:
            return False
        if pd.isna(evf) or pd.isna(laf):
            return False
        if evf < 98.0:
            return False
        widen = max(0.0, evf - 98.0)
        low = max(8.0, 26.0 - widen)
        high = min(50.0, 30.0 + widen)
        return low <= laf <= high

    frames = []
    try:
        for ws, we in windows(start_s, end_s):
            df = statcast(start_dt=ws, end_dt=we)
            if df is not None and not df.empty:
                frames.append(df[['batter', 'launch_speed', 'launch_angle', 'game_pk', 'at_bat_number']].copy())
        if not frames:
            raise RuntimeError('no statcast data')
        data = pd.concat(frames, axis=0, ignore_index=True)
    except Exception:
        metrics = {p.get('name'): {'exit_velocity': None, 'barrel_rate': None} for p in players if p.get('name')}
        save_json({'date': date, 'metrics': metrics}, os.path.join(DATA_DIR, f'statcast-metrics-{date}.json'))
        return

    # Compute per-batter aggregates
    data = data[pd.notna(data['batter'])]
    data['batter'] = data['batter'].astype('Int64')
    # Avg EV: mean of launch_speed over BBE with non-null speed
    ev_df = data[pd.notna(data['launch_speed'])].groupby('batter')['launch_speed'].mean().rename('avg_ev')
    # PA: unique (game_pk, at_bat_number) per batter
    pa_df = data.dropna(subset=['game_pk', 'at_bat_number']).copy()
    pa_df['pa_key'] = pa_df['game_pk'].astype('Int64').astype(str) + '-' + pa_df['at_bat_number'].astype('Int64').astype(str)
    pa_counts = pa_df.groupby('batter')['pa_key'].nunique().rename('pa')
    # Barrels
    mask_bbe = pd.notna(data['launch_speed']) & pd.notna(data['launch_angle'])
    bbe = data[mask_bbe].copy()
    bbe['is_barrel_calc'] = [is_barrel(ev, la) for ev, la in zip(bbe['launch_speed'], bbe['launch_angle'])]
    brl_counts = bbe.groupby('batter')['is_barrel_calc'].sum(min_count=1).rename('barrels')
    # Merge
    agg = pd.concat([ev_df, pa_counts, brl_counts], axis=1)
    # Compute rate; avoid div by zero
    agg['barrel_rate'] = agg.apply(lambda r: (float(r['barrels']) / float(r['pa'])) if pd.notna(r['barrels']) and pd.notna(r['pa']) and r['pa'] > 0 else None, axis=1)

    # Build metrics for the active roster
    metrics: Dict[str, Dict[str, float]] = {}
    for pid, name in ids.items():
        row = agg.loc[pid] if pid in agg.index else None
        ev = float(row['avg_ev']) if row is not None and pd.notna(row['avg_ev']) else None
        brl = float(row['barrel_rate']) if row is not None and pd.notna(row['barrel_rate']) else None
        metrics[name] = {'exit_velocity': (round(ev, 2) if ev is not None else None), 'barrel_rate': (round(brl, 4) if brl is not None else None)}

    save_json({'date': date, 'metrics': metrics}, os.path.join(DATA_DIR, f'statcast-metrics-{date}.json'))


def fetch_pitcher_advanced(date: str):
    """
    Real pitcher advanced snapshot using Statcast barrels allowed (season-to-date) and FanGraphs HR/FB, FB%.
    Output: { date, pitchers: [ { name, barrel_rate_allowed, hr_fb, fb_pct, vsR:{xslg}, vsL:{xslg} } ] }
    """
    pitchers = load_json(os.path.join(DATA_DIR, f'pitcher-stats-{date}.json')).get('pitchers', [])
    if not pitchers:
        # Try probable pitchers from schedule as a fallback
        sched = load_json(os.path.join(DATA_DIR, f'fresh-schedule-{date}.json')) or {}
        prob_names: List[str] = []
        for d in (sched.get('dates') or []):
            for g in d.get('games', []):
                for side in ('home', 'away'):
                    pp = (g.get('teams') or {}).get(side, {}).get('probablePitcher') or {}
                    if pp.get('fullName'):
                        prob_names.append(pp['fullName'])
        prob_names = list(dict.fromkeys(prob_names))
        base = [{'name': n, 'barrel_rate_allowed': None, 'hr_fb': None, 'fb_pct': None, 'vsR': {'xslg': None}, 'vsL': {'xslg': None}} for n in prob_names]
        save_json({'date': date, 'pitchers': base}, os.path.join(DATA_DIR, f'pitcher-advanced-{date}.json'))
        return
    name_by_id = {int(p.get('mlbam_id')): p.get('name') for p in pitchers if p.get('mlbam_id')}
    start, end = _season_dates(date)
    adv_map: Dict[str, Dict[str, Any]] = {}
    # Statcast barrels allowed per PA (approx) via pitcher leaderboard
    try:
        dfp = statcast_pitcher_exitvelo_barrels(start, end)
        pid_col = 'player_id' if 'player_id' in dfp.columns else 'pitcher'
        brl_col = 'brl_pa' if 'brl_pa' in dfp.columns else ('brl_percent' if 'brl_percent' in dfp.columns else None)
        for _, row in dfp.iterrows():
            try:
                pid = int(row.get(pid_col))
            except Exception:
                continue
            name = name_by_id.get(pid)
            if not name:
                continue
            brl = None
            try:
                brl = float(row.get(brl_col)) if brl_col else None
                if brl and brl > 1.0:
                    brl = brl / 100.0
            except Exception:
                pass
            adv_map[name] = {'name': name, 'barrel_rate_allowed': brl, 'hr_fb': None, 'fb_pct': None, 'vsR': {'xslg': None}, 'vsL': {'xslg': None}}
    except Exception:
        pass
    # FanGraphs season pitching stats for HR/FB and FB%
    try:
        year = datetime.strptime(date, '%Y-%m-%d').year
        fg = pitching_stats(year)
        # Normalize columns
        # Columns often: 'Name', 'HR/FB', 'FB%'
        name_col = 'Name' if 'Name' in fg.columns else 'name'
        hrfb_col = 'HR/FB' if 'HR/FB' in fg.columns else ('HR/FB%' if 'HR/FB%' in fg.columns else None)
        fb_col = 'FB%' if 'FB%' in fg.columns else None
        for _, row in fg.iterrows():
            n = row.get(name_col)
            if not n:
                continue
            try:
                hrfb = row.get(hrfb_col)
                fb = row.get(fb_col)
                # Convert to fraction: if value > 1, assume percentage; else assume already fraction
                def to_frac(val):
                    if val is None or (isinstance(val, float) and pd.isna(val)):
                        return None
                    s = str(val).replace('%', '').strip()
                    try:
                        x = float(s)
                    except Exception:
                        return None
                    return (x / 100.0) if x > 1.0 else x
                hrfb_v = to_frac(hrfb) if hrfb_col else None
                fb_v = to_frac(fb) if fb_col else None
            except Exception:
                hrfb_v = None
                fb_v = None
            cur = adv_map.get(n, {'name': n, 'barrel_rate_allowed': None, 'hr_fb': None, 'fb_pct': None, 'vsR': {'xslg': None}, 'vsL': {'xslg': None}})
            if hrfb_v is not None:
                cur['hr_fb'] = hrfb_v
            if fb_v is not None:
                cur['fb_pct'] = fb_v
            adv_map[n] = cur
    except Exception:
        pass
    # Ensure we at least include probable pitchers with None if adv_map is sparse
    if not adv_map:
        adv_map = {p.get('name'): {'name': p.get('name'), 'barrel_rate_allowed': None, 'hr_fb': None, 'fb_pct': None, 'vsR': {'xslg': None}, 'vsL': {'xslg': None}} for p in pitchers if p.get('name')}
    out = {'date': date, 'pitchers': list(adv_map.values())}
    save_json(out, os.path.join(DATA_DIR, f'pitcher-advanced-{date}.json'))


def fetch_pitch_type_metrics(date: str):
    """
    Real-ish pitch type usage for probable pitchers over the last 60 days using Statcast events.
    Output: { date, pitchers: { name: { top_pitches: [{type, usage}] } }, batters: { name: { xslg_by_pitch: {} } } }
    """
    schedule = load_json(os.path.join(DATA_DIR, f'fresh-schedule-{date}.json')) or load_json(os.path.join(DATA_DIR, 'todays-schedule.json'))
    pitchers_today = []
    for d in (schedule.get('dates') or []):
        for g in d.get('games', []):
            teams = g.get('teams') or {}
            for side in ('home', 'away'):
                pp = teams.get(side, {}).get('probablePitcher') or {}
                if pp.get('id') and pp.get('fullName'):
                    pitchers_today.append({'id': int(pp['id']), 'name': pp['fullName']})
    # Deduplicate
    seen = set()
    pitchers_today = [p for p in pitchers_today if not (p['id'] in seen or seen.add(p['id']))]

    # Query statcast_pitcher per pitcher for last 60 days to compute pitch usage
    try:
        from pybaseball import statcast_pitcher
    except Exception:
        # Fallback: populate structure with probable or starter pitchers and empty top_pitches
        pit = {p['name']: {'top_pitches': []} for p in pitchers_today}
        if not pit:
            ps = load_json(os.path.join(DATA_DIR, f'pitcher-stats-{date}.json')).get('pitchers', [])
            for p in ps:
                n = p.get('name')
                if n:
                    pit[n] = {'top_pitches': []}
        save_json({'date': date, 'pitchers': pit, 'batters': {}}, os.path.join(DATA_DIR, f'pitch-type-metrics-{date}.json'))
        return
    end_d = datetime.strptime(date, '%Y-%m-%d').date()
    start_d = (end_d - timedelta(days=60)).strftime('%Y-%m-%d')
    end_s = end_d.strftime('%Y-%m-%d')

    pit: Dict[str, Dict[str, Any]] = {}
    # Canonicalize pitch names to align pitcher and batter datasets
    def canon_pitch(n: str) -> str:
        s = (n or '').strip()
        m = {
            'Four-Seam Fastball': '4-Seam Fastball',
            'Four-seam Fastball': '4-Seam Fastball',
            '4-seam Fastball': '4-Seam Fastball',
            'FF': '4-Seam Fastball',
            'Two-Seam Fastball': 'Sinker',
            '2-Seam Fastball': 'Sinker',
            'FT': 'Sinker',
            'FS': 'Split-Finger',
            'CU': 'Curveball',
            'KC': 'Knuckle Curve',
            'SL': 'Slider',
            'SI': 'Sinker',
            'CH': 'Changeup',
        }
        return m.get(s, s)
    def calc_usage(pid: int, name: str):
        try:
            df = statcast_pitcher(start_d, end_s, pid)
            if df is None or len(df) == 0:
                return name, []
            # Column could be 'pitch_name' or 'pitch_type'
            col = 'pitch_name' if 'pitch_name' in df.columns else ('pitch_type' if 'pitch_type' in df.columns else None)
            if not col:
                return name, []
            # Usage by canonical pitch name
            names = df[col].dropna().map(canon_pitch)
            counts = names.value_counts()
            total = float(counts.sum()) if counts.sum() else 0.0
            usage_pct = (counts / total * 100.0) if total > 0 else counts * 0.0
            # HR per 100 pitches by pitch
            hr_mask = (df['events'] == 'home_run') if 'events' in df.columns else None
            hr_counts = names[hr_mask].value_counts() if hr_mask is not None else None
            top = []
            for k, v in usage_pct.sort_values(ascending=False).head(2).items():
                hr100 = None
                try:
                    hrc = float(hr_counts.get(k, 0.0)) if hr_counts is not None else 0.0
                    denom = float(counts.get(k, 0.0))
                    hr100 = (hrc / denom * 100.0) if denom > 0 else 0.0
                except Exception:
                    hr100 = None
                top.append({'type': k, 'usage': round(float(v), 2), 'hr_per_100': (round(hr100, 2) if hr100 is not None else None)})
            return name, top
        except Exception:
            return name, []

    # If no probables (final games), fallback to pitchers from pitcher-stats
    if not pitchers_today:
        ps = load_json(os.path.join(DATA_DIR, f'pitcher-stats-{date}.json')).get('pitchers', [])
        pitchers_today = [{'id': int(p['mlbam_id']), 'name': p['name']} for p in ps if p.get('mlbam_id') and p.get('name')]

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(calc_usage, p['id'], p['name']) for p in pitchers_today]
        for f in as_completed(futures):
            name, top = f.result()
            pit[name] = {'top_pitches': top}

    # Compute batter xSLG by pitch for last 60 days for hitters in player-stats
    batters = load_json(os.path.join(DATA_DIR, f'player-stats-{date}.json')).get('players', [])
    id_by_name = {p.get('mlbam_id'): p.get('name') for p in batters if p.get('mlbam_id') and p.get('name')}
    batter_x: Dict[str, Dict[str, float]] = {}
    try:
        from pybaseball import statcast
        df = statcast(start_dt=start_d, end_dt=end_s)
        if df is not None and len(df) > 0:
            # Keep batter events with pitch_name or pitch_type
            col = 'pitch_name' if 'pitch_name' in df.columns else ('pitch_type' if 'pitch_type' in df.columns else None)
            if col:
                cols = ['batter', 'events', col]
                sdf = df[cols].copy()
                sdf = sdf.dropna(subset=['batter', col])
                # Canonical pitch names
                sdf['canon_pitch'] = sdf[col].map(canon_pitch)
                # Total bases and AB determination
                tb_map = {'single':1,'double':2,'triple':3,'home_run':4}
                sdf['tb'] = sdf['events'].map(tb_map).fillna(0)
                # AB events include outs in play and hits; exclude walks/HBP/sac
                ab_events = set(['single','double','triple','home_run','field_out','force_out','grounded_into_double_play','double_play','field_error','fielders_choice','fielders_choice_out','strikeout','strikeout_double_play','other_out','sac_fly_double_play','flyout','lineout','pop_out','groundout'])
                sdf['is_ab'] = sdf['events'].isin(ab_events).astype(int)
                gb = sdf.groupby(['batter', 'canon_pitch']).agg(tb_sum=('tb','sum'), ab_cnt=('is_ab','sum')).reset_index()
                gb['xslg'] = gb.apply(lambda r: (float(r['tb_sum'])/float(r['ab_cnt'])) if r['ab_cnt']>0 else 0.0, axis=1)
                # Map to names
                for _, row in gb.iterrows():
                    bid = int(row['batter']) if pd.notna(row['batter']) else None
                    name = id_by_name.get(bid)
                    if not name:
                        continue
                    pitch = str(row['canon_pitch'])
                    xslg = float(row['xslg'])
                    batter_x.setdefault(name, {})[pitch] = round(xslg, 3)
    except Exception:
        batter_x = {}
    out = {'date': date, 'pitchers': pit, 'batters': batter_x}
    save_json(out, os.path.join(DATA_DIR, f'pitch-type-metrics-{date}.json'))


def fetch_bullpen_metrics(date: str):
    """
    Real team HR/9 from FanGraphs team pitching (season to date); expected IP for starters from schedule.
    Output: { date, bullpens: { TEAM_ABBR: {hr9} }, starters: { name: {expected_ip} } }
    """
    schedule = load_json(os.path.join(DATA_DIR, f'fresh-schedule-{date}.json')) or load_json(os.path.join(DATA_DIR, 'todays-schedule.json'))
    team_ids = _extract_team_ids(schedule)
    id_to_abbr, abbr_to_id = _team_id_to_abbr_map(team_ids)
    year = datetime.strptime(date, '%Y-%m-%d').year
    bullpens: Dict[str, Dict[str, float]] = {}
    # Try MLB StatsAPI per-team pitching stats to compute HR/9
    for abbr, tid in abbr_to_id.items():
        try:
            url = f"https://statsapi.mlb.com/api/v1/teams/{tid}/stats?group=pitching&stats=season&season={year}"
            data = http_json(url)
            splits = (((data.get('stats') or [{}])[0].get('splits')) or [])
            hr = 0
            ip = 0.0
            if splits:
                st = (splits[0].get('stat') or {})
                hr = int(st.get('homeRuns') or st.get('homeRunsAllowed') or 0)
                # inningsPitched like "123.1" where .1 is 1/3 inning
                ip_str = st.get('inningsPitched') or '0.0'
                try:
                    parts = str(ip_str).split('.')
                    whole = int(parts[0])
                    frac = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
                    ip = whole + (frac / 10.0)  # MLB format uses .1 = 1/3, .2 = 2/3
                    # Convert to true innings
                    thirds = round((ip - whole) * 10)
                    ip = whole + (thirds / 3.0)
                except Exception:
                    ip = 0.0
            hr9 = (hr * 9.0 / ip) if ip > 0 else None
            bullpens[abbr] = {'hr9': hr9}
        except Exception:
            bullpens[abbr] = {'hr9': None}

    # Expected IP from schedule probable pitchers
    starters = {}
    dates = schedule.get('dates') or []
    games = (dates[0].get('games') if dates else []) or schedule.get('games') or []
    for g in games:
        for side in ('home', 'away'):
            pp = (g.get('teams') or {}).get(side, {}).get('probablePitcher') or {}
            n = pp.get('fullName') or pp.get('name')
            if n:
                starters[n] = {'expected_ip': 5.5}
    # Fallback if empty: use names from pitcher-stats
    if not starters:
        ps = load_json(os.path.join(DATA_DIR, f'pitcher-stats-{date}.json')).get('pitchers', [])
        for p in ps:
            n = p.get('name')
            if n:
                starters[n] = {'expected_ip': 5.5}
    out = {'date': date, 'bullpens': bullpens, 'starters': starters}
    save_json(out, os.path.join(DATA_DIR, f'bullpen-metrics-{date}.json'))


def fetch_implied_totals(date: str):
    """
    Real implied team totals using The Odds API if ODDS_API_KEY is set.
    If not available, defaults to None.
    Structure: { date, teams: { TEAM: implied_runs } }
    """
    schedule = load_json(os.path.join(DATA_DIR, f'fresh-schedule-{date}.json')) or load_json(os.path.join(DATA_DIR, 'todays-schedule.json'))
    teams = teams_from_schedule(schedule)
    # Normalize schedule abbreviations to standard MLB codes used by odds mapping
    NORMALIZE_ABBR = {
        'WSN': 'WSH', 'CHW': 'CWS', 'TBR': 'TB', 'KCR': 'KC', 'SDP': 'SD',
        'AZ': 'ARI', 'ATH': 'OAK'
    }
    norm_teams = []
    norm_to_orig: Dict[str, str] = {}
    for t in teams:
        nt = NORMALIZE_ABBR.get(t, t)
        if nt not in norm_teams:
            norm_teams.append(nt)
        # Prefer first seen original key for output mapping later
        norm_to_orig.setdefault(nt, t)
    out_map = {t: None for t in norm_teams}
    api_key = os.getenv('ODDS_API_KEY') or os.getenv('THE_ODDS_API_KEY') or None
    def american_to_prob(odds):
        try:
            o = float(odds)
            if o < 0:
                return (-o) / ((-o) + 100.0)
            else:
                return 100.0 / (o + 100.0)
        except Exception:
            return None
    def pick_number(vals):
        vals = [float(v) for v in vals if v is not None]
        if not vals:
            return None
        vals.sort()
        return vals[len(vals)//2]  # median
    def fill_from_total_and_moneyline(T, ml_home, ml_away, ha, aa):
        # Compute from game total and moneylines; de-vig probabilities then split
        need_ha = out_map.get(ha) is None
        need_aa = out_map.get(aa) is None
        if not (need_ha or need_aa):
            return
        ph = american_to_prob(pick_number(ml_home)) if isinstance(ml_home, (list, tuple)) else american_to_prob(ml_home)
        pa = american_to_prob(pick_number(ml_away)) if isinstance(ml_away, (list, tuple)) else american_to_prob(ml_away)
        if T is not None and ph is not None and pa is not None:
            s = ph + pa
            if s > 0:
                ph /= s
                pa /= s
            k = 1.4
            delta = k * (ph - 0.5)
            home_tot = max(2.0, min(8.5, (T/2.0) + delta))
            away_tot = max(2.0, min(8.5, (T/2.0) - delta))
            if need_ha and ha in out_map:
                out_map[ha] = home_tot
            if need_aa and aa in out_map:
                out_map[aa] = away_tot
        elif T is not None:
            even = max(2.0, min(8.5, T / 2.0))
            if need_ha and ha in out_map and out_map.get(ha) is None:
                out_map[ha] = even
            if need_aa and aa in out_map and out_map.get(aa) is None:
                out_map[aa] = even

    # 1) Try The Odds API if key provided
    if api_key:
        try:
            # Fetch totals and team totals (no date filter in free tier; returns upcoming)
            base = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/"
            params = f"?regions=us,us2,eu,uk,au&markets=team_totals,totals,h2h&oddsFormat=american&dateFormat=iso&apiKey={api_key}"
            url1 = base + params
            r = requests.get(url1, timeout=30)
            games = None
            if r.status_code == 200:
                games = r.json()
                # Debug: dump raw odds to help diagnose mapping issues
                try:
                    save_json({'date': date, 'url': url1, 'count': len(games) if isinstance(games, list) else None, 'raw': games}, os.path.join(DATA_DIR, f'odds-raw-{date}.json'))
                except Exception:
                    pass
                # If list is empty, try again without team_totals market (some plans/bookmakers omit it)
                if isinstance(games, list) and len(games) == 0:
                    url2 = base + f"?regions=us,eu,uk,au&markets=totals,h2h&oddsFormat=american&dateFormat=iso&apiKey={api_key}"
                    r2 = requests.get(url2, timeout=30)
                    if r2.status_code == 200:
                        games = r2.json()
                        try:
                            save_json({'date': date, 'url': url2, 'count': len(games) if isinstance(games, list) else None, 'raw': games}, os.path.join(DATA_DIR, f'odds-raw-2-{date}.json'))
                        except Exception:
                            pass
                    else:
                        # Log non-200 from fallback
                        try:
                            save_json({'date': date, 'url': url2, 'status': r2.status_code, 'text': r2.text[:500]}, os.path.join(DATA_DIR, f'odds-debug-2-{date}.json'))
                        except Exception:
                            pass
            else:
                # Non-200 primary; log debug and try fallback without team_totals
                try:
                    save_json({'date': date, 'url': url1, 'status': r.status_code, 'text': r.text[:500]}, os.path.join(DATA_DIR, f'odds-debug-{date}.json'))
                except Exception:
                    pass
                url2 = base + f"?regions=us,eu,uk,au&markets=totals,h2h&oddsFormat=american&dateFormat=iso&apiKey={api_key}"
                r2 = requests.get(url2, timeout=30)
                if r2.status_code == 200:
                    games = r2.json()
                    try:
                        save_json({'date': date, 'url': url2, 'count': len(games) if isinstance(games, list) else None, 'raw': games}, os.path.join(DATA_DIR, f'odds-raw-2-{date}.json'))
                    except Exception:
                        pass
                else:
                    try:
                        save_json({'date': date, 'url': url2, 'status': r2.status_code, 'text': r2.text[:500]}, os.path.join(DATA_DIR, f'odds-debug-2-{date}.json'))
                    except Exception:
                        pass

            # If we have games data, parse it
            if isinstance(games, list):
                # Team name normalization from odds to MLB abbr
                NAME_TO_ABBR = {
                    'Arizona Diamondbacks': 'ARI', 'Atlanta Braves': 'ATL', 'Baltimore Orioles': 'BAL', 'Boston Red Sox': 'BOS',
                    'Chicago Cubs': 'CHC', 'Chicago White Sox': 'CWS', 'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE',
                    'Colorado Rockies': 'COL', 'Detroit Tigers': 'DET', 'Houston Astros': 'HOU', 'Kansas City Royals': 'KC',
                    'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD', 'Miami Marlins': 'MIA', 'Milwaukee Brewers': 'MIL',
                    'Minnesota Twins': 'MIN', 'New York Mets': 'NYM', 'New York Yankees': 'NYY', 'Oakland Athletics': 'OAK',
                    'Philadelphia Phillies': 'PHI', 'Pittsburgh Pirates': 'PIT', 'San Diego Padres': 'SD', 'Seattle Mariners': 'SEA',
                    'San Francisco Giants': 'SF', 'St. Louis Cardinals': 'STL', 'Tampa Bay Rays': 'TB', 'Texas Rangers': 'TEX',
                    'Toronto Blue Jays': 'TOR', 'Washington Nationals': 'WSH',
                    # Common short variants seen in odds feeds
                    'LA Dodgers': 'LAD', 'LA Angels': 'LAA', 'NY Mets': 'NYM', 'NY Yankees': 'NYY',
                    'SF Giants': 'SF', 'SD Padres': 'SD', 'TB Rays': 'TB', 'KC Royals': 'KC', 'CWS White Sox': 'CWS', 'CHI White Sox': 'CWS',
                    'CHI Cubs': 'CHC', 'Oakland A\'s': 'OAK', 'Arizona D-backs': 'ARI', 'Arizona Dbacks': 'ARI'
                }
                def _norm(s: str) -> str:
                    import re
                    return re.sub(r"[^a-z0-9]", "", (s or "").lower())
                # Build a normalized lookup for robust matching
                NORM_TO_ABBR = { _norm(k): v for k, v in NAME_TO_ABBR.items() }
                # Also include bare nicknames
                nickname_variants = {
                    'dodgers':'LAD','angels':'LAA','yankees':'NYY','mets':'NYM','redsox':'BOS','orioles':'BAL','rays':'TB',
                    'bluejays':'TOR','guardians':'CLE','tigers':'DET','twins':'MIN','royals':'KC','white sox':'CWS','whitesox':'CWS','cubs':'CHC',
                    'astros':'HOU','mariners':'SEA','rangers':'TEX','athletics':'OAK','as':'OAK','a\'s':'OAK','dbacks':'ARI','dbacks':'ARI','diamondbacks':'ARI',
                    'padres':'SD','giants':'SF','rockies':'COL','dodger':'LAD','angel':'LAA','phillies':'PHI','pirates':'PIT','brewers':'MIL','braves':'ATL','nationals':'WSH','cardinals':'STL','marlins':'MIA','reds':'CIN'
                }
                for k, v in list(nickname_variants.items()):
                    NORM_TO_ABBR.setdefault(_norm(k), v)
                def name_to_abbr(name: str) -> str | None:
                    if not name:
                        return None
                    # exact first
                    if name in NAME_TO_ABBR:
                        return NAME_TO_ABBR[name]
                    n = _norm(name)
                    if n in NORM_TO_ABBR:
                        return NORM_TO_ABBR[n]
                    # try substring contains
                    for nk, ab in NORM_TO_ABBR.items():
                        if nk and nk in n:
                            return ab
                    return None
                # Iterate odds games and compute implied totals per game
                for g in games:
                    home_name = g.get('home_team')
                    away_name = g.get('away_team')
                    ha = name_to_abbr(home_name)
                    aa = name_to_abbr(away_name)
                    if not ha or not aa:
                        continue
                    markets = g.get('bookmakers', [])
                    team_totals = {}
                    totals_points = []
                    ml_home = []
                    ml_away = []
                    for bk in markets:
                        for mk in bk.get('markets', []):
                            key = mk.get('key')
                            outs = mk.get('outcomes', [])
                            if key == 'team_totals':
                                for out in outs:
                                    team_str = out.get('team') or out.get('description') or out.get('name') or ''
                                    point = out.get('point') or out.get('total') or out.get('line')
                                    abbr = name_to_abbr(team_str)
                                    try:
                                        if abbr and point is not None:
                                            team_totals[abbr] = float(point)
                                    except Exception:
                                        pass
                            elif key == 'totals':
                                for out in outs:
                                    pt = out.get('point') or out.get('total') or out.get('line')
                                    try:
                                        if pt is not None:
                                            totals_points.append(float(pt))
                                    except Exception:
                                        pass
                            elif key == 'h2h':
                                for out in outs:
                                    if out.get('name') == home_name:
                                        ml_home.append(out.get('price'))
                                    elif out.get('name') == away_name:
                                        ml_away.append(out.get('price'))
                    if team_totals.get(ha) is not None and ha in out_map:
                        out_map[ha] = team_totals[ha]
                    if team_totals.get(aa) is not None and aa in out_map:
                        out_map[aa] = team_totals[aa]
                    T = pick_number(totals_points)
                    fill_from_total_and_moneyline(T, ml_home, ml_away, ha, aa)
        except Exception:
            # Continue to ESPN fallback
            pass

    # 2) ESPN scoreboard fallback by date
    try:
        yyyymmdd = date.replace('-', '')
        espn_url = f"https://site.api.espn.com/apis/v2/sports/baseball/mlb/scoreboard?dates={yyyymmdd}"
        er = requests.get(espn_url, timeout=20)
        if er.status_code == 200:
            ed = er.json()
            events = ed.get('events', [])
            for ev in events:
                comps = ev.get('competitions') or []
                if not comps:
                    continue
                comp = comps[0]
                competitors = comp.get('competitors', [])
                ha = aa = None
                home_name = away_name = None
                # Map ESPN abbr to MLB abbr (they usually align)
                for c in competitors:
                    team = c.get('team') or {}
                    abbr = team.get('abbreviation') or team.get('shortDisplayName') or ''
                    if c.get('homeAway') == 'home':
                        ha = NORMALIZE_ABBR.get(abbr, abbr)
                        home_name = team.get('displayName')
                    elif c.get('homeAway') == 'away':
                        aa = NORMALIZE_ABBR.get(abbr, abbr)
                        away_name = team.get('displayName')
                if not ha or not aa:
                    continue
                odds_list = comp.get('odds', []) or []
                T = None
                ml_home = None
                ml_away = None
                # Examine all odds providers, pick first that yields something
                import re as _re
                for o in odds_list:
                    if T is None:
                        try:
                            if o.get('overUnder') is not None:
                                T = float(o.get('overUnder'))
                            elif o.get('details'):
                                m = _re.search(r"(?i)O/?U\s*([0-9]+(?:\.[0-9]+)?)", str(o.get('details')))
                                if m:
                                    T = float(m.group(1))
                        except Exception:
                            pass
                    if ml_home is None or ml_away is None:
                        try:
                            if ml_home is None:
                                ml_home = o.get('homeTeamOdds', {}).get('moneyLine')
                            if ml_away is None:
                                ml_away = o.get('awayTeamOdds', {}).get('moneyLine')
                        except Exception:
                            pass
                    if T is not None and ml_home is not None and ml_away is not None:
                        break
                # Prefer direct team totals if any appear in detail text (rare on ESPN)
                fill_from_total_and_moneyline(T, ml_home, ml_away, ha, aa)
    except Exception:
        pass

    # As a final fallback, set a league-average implied runs (4.5) so it's not blank
    filled_norm = {k: (out_map.get(k) if out_map.get(k) is not None else 4.5) for k in out_map.keys()}
    # Map back to original schedule keys to be consistent with other files
    final_out = {norm_to_orig.get(k, k): v for k, v in filled_norm.items()}
    save_json({'date': date, 'teams': final_out}, os.path.join(DATA_DIR, f'implied-totals-{date}.json'))


def fetch_lineups(date: str):
    """
    Real lineups where available using MLB StatsAPI boxscore for each game in today's schedule.
    Output: { date, lineups: { TEAM: [ {name, slot} ] } }
    """
    schedule = load_json(os.path.join(DATA_DIR, f'fresh-schedule-{date}.json')) or load_json(os.path.join(DATA_DIR, 'todays-schedule.json'))
    dates = schedule.get('dates') or []
    games = (dates[0].get('games') if dates else []) or schedule.get('games') or []
    lineups: Dict[str, List[Dict[str, Any]]] = {}
    def fetch_game_lineup(game_pk: int):
        try:
            url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
            r = requests.get(url, timeout=20)
            if r.status_code != 200:
                return None
            return r.json()
        except Exception:
            return None
    # Collect gamePks
    gpks = []
    for g in games:
        if g.get('gamePk'):
            gpks.append(int(g['gamePk']))
    # Map abbr from schedule teams using IDs
    id_to_abbr, _ = _team_id_to_abbr_map(_extract_team_ids(schedule))
    for abbr in id_to_abbr.values():
        if abbr and abbr not in lineups:
            lineups[abbr] = []
    # Parallel fetch
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(fetch_game_lineup, pk) for pk in gpks]
        for f in as_completed(futures):
            data = f.result()
            if not data:
                continue
            teams_data = data.get('teams') or {}
            for side in ('home', 'away'):
                td = teams_data.get(side) or {}
                team = (td.get('team') or {}).get('abbreviation') or (td.get('team') or {}).get('triCode')
                if not team:
                    continue
                players = td.get('players') or {}
                entries = []
                for k, p in players.items():
                    try:
                        order = p.get('battingOrder')
                        if not order:
                            continue
                        # battingOrder is like '101', '902'; take first two digits for slot
                        slot = int(str(order)[:2])
                        name = (p.get('person') or {}).get('fullName')
                        if name and 1 <= slot <= 9:
                            entries.append({'name': name, 'slot': slot})
                    except Exception:
                        continue
                if entries:
                    # Sort by slot and keep top 9
                    entries = sorted(entries, key=lambda x: x['slot'])[:9]
                    lineups[team] = entries
                # If no entries found, try pulling projected starters from live feed probable lineups
                if not lineups.get(team):
                    try:
                        live = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{int(td.get('team',{}).get('gamePk',0))}/feed/live", timeout=20)
                        if live.status_code == 200:
                            ld = live.json()
                            roster = (((ld.get('gameData') or {}).get('players')) or {})
                            # This is a stretch; if boxscore has no orders yet, we will fall back below
                    except Exception:
                        pass
    # Fallback: if any lineup empty, synthesize from active roster data (player-stats)
    if any(len(v) == 0 for v in lineups.values()):
        players = load_json(os.path.join(DATA_DIR, f'player-stats-{date}.json')).get('players', [])
        by_team: Dict[str, List[dict]] = {}
        for p in players:
            t = p.get('team')
            if not t:
                continue
            by_team.setdefault(t, []).append(p)
        for team, entries in lineups.items():
            if entries:
                continue
            roster = [p for p in by_team.get(team, []) if p.get('position') not in ('Pitcher', 'Unknown')]
            # Deduplicate by name preserve order
            seen = set()
            uniq = []
            for p in roster:
                n = p.get('name')
                if not n or n in seen:
                    continue
                seen.add(n)
                uniq.append(p)
            uniq = uniq[:9]
            lineups[team] = [{'name': p.get('name'), 'slot': i+1} for i, p in enumerate(uniq)]
    save_json({'date': date, 'lineups': lineups}, os.path.join(DATA_DIR, f'lineups-{date}.json'))
    # Also save a projected-lineups alias for clarity
    save_json({'date': date, 'lineups': lineups}, os.path.join(DATA_DIR, f'projected-lineups-{date}.json'))


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    date = args.date

    fetch_statcast_metrics(date)
    fetch_pitcher_advanced(date)
    fetch_pitch_type_metrics(date)
    fetch_bullpen_metrics(date)
    fetch_implied_totals(date)
    fetch_lineups(date)


if __name__ == '__main__':
    main()
