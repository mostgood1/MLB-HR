#!/usr/bin/env python3
"""
Deterministic HR Score Generator (self-contained copy)

This is a copy of the core generator adjusted to use hr_app/data by default.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import unicodedata
import re

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, 'data')

TEAM_MAP = {
    'ATH': 'OAK',
    'AZ': 'ARI',
    'WSH': 'WSH',
    'SF': 'SF',
    'SD': 'SD',
    'TB': 'TB',
    'CWS': 'CWS',
}

SCHED_ABBR_MAP = {
    'WSN': 'WSH',
    'CHW': 'CWS',
    'TBR': 'TB',
    'KCR': 'KC',
    'SDP': 'SD',
}

PARK_ALIAS = {
    'ARI': 'AZ',
    'CWS': 'CHW',
}

# Venues with domes or retractable roofs where weather effects should be minimized
ROOFED_VENUES = {
    'Tropicana Field',
    'American Family Field',
    'Rogers Centre',
    'Chase Field',
    'loanDepot Park',
    'Globe Life Field',
    'T-Mobile Park',
    'Minute Maid Park',
    'Miller Park',  # historical name of American Family Field
}

# Fallback mapping from full team names (as provided by MLB schedule) to abbreviations
TEAM_NAME_TO_ABBR = {
    'Arizona Diamondbacks': 'ARI', 'Atlanta Braves': 'ATL', 'Baltimore Orioles': 'BAL', 'Boston Red Sox': 'BOS',
    'Chicago Cubs': 'CHC', 'Chicago White Sox': 'CWS', 'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE',
    'Colorado Rockies': 'COL', 'Detroit Tigers': 'DET', 'Houston Astros': 'HOU', 'Kansas City Royals': 'KC',
    'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD', 'Miami Marlins': 'MIA', 'Milwaukee Brewers': 'MIL',
    'Minnesota Twins': 'MIN', 'New York Mets': 'NYM', 'New York Yankees': 'NYY', 'Oakland Athletics': 'OAK',
    'Philadelphia Phillies': 'PHI', 'Pittsburgh Pirates': 'PIT', 'San Diego Padres': 'SD', 'Seattle Mariners': 'SEA',
    'San Francisco Giants': 'SF', 'St. Louis Cardinals': 'STL', 'Tampa Bay Rays': 'TB', 'Texas Rangers': 'TEX',
    'Toronto Blue Jays': 'TOR', 'Washington Nationals': 'WSH'
}


def _norm_team(abbr: Optional[str]) -> Optional[str]:
    if not abbr:
        return abbr
    a = abbr.upper()
    a = SCHED_ABBR_MAP.get(a, a)
    return TEAM_MAP.get(a, a)


def _find_park_key(team_abbr: Optional[str], park_factors: dict, weather_conditions: dict) -> Optional[str]:
    if not team_abbr:
        return None
    primary = f"{team_abbr}_park"
    if primary in park_factors or primary in weather_conditions:
        return primary
    alias = PARK_ALIAS.get(team_abbr)
    if alias:
        alt = f"{alias}_park"
        if alt in park_factors or alt in weather_conditions:
            return alt
    return primary


def _norm_name_simple(n: str) -> str:
    if not n:
        return ''
    s = unicodedata.normalize('NFKD', str(n)).encode('ascii', 'ignore').decode('ascii')
    s = s.lower().strip()
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s)
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _list_data_files(prefix: str) -> List[str]:
    try:
        return sorted([f for f in os.listdir(DATA_DIR) if f.startswith(prefix)], reverse=True)
    except FileNotFoundError:
        return []


def _pick_dated_file(prefix: str, date_str: str) -> Tuple[str, str]:
    desired = f"{prefix}{date_str}.json"
    path = os.path.join(DATA_DIR, desired)
    if os.path.exists(path):
        return path, date_str
    candidates = _list_data_files(prefix)
    for f in candidates:
        if f.endswith('.json'):
            return os.path.join(DATA_DIR, f), f[len(prefix):-5]
    raise FileNotFoundError(f"No data files found for prefix '{prefix}'")


def _pick_dated_file_optional(prefix: str, date_str: str) -> Tuple[Optional[str], Optional[str]]:
    desired = os.path.join(DATA_DIR, f"{prefix}{date_str}.json")
    if os.path.exists(desired):
        return desired, date_str
    candidates = _list_data_files(prefix)
    for f in candidates:
        if f.endswith('.json'):
            return os.path.join(DATA_DIR, f), f[len(prefix):-5]
    return None, None


def _safe_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        s = str(value).strip()
        if s.startswith('.'):
            s = '0' + s
        return float(s)
    except Exception:
        return default


def _normalize(values: List[float]) -> Dict[float, float]:
    if not values:
        return {}
    lo, hi = min(values), max(values)
    if hi - lo < 1e-9:
        return {v: 50.0 for v in values}
    return {v: 100.0 * (v - lo) / (hi - lo) for v in values}


def _load_json(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _index_recent_form(recent: dict) -> Dict[str, float]:
    idx = {}
    for p in recent.get('players', []):
        n = p.get('name')
        if n:
            # Accept explicit rate or fallback to HR count over last 14 days
            if p.get('last_14_day_hr_rate') is not None:
                try:
                    idx[n] = float(p.get('last_14_day_hr_rate'))
                except Exception:
                    idx[n] = 0.0
            else:
                try:
                    cnt = float(p.get('last_14_day_hr') or 0.0)
                    idx[n] = cnt / 14.0
                except Exception:
                    idx[n] = 0.0
    return idx


def _index_pitchers(pitchers: dict) -> Dict[str, dict]:
    idx = {}
    for p in pitchers.get('pitchers', []):
        name = p.get('name')
        if not name:
            continue
        current = idx.get(name)
        era = _safe_float(p.get('era'), 0.0)
        hr = int(p.get('homeRunsAllowed') or 0)
        if current is None:
            idx[name] = {**p, 'era_f': era, 'hr_allowed_i': hr}
        else:
            if era > current.get('era_f', 0.0) or hr > current.get('hr_allowed_i', 0):
                idx[name] = {**p, 'era_f': era, 'hr_allowed_i': hr}
    return idx


def _index_statcast(metrics: dict) -> Dict[str, dict]:
    return metrics.get('metrics', {}) if metrics else {}


def _load_h2h_js_optional(path: str) -> Dict[str, Dict[str, dict]]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            txt = f.read()
        marker = 'const hitterVsPitcherData ='
        i = txt.find(marker)
        if i == -1:
            return {}
        body = txt[i + len(marker):].strip()
        if body.endswith(';'):
            body = body[:-1]
        body = re.sub(r"//.*", "", body)
        body = re.sub(r"/\*[\s\S]*?\*/", "", body)
        start = body.find('{')
        end = body.rfind('}')
        if start == -1 or end == -1:
            return {}
        json_like = body[start:end+1]
        return json.loads(json_like)
    except Exception:
        return {}


def _index_pitcher_advanced(data: Optional[dict]) -> Dict[str, dict]:
    if not data:
        return {}
    out = {}
    for p in data.get('pitchers', []):
        n = p.get('name')
        if not n:
            continue
        out[n] = p
    return out


def _index_pitch_type(data: Optional[dict]):
    if not data:
        return {}, {}
    pit = {}
    bat = {}
    pitchers = data.get('pitchers') or {}
    batters = data.get('batters') or {}
    for name, pd in pitchers.items():
        tp = pd.get('top_pitches') or []
        tp_sorted = sorted(tp, key=lambda x: float(x.get('usage') or 0), reverse=True)[:2]
        pit[name] = tp_sorted
    for name, bd in batters.items():
        # Accept either nested {'xslg_by_pitch': {...}} or a flat mapping of pitch_name -> xSLG
        if isinstance(bd, dict) and 'xslg_by_pitch' in bd and isinstance(bd.get('xslg_by_pitch'), dict):
            bat[name] = bd.get('xslg_by_pitch') or {}
        elif isinstance(bd, dict):
            bat[name] = bd
        else:
            bat[name] = {}
    return pit, bat


def _index_bullpen(data: Optional[dict]):
    if not data:
        return {}, {}
    bp = {}
    starters = {}
    for t, bd in (data.get('bullpens') or {}).items():
        try:
            bp[_norm_team(t)] = float(bd.get('hr9'))
        except Exception:
            continue
    for n, sd in (data.get('starters') or {}).items():
        try:
            starters[n] = float(sd.get('expected_ip'))
        except Exception:
            continue
    return bp, starters


def _index_implied(data: Optional[dict]) -> Dict[str, float]:
    if not data:
        return {}
    if 'teams' in data:
        out = {}
        for t, td in (data.get('teams') or {}).items():
            # Accept either a dict with fields or a direct numeric value
            try:
                if isinstance(td, (int, float, str)):
                    out[_norm_team(t)] = _safe_float(td, 0.0)
                elif isinstance(td, dict):
                    out[_norm_team(t)] = _safe_float(td.get('implied_runs') or td.get('implied_hr') or 0)
            except Exception:
                pass
        return out
    out = {}
    for t, v in data.items():
        try:
            out[_norm_team(t)] = float(v)
        except Exception:
            pass
    return out


def _park_weather_factor(ballpark: dict, weather: dict) -> float:
    if not ballpark and not weather:
        return 1.0
    hr_factor = float(ballpark.get('hr_factor', 1.0)) if ballpark else 1.0
    bonus = 0.0
    # Neutralize weather if dome/roof likely closed
    venue_name = (ballpark or {}).get('venue_name') or ''
    roof_closed = (weather or {}).get('roof') == 'closed' or venue_name in ROOFED_VENUES
    if weather and not roof_closed:
        wind = (weather.get('wind_direction') or '').lower()
        ws = float(weather.get('wind_speed') or 0)
        if 'out' in wind:
            bonus += min(0.03, 0.005 * max(0.0, ws))
        elif 'in' in wind:
            bonus -= min(0.02, 0.004 * max(0.0, ws))
        temp = float(weather.get('temperature') or 70)
        if temp >= 85:
            bonus += 0.02
        elif temp <= 55:
            bonus -= 0.02
    return max(0.9, min(1.1, hr_factor * (1.0 + bonus)))


def _compute_scores(date_str: Optional[str] = None) -> Dict:
    target_date = date_str or datetime.now().strftime('%Y-%m-%d')

    player_path, players_date = _pick_dated_file('player-stats-', target_date)
    pitcher_path, pitchers_date = _pick_dated_file('pitcher-stats-', target_date)
    recent_path, recent_date = _pick_dated_file('recent-performance-', target_date)

    alt_sched = os.path.join(DATA_DIR, 'todays-schedule.json')
    use_todays = False
    if os.path.exists(alt_sched):
        try:
            ts = _load_json(alt_sched)
            ts_date = None
            if isinstance(ts.get('dates'), list) and ts['dates']:
                ts_date = ts['dates'][0].get('date')
            if ts_date == target_date:
                schedule_path = alt_sched
                schedule_date = ts_date
                use_todays = True
        except Exception:
            pass
    if not use_todays:
        try:
            schedule_path, schedule_date = _pick_dated_file('fresh-schedule-', target_date)
        except FileNotFoundError:
            if os.path.exists(alt_sched):
                schedule_path = alt_sched
                try:
                    schedule_date = ts_date or target_date
                except Exception:
                    schedule_date = target_date
            else:
                raise

    try:
        statcast_path, statcast_date = _pick_dated_file('statcast-metrics-', target_date)
    except FileNotFoundError:
        statcast_path = os.path.join(DATA_DIR, 'statcast-metrics.json')
        statcast_date = _load_json(statcast_path).get('date', 'unknown') if os.path.exists(statcast_path) else 'unknown'

    try:
        ballpark_path, bw_date = _pick_dated_file('ballpark-weather-', target_date)
        ballpark_data = _load_json(ballpark_path)
    except FileNotFoundError:
        ballpark_data = {}

    players_data = _load_json(player_path)
    pitchers_data = _load_json(pitcher_path)
    recent_data = _load_json(recent_path)
    schedule = _load_json(schedule_path)
    statcast = _load_json(statcast_path) if os.path.exists(statcast_path) else {}

    try:
        p_adv_path, _ = _pick_dated_file_optional('pitcher-advanced-', target_date)
        pitcher_adv = _load_json(p_adv_path) if p_adv_path else None
    except Exception:
        pitcher_adv = None
    try:
        pitch_type_path, _ = _pick_dated_file_optional('pitch-type-metrics-', target_date)
        pitch_type_data = _load_json(pitch_type_path) if pitch_type_path else None
    except Exception:
        pitch_type_data = None
    try:
        bullpen_path, _ = _pick_dated_file_optional('bullpen-metrics-', target_date)
        bullpen_data = _load_json(bullpen_path) if bullpen_path else None
    except Exception:
        bullpen_data = None
    try:
        implied_path, _ = _pick_dated_file_optional('implied-totals-', target_date)
        implied_data = _load_json(implied_path) if implied_path else None
    except Exception:
        implied_data = None
    try:
        lineups_path, _ = _pick_dated_file_optional('lineups-', target_date)
        lineups_data = _load_json(lineups_path) if lineups_path else None
    except Exception:
        lineups_data = None

    recent_idx = _index_recent_form(recent_data)
    pitcher_idx = _index_pitchers(pitchers_data)
    statcast_idx = _index_statcast(statcast)
    h2h_idx = _load_h2h_js_optional(os.path.join(DATA_DIR, 'hitter-vs-pitcher.js'))
    pitcher_adv_idx = _index_pitcher_advanced(pitcher_adv)
    pitcher_top_pitches_idx, batter_xslg_by_pitch_idx = _index_pitch_type(pitch_type_data)
    bullpen_hr9_by_team, starter_expected_ip = _index_bullpen(bullpen_data)
    implied_by_team = _index_implied(implied_data)

    lineup_slot_by_player = {}
    lineup_slot_by_norm_player = {}
    if lineups_data and isinstance(lineups_data.get('lineups'), dict):
        for team_abbr, entries in (lineups_data.get('lineups') or {}).items():
            t = _norm_team(team_abbr)
            if not t:
                continue
            for e in entries:
                n = (e.get('name') or '').strip()
                try:
                    slot = int(e.get('slot'))
                except Exception:
                    slot = None
                if n and slot:
                    lineup_slot_by_player[(t, n)] = slot
                    lineup_slot_by_norm_player[(t, _norm_name_simple(n))] = slot

    opp_pitcher_by_team = {}
    games = schedule.get('games') or schedule.get('dates', [{}])[0].get('games', [])
    for g in games:
        teams_obj = g.get('teams') or {}
        home_abbr = None
        away_abbr = None
        if 'home_team' in g and 'away_team' in g:
            home_abbr = _norm_team(g.get('home_team'))
            away_abbr = _norm_team(g.get('away_team'))
        elif teams_obj:
            try:
                home_team = teams_obj.get('home', {}).get('team', {})
                away_team = teams_obj.get('away', {}).get('team', {})
                # Try multiple fields and fallback to full-name mapping
                def team_to_abbr(tobj: dict) -> Optional[str]:
                    ab = tobj.get('abbreviation') or tobj.get('triCode') or tobj.get('teamCode')
                    if not ab:
                        nm = tobj.get('name') or tobj.get('teamName') or tobj.get('shortName')
                        ab = TEAM_NAME_TO_ABBR.get(nm or '', None)
                    return _norm_team(ab) if ab else None
                home_abbr = team_to_abbr(home_team)
                away_abbr = team_to_abbr(away_team)
            except Exception:
                pass
        home_p = (g.get('home_pitcher') or {}).get('name')
        away_p = (g.get('away_pitcher') or {}).get('name')
        if not home_p or not away_p:
            pp = g.get('probablePitchers') or {}
            hp = pp.get('home') or {}
            ap = pp.get('away') or {}
            home_p = home_p or hp.get('fullName') or hp.get('name')
            away_p = away_p or ap.get('fullName') or ap.get('name')
        if (not home_p or not away_p) and teams_obj:
            try:
                hp = teams_obj.get('home', {}).get('probablePitcher') or {}
                ap = teams_obj.get('away', {}).get('probablePitcher') or {}
                home_p = home_p or hp.get('fullName') or hp.get('lastFirstName') or hp.get('name')
                away_p = away_p or ap.get('fullName') or ap.get('lastFirstName') or ap.get('name')
            except Exception:
                pass
        home = home_abbr
        away = away_abbr
        if home and away:
            opp_pitcher_by_team[home] = {'opp_pitcher': away_p or 'TBD', 'home': True, 'opp_team': away}
            opp_pitcher_by_team[away] = {'opp_pitcher': home_p or 'TBD', 'home': False, 'opp_team': home}

    park_factors = (ballpark_data or {}).get('ballpark_factors', {})
    weather_conditions = (ballpark_data or {}).get('weather_conditions', {})

    adv_barrels, adv_hrfb, adv_fbpct, adv_vshand_vals = [], [], [], []
    for pi in pitcher_adv_idx.values():
        if pi.get('barrel_rate_allowed') is not None:
            adv_barrels.append(float(pi.get('barrel_rate_allowed')))
        if pi.get('hr_fb') is not None:
            adv_hrfb.append(float(pi.get('hr_fb')))
        if pi.get('fb_pct') is not None:
            adv_fbpct.append(float(pi.get('fb_pct')))
        for hand_key in ('vsR', 'vsL'):
            h = pi.get(hand_key) or {}
            v = h.get('xslg') or h.get('hr_per_pa')
            if v is not None:
                try:
                    adv_vshand_vals.append(float(v))
                except Exception:
                    pass
    adv_barrels_norm = _normalize(adv_barrels) if adv_barrels else {}
    adv_hrfb_norm = _normalize(adv_hrfb) if adv_hrfb else {}
    adv_fbpct_norm = _normalize(adv_fbpct) if adv_fbpct else {}
    adv_vshand_norm = _normalize(adv_vshand_vals) if adv_vshand_vals else {}

    bullpen_hr9_by_team_norm = {}
    bullpen_hr9_by_team = {}
    
    bullpen_hr9_by_team_norm = _normalize(list(bullpen_hr9_by_team.values())) if bullpen_hr9_by_team else {}

    teams_today = set()
    for k, v in opp_pitcher_by_team.items():
        teams_today.add(k)
        if v.get('opp_team'):
            teams_today.add(v['opp_team'])
    implied_vals_today = [implied_by_team.get(t) for t in teams_today if implied_by_team.get(t) is not None]
    market_scaler_by_team = {}
    if implied_vals_today:
        lo = min(implied_vals_today)
        hi = max(implied_vals_today)
        span = hi - lo if hi > lo else 1.0
        for t in teams_today:
            v = implied_by_team.get(t)
            if v is None:
                market_scaler_by_team[t] = 1.0
            else:
                norm01 = (v - lo) / span
                market_scaler_by_team[t] = 0.98 + 0.06 * max(0.0, min(1.0, norm01))
    else:
        for t in teams_today:
            market_scaler_by_team[t] = 1.0

    hitters = players_data.get('players') or []
    season_hrs, iso_vals, slg_vals, ev_vals, brl_vals = [], [], [], [], []
    filtered_hitters = []
    for p in hitters:
        pos = (p.get('position') or '').lower()
        if 'pitch' in pos:
            continue
        filtered_hitters.append(p)
        season_hrs.append(int(p.get('homeRuns') or 0))
        ba = _safe_float(p.get('battingAvg'))
        slg = _safe_float(p.get('sluggingPerc'))
        iso = max(0.0, slg - ba)
        slg_vals.append(slg)
        iso_vals.append(iso)
        sc = statcast_idx.get(p.get('name') or '', {})
        ev_vals.append(_safe_float(sc.get('exit_velocity')))
        brl_vals.append(_safe_float(sc.get('barrel_rate')))

    season_hr_norm = _normalize(season_hrs)
    iso_norm = _normalize(iso_vals)
    slg_norm = _normalize(slg_vals)
    ev_norm = _normalize(ev_vals)
    brl_norm = _normalize(brl_vals)

    pitcher_eras = [pi.get('era_f', 0.0) for pi in pitcher_idx.values() if pi.get('era_f') is not None]
    pitcher_hrs = [pi.get('hr_allowed_i', 0) for pi in pitcher_idx.values()]
    era_norm = _normalize(pitcher_eras) if pitcher_eras else {}
    hr_allowed_norm = _normalize(pitcher_hrs) if pitcher_hrs else {}

    results = []

    games = schedule.get('games') or schedule.get('dates', [{}])[0].get('games', [])

    for p in filtered_hitters:
        name = p.get('name')
        raw_team = p.get('team')
        team = _norm_team(raw_team)
        if not name or not team:
            continue
        if team not in opp_pitcher_by_team:
            continue

        ba = _safe_float(p.get('battingAvg'))
        slg = _safe_float(p.get('sluggingPerc'))
        iso = max(0.0, slg - ba)
        season_hr = int(p.get('homeRuns') or 0)
        recent_rate = float(recent_idx.get(name, 0.0))
        sc = statcast_idx.get(name, {})

        power_comp = (
            0.28 * season_hr_norm.get(season_hr, 50.0) +
            0.18 * iso_norm.get(iso, 50.0) +
            0.10 * slg_norm.get(slg, 50.0) +
            0.22 * ev_norm.get(_safe_float(sc.get('exit_velocity')), 50.0) +
            0.22 * brl_norm.get(_safe_float(sc.get('barrel_rate')), 50.0)
        )

        recent_comp = min(100.0, max(0.0, recent_rate * 100.0))

        opp_info = opp_pitcher_by_team.get(team, {})
        opp_name = opp_info.get('opp_pitcher') or 'TBD'
        opp_team = opp_info.get('opp_team')
        p_era = None
        p_hr_allowed = None
        pitcher_comp = 50.0
        opp_pi = None
        if opp_name:
            opp_pi = pitcher_idx.get(opp_name)
            if opp_pi is None:
                def _nn(n: str) -> str:
                    s = n.lower().strip()
                    s = re.sub(r"[\.'`-]", "", s)
                    s = re.sub(r"\s+", " ", s)
                    return s
                norm_lookup = { _nn(k): k for k in pitcher_idx.keys() }
                key = _nn(opp_name)
                if key in norm_lookup:
                    opp_pi = pitcher_idx.get(norm_lookup[key])
        if opp_name and opp_pi:
            p_era = opp_pi.get('era_f')
            p_hr_allowed = opp_pi.get('hr_allowed_i')
            e_score = _normalize([pi.get('era_f', 0.0) for pi in pitcher_idx.values()]).get(p_era, 50.0) if p_era is not None else 50.0
            h_score = _normalize([pi.get('hr_allowed_i', 0) for pi in pitcher_idx.values()]).get(p_hr_allowed, 50.0) if p_hr_allowed is not None else 50.0
            adv = pitcher_adv_idx.get(opp_name)
            batter_hand = (p.get('bats') or p.get('batting_hand') or (p.get('battingSide') or {}).get('code') or '').upper()
            vhand_val = None
            if adv:
                if batter_hand == 'R' and (adv.get('vsR') or {}).get('xslg') is not None:
                    vhand_val = float(adv['vsR']['xslg'])
                elif batter_hand == 'L' and (adv.get('vsL') or {}).get('xslg') is not None:
                    vhand_val = float(adv['vsL']['xslg'])
                elif batter_hand == 'R' and (adv.get('vsR') or {}).get('hr_per_pa') is not None:
                    vhand_val = float(adv['vsR']['hr_per_pa'])
                elif batter_hand == 'L' and (adv.get('vsL') or {}).get('hr_per_pa') is not None:
                    vhand_val = float(adv['vsL']['hr_per_pa'])
            vhand_score = _normalize([vhand_val] if vhand_val is not None else [50.0]).get(vhand_val, 50.0)
            barrel_score = 50.0
            hrfb_score = 50.0
            fbpct_score = 50.0
            pitcher_comp = (
                0.35 * e_score +
                0.25 * h_score +
                0.15 * barrel_score +
                0.10 * hrfb_score +
                0.05 * fbpct_score +
                0.10 * vhand_score
            )
            if opp_team:
                bp_norm = None
                exp_ip = 6.0
                try:
                    w_start = max(0.0, min(1.0, float(exp_ip) / 9.0))
                except Exception:
                    w_start = 0.7
                if bp_norm is not None:
                    pitcher_comp = w_start * pitcher_comp + (1.0 - w_start) * bp_norm

        is_home = opp_info.get('home') is True
        park_team = team if is_home else None
        if not park_team:
            for g in games:
                teams_obj = g.get('teams') or {}
                ht = _norm_team(g.get('home_team'))
                at = _norm_team(g.get('away_team'))
                if not ht or not at:
                    if teams_obj:
                        ht = _norm_team(teams_obj.get('home', {}).get('team', {}).get('abbreviation'))
                        at = _norm_team(teams_obj.get('away', {}).get('team', {}).get('abbreviation'))
                if ht and at and (team == ht or team == at):
                    park_team = ht
                    break
        # Determine park key with alias support and compute park/weather factor
        park_key = _find_park_key(park_team, park_factors, weather_conditions) if park_team else None
        park_factor = _park_weather_factor(
            (park_factors.get(park_key) if park_key else {}),
            (weather_conditions.get(park_key) if park_key else {})
        )

        # H2H bonus: small bounded bump if batter has strong SLG/HR history vs the pitcher
        h2h_bonus = 0.0
        if opp_name and name:
            try:
                # Prefer dated JSON; fall back to JS map already loaded if needed
                h2h_path, _ = _pick_dated_file_optional('hitter-vs-pitcher-', target_date)
                h2h_json = _load_json(h2h_path) if h2h_path else None
                h2h_map = (h2h_json.get('h2h') if isinstance(h2h_json, dict) else None) or {}
            except Exception:
                h2h_map = {}
            rec = ((h2h_map.get(name) or {}).get(opp_name)) if h2h_map else None
            if not rec and h2h_idx:
                rec = ((h2h_idx.get(name) or {}).get(opp_name))
            # Note: If needed, we could add normalized-name fallback here.
            if rec and isinstance(rec, dict):
                pa = int(rec.get('pa') or 0)
                hr = int(rec.get('hr') or 0)
                h2h_slg = _safe_float(rec.get('slg'))
                # Only consider if sample is non-trivial
                if pa >= 3:
                    # Compare to player's baseline SLG
                    player_slg = _safe_float(p.get('sluggingPerc'))
                    baseline = player_slg if player_slg > 0 else 0.0
                    delta = max(0.0, h2h_slg - baseline)
                    # Weight: scale with log(PA) and add extra for HRs
                    weight = min(1.0, (0.2 + 0.15 * min(3.0, (pa / 6.0))) + 0.1 * min(2, hr))
                    h2h_bonus = min(2.0, delta * 10.0 * weight)

        pitchtype_bonus = 0.0
        if opp_name and name:
            top_pitches = pitcher_top_pitches_idx.get(opp_name) or []
            xslg_by_pitch = batter_xslg_by_pitch_idx.get(name) or {}
            sc_xslg = _safe_float(sc.get('xslg')) if sc else 0.0
            baseline = sc_xslg if sc_xslg > 0 else slg
            agg = 0.0
            if xslg_by_pitch:
                for entry in top_pitches:
                    ptype = entry.get('type')
                    if not ptype:
                        continue
                    batter_vs = _safe_float(xslg_by_pitch.get(ptype))
                    if batter_vs <= 0 or baseline <= 0:
                        continue
                    delta = batter_vs - baseline
                    usage = _safe_float(entry.get('usage')) / 100.0
                    hr100 = _safe_float(entry.get('hr_per_100'))
                    weight = max(0.0, min(1.0, usage)) * (1.0 + min(0.5, hr100 / 3.0) if hr100 > 0 else 1.0)
                    agg += max(0.0, delta) * weight
            else:
                # Fallback: slight bonus based primarily on pitcher's top pitch usage
                for entry in top_pitches:
                    usage = _safe_float(entry.get('usage')) / 100.0
                    # Without batter xSLG-by-pitch or HR/100, give a tiny usage-based boost
                    agg += max(0.0, min(1.0, usage)) * 0.05
            if agg > 0:
                pitchtype_bonus = max(0.0, min(3.0, agg * 6.0))
        hr_score = (
            0.52 * power_comp +
            0.12 * recent_comp +
            0.26 * pitcher_comp +
            0.07 * (park_factor * 100.0) +
            h2h_bonus +
            pitchtype_bonus
        )
        market_factor = market_scaler_by_team.get(team, 1.0)
        hr_score = hr_score * market_factor

        pa_multiplier = 1.0
        slot = lineup_slot_by_player.get((team, name))
        if not slot:
            slot = lineup_slot_by_norm_player.get((team, _norm_name_simple(name)))
        if slot:
            pa_map = {1:1.08, 2:1.06, 3:1.05, 4:1.04, 5:1.03, 6:1.02, 7:1.01, 8:1.00, 9:0.99}
            pa_multiplier = pa_map.get(int(slot), 1.0)
            hr_score = hr_score * pa_multiplier

        hr_score = max(0.0, min(100.0, round(hr_score, 1)))

        factors = {
            'power_comp': round(power_comp, 1),
            'recent_comp': round(recent_comp, 1),
            'pitcher_comp': round(pitcher_comp, 1),
            'park_weather_pct': round((park_factor - 1.0) * 100.0, 1),
            'h2h_bonus': round(h2h_bonus, 2),
            'pitchtype_bonus': round(pitchtype_bonus, 1),
            'market_scaler_pct': round((market_factor - 1.0) * 100.0, 1),
            'pa_multiplier_pct': round((pa_multiplier - 1.0) * 100.0, 1)
        }

        results.append({
            'name': name,
            'team': team,
            'position': p.get('position') or 'Unknown',
            'hr_score': hr_score,
            'homer_likelihood_score': hr_score,
            'stats': {
                'homeRuns': season_hr,
                'battingAvg': _safe_float(p.get('battingAvg')),
                'slugging': _safe_float(p.get('sluggingPerc')),
                'iso': round(iso, 3)
            },
            'opposing_pitcher': opp_name or 'TBD',
            'pitcher_era': p_era if p_era is not None else None,
            'pitcher_hr_allowed': p_hr_allowed if p_hr_allowed is not None else None,
            'factors': factors
        })

    results.sort(key=lambda r: r['hr_score'], reverse=True)

    meta_date = schedule_date or players_date or target_date
    return {
        'date': meta_date,
        'generated_at': datetime.now().isoformat(),
        'source_dates': {
            'players': players_date,
            'pitchers': pitchers_date,
            'recent': recent_date,
            'schedule': schedule_date,
            'statcast': statcast_date
        },
        'total_players': len(results),
        'players': results
    }


def generate(date_str: Optional[str] = None, save: bool = True) -> Dict:
    data = _compute_scores(date_str)
    if save:
        out_path = os.path.join(DATA_DIR, f"hr-scores-{data['date']}.json")
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        print(f"Saved HR scores to {out_path} with {data['total_players']} players")
    return data


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate deterministic HR scores (self-contained)')
    parser.add_argument('--date', help='Target date YYYY-MM-DD (optional)')
    args = parser.parse_args()
    generate(args.date)
