#!/usr/bin/env python3
from __future__ import annotations
import os
from datetime import datetime

APP_DIR = os.path.dirname(os.path.abspath(__file__))

def _load_env_from_dotenv():
    """Load simple KEY=VALUE pairs from a local .env file if present."""
    env_path = os.path.join(APP_DIR, '.env')
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith('#'):
                    continue
                if '=' in s:
                    k, v = s.split('=', 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k and v and k not in os.environ:
                        os.environ[k] = v
    except Exception:
        pass


def main():
    import argparse, subprocess, sys
    parser = argparse.ArgumentParser(description='Self-contained daily runner')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    date = args.date

    # Load .env so tools (implied totals) can see ODDS_API_KEY if not in OS env
    _load_env_from_dotenv()
    if os.getenv('ODDS_API_KEY') or os.getenv('THE_ODDS_API_KEY'):
        print('[env] Detected The Odds API key in environment')
    else:
        print('[env] No The Odds API key found; implied totals will fall back to ESPN/4.5')

    # Also compute yesterday for HR hitters
    from datetime import timedelta
    yday = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    steps = [
        [sys.executable, os.path.join(APP_DIR, 'tools', 'fetch_basics.py'), '--date', date],
        [sys.executable, os.path.join(APP_DIR, 'tools', 'fetch_extras.py'), '--date', date],
    [sys.executable, os.path.join(APP_DIR, 'tools', 'fetch_player_hr_odds.py'), '--date', date],
        # Use the robust H2H fetcher that writes both JS and dated JSON
        [sys.executable, os.path.join(APP_DIR, 'tools', 'fetch_hitter_vs_pitcher.py'), '--date', date],
        [sys.executable, os.path.join(APP_DIR, 'tools', 'fetch_hr_hitters.py'), '--date', yday],
        [sys.executable, os.path.join(APP_DIR, 'tools', 'fetch_hr_hitters.py'), '--date', date],
        [sys.executable, os.path.join(APP_DIR, 'generate_hr_scores_core.py'), '--date', date],
    ]
    for cmd in steps:
        print('Running:', ' '.join(cmd))
        subprocess.check_call(cmd)
    print('Daily update complete.')

if __name__ == '__main__':
    main()
