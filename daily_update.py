#!/usr/bin/env python3
from __future__ import annotations
import os
from datetime import datetime

APP_DIR = os.path.dirname(os.path.abspath(__file__))

def main():
    import argparse, subprocess, sys
    parser = argparse.ArgumentParser(description='Self-contained daily runner')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    date = args.date

    # Also compute yesterday for HR hitters
    from datetime import timedelta
    yday = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    steps = [
        [sys.executable, os.path.join(APP_DIR, 'tools', 'fetch_basics.py'), '--date', date],
    [sys.executable, os.path.join(APP_DIR, 'tools', 'fetch_extras.py'), '--date', date],
    [sys.executable, os.path.join(APP_DIR, 'tools', 'fetch_h2h.py'), '--date', date],
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
