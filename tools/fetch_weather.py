#!/usr/bin/env python3
from __future__ import annotations
import os
from datetime import datetime
import argparse

# Import the existing fetcher and save utility
import sys
APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
from tools.fetch_basics import fetch_ballpark_weather, DATA_DIR, save


def main():
    parser = argparse.ArgumentParser(description='Fetch only ballpark weather/park factors')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()
    date = args.date

    out = fetch_ballpark_weather(date)
    save(out, os.path.join(DATA_DIR, f'ballpark-weather-{date}.json'))


if __name__ == '__main__':
    main()
