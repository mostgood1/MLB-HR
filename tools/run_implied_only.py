#!/usr/bin/env python3
import sys, os
from datetime import datetime
sys.path.append(os.path.dirname(__file__))
from fetch_extras import fetch_implied_totals

def main():
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    fetch_implied_totals(date)

if __name__ == '__main__':
    main()
