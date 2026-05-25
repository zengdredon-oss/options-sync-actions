"""
Manual contract registration — добавить конкретный OCC ticker в систему.

Использование: когда пользователь говорит в чат «добавь GLD Jun17 2027 725C»,
Claude (или человек) запускает этот скрипт. Он:
  1. Парсит вход в OCC формат (или принимает уже-готовый OCC)
  2. Проверяет что контракт существует в Polygon (защита от опечаток)
  3. INSERT в contracts с discovery_source='manual'
  4. Backfill 2-летней истории через Polygon
  5. (Опционально) Добавить в tracked_contracts если флаг --watch

Поддерживает 2 формата ввода:
  - Полный OCC: O:GLD270617C00725000
  - Human-readable: --underlying GLD --exp 2027-06-17 --strike 725 --type C

Примеры:
  python add_manual_contract.py O:GLD270617C00725000
  python add_manual_contract.py --underlying GLD --exp 2027-06-17 --strike 725 --type C
  python add_manual_contract.py O:GLD270617C00725000 --watch  # сразу в watch-list
  python add_manual_contract.py O:GLD270617C00725000 --no-bars # только metadata

Использование в чате (через документацию):
  - Пользователь: «добавь GLD 725C на 17 июня 2027»
  - Claude: python add_manual_contract.py --underlying GLD --exp 2027-06-17 --strike 725 --type C --watch
"""
import argparse
import os
import re
import sys
import time
import urllib.request
import urllib.error
import json
from datetime import date, datetime, timedelta

from lib.d1 import D1Client
from lib.polygon import PolygonClient, PolygonError
from lib.telegram import send_alert


def log(msg):
    print(f'[{datetime.utcnow().strftime("%H:%M:%S")}] {msg}', flush=True)


def parse_human_to_occ(underlying, exp_date, strike, call_put):
    """
    Build OCC ticker from human-readable inputs.

    underlying: 'GLD'
    exp_date: '2027-06-17'
    strike: 725.0
    call_put: 'C' or 'P'

    Returns: 'O:GLD270617C00725000'
    """
    d = date.fromisoformat(exp_date)
    yy = f'{d.year % 100:02d}'
    mm = f'{d.month:02d}'
    dd = f'{d.day:02d}'
    cp = call_put.upper()
    if cp not in ('C', 'P'):
        raise ValueError(f'type must be C or P, got {call_put}')
    # Strike × 1000, 8 digits with leading zeros
    strike_int = int(round(float(strike) * 1000))
    if strike_int > 99999999:
        raise ValueError(f'strike too large: {strike}')
    return f'O:{underlying}{yy}{mm}{dd}{cp}{strike_int:08d}'


def parse_occ(occ):
    """Reverse: O:GLD270617C00725000 → (underlying, exp_date, strike, type)."""
    m = re.match(r'^O:([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$', occ)
    if not m:
        return None
    underlying, yy, mm, dd, cp, strike_str = m.groups()
    year = 2000 + int(yy)
    exp_date = f'{year}-{mm}-{dd}'
    strike = int(strike_str) / 1000.0
    return underlying, exp_date, strike, 'call' if cp == 'C' else 'put'


def verify_in_polygon(poly, occ):
    """
    Verify that OCC ticker exists in Polygon. Returns dict with contract info
    or None if not found.
    Protects against typos / non-existent contracts.
    """
    key = poly.keys[0]
    url = f'https://api.polygon.io/v3/reference/options/contracts/{occ}?apiKey={key}'
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        return data.get('results')
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def insert_contract(d1, occ, contract_info):
    """INSERT contract metadata with discovery_source='manual'."""
    parsed = parse_occ(occ)
    if not parsed:
        raise ValueError(f'cannot parse OCC: {occ}')
    underlying, exp_date, strike, ctype = parsed

    # Check if already exists
    existing = d1.select('SELECT ticker, discovery_source FROM contracts WHERE ticker = ?', [occ])
    if existing:
        log(f'  Contract already in D1 (discovery_source={existing[0].get("discovery_source")})')
        # Update discovery_source to 'manual' to mark user-requested
        d1.execute(
            "UPDATE contracts SET discovery_source = 'manual' WHERE ticker = ?",
            [occ]
        )
        return False

    # New insert
    d1.execute(
        "INSERT INTO contracts (ticker, underlying, contract_type, strike_price, expiration_date, "
        "exercise_style, shares_per_contract, primary_exchange, cfi, expired, discovered_at, discovery_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            occ, underlying, ctype, strike, exp_date,
            contract_info.get('exercise_style') if contract_info else None,
            contract_info.get('shares_per_contract', 100) if contract_info else 100,
            contract_info.get('primary_exchange') if contract_info else None,
            contract_info.get('cfi') if contract_info else None,
            0,
            datetime.utcnow().isoformat() + 'Z',
            'manual',
        ]
    )
    return True


def backfill_bars(d1, poly, occ, days_back=730):
    """Pull 2 years of daily bars for one contract."""
    today = date.today()
    from_date = (today - timedelta(days=days_back)).isoformat()
    to_date = today.isoformat()
    log(f'  Backfilling bars for {occ} ({from_date} → {to_date})...')
    bars = poly.get_daily_bars(occ, from_date, to_date)
    if not bars:
        log(f'  No bars in Polygon (contract never traded)')
        return 0
    now_ts = int(time.time())
    rows = []
    for b in bars:
        t_ms = b.get('t')
        if not t_ms:
            continue
        bar_date = date.fromtimestamp(t_ms / 1000).isoformat()
        rows.append((
            occ, bar_date, t_ms,
            b.get('o'), b.get('h'), b.get('l'), b.get('c'),
            b.get('v'), b.get('vw'), b.get('n'),
            'polygon', now_ts,
        ))
    if rows:
        d1.batch_insert(
            "INSERT OR REPLACE INTO option_bars (ticker, date, t, o, h, l, c, v, vw, n, source, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
            batch_size=100,
        )
    log(f'  Wrote {len(rows)} bars')
    return len(rows)


def add_to_watchlist(d1, occ):
    """Add to tracked_contracts (watch-list)."""
    d1.execute(
        "INSERT OR IGNORE INTO tracked_contracts (ticker, added_at, active) VALUES (?, ?, 1)",
        [occ, int(time.time())]
    )
    log(f'  Added {occ} to watch-list ⭐')


def main():
    ap = argparse.ArgumentParser(description='Manual contract registration')
    ap.add_argument('occ', nargs='?', help='Full OCC ticker (e.g. O:GLD270617C00725000)')
    ap.add_argument('--underlying', help='Alternative: underlying ticker (GLD)')
    ap.add_argument('--exp', help='Alternative: expiration YYYY-MM-DD')
    ap.add_argument('--strike', type=float, help='Alternative: strike price')
    ap.add_argument('--type', dest='ctype', default='C', help='Alternative: C or P (default C)')
    ap.add_argument('--watch', action='store_true', help='Also add to watch-list ⭐')
    ap.add_argument('--no-bars', action='store_true', help='Skip 2-year backfill (metadata only)')
    args = ap.parse_args()

    # Compose OCC ticker
    if args.occ:
        occ = args.occ
        if not occ.startswith('O:'):
            occ = 'O:' + occ
    elif args.underlying and args.exp and args.strike:
        occ = parse_human_to_occ(args.underlying, args.exp, args.strike, args.ctype)
        log(f'Composed OCC: {occ}')
    else:
        ap.error('Must provide either OCC ticker or --underlying + --exp + --strike')

    parsed = parse_occ(occ)
    if not parsed:
        log(f'ERROR: invalid OCC format: {occ}')
        sys.exit(1)
    log(f'Parsed: underlying={parsed[0]}, exp={parsed[1]}, strike={parsed[2]}, type={parsed[3]}')

    # Step 1: verify exists in Polygon
    poly = PolygonClient()
    log(f'Verifying {occ} in Polygon...')
    info = verify_in_polygon(poly, occ)
    if not info:
        log(f'ERROR: contract not found in Polygon — opечатка?')
        sys.exit(2)
    log(f'  OK: {info.get("contract_type")} strike {info.get("strike_price")} exp {info.get("expiration_date")}')

    # Step 2: insert metadata
    d1 = D1Client()
    log(f'Inserting into D1...')
    new = insert_contract(d1, occ, info)
    log(f'  {"INSERTED" if new else "UPDATED (already existed)"}')

    # Step 3: backfill bars
    n_bars = 0
    if not args.no_bars:
        n_bars = backfill_bars(d1, poly, occ)

    # Step 4: optional watch-list
    if args.watch:
        add_to_watchlist(d1, occ)

    # Step 5: notify
    msg = f'✅ Manual contract added: {occ}\n'
    msg += f'  {info.get("underlying_ticker")} {info.get("contract_type")} strike ${info.get("strike_price")} exp {info.get("expiration_date")}\n'
    msg += f'  Bars backfilled: {n_bars}\n'
    if args.watch:
        msg += f'  Added to watch-list ⭐\n'
    log(msg)
    try:
        send_alert(msg)
    except Exception as e:
        log(f'WARN: telegram alert failed: {e}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
