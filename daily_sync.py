"""
Daily sync from Polygon Free → Cloudflare D1.

Three phases:
  1. DISCOVERY — find new monthly contracts via /v3/reference/options/contracts
  2. PRICE UPDATE — fetch last 14 days of bars for every active monthly contract
  3. AGGREGATOR — fill option_bars gaps from yesterday using yahoo_quotes

Runs once per day at 22:05 UTC via GHA workflow. Silent on success, Telegram alert on failure.

CLI:
  python daily_sync.py                         # all tickers, full sync
  python daily_sync.py --tickers GLD           # one ticker only
  python daily_sync.py --max-contracts 50      # limit for testing
  python daily_sync.py --skip-discovery        # only price update
  python daily_sync.py --skip-prices           # only discovery
"""
import argparse
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta

from lib.d1 import D1Client, D1Error
from lib.polygon import PolygonClient, PolygonError, is_monthly_leaps
from lib.telegram import send_alert


def log(msg):
    print(f'[{datetime.utcnow().strftime("%H:%M:%S")}] {msg}', flush=True)


# =====================================================================================
# Phase 1 — Discovery
# =====================================================================================
def discovery_for_ticker(d1, poly, ticker):
    """Find new monthly contracts for one underlying. Returns (n_new, n_total_scanned)."""
    log(f'  [discovery] {ticker}: scanning Polygon contracts...')
    new_rows = []
    n_total = 0
    n_monthly = 0
    n_call = 0

    # Get list of contracts already in D1
    existing_resp = d1.select(
        "SELECT ticker FROM contracts WHERE underlying = ?",
        [ticker],
    )
    existing = {r['ticker'] for r in existing_resp}

    # Fetch active contracts from Polygon
    try:
        for c in poly.list_contracts(ticker, expired=False, limit=1000):
            n_total += 1
            if c.get('contract_type') != 'call':
                continue
            n_call += 1
            exp = c.get('expiration_date', '')
            if not is_monthly_leaps(exp):
                continue
            n_monthly += 1
            occ = c.get('ticker')
            if not occ or occ in existing:
                continue
            new_rows.append((
                occ,
                ticker,
                'call',
                c.get('strike_price'),
                exp,
                c.get('exercise_style'),
                c.get('shares_per_contract', 100),
                c.get('primary_exchange'),
                c.get('cfi'),
                0,  # expired = 0 since we queried expired=false
                datetime.utcnow().isoformat() + 'Z',
            ))
    except PolygonError as e:
        log(f'  [discovery] {ticker}: WARN polygon error: {e}')

    log(f'  [discovery] {ticker}: {n_total} polygon contracts, {n_call} calls, '
        f'{n_monthly} monthly, {len(new_rows)} NEW')

    if new_rows:
        d1.batch_insert(
            "INSERT OR IGNORE INTO contracts "
            "(ticker, underlying, contract_type, strike_price, expiration_date, "
            " exercise_style, shares_per_contract, primary_exchange, cfi, "
            " expired, discovered_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            new_rows,
            batch_size=100,
        )
        log(f'  [discovery] {ticker}: inserted {len(new_rows)} new contracts')

    return len(new_rows), n_total


# =====================================================================================
# Phase 2 — Price update
# =====================================================================================
def price_update(d1, poly, ticker=None, max_contracts=None, days_back=14):
    """
    For every active monthly contract (filtered by ticker if given), fetch the last N days
    of daily bars from Polygon. INSERT OR REPLACE into option_bars with source='polygon'.

    Returns dict {n_contracts, n_bars_written, n_no_data, n_errors}.
    """
    today = date.today()
    from_date = (today - timedelta(days=days_back)).isoformat()
    to_date = today.isoformat()

    # Query active contracts
    where = "expired = 0"
    params = []
    if ticker:
        where += " AND underlying = ?"
        params.append(ticker)
    sql = f"SELECT ticker FROM contracts WHERE {where} ORDER BY ticker"
    rows = d1.select(sql, params)
    contracts = [r['ticker'] for r in rows]
    if max_contracts:
        contracts = contracts[:max_contracts]

    log(f'  [price] {len(contracts)} contracts to update (range {from_date}..{to_date})')

    n_bars_total = 0
    n_no_data = 0
    n_errors = 0
    bars_buffer = []  # collect rows, batch insert every N
    BATCH_SIZE = 200
    PROGRESS_EVERY = 500

    started = time.time()
    for i, occ in enumerate(contracts, 1):
        if i % PROGRESS_EVERY == 0:
            elapsed = time.time() - started
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(contracts) - i) / rate if rate > 0 else 0
            log(f'  [price] {i}/{len(contracts)} done ({rate:.1f} req/sec, '
                f'~{remaining/60:.0f} min remaining), bars written: {n_bars_total}')

        try:
            bars = poly.get_daily_bars(occ, from_date, to_date)
        except PolygonError as e:
            log(f'  [price] {occ}: ERROR {e}')
            n_errors += 1
            continue

        if not bars:
            n_no_data += 1
            continue

        now_ts = int(time.time())
        for b in bars:
            # Convert timestamp ms → YYYY-MM-DD
            t_ms = b.get('t')
            if not t_ms:
                continue
            bar_date = date.fromtimestamp(t_ms / 1000).isoformat()
            bars_buffer.append((
                occ, bar_date, t_ms,
                b.get('o'), b.get('h'), b.get('l'), b.get('c'),
                b.get('v'), b.get('vw'), b.get('n'),
                'polygon', now_ts,
            ))

        # Flush buffer
        if len(bars_buffer) >= BATCH_SIZE:
            n_bars_total += _flush_bars(d1, bars_buffer)
            bars_buffer.clear()

    # Final flush
    if bars_buffer:
        n_bars_total += _flush_bars(d1, bars_buffer)

    elapsed = time.time() - started
    log(f'  [price] DONE: {n_bars_total} bars written, {n_no_data} contracts with no data, '
        f'{n_errors} errors. Time: {elapsed/60:.1f} min')

    return {
        'n_contracts': len(contracts),
        'n_bars_written': n_bars_total,
        'n_no_data': n_no_data,
        'n_errors': n_errors,
    }


def _flush_bars(d1, rows):
    d1.batch_insert(
        "INSERT OR REPLACE INTO option_bars "
        "(ticker, date, t, o, h, l, c, v, vw, n, source, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
        batch_size=100,
    )
    return len(rows)


# =====================================================================================
# Phase 3 — Aggregator (Yahoo gap fill into option_bars)
# =====================================================================================
def aggregator_fill_gaps(d1):
    """
    For every active contract that DOES NOT have a Polygon bar for yesterday but
    DOES have a Yahoo snapshot in yahoo_quotes — write a synthetic bar with source='yahoo_mid'.
    """
    today = date.today()
    yesterday = (today - timedelta(days=1)).isoformat()

    # Use yesterday's date as the bar date
    sql = f"""
        SELECT y.ticker AS ticker,
               MAX(y.ts) AS last_ts,
               (SELECT mid FROM yahoo_quotes WHERE ticker = y.ticker AND ts = MAX(y.ts)) AS mid
        FROM yahoo_quotes y
        WHERE date(y.ts, 'unixepoch') = ?
        GROUP BY y.ticker
    """
    rows = d1.select(sql, [yesterday])

    if not rows:
        log(f'  [aggregator] no Yahoo snapshots for {yesterday} — nothing to do')
        return {'n_filled': 0}

    # For each, check if Polygon already has a bar; if not, insert synthetic
    n_filled = 0
    now_ts = int(time.time())
    inserts = []
    for r in rows:
        occ = r['ticker']
        mid = r.get('mid')
        if mid is None:
            continue
        # Check if Polygon bar exists for yesterday
        check = d1.select(
            "SELECT 1 FROM option_bars WHERE ticker = ? AND date = ? AND source = 'polygon' LIMIT 1",
            [occ, yesterday],
        )
        if check:
            continue  # Polygon already has this bar
        # No Polygon bar — insert synthetic from Yahoo mid
        inserts.append((
            occ, yesterday, r.get('last_ts'),
            None, None, None, mid,  # o/h/l unknown, c=mid
            0, None, None,
            'yahoo_mid', now_ts,
        ))

    if inserts:
        d1.batch_insert(
            "INSERT OR REPLACE INTO option_bars "
            "(ticker, date, t, o, h, l, c, v, vw, n, source, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            inserts,
        )
        n_filled = len(inserts)

    log(f'  [aggregator] filled {n_filled} synthetic yahoo_mid bars for {yesterday}')
    return {'n_filled': n_filled}


# =====================================================================================
# Job logging
# =====================================================================================
def log_job_start(d1, job_type):
    """Returns job id."""
    meta = d1.execute(
        "INSERT INTO job_runs (job_type, started_at, status) VALUES (?, ?, ?)",
        [job_type, int(time.time()), 'running'],
    )
    return meta.get('last_row_id')


def log_job_finish(d1, job_id, status, contracts_updated=0, error_msg=None):
    d1.execute(
        "UPDATE job_runs SET finished_at = ?, status = ?, contracts_updated = ?, error_msg = ? "
        "WHERE id = ?",
        [int(time.time()), status, contracts_updated, error_msg, job_id],
    )


# =====================================================================================
# Main
# =====================================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tickers', nargs='*', help='Limit to these tickers (default: all active)')
    ap.add_argument('--max-contracts', type=int, help='Limit contracts per ticker (testing)')
    ap.add_argument('--skip-discovery', action='store_true')
    ap.add_argument('--skip-prices', action='store_true')
    ap.add_argument('--skip-aggregator', action='store_true')
    ap.add_argument('--days-back', type=int, default=14, help='How many days of history per request')
    args = ap.parse_args()

    d1 = D1Client()
    poly = PolygonClient()

    started = time.time()
    job_id = log_job_start(d1, 'daily_sync')
    log(f'=== daily_sync started, job_id={job_id} ===')

    summary = {'discovery_new': 0, 'price_bars': 0, 'aggregator_filled': 0, 'errors': []}
    try:
        # Get list of active tickers
        if args.tickers:
            tickers = args.tickers
        else:
            tickers = [r['symbol'] for r in d1.select(
                "SELECT symbol FROM tickers WHERE active = 1 ORDER BY symbol")]
        log(f'Tickers: {tickers}')

        # Phase 1: discovery
        if not args.skip_discovery:
            log('=== Phase 1: Discovery ===')
            for tk in tickers:
                n_new, _ = discovery_for_ticker(d1, poly, tk)
                summary['discovery_new'] += n_new

        # Phase 2: price update
        if not args.skip_prices:
            log('=== Phase 2: Price update ===')
            for tk in tickers:
                log(f'  [price] {tk}...')
                res = price_update(d1, poly, tk, args.max_contracts, args.days_back)
                summary['price_bars'] += res['n_bars_written']
                if res['n_errors'] > 0:
                    summary['errors'].append(f'{tk}: {res["n_errors"]} errors')

        # Phase 3: aggregator
        if not args.skip_aggregator:
            log('=== Phase 3: Aggregator (Yahoo gap fill) ===')
            res = aggregator_fill_gaps(d1)
            summary['aggregator_filled'] = res['n_filled']

        elapsed = time.time() - started
        log(f'=== DONE in {elapsed/60:.1f} min: {summary} ===')
        log_job_finish(d1, job_id, 'success', contracts_updated=summary['price_bars'])

        # Silent on success — no Telegram

        if summary['errors']:
            # Partial errors — send a warning but still mark success
            send_alert(
                f'⚠ Daily sync завершился с warnings ({len(summary["errors"])}):\n' +
                '\n'.join(summary['errors'][:5]) +
                f'\nDuration: {elapsed/60:.1f} min, bars written: {summary["price_bars"]}'
            )

        return 0

    except Exception as e:
        tb = traceback.format_exc()
        log(f'FATAL: {e}\n{tb}')
        try:
            log_job_finish(d1, job_id, 'error', error_msg=str(e)[:500])
        except Exception:
            pass
        send_alert(
            f'❌ Daily sync FAILED\n\n'
            f'Error: {e}\n\n'
            f'Phase: see GHA logs.\n'
            f'Job ID: {job_id}\n'
            f'Duration: {(time.time()-started)/60:.1f} min'
        )
        return 1


if __name__ == '__main__':
    sys.exit(main())
