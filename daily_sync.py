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
from lib.polygon import (
    PolygonClient, PolygonError,
    classify_expirations, MIN_TTE_DAYS_FOR_DISCOVERY, MIN_STRIKES_FOR_MONTHLY,
)
from lib.telegram import send_alert


def log(msg):
    print(f'[{datetime.utcnow().strftime("%H:%M:%S")}] {msg}', flush=True)


# =====================================================================================
# Phase 1 — Discovery
# =====================================================================================
def mark_expired_contracts(d1):
    """Update expired flag for all contracts whose expiration_date has passed.
    Run before discovery so we don't re-fetch quotes for dead contracts."""
    meta = d1.execute(
        "UPDATE contracts SET expired = 1 "
        "WHERE expired = 0 AND expiration_date < date('now')"
    )
    n = meta.get('rows_written', 0) or meta.get('changes', 0) or 0
    if n > 0:
        log(f'  [discovery] marked {n} contracts as expired (date passed)')
    return n


def discovery_for_ticker(d1, poly, ticker):
    """
    Find new monthly/LEAPS contracts for one underlying.

    Two-pass:
      1. Pull ALL active calls from Polygon (could be 5K+ for liquid tickers).
      2. classify_expirations() group by expiration_date → for each date compute
         n_strikes / range_pct / TTE. Keep only is_monthly_or_leap=True dates.
         This catches Thursday LEAPS, EOM quarterly etc — no hardcoded weekday.
      3. INSERT new contracts (discovery_source='auto').

    Returns (n_new, n_total_scanned).
    """
    log(f'  [discovery] {ticker}: scanning Polygon contracts...')
    n_total = 0
    n_call = 0
    calls_buffer = []  # collect ALL call contracts before classifying

    # Get list of contracts already in D1
    existing_resp = d1.select(
        "SELECT ticker FROM contracts WHERE underlying = ?",
        [ticker],
    )
    existing = {r['ticker'] for r in existing_resp}

    # Pass 1: fetch all active calls from Polygon
    try:
        for c in poly.list_contracts(ticker, expired=False, limit=1000):
            n_total += 1
            if c.get('contract_type') != 'call':
                continue
            n_call += 1
            calls_buffer.append(c)
    except PolygonError as e:
        log(f'  [discovery] {ticker}: WARN polygon error: {e}')

    # Pass 2: classify expirations
    exp_classes = classify_expirations(calls_buffer)
    accepted_exps = {e for e, m in exp_classes.items() if m['is_monthly_or_leap']}

    # Log breakdown of rejected expirations (for transparency)
    rejected = [(e, m) for e, m in exp_classes.items() if not m['is_monthly_or_leap']]
    if rejected:
        log(f'  [discovery] {ticker}: rejected {len(rejected)} expirations '
            f'(TTE<{MIN_TTE_DAYS_FOR_DISCOVERY}d or n_strikes<{MIN_STRIKES_FOR_MONTHLY}):')
        for e, m in sorted(rejected)[:5]:
            log(f'      {e} ({m["n_strikes"]} strikes, TTE={m["tte_days"]}d): {m["reject_reason"]}')
        if len(rejected) > 5:
            log(f'      ... and {len(rejected) - 5} more')

    # Build new rows
    new_rows = []
    n_monthly = 0
    for c in calls_buffer:
        exp = c.get('expiration_date', '')
        if exp not in accepted_exps:
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
            'auto',  # discovery_source
        ))

    log(f'  [discovery] {ticker}: {n_total} polygon contracts, {n_call} calls, '
        f'{len(accepted_exps)} expirations accepted, {n_monthly} contracts pass filter, '
        f'{len(new_rows)} NEW')

    if new_rows:
        d1.batch_insert(
            "INSERT OR IGNORE INTO contracts "
            "(ticker, underlying, contract_type, strike_price, expiration_date, "
            " exercise_style, shares_per_contract, primary_exchange, cfi, "
            " expired, discovered_at, discovery_source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            new_rows,
            batch_size=100,
        )
        log(f'  [discovery] {ticker}: inserted {len(new_rows)} new contracts')

    return len(new_rows), n_total


# =====================================================================================
# Phase 2 — Price update
# =====================================================================================
def price_update(d1, poly, ticker=None, max_contracts=None, days_back=14, force_all=False, max_runtime_min=None):
    """
    For every active monthly contract (filtered by ticker if given), fetch the last N days
    of daily bars from Polygon. INSERT OR REPLACE into option_bars with source='polygon'.

    SKIP-IF-RECENT (default): пропускаем контракты, у которых уже есть Polygon bar
    обновлённый за последние 20 часов. Self-correcting: если sync упал на середине,
    следующий запуск берёт только оставшиеся. Override через force_all=True.

    max_runtime_min: если задано, прерывает обработку gracefully после N минут — чтобы
    оставить время на phase 3 (aggregator) до GHA 6-hour timeout.

    Returns dict {n_contracts, n_bars_written, n_no_data, n_errors, n_skipped, stopped_early}.
    """
    today = date.today()
    from_date = (today - timedelta(days=days_back)).isoformat()
    to_date = today.isoformat()
    cutoff_ts = int(time.time()) - 20 * 3600  # 20 hours ago

    # Query active contracts.
    # Sort by expiration_date DESC: long-dated LEAPS первыми (наша target зона far-OTM
    # longshots — приоритет дальним), затем ближе к экспирации.
    where = "c.expired = 0 AND c.expiration_date >= date('now')"
    params = []
    if ticker:
        where += " AND c.underlying = ?"
        params.append(ticker)

    if force_all:
        sql = f"""
            SELECT c.ticker FROM contracts c
            WHERE {where}
            ORDER BY c.expiration_date DESC, c.strike_price DESC
        """
    else:
        # Skip-if-recent: исключаем те, у которых уже есть свежий Polygon bar
        sql = f"""
            SELECT c.ticker FROM contracts c
            WHERE {where}
              AND NOT EXISTS (
                SELECT 1 FROM option_bars b
                WHERE b.ticker = c.ticker
                  AND b.source = 'polygon'
                  AND b.updated_at > ?
              )
            ORDER BY c.expiration_date DESC, c.strike_price DESC
        """
        params.append(cutoff_ts)

    rows = d1.select(sql, params)
    contracts = [r['ticker'] for r in rows]

    # Count total active (для информативности)
    total_active_resp = d1.select(
        f"SELECT COUNT(*) AS n FROM contracts c WHERE {where.split(' AND c.underlying')[0]}"
        + (" AND c.underlying = ?" if ticker else ''),
        ([ticker] if ticker else []),
    )
    total_active = total_active_resp[0]['n'] if total_active_resp else 0
    n_skipped = max(0, total_active - len(contracts))

    if max_contracts:
        contracts = contracts[:max_contracts]

    log(f'  [price] {len(contracts)} contracts to update '
        f'(skipped {n_skipped} as fresh, range {from_date}..{to_date}, max_runtime={max_runtime_min}min)')

    log(f'  [price] {len(contracts)} contracts to update (range {from_date}..{to_date})')

    n_bars_total = 0
    n_no_data = 0
    n_errors = 0
    n_consecutive_errors = 0
    bars_buffer = []  # collect rows, batch insert every N
    BATCH_SIZE = 200
    PROGRESS_EVERY = 500
    MAX_CONSECUTIVE_ERRORS = 20  # safety: если Polygon полностью down, выходим
    stopped_early = False
    stop_reason = None

    started = time.time()
    deadline = started + max_runtime_min * 60 if max_runtime_min else None

    for i, occ in enumerate(contracts, 1):
        # Check max_runtime BEFORE each request
        if deadline and time.time() > deadline:
            stopped_early = True
            stop_reason = f'max_runtime ({max_runtime_min}min) reached at {i}/{len(contracts)}'
            log(f'  [price] STOP: {stop_reason}')
            break

        if i % PROGRESS_EVERY == 0:
            elapsed = time.time() - started
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(contracts) - i) / rate if rate > 0 else 0
            log(f'  [price] {i}/{len(contracts)} done ({rate:.1f} req/sec, '
                f'~{remaining/60:.0f} min remaining), bars written: {n_bars_total}, '
                f'errors: {n_errors}')

        try:
            bars = poly.get_daily_bars(occ, from_date, to_date)
            n_consecutive_errors = 0  # reset на success
        except PolygonError as e:
            log(f'  [price] {occ}: ERROR {e}')
            n_errors += 1
            n_consecutive_errors += 1
            # Если много подряд errors — вероятно Polygon полностью down, выходим
            if n_consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                stopped_early = True
                stop_reason = f'{n_consecutive_errors} consecutive errors — Polygon likely down'
                log(f'  [price] STOP: {stop_reason}')
                break
            continue

        if not bars:
            n_no_data += 1
            # Записываем sentinel-bar чтобы skip-if-recent работал для no-data контрактов
            # (иначе sync будет долбить их каждый день безрезультатно).
            # Sentinel: updated_at = сейчас, но НИ одного реального bar нет
            # → contract считается "обновлённым" но в option_bars пусто.
            # Решение: фиктивный insert в option_bars не делаем (это засорит DB),
            # вместо этого используем NEW таблицу contract_sync_log (см. ниже)
            # ИЛИ проще — в skip-if-recent проверяем "за последние 20ч был INSERT для этого ticker".
            # На данный момент: no_data контракты будут retry-иться каждый sync. Это OK
            # потому что Polygon req для empty — быстрый (1-2 sec).
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
            try:
                n_bars_total += _flush_bars(d1, bars_buffer)
            except Exception as e:
                log(f'  [price] FLUSH ERROR: {e} (continuing)')
            bars_buffer.clear()

    # Final flush
    if bars_buffer:
        try:
            n_bars_total += _flush_bars(d1, bars_buffer)
        except Exception as e:
            log(f'  [price] FINAL FLUSH ERROR: {e}')

    elapsed = time.time() - started
    log(f'  [price] DONE: {n_bars_total} bars written, {n_no_data} contracts with no data, '
        f'{n_errors} errors{" (STOPPED EARLY: " + stop_reason + ")" if stopped_early else ""}. '
        f'Time: {elapsed/60:.1f} min')

    return {
        'n_contracts': len(contracts),
        'n_bars_written': n_bars_total,
        'n_no_data': n_no_data,
        'n_errors': n_errors,
        'n_skipped': n_skipped,
        'stopped_early': stopped_early,
        'stop_reason': stop_reason,
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

    # Get LAST snapshot per ticker for yesterday — using JOIN with subquery
    # (SQLite/D1 doesn't allow MAX() inside correlated subquery the same row).
    sql = """
        SELECT y.ticker AS ticker, y.ts AS last_ts, y.mid AS mid
        FROM yahoo_quotes y
        INNER JOIN (
            SELECT ticker, MAX(ts) AS max_ts
            FROM yahoo_quotes
            WHERE date(ts, 'unixepoch') = ?
            GROUP BY ticker
        ) latest
          ON y.ticker = latest.ticker AND y.ts = latest.max_ts
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
    ap.add_argument('--force-all', action='store_true',
                    help='Disable skip-if-recent — re-fetch all contracts even if updated <20h ago')
    ap.add_argument('--max-runtime-min', type=int, default=240,
                    help='Stop price_update gracefully after N minutes (leave time for aggregator). '
                         'Default 240 (4h) — leaves 2h buffer для GHA 6h timeout. Set 0 to disable.')
    args = ap.parse_args()
    max_runtime = args.max_runtime_min if args.max_runtime_min and args.max_runtime_min > 0 else None

    d1 = D1Client()
    poly = PolygonClient()

    started = time.time()
    job_id = log_job_start(d1, 'daily_sync')
    log(f'=== daily_sync started, job_id={job_id} ===')

    summary = {'discovery_new': 0, 'price_bars': 0, 'aggregator_filled': 0,
               'errors': [], 'partial': False, 'partial_reason': None,
               'price_skipped': 0, 'price_no_data': 0}
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
            # Mark expired contracts first (idempotent, safe to run daily)
            mark_expired_contracts(d1)
            for tk in tickers:
                n_new, _ = discovery_for_ticker(d1, poly, tk)
                summary['discovery_new'] += n_new

        # Phase 2: price update
        if not args.skip_prices:
            log('=== Phase 2: Price update ===')
            # Распределяем max_runtime между тикерами равномерно
            per_ticker_runtime = max_runtime // len(tickers) if (max_runtime and tickers) else None
            for tk in tickers:
                log(f'  [price] {tk}...')
                res = price_update(d1, poly, tk, args.max_contracts, args.days_back,
                                   force_all=args.force_all, max_runtime_min=per_ticker_runtime)
                summary['price_bars'] += res['n_bars_written']
                summary['price_skipped'] += res.get('n_skipped', 0)
                summary['price_no_data'] += res.get('n_no_data', 0)
                if res['n_errors'] > 0:
                    summary['errors'].append(f'{tk}: {res["n_errors"]} errors')
                if res.get('stopped_early'):
                    summary['partial'] = True
                    summary['partial_reason'] = res.get('stop_reason', 'stopped early')
                    log(f'  [price] {tk}: STOPPED EARLY — next sync продолжит с unfinished контрактов')

        # Phase 3: aggregator
        if not args.skip_aggregator:
            log('=== Phase 3: Aggregator (Yahoo gap fill) ===')
            res = aggregator_fill_gaps(d1)
            summary['aggregator_filled'] = res['n_filled']

        elapsed = time.time() - started
        log(f'=== DONE in {elapsed/60:.1f} min: {summary} ===')
        status = 'partial' if summary['partial'] else 'success'
        log_job_finish(d1, job_id, status, contracts_updated=summary['price_bars'])

        # Partial sync (stopped early) — send info alert, не critical
        if summary['partial']:
            send_alert(
                f'⏱ Daily sync завершён частично\n\n'
                f'Reason: {summary["partial_reason"]}\n'
                f'Discovery: +{summary["discovery_new"]} новых контрактов\n'
                f'Price: {summary["price_bars"]} баров записано, '
                f'{summary["price_skipped"]} skipped (already fresh), '
                f'{summary["price_no_data"]} no-data\n'
                f'Aggregator: {summary["aggregator_filled"]} yahoo_mid fills\n'
                f'Duration: {elapsed/60:.1f} min, Job: {job_id}\n\n'
                f'Следующий sync продолжит с оставшихся (skip-if-recent работает).'
            )
        elif summary['errors']:
            # Partial errors — send a warning but still mark success
            send_alert(
                f'⚠ Daily sync завершился с warnings ({len(summary["errors"])}):\n' +
                '\n'.join(summary['errors'][:5]) +
                f'\nDuration: {elapsed/60:.1f} min, bars written: {summary["price_bars"]}'
            )
        # else: silent on clean success

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
