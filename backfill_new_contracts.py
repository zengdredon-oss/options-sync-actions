"""
Backfill 2-year history для контрактов которых нет в option_bars.

Используется ONCE после того как audit нашёл новые LEAPS (Thursday LEAPS,
EOM quarterly) и они добавлены в contracts table.

Стандартный daily_sync.py делает только last 14 days — этого не хватает
для контрактов которых раньше не было в системе. Этот скрипт делает
2-летний backfill (через Polygon Free), затем daily_sync будет работать как обычно.

Запуск:
  python backfill_new_contracts.py                     # все контракты без баров
  python backfill_new_contracts.py --tickers GLD       # только один underlying
  python backfill_new_contracts.py --max-contracts 100 # для теста

Время: 4958 контрактов × ~6 сек (3 keys @ 15 req/min) = ~5.5 часов.
GHA имеет timeout 6 часов на job — поэтому скрипт принтит прогресс
и делает checkpointing через job_runs.
"""
import argparse
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta

from lib.d1 import D1Client
from lib.polygon import PolygonClient, PolygonError
from lib.telegram import send_alert


def log(msg):
    print(f'[{datetime.utcnow().strftime("%H:%M:%S")}] {msg}', flush=True)


def find_contracts_without_bars(d1, tickers=None, only_recent=True):
    """
    Return list of OCC tickers for active contracts that have ZERO option_bars rows.
    only_recent=True → exclude already-expired contracts (no point backfilling those).
    """
    where = "c.expired = 0"
    params = []
    if tickers:
        placeholders = ','.join('?' for _ in tickers)
        where += f" AND c.underlying IN ({placeholders})"
        params.extend(tickers)

    sql = f"""
        SELECT c.ticker
        FROM contracts c
        LEFT JOIN (SELECT DISTINCT ticker FROM option_bars) ob ON ob.ticker = c.ticker
        WHERE {where} AND ob.ticker IS NULL
        ORDER BY c.expiration_date DESC, c.ticker
    """
    rows = d1.select(sql, params)
    return [r['ticker'] for r in rows]


def backfill_one_contract(poly, occ, from_date, to_date):
    """Fetch all daily bars for one contract. Returns list of bar dicts."""
    return poly.get_daily_bars(occ, from_date, to_date)


def _flush_bars(d1, rows):
    d1.batch_insert(
        "INSERT OR REPLACE INTO option_bars "
        "(ticker, date, t, o, h, l, c, v, vw, n, source, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
        batch_size=100,
    )
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tickers', nargs='*', help='Limit to these underlying tickers')
    ap.add_argument('--max-contracts', type=int, help='Limit total contracts (testing)')
    ap.add_argument('--days-back', type=int, default=730, help='How many days of history (default: 2 years)')
    args = ap.parse_args()

    d1 = D1Client()
    poly = PolygonClient()

    started = time.time()
    log('=== backfill_new_contracts started ===')

    # Job log
    meta = d1.execute(
        "INSERT INTO job_runs (job_type, started_at, status) VALUES (?, ?, ?)",
        ['backfill_new', int(started), 'running']
    )
    job_id = meta.get('last_row_id')

    summary = {'n_contracts': 0, 'n_bars': 0, 'n_no_data': 0, 'n_errors': 0}

    try:
        # Find contracts without bars
        log('Finding contracts without bars...')
        contracts = find_contracts_without_bars(d1, args.tickers)
        log(f'  Found {len(contracts)} contracts without bars')
        if args.max_contracts:
            contracts = contracts[:args.max_contracts]
            log(f'  Limited to {len(contracts)} for testing')

        if not contracts:
            log('Nothing to do. Exiting.')
            d1.execute(
                "UPDATE job_runs SET finished_at = ?, status = ?, contracts_updated = ? WHERE id = ?",
                [int(time.time()), 'success', 0, job_id]
            )
            return 0

        today = date.today()
        from_date = (today - timedelta(days=args.days_back)).isoformat()
        to_date = today.isoformat()

        bars_buffer = []
        BATCH_SIZE = 200
        PROGRESS_EVERY = 100

        for i, occ in enumerate(contracts, 1):
            if i % PROGRESS_EVERY == 0:
                elapsed = time.time() - started
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (len(contracts) - i) / rate if rate > 0 else 0
                log(f'  {i}/{len(contracts)} ({rate:.2f} req/sec, '
                    f'~{remaining/60:.0f} min remaining), bars: {summary["n_bars"]}')

            try:
                bars = backfill_one_contract(poly, occ, from_date, to_date)
            except PolygonError as e:
                log(f'  {occ}: ERROR {e}')
                summary['n_errors'] += 1
                continue

            if not bars:
                summary['n_no_data'] += 1
                continue

            now_ts = int(time.time())
            for b in bars:
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

            if len(bars_buffer) >= BATCH_SIZE:
                summary['n_bars'] += _flush_bars(d1, bars_buffer)
                bars_buffer.clear()

        if bars_buffer:
            summary['n_bars'] += _flush_bars(d1, bars_buffer)

        summary['n_contracts'] = len(contracts)
        elapsed = time.time() - started
        log(f'=== DONE in {elapsed/60:.1f} min: {summary} ===')

        d1.execute(
            "UPDATE job_runs SET finished_at = ?, status = ?, contracts_updated = ?, error_msg = ? WHERE id = ?",
            [int(time.time()), 'success', summary['n_bars'],
             f"contracts={summary['n_contracts']}, no_data={summary['n_no_data']}, errors={summary['n_errors']}",
             job_id]
        )

        send_alert(
            f'✅ Backfill new contracts завершён\n'
            f'Contracts: {summary["n_contracts"]}\n'
            f'Bars written: {summary["n_bars"]}\n'
            f'No data: {summary["n_no_data"]}, errors: {summary["n_errors"]}\n'
            f'Duration: {elapsed/60:.1f} min'
        )
        return 0

    except Exception as e:
        tb = traceback.format_exc()
        log(f'FATAL: {e}\n{tb}')
        d1.execute(
            "UPDATE job_runs SET finished_at = ?, status = ?, error_msg = ? WHERE id = ?",
            [int(time.time()), 'error', str(e)[:500], job_id]
        )
        send_alert(f'❌ Backfill new contracts FAILED\n\nError: {e}\nJob ID: {job_id}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
