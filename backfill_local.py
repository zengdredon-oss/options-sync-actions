"""
Backfill 2-year bars LOCALLY → SQL file → manual upload via wrangler.

Подход: не зависит от CLOUDFLARE_API_TOKEN (его нет на компе пользователя).
Pulls bars from Polygon (3-key rotation), пишет SQL INSERT statements в файл.
Затем пользователь делает batch upload одной командой:
  wrangler d1 execute options-tracker-db --remote --file=backfill_bars_chunk_NN.sql

Restart-friendly: каждый processed контракт пишется в progress.txt; повторный
запуск пропускает уже обработанные.

Запуск:
  python backfill_local.py                            # все контракты без баров (из new_contracts.json)
  python backfill_local.py --tickers GLD              # только GLD
  python backfill_local.py --start-from O:GLD2601...  # продолжить с конкретного OCC
"""
import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

from lib.polygon import PolygonClient, PolygonError


def log(msg):
    print(f'[{datetime.utcnow().strftime("%H:%M:%S")}] {msg}', flush=True)


# Output dir for SQL chunks + progress
OUT_DIR = 'backfill_output'
PROGRESS_FILE = os.path.join(OUT_DIR, 'progress.txt')
CHUNK_BYTES_LIMIT = 80_000  # safe under D1 100KB statement limit


def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE) as f:
        return {line.strip() for line in f if line.strip()}


def save_progress(occ):
    with open(PROGRESS_FILE, 'a') as f:
        f.write(occ + '\n')


def num_or_null(v):
    if v is None:
        return 'NULL'
    return str(v)


def esc(s):
    return "'" + str(s).replace("'", "''") + "'"


class SqlChunkWriter:
    """Writes SQL INSERT statements to chunked files (each <= ~80KB)."""

    def __init__(self, out_dir):
        self.out_dir = out_dir
        self.chunk_idx = 0
        self.current_values = []
        self.current_bytes = 0
        os.makedirs(out_dir, exist_ok=True)
        # Find existing chunks to resume numbering
        existing = [f for f in os.listdir(out_dir) if f.startswith('backfill_bars_') and f.endswith('.sql')]
        if existing:
            nums = []
            for f in existing:
                try:
                    nums.append(int(f.replace('backfill_bars_chunk_', '').replace('.sql', '')))
                except ValueError:
                    pass
            self.chunk_idx = max(nums) + 1 if nums else 0

    def add_row(self, occ, bar_date, t, o, h, l, c, v, vw, n, now_ts):
        val = (f'({esc(occ)},{esc(bar_date)},{num_or_null(t)},'
               f'{num_or_null(o)},{num_or_null(h)},{num_or_null(l)},{num_or_null(c)},'
               f'{num_or_null(v)},{num_or_null(vw)},{num_or_null(n)},'
               f"'polygon',{now_ts})")
        if self.current_bytes + len(val) > CHUNK_BYTES_LIMIT and self.current_values:
            self.flush()
        self.current_values.append(val)
        self.current_bytes += len(val) + 1  # +1 for comma

    def flush(self):
        if not self.current_values:
            return
        sql = ('INSERT OR REPLACE INTO option_bars '
               '(ticker, date, t, o, h, l, c, v, vw, n, source, updated_at) VALUES ')
        sql += ','.join(self.current_values) + ';\n'
        fname = os.path.join(self.out_dir, f'backfill_bars_chunk_{self.chunk_idx:04d}.sql')
        with open(fname, 'w', encoding='utf-8') as f:
            f.write(sql)
        log(f'  Wrote chunk {self.chunk_idx:04d} ({len(self.current_values)} bars, {self.current_bytes/1024:.1f} KB)')
        self.chunk_idx += 1
        self.current_values = []
        self.current_bytes = 0


def load_contracts_to_backfill(args):
    """
    Returns list of (occ_ticker, ...) — контракты которым нужен backfill.

    Если есть new_contracts.json — берёт оттуда.
    Иначе ругается (нужно сначала запустить discovery).
    """
    if not os.path.exists('new_contracts.json'):
        log('ERROR: new_contracts.json не найден. Сначала запусти discovery.')
        sys.exit(1)
    with open('new_contracts.json') as f:
        contracts = json.load(f)
    if args.tickers:
        contracts = [c for c in contracts if c.get('underlying_ticker') in args.tickers]
    return [c['ticker'] for c in contracts if c.get('ticker')]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tickers', nargs='*', help='Limit to these underlying tickers')
    ap.add_argument('--days-back', type=int, default=730, help='Days of history (default 2y)')
    ap.add_argument('--max', type=int, help='Limit total contracts (testing)')
    ap.add_argument('--start-from', help='Resume from specific OCC ticker')
    args = ap.parse_args()

    poly = PolygonClient()
    today = date.today()
    from_date = (today - timedelta(days=args.days_back)).isoformat()
    to_date = today.isoformat()
    log(f'Backfill range: {from_date} - {to_date}')

    contracts = load_contracts_to_backfill(args)
    log(f'Total contracts to backfill: {len(contracts)}')

    # Apply --start-from skip
    if args.start_from:
        try:
            idx = contracts.index(args.start_from)
            contracts = contracts[idx:]
            log(f'Resuming from index {idx}: {args.start_from}')
        except ValueError:
            log(f'--start-from {args.start_from} not found in list')
            sys.exit(2)

    # Load progress (already-done)
    done = load_progress()
    log(f'Already done: {len(done)}')
    contracts = [c for c in contracts if c not in done]
    log(f'Remaining: {len(contracts)}')

    if args.max:
        contracts = contracts[:args.max]
        log(f'Limited to {len(contracts)} for testing')

    if not contracts:
        log('Nothing to do. Run wrangler upload commands (see backfill_output/).')
        return 0

    writer = SqlChunkWriter(OUT_DIR)
    started = time.time()
    n_bars_total = 0
    n_no_data = 0
    n_errors = 0
    PROGRESS_EVERY = 50

    now_ts = int(time.time())
    for i, occ in enumerate(contracts, 1):
        if i % PROGRESS_EVERY == 0:
            elapsed = time.time() - started
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(contracts) - i) / rate if rate > 0 else 0
            log(f'  {i}/{len(contracts)} ({rate:.2f}/sec, ~{remaining/60:.0f} min left), '
                f'bars: {n_bars_total}, no_data: {n_no_data}, errors: {n_errors}')

        try:
            bars = poly.get_daily_bars(occ, from_date, to_date)
        except PolygonError as e:
            log(f'  {occ}: ERROR {e}')
            n_errors += 1
            continue

        if not bars:
            n_no_data += 1
            save_progress(occ)
            continue

        for b in bars:
            t_ms = b.get('t')
            if not t_ms:
                continue
            bar_date = date.fromtimestamp(t_ms / 1000).isoformat()
            writer.add_row(
                occ, bar_date, t_ms,
                b.get('o'), b.get('h'), b.get('l'), b.get('c'),
                b.get('v'), b.get('vw'), b.get('n'),
                now_ts,
            )
            n_bars_total += 1
        save_progress(occ)

    writer.flush()
    elapsed = time.time() - started

    log(f'=== DONE in {elapsed/60:.1f} min ===')
    log(f'  Contracts processed: {len(contracts)}')
    log(f'  Bars written: {n_bars_total}')
    log(f'  No data: {n_no_data}, Errors: {n_errors}')
    log(f'  SQL chunks: see {OUT_DIR}/')
    log(f'')
    log(f'Next step — upload chunks to D1 via wrangler:')
    log(f'  cd ../tracker')
    log(f'  for f in ../sync-actions/{OUT_DIR}/backfill_bars_chunk_*.sql; do')
    log(f'    CLOUDFLARE_ACCOUNT_ID=1322917b0252f7b550560a6d60cc42f8 \\')
    log(f'      wrangler d1 execute options-tracker-db --remote --file="$f"')
    log(f'  done')


if __name__ == '__main__':
    sys.exit(main())
