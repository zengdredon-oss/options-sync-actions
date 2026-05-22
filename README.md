# options-sync-actions

Daily sync from Polygon Free → Cloudflare D1 for the **options-tracker** project.

This is a **standalone, public** GitHub repo that runs a single GitHub Actions
workflow once per day at 22:05 UTC. Public to get unlimited GHA minutes (free).

**No secrets in this repo** — all credentials live in GitHub Secrets (encrypted).

## What it does

Once per day, after US market close (22:05 UTC):

1. **Discovery** (~2 min) — for each ticker in D1 `tickers WHERE active=1`,
   queries Polygon `/v3/reference/options/contracts` to find new monthly contracts
   (filters 3rd-Friday-of-month) and inserts them into D1 `contracts` table.

2. **Price update** (~5 hours) — for every active monthly contract in D1,
   fetches the last 14 trading days of daily bars from Polygon
   (`/v2/aggs/ticker/.../range/1/day/...`) and writes them to D1 `option_bars`
   using INSERT OR REPLACE.

3. **Aggregator** (~1 min) — for any active contract that does NOT have a
   Polygon bar for yesterday but DOES have a Yahoo snapshot (in `yahoo_quotes`,
   populated by a separate Cloudflare Worker), writes a synthetic bar to
   `option_bars` with `source='yahoo_mid'`.

4. **Status alert** — only if step 1/2/3 errored or didn't complete. Silent on success.

## Rate limit / sizing

- Polygon Free: 5 req/min/key. Repo has 3 keys → 15 req/min rotation.
- ~5928 active monthly contracts × 1 request each = ~6.6 hours single-key,
  ~2.5 hours with 3-key rotation. Per-job limit = 6 hours so we use 3 keys.
- See full sizing analysis in `../docs/online_tracker_architecture.md`.

## Required GitHub Secrets

Set in **Settings → Secrets and variables → Actions**:

| Secret | Where to get |
|---|---|
| `POLYGON_API_KEY` | https://polygon.io/dashboard/api-keys (key #1) |
| `POLYGON_API_KEY_2` | (key #2 — Polygon allows multiple keys per account) |
| `POLYGON_API_KEY_3` | (key #3) |
| `CLOUDFLARE_ACCOUNT_ID` | `1322917b0252f7b550560a6d60cc42f8` (zengdredon@gmail.com account) |
| `CLOUDFLARE_API_TOKEN` | https://dash.cloudflare.com/profile/api-tokens → Create Token → custom token with `Account → D1 → Edit` permission |
| `CLOUDFLARE_D1_DB_ID` | `784a3f6c-1e56-4cb5-8be2-5a2e44f4356e` (the `options-tracker-db` UUID) |
| `TELEGRAM_BOT_TOKEN` | From @BotFather in Telegram |
| `TELEGRAM_CHAT_ID` | Your private chat ID with the bot |

## Manual trigger

Workflow has `workflow_dispatch` enabled. To run sync manually:
1. Go to **Actions → Daily sync → Run workflow**
2. Choose branch `main` → **Run workflow**

## Local development / testing

```bash
# Install deps (stdlib only — urllib for HTTP)
python3 --version  # 3.10+

# Test (uses real credentials, will write to D1):
export POLYGON_API_KEY=...
export POLYGON_API_KEY_2=...
export POLYGON_API_KEY_3=...
export CLOUDFLARE_ACCOUNT_ID=...
export CLOUDFLARE_API_TOKEN=...
export CLOUDFLARE_D1_DB_ID=...
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...

# Limit to a single ticker for testing:
python daily_sync.py --tickers GLD --max-contracts 50
```

## Files

- `daily_sync.py` — main sync script (discovery + price update)
- `aggregate_yahoo_gaps.py` — aggregator (Yahoo fallback into option_bars)
- `lib/` — shared helpers (D1 client, Polygon client, Telegram sender)
- `.github/workflows/sync.yml` — GHA workflow

## Architecture documents (private)

This repo intentionally has minimal docs — full architecture lives in the
private project repo on the user's machine:

- `docs/online_tracker_architecture.md` — full system design
- `tracker/POLYGON_BACKFILL_GUIDE.md` — Polygon API conventions
- `docs/notification_system_design.md` — alert config / dedup

If you're a new collaborator, ask the owner for access to those docs.
