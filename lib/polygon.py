"""
Polygon Free tier client with multi-key rotation.

Strategy:
- 3 API keys (POLYGON_API_KEY, POLYGON_API_KEY_2, POLYGON_API_KEY_3) rotate
  round-robin → effectively 15 req/min vs 5 with one key.
- On HTTP 429 (rate limit) — sleep 12s and try next key.
- All endpoints we use are on Free tier:
  - /v3/reference/options/contracts (discovery)
  - /v2/aggs/ticker/{occ}/range/1/day/{from}/{to} (price update)
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import date


class PolygonClient:
    def __init__(self, keys=None):
        if keys is None:
            keys = []
            for env_name in ['POLYGON_API_KEY', 'POLYGON_API_KEY_2', 'POLYGON_API_KEY_3']:
                v = os.environ.get(env_name)
                if v:
                    keys.append(v)
            if not keys:
                raise ValueError('No POLYGON_API_KEY* found in environment')
        self.keys = keys
        self.key_idx = 0
        self.req_count = 0

    def _next_key(self):
        k = self.keys[self.key_idx % len(self.keys)]
        self.key_idx += 1
        return k

    def get(self, url, extra_params=None, max_retries=3, timeout=12):
        """
        GET with key rotation and 429 retry.

        Defaults tuned для daily_sync (2026-05-26 issues):
          - timeout=12 (was 30): Polygon обычно отвечает в <2s; если >12s — что-то висит,
            проще пропустить контракт чем убить job timeout.
          - max_retries=3 (was 5): для контрактов где Polygon stably медленный (deep OTM
            never-traded), достаточно 2-3 попыток. Не блокирует job.
        """
        # Strip any existing apiKey from url
        if 'apiKey=' in url:
            url = url.split('apiKey=')[0].rstrip('&?')
            url = url.rstrip('&?')

        for attempt in range(max_retries):
            key = self._next_key()
            sep = '&' if '?' in url else '?'
            full_url = f'{url}{sep}apiKey={key}'
            if extra_params:
                for k, v in extra_params.items():
                    full_url += f'&{k}={v}'

            try:
                req = urllib.request.Request(full_url)
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    self.req_count += 1
                    return json.loads(resp.read().decode('utf-8'))
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    # Rate limit — wait and try next key
                    time.sleep(12)
                    continue
                elif e.code in (500, 502, 503, 504):
                    time.sleep(2 ** attempt)
                    continue
                elif e.code == 403:
                    # Not authorized for endpoint — fail immediately, not retry
                    raise PolygonError(f'403 NOT_AUTHORIZED for {url} (Free tier limitation?)')
                else:
                    raise PolygonError(f'HTTP {e.code}: {url}') from e
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                # TimeoutError = socket read timeout. URLError = connection refused/DNS.
                # OSError = misc network issues. На последнем attempt — raise.
                if attempt < max_retries - 1:
                    time.sleep(min(2 ** attempt, 8))  # cap backoff at 8s
                    continue
                raise PolygonError(f'Network error: {type(e).__name__}: {e}') from e
        raise PolygonError(f'All retries exhausted for {url}')

    def list_contracts(self, underlying, expired=False, limit=1000, contract_type='call'):
        """
        Generator over contracts for an underlying. Paginates via next_url.

        contract_type filter applied at Polygon side (saves pages — without it
        Polygon mixes calls and puts in pagination, doubling work).
        """
        ct_param = f'&contract_type={contract_type}' if contract_type else ''
        url = (f'https://api.polygon.io/v3/reference/options/contracts'
               f'?underlying_ticker={underlying}&expired={"true" if expired else "false"}'
               f'{ct_param}&limit={limit}')
        page = 0
        while url:
            page += 1
            try:
                resp = self.get(url)
            except PolygonError:
                # Pause briefly and rethrow — caller logs
                raise
            results = resp.get('results', []) or []
            for r in results:
                yield r
            # Pagination
            url = resp.get('next_url')
            if url and 'apiKey=' not in url:
                # next_url doesn't include the key; our get() will add it
                pass

    def get_daily_bars(self, occ_ticker, from_date, to_date, adjusted=True):
        """Returns list of bar dicts: {v, vw, o, c, h, l, t, n} (or empty if no data)."""
        url = (f'https://api.polygon.io/v2/aggs/ticker/{occ_ticker}'
               f'/range/1/day/{from_date}/{to_date}'
               f'?adjusted={"true" if adjusted else "false"}')
        resp = self.get(url)
        return resp.get('results', []) or []


class PolygonError(Exception):
    pass


# =====================================================================================
# Expiration classification — robust, без hardcoded дней недели
#
# Реальность: CBOE листит LEAPS не только на 3-ю пятницу. Например GLD имеет LEAPS на
#   2027-06-17 (четверг, 164 страйка) — это игнорировалось старым фильтром weekday==Friday.
# Кроме того есть EOM quarterly options (последний рабочий день квартала, среда/вторник).
#
# Polygon API не предоставляет поля типа `is_leap` — все опционы имеют одинаковый cfi='OCASPS'.
# Authoritative LEAPS feed в свободном доступе НЕ существует (CBOE/OCC публикуют только specs).
#
# Поэтому используем эмпирическую классификацию по характеристикам expiration date:
#   - LEAPS / monthly = широкий strike range + достаточное количество страйков + не weekly
# Двухуровневый критерий:
#   1. TTE_at_discovery >= MIN_TTE_DAYS  — отсекает true weeklies (живут 1-2 недели)
#   2. n_strikes >= MIN_STRIKES          — отсекает 0DTE с узким ATM
#
# Defaults подобраны эмпирически: на GLD 2026-2027 LEAPS/monthly имеют n_strikes 100-280,
# weeklies — около 100, 0DTE — <30. TTE LEAPS — всегда >270 при первой discovery.
# =====================================================================================
MIN_TTE_DAYS_FOR_DISCOVERY = 30
MIN_STRIKES_FOR_MONTHLY = 50

# Strong override: если listing очень wide — это LEAPS даже если TTE < 30 дней.
# Нужно чтобы не пропустить Thursday LEAPS близко к экспирации (typical pattern:
# 3rd Thursday в месяце экспирации, TTE 7-28 дней).
# Эмпирически на GLD: weekly Friday имеет n=152 range=251%, LEAPS имеет n>=150 AND range>=400%.
WIDE_LISTING_N_STRIKES = 150
WIDE_LISTING_RANGE_PCT = 400


def classify_expirations(contracts_list, today=None):
    """
    Group contracts by expiration_date and compute per-expiration metrics.

    Args:
        contracts_list: список dict'ов от Polygon с полями expiration_date, strike_price.
        today: date для подсчёта TTE (default = date.today()).

    Returns:
        dict {expiration_date: {
            'n_strikes': int,
            'min_strike': float,
            'max_strike': float,
            'range_pct': float,
            'tte_days': int,
            'is_monthly_or_leap': bool,
            'reject_reason': str | None,
        }}

    Принимаем expiration date если ОДНО из:
      A. n_strikes >= WIDE (150) AND range_pct >= WIDE_RANGE (400) — "очень wide listing"
         (LEAPS даже если TTE < 30 — это включает Thursday LEAPS близко к экспирации).
      B. n_strikes >= 50 AND TTE >= 30 — стандартное monthly/quarterly
         (weeklies успевают экспирировать в течение 30 дней).

    На GLD 2026-2027 это:
      ✅ ACCEPT: все Friday-monthly (3-я пятница), Thursday LEAPS (2026-06-18, 2027-06-17),
                 EOM quarterly (Tue/Wed last day of quarter).
      ❌ REJECT: weeklies/daily в ближайшие 30 дней, 0DTE.

    Возможный false positive: weekly с TTE>=30 (например 4-я пятница месяца). Не критично —
    они экспирят в течение 1-4 недель, шума мало. UI может дополнительно фильтровать по TTE.
    """
    from collections import defaultdict
    if today is None:
        today = date.today()
    groups = defaultdict(list)
    for c in contracts_list:
        exp = c.get('expiration_date')
        sp = c.get('strike_price')
        if exp and sp is not None:
            groups[exp].append(sp)

    result = {}
    for exp, strikes in groups.items():
        try:
            d = date.fromisoformat(exp)
        except (ValueError, TypeError):
            continue
        n = len(strikes)
        min_k = min(strikes) if strikes else 0
        max_k = max(strikes) if strikes else 0
        range_pct = (max_k - min_k) / max(min_k, 1) * 100 if min_k > 0 else 0
        tte_days = (d - today).days

        # Reject TTE in the past (already expired)
        if tte_days < 0:
            reject = f'tte={tte_days}d (expired)'
            accept = False
        # Rule A: wide listing → LEAPS, accept regardless of TTE
        elif n >= WIDE_LISTING_N_STRIKES and range_pct >= WIDE_LISTING_RANGE_PCT:
            accept = True
            reject = None
        # Rule B: standard monthly/quarterly
        elif n >= MIN_STRIKES_FOR_MONTHLY and tte_days >= MIN_TTE_DAYS_FOR_DISCOVERY:
            accept = True
            reject = None
        else:
            # Compose reject reason for transparency
            reasons = []
            if n < MIN_STRIKES_FOR_MONTHLY:
                reasons.append(f'n_strikes={n}<{MIN_STRIKES_FOR_MONTHLY}')
            if tte_days < MIN_TTE_DAYS_FOR_DISCOVERY:
                reasons.append(f'tte={tte_days}d<{MIN_TTE_DAYS_FOR_DISCOVERY}')
            if n < WIDE_LISTING_N_STRIKES or range_pct < WIDE_LISTING_RANGE_PCT:
                reasons.append(f'not wide enough (need n>={WIDE_LISTING_N_STRIKES} AND range>={WIDE_LISTING_RANGE_PCT}%)')
            reject = ' AND '.join(reasons) or 'unclassified'
            accept = False

        result[exp] = {
            'n_strikes': n,
            'min_strike': min_k,
            'max_strike': max_k,
            'range_pct': range_pct,
            'tte_days': tte_days,
            'is_monthly_or_leap': accept,
            'reject_reason': reject,
        }
    return result


def is_monthly_leaps(expiration_date_str):
    """
    DEPRECATED. Legacy hardcoded filter: weekday=Friday AND day 15-21.
    Используется только для совместимости со старым кодом — новый код должен
    использовать classify_expirations() с двухуровневым фильтром.

    Bug этого фильтра (обнаружено 2026-05-25): пропускает Thursday LEAPS
    (например GLD 2027-06-17) и EOM quarterly (последний рабочий день квартала).
    """
    try:
        d = date.fromisoformat(expiration_date_str)
        return d.weekday() == 4 and 15 <= d.day <= 21
    except (ValueError, TypeError):
        return False
