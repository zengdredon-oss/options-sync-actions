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

    def get(self, url, extra_params=None, max_retries=3):
        """GET with key rotation and 429 retry."""
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
                with urllib.request.urlopen(req, timeout=30) as resp:
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
            except urllib.error.URLError as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise PolygonError(f'URL error: {e}') from e
        raise PolygonError(f'All retries exhausted for {url}')

    def list_contracts(self, underlying, expired=False, limit=1000):
        """Generator over all contracts for an underlying. Paginates via next_url."""
        url = (f'https://api.polygon.io/v3/reference/options/contracts'
               f'?underlying_ticker={underlying}&expired={"true" if expired else "false"}&limit={limit}')
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


def is_monthly_leaps(expiration_date_str):
    """
    True iff this is a "standard monthly" expiration (3rd Friday of month).
    These include LEAPS (>9 months out) and shorter monthlies. We trade only these.
    Python weekday: Monday=0, Friday=4.
    """
    try:
        d = date.fromisoformat(expiration_date_str)
        return d.weekday() == 4 and 15 <= d.day <= 21
    except (ValueError, TypeError):
        return False
