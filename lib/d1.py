"""
D1 HTTP client (no external dependencies).

Cloudflare D1 REST API: https://developers.cloudflare.com/api/operations/cloudflare-d1-query-database

POST /accounts/{account_id}/d1/database/{database_id}/query
  Headers:
    Authorization: Bearer {api_token}
    Content-Type: application/json
  Body:
    { "sql": "SELECT ...", "params": [...] }

Returns:
    { "result": [{ "results": [...], "success": true, ... }], "success": true }
"""
import json
import os
import time
import urllib.request
import urllib.error


class D1Client:
    """Minimal D1 client using Cloudflare REST API."""

    def __init__(self, account_id=None, database_id=None, api_token=None):
        self.account_id = account_id or os.environ['CLOUDFLARE_ACCOUNT_ID']
        self.database_id = database_id or os.environ['CLOUDFLARE_D1_DB_ID']
        self.api_token = api_token or os.environ['CLOUDFLARE_API_TOKEN']
        self.base_url = (
            f'https://api.cloudflare.com/client/v4/accounts/'
            f'{self.account_id}/d1/database/{self.database_id}'
        )

    def query(self, sql, params=None, retries=3, retry_delay=2):
        """Execute SQL. Returns rows as list of dicts (for SELECT) or meta dict (for INSERT/UPDATE)."""
        body = {'sql': sql}
        if params is not None:
            body['params'] = params
        data = json.dumps(body).encode('utf-8')

        last_err = None
        for attempt in range(retries):
            try:
                req = urllib.request.Request(
                    self.base_url + '/query',
                    data=data,
                    method='POST',
                    headers={
                        'Authorization': f'Bearer {self.api_token}',
                        'Content-Type': 'application/json',
                    },
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.loads(resp.read().decode('utf-8'))
                    if not payload.get('success'):
                        errs = payload.get('errors') or []
                        msg = '; '.join(e.get('message', str(e)) for e in errs)
                        raise D1Error(f'D1 query failed: {msg}')
                    # result is a list (D1 supports multi-statement, we use one)
                    results = payload.get('result', [{}])[0]
                    return results
            except urllib.error.HTTPError as e:
                last_err = e
                body_str = ''
                try:
                    body_str = e.read().decode('utf-8')[:500]
                except Exception:
                    pass
                # 429 / 5xx → retry
                if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                raise D1Error(f'HTTP {e.code}: {body_str}') from e
            except urllib.error.URLError as e:
                last_err = e
                if attempt < retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                raise D1Error(f'URL error: {e}') from e
        raise D1Error(f'All retries failed: {last_err}')

    def select(self, sql, params=None):
        """SELECT — returns list of dict rows."""
        r = self.query(sql, params)
        return r.get('results', []) if r.get('success', True) else []

    def execute(self, sql, params=None):
        """INSERT/UPDATE/DELETE — returns meta dict (rows_written, last_row_id, ...)."""
        r = self.query(sql, params)
        return r.get('meta', {})

    def batch_insert(self, sql, rows, batch_size=200):
        """
        Multi-VALUES INSERT in chunks.

        IMPORTANT: D1 has a hard limit of 100 bound parameters per statement.
        With multi-row INSERT, params = rows * cols, which quickly exceeds 100.
        Workaround: we INLINE values into SQL (no bind params) — D1 allows
        much larger raw SQL (up to ~1MB per execute).

        sql_template should end with VALUES (?,?,...) — we use the placeholder
        count to determine column count but build inline values ourselves.
        rows: list of tuples (each tuple matches the column order in sql_template).

        Example:
            sql_template = "INSERT OR REPLACE INTO option_bars (ticker, date, c) VALUES (?,?,?)"
            client.batch_insert(sql_template, [(t1, d1, c1), (t2, d2, c2), ...])
        """
        if not rows:
            return 0

        upper = sql.upper()
        idx = upper.rfind('VALUES')
        if idx < 0:
            raise ValueError('sql must contain VALUES clause')
        prefix = sql[:idx]  # everything up to "VALUES "
        # Single-row template — count placeholders to validate column count
        tmpl = sql[idx + len('VALUES'):].strip()
        n_cols = tmpl.count('?')

        total_written = 0
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            # Build inline values block
            inline_rows = []
            for r in chunk:
                if len(r) != n_cols:
                    raise ValueError(f'row has {len(r)} values, expected {n_cols}: {r}')
                inline_rows.append('(' + ', '.join(_sql_literal(v) for v in r) + ')')
            values_block = ', '.join(inline_rows)
            full_sql = f'{prefix} VALUES {values_block}'
            meta = self.execute(full_sql)
            total_written += meta.get('rows_written', 0)
        return total_written


def _sql_literal(v):
    """Convert Python value to SQL literal (for inline INSERT without bind params)."""
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return '1' if v else '0'
    if isinstance(v, (int, float)):
        return str(v)
    # String — escape single quotes
    s = str(v).replace("'", "''")
    return f"'{s}'"


class D1Error(Exception):
    pass
