"""
Minimal Telegram Bot API client (sendMessage only).

Used for status alerts ONLY (sync failures). Daily success — silent (no spam).
"""
import json
import os
import urllib.parse
import urllib.request


def send_alert(text, parse_mode=None, token=None, chat_id=None):
    """Send a Telegram message. Returns True on success, False on failure."""
    token = token or os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = chat_id or os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat_id:
        print(f'WARN: TELEGRAM_BOT_TOKEN/CHAT_ID not set, would have sent: {text[:80]}...')
        return False

    data = {'chat_id': str(chat_id), 'text': text, 'disable_web_page_preview': 'true'}
    if parse_mode:
        data['parse_mode'] = parse_mode

    url = f'https://api.telegram.org/bot{token}/sendMessage'
    body = urllib.parse.urlencode(data).encode('utf-8')
    try:
        req = urllib.request.Request(url, data=body, method='POST')
        with urllib.request.urlopen(req, timeout=10) as resp:
            r = json.loads(resp.read().decode('utf-8'))
            return r.get('ok', False)
    except Exception as e:
        print(f'WARN: Telegram send failed: {e}')
        return False
