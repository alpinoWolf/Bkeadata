"""
Polymarket BTC 15-min Data Collector
Logs bid/ask/liquidity every second from 10:01 to 11:30
ONLY when first two 5-min candles of a 15-min window are GG or RR
"""

import requests
import time
import csv
import os
import json
from datetime import datetime, timezone
import pytz

ET = pytz.timezone('America/New_York')

# ── Polymarket API endpoints ──────────────────────────────────
GAMMA_API = 'https://gamma-api.polymarket.com/markets'
CLOB_API  = 'https://clob.polymarket.com'

# ── Timing constants ──────────────────────────────────────────
FIVE_MIN  = 300   # 5 minutes in seconds
FIFT_MIN  = 900   # 15 minutes in seconds
LOG_START = 601   # 10 min 1 sec into 15-min candle
LOG_END   = 840   # 14 min into 15-min candle


def floor_to_boundary(ts, boundary):
    return ts - (ts % boundary)


def get_15min_boundaries():
    now         = int(time.time())
    boundary_15 = floor_to_boundary(now, FIFT_MIN)
    c1_start    = boundary_15
    c2_start    = boundary_15 + FIVE_MIN
    return boundary_15, c1_start, c2_start


def get_market_result(ts_start, interval='5m'):
    """
    Fetch result of a resolved Polymarket BTC up/down market.
    Returns 'UP', 'DOWN', or None if not yet resolved.
    outcomePrices comes back as a stringified list.
    Must use json.loads() — NOT direct indexing.
    """
    slug = f'btc-updown-{interval}-{ts_start}'
    try:
        resp = requests.get(
            GAMMA_API,
            params={'slug': slug},
            timeout=10
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data:
            return None

        market = data[0] if isinstance(data, list) else data

        if market.get('closed') or market.get('resolved'):
            outcome_raw = market.get('outcomePrices', '[]')
            try:
                outcome = json.loads(outcome_raw)
            except Exception:
                outcome = []

            if outcome and float(outcome[0]) >= 0.99:
                return 'UP'
            elif len(outcome) > 1 and float(outcome[1]) >= 0.99:
                return 'DOWN'
        return None

    except Exception as e:
        print(f'  Error fetching market result: {e}')
        return None


def get_token_ids(ts_start, interval='15m'):
    """
    Get YES and NO token IDs for the active 15-min market.
    clobTokenIds comes back as a stringified list.
    Must use json.loads() — NOT direct indexing.
    """
    slug = f'btc-updown-{interval}-{ts_start}'
    try:
        resp = requests.get(
            GAMMA_API,
            params={'slug': slug},
            timeout=10
        )
        if resp.status_code != 200:
            return None, None
        data = resp.json()
        if not data:
            return None, None

        market = data[0] if isinstance(data, list) else data

        tokens_raw = market.get('clobTokenIds', '[]')
        try:
            tokens = json.loads(tokens_raw)
        except Exception:
            tokens = market.get('tokens', [])

        if len(tokens) >= 2:
            yes_id = tokens[0].get('token_id', tokens[0]) \
                     if isinstance(tokens[0], dict) else tokens[0]
            no_id  = tokens[1].get('token_id', tokens[1]) \
                     if isinstance(tokens[1], dict) else tokens[1]
            return yes_id, no_id
        return None, None

    except Exception as e:
        print(f'  Error fetching token IDs: {e}')
        return None, None


def get_order_book(token_id):
    if not token_id:
        return {}
    try:
        resp = requests.get(
            f'{CLOB_API}/book',
            params={'token_id': token_id},
            timeout=5
        )
        if resp.status_code != 200:
            return {}
        book = resp.json()

        bids = book.get('bids', [])
        asks = book.get('asks', [])

        best_bid = float(bids[0]['price']) if bids else 0.0
        best_ask = float(asks[0]['price']) if asks else 0.0

        bid_liq = sum(
            float(b['price']) * float(b['size'])
            for b in bids[:5]
        )
        ask_liq = sum(
            float(a['price']) * float(a['size'])
            for a in asks[:5]
        )

        spread = round(best_ask - best_bid, 4) if best_bid and best_ask else 0

        return {
            'best_bid': best_bid,
            'best_ask': best_ask,
            'spread':   spread,
            'bid_liq':  round(bid_liq, 2),
            'ask_liq':  round(ask_liq, 2),
        }
    except Exception:
        return {}


def get_last_trade(token_id):
    if not token_id:
        return 0.0, ''
    try:
        resp = requests.get(
            f'{CLOB_API}/last-trade-price',
            params={'token_id': token_id},
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            return float(data.get('price', 0)), data.get('side', '')
        return 0.0, ''
    except Exception:
        return 0.0, ''


def get_15min_outcome(ts_start):
    return get_market_result(ts_start, interval='15m')


def run_collector():
    now = int(time.time())
    print(f'Running at: {datetime.fromtimestamp(now, tz=ET).strftime("%Y-%m-%d %H:%M:%S ET")}')

    boundary_15, c1_start, c2_start = get_15min_boundaries()
    seconds_into_15 = now - boundary_15

    print(f'15-min window started: {datetime.fromtimestamp(boundary_15, tz=ET).strftime("%H:%M:%S ET")}')
    print(f'Seconds into window: {seconds_into_15}')

    if not (LOG_START <= seconds_into_15 <= LOG_END + 10):
        print(f'Not in logging window ({LOG_START}-{LOG_END}). Exiting.')
        return
        # Wait 60 seconds for Polymarket to settle C2 result
    print('In window — waiting 60s for C2 to settle...')
    time.sleep(60)

    print(f'Checking C1 (starts {c1_start}) and C2 (starts {c2_start})...')

    c1_result = get_market_result(c1_start, '5m')
    c2_result = get_market_result(c2_start, '5m')

    print(f'C1 result: {c1_result}')
    print(f'C2 result: {c2_result}')

    if c1_result is None or c2_result is None:
        print('C1 or C2 not yet resolved. Exiting.')
        return

    if   c1_result == 'UP'   and c2_result == 'UP':
        sequence = 'GG'
    elif c1_result == 'DOWN' and c2_result == 'DOWN':
        sequence = 'RR'
    else:
        print(f'Mixed sequence ({c1_result},{c2_result}). Not logging. Exiting.')
        return

    print(f'{sequence} confirmed! Starting data collection...')

    yes_id, no_id = get_token_ids(boundary_15, '15m')
    if not yes_id or not no_id:
        print('Could not get token IDs. Exiting.')
        return

    print(f'YES token: {str(yes_id)[:20]}...')
    print(f'NO  token: {str(no_id)[:20]}...')

    os.makedirs('data', exist_ok=True)
    window_str = datetime.fromtimestamp(boundary_15, tz=ET).strftime('%Y%m%d_%H%M')
    filename   = f'data/{window_str}_{sequence}.csv'

    fieldnames = [
        'timestamp_utc', 'timestamp_et',
        'sequence_type', 'seconds_elapsed',
        'yes_best_bid',  'yes_best_ask',  'yes_spread',
        'yes_bid_liq',   'yes_ask_liq',
        'no_best_bid',   'no_best_ask',   'no_spread',
        'no_bid_liq',    'no_ask_liq',
        'last_trade_price', 'last_trade_side',
        'candle_outcome'
    ]

    rows = []

    while True:
        loop_now     = int(time.time())
        secs_into_15 = loop_now - boundary_15

        if secs_into_15 > LOG_END:
            print(f'Reached end of logging window at {secs_into_15}s. Stopping.')
            break

        ts_utc = datetime.fromtimestamp(loop_now, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        ts_et  = datetime.fromtimestamp(loop_now, tz=ET).strftime('%Y-%m-%d %H:%M:%S')

        yes_book              = get_order_book(yes_id)
        no_book               = get_order_book(no_id)
        last_price, last_side = get_last_trade(yes_id)

        row = {
            'timestamp_utc'    : ts_utc,
            'timestamp_et'     : ts_et,
            'sequence_type'    : sequence,
            'seconds_elapsed'  : secs_into_15 - 601,
            'yes_best_bid'     : yes_book.get('best_bid', ''),
            'yes_best_ask'     : yes_book.get('best_ask', ''),
            'yes_spread'       : yes_book.get('spread',   ''),
            'yes_bid_liq'      : yes_book.get('bid_liq',  ''),
            'yes_ask_liq'      : yes_book.get('ask_liq',  ''),
            'no_best_bid'      : no_book.get('best_bid',  ''),
            'no_best_ask'      : no_book.get('best_ask',  ''),
            'no_spread'        : no_book.get('spread',    ''),
            'no_bid_liq'       : no_book.get('bid_liq',   ''),
            'no_ask_liq'       : no_book.get('ask_liq',   ''),
            'last_trade_price' : last_price,
            'last_trade_side'  : last_side,
            'candle_outcome'   : ''
        }
        rows.append(row)
        print(f'  [{secs_into_15}s] YES={yes_book.get("best_ask","")} NO={no_book.get("best_ask","")}')

        time.sleep(1)

    outcome = get_15min_outcome(boundary_15)
    print(f'15m candle outcome: {outcome}')

    for row in rows:
        row['candle_outcome'] = outcome or 'PENDING'

    write_header = not os.path.exists(filename)
    with open(filename, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    print(f'Saved {len(rows)} rows to {filename}')

    if outcome is None:
        pending = {
            'boundary_15': boundary_15,
            'filename':    filename,
            'sequence':    sequence
        }
        with open('data/pending_outcome.json', 'w') as f:
            json.dump(pending, f)
        print('Outcome pending — will update on next run')

    pending_file = 'data/pending_outcome.json'
    if os.path.exists(pending_file):
        try:
            with open(pending_file) as f:
                pending = json.load(f)
            prev_outcome = get_15min_outcome(pending['boundary_15'])
            if prev_outcome:
                rows_updated = []
                with open(pending['filename'], 'r') as f:
                    reader = csv.DictReader(f)
                    for r in reader:
                        r['candle_outcome'] = prev_outcome
                        rows_updated.append(r)
                with open(pending['filename'], 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows_updated)
                os.remove(pending_file)
                print(f'Updated pending outcome: {prev_outcome}')
        except Exception as e:
            print(f'Error updating pending: {e}')


if __name__ == '__main__':
    run_collector()
