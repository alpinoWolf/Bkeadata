"""
Polymarket BTC 15-min Data Collector
Determines C1, C2, and 15m direction ourselves from YES price.
No waiting for official settlement — read price, decide ourselves.
Logs bid/ask/liquidity every second from C2 close to 14:00.
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
FIVE_MIN   = 300   # 5 minutes in seconds
FIFT_MIN   = 900   # 15 minutes in seconds
C1_CLOSE   = 300   # C1 closes at 5:00
C2_CLOSE   = 600   # C2 closes at 10:00
CANDLE_END = 900   # 15-min candle closes at 15:00
LOG_START  = 300   # enter window from 5:00 onwards
LOG_END    = 840   # stop logging at 14:00

# ── Direction threshold ───────────────────────────────────────
UP_THRESHOLD   = 0.75
DOWN_THRESHOLD = 0.25


def floor_to_boundary(ts, boundary):
    return ts - (ts % boundary)


def get_15min_boundaries():
    now         = int(time.time())
    boundary_15 = floor_to_boundary(now, FIFT_MIN)
    c1_start    = boundary_15
    c2_start    = boundary_15 + FIVE_MIN
    return boundary_15, c1_start, c2_start


def get_token_ids_for_market(ts_start, interval='5m'):
    """
    Get YES token ID for any market.
    clobTokenIds comes back as stringified list — use json.loads.
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


def get_yes_price(token_id):
    """
    Read current YES price midpoint from order book.
    After close: near 1.0 = UP won, near 0.0 = DOWN won.
    Returns float 0-1 or None on failure.
    """
    if not token_id:
        return None
    try:
        resp = requests.get(
            f'{CLOB_API}/book',
            params={'token_id': token_id},
            timeout=5
        )
        if resp.status_code != 200:
            return None
        book = resp.json()

        bids = book.get('bids', [])
        asks = book.get('asks', [])

        best_bid = float(bids[0]['price']) if bids else None
        best_ask = float(asks[0]['price']) if asks else None

        if best_bid is not None and best_ask is not None:
            return round((best_bid + best_ask) / 2, 4)
        elif best_bid is not None:
            return best_bid
        elif best_ask is not None:
            return best_ask
        return None

    except Exception as e:
        print(f'  Error reading YES price: {e}')
        return None


def price_to_direction(yes_price, label=''):
    """
    Convert YES price to UP or DOWN.
    >= 0.75 → UP
    <= 0.25 → DOWN
    Between → None (unclear, skip)
    """
    if yes_price is None:
        print(f'  {label}: No price — skipping')
        return None
    print(f'  {label}: YES price = {yes_price}')
    if yes_price >= UP_THRESHOLD:
        print(f'  {label}: Direction = UP')
        return 'UP'
    elif yes_price <= DOWN_THRESHOLD:
        print(f'  {label}: Direction = DOWN')
        return 'DOWN'
    else:
        print(f'  {label}: Unclear ({yes_price}) — skipping')
        return None


def get_order_book(token_id):
    """Full order book with top 5 levels of liquidity."""
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

        spread = round(best_ask - best_bid, 4) \
                 if best_bid and best_ask else 0

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
    """Get last trade price and side."""
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


def run_collector():
    """Main logic."""

    now = int(time.time())
    print(f'Running at: '
          f'{datetime.fromtimestamp(now, tz=ET).strftime("%Y-%m-%d %H:%M:%S ET")}')

    boundary_15, c1_start, c2_start = get_15min_boundaries()
    seconds_into_15 = now - boundary_15

    print(f'15-min window started: '
          f'{datetime.fromtimestamp(boundary_15, tz=ET).strftime("%H:%M:%S ET")}')
    print(f'Seconds into window: {seconds_into_15}')

    # ── Only proceed if between 5:00 and 14:00 ───────────────
    if not (LOG_START <= seconds_into_15 <= LOG_END):
        print(f'Not in window ({LOG_START}-{LOG_END}s). Exiting.')
        return

    # ── Get all token IDs upfront ─────────────────────────────
    print('Fetching token IDs...')
    c1_yes_id, _  = get_token_ids_for_market(c1_start,    '5m')
    c2_yes_id, _  = get_token_ids_for_market(c2_start,    '5m')
    yes_id, no_id = get_token_ids_for_market(boundary_15, '15m')

    if not c1_yes_id:
        print('Cannot get C1 token ID. Exiting.')
        return
    if not c2_yes_id:
        print('Cannot get C2 token ID. Exiting.')
        return
    if not yes_id or not no_id:
        print('Cannot get 15-min token IDs. Exiting.')
        return

    # ── Determine C1 direction ────────────────────────────────
    # C1 closes at 5:00 (300s)
    # If we are already past it — read now
    # If not — wait until it closes then read
    now_check       = int(time.time())
    secs_now        = now_check - boundary_15

    if secs_now < C1_CLOSE:
        wait = C1_CLOSE - secs_now + 3
        print(f'Waiting {wait}s for C1 to close...')
        time.sleep(wait)

    c1_price  = get_yes_price(c1_yes_id)
    c1_result = price_to_direction(c1_price, 'C1')

    if c1_result is None:
        print('C1 direction unclear. Exiting.')
        return

    # ── Determine C2 direction ────────────────────────────────
    # C2 closes at 10:00 (600s)
    # If we are already past it — read now
    # If not — wait until it closes then read
    now_check       = int(time.time())
    secs_now        = now_check - boundary_15

    if secs_now < C2_CLOSE:
        wait = C2_CLOSE - secs_now + 3
        print(f'Waiting {wait}s for C2 to close...')
        time.sleep(wait)

    c2_price  = get_yes_price(c2_yes_id)
    c2_result = price_to_direction(c2_price, 'C2')

    if c2_result is None:
        print('C2 direction unclear. Exiting.')
        return

    # ── Determine sequence ────────────────────────────────────
    if   c1_result == 'UP'   and c2_result == 'UP':
        sequence = 'GG'
    elif c1_result == 'DOWN' and c2_result == 'DOWN':
        sequence = 'RR'
    else:
        print(f'Mixed ({c1_result},{c2_result}). Not logging. Exiting.')
        return

    print(f'✅ {sequence} confirmed! Starting data collection...')

    # ── Prepare CSV ───────────────────────────────────────────
    os.makedirs('data', exist_ok=True)
    window_str = datetime.fromtimestamp(
        boundary_15, tz=ET).strftime('%Y%m%d_%H%M')
    filename = f'data/{window_str}_{sequence}.csv'

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

    # ── Log every second from now until 14:00 ────────────────
    print(f'Logging from now until {LOG_END}s mark...')

    while True:
        loop_now     = int(time.time())
        secs_into_15 = loop_now - boundary_15

        if secs_into_15 > LOG_END:
            print(f'End of logging window at {secs_into_15}s. Stopping.')
            break

        ts_utc = datetime.fromtimestamp(
            loop_now, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        ts_et  = datetime.fromtimestamp(
            loop_now, tz=ET).strftime('%Y-%m-%d %H:%M:%S')

        yes_book              = get_order_book(yes_id)
        no_book               = get_order_book(no_id)
        last_price, last_side = get_last_trade(yes_id)

        row = {
            'timestamp_utc'    : ts_utc,
            'timestamp_et'     : ts_et,
            'sequence_type'    : sequence,
            'seconds_elapsed'  : secs_into_15 - C2_CLOSE,
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
        print(f'  [{secs_into_15}s] '
              f'YES={yes_book.get("best_ask","")} '
              f'NO={no_book.get("best_ask","")}')

        time.sleep(1)

    # ── Wait for 15-min candle to close then read its price ───
    # Candle closes at 15:00 (900s)
    # Wait until we are past 900s then read YES price ourselves
    now_check   = int(time.time())
    secs_now    = now_check - boundary_15

    if secs_now < CANDLE_END:
        wait = CANDLE_END - secs_now + 3
        print(f'Waiting {wait}s for 15-min candle to close...')
        time.sleep(wait)

    # Read 15-min YES price and decide direction ourselves
    print('Reading 15-min candle outcome from YES price...')
    outcome_price = get_yes_price(yes_id)
    outcome       = price_to_direction(outcome_price, '15m candle')

    print(f'15m outcome: {outcome} (YES price was {outcome_price})')

    # ── Fill outcome into all rows ────────────────────────────
    for row in rows:
        row['candle_outcome'] = outcome or 'PENDING'

    # ── Write CSV ─────────────────────────────────────────────
    write_header = not os.path.exists(filename)
    with open(filename, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    print(f'✅ Saved {len(rows)} rows to {filename}')

    # ── Handle pending outcome ────────────────────────────────
    # If outcome is still unclear — save for next run to retry
    if outcome is None:
        pending = {
            'boundary_15': boundary_15,
            'filename':    filename,
            'sequence':    sequence
        }
        with open('data/pending_outcome.json', 'w') as f:
            json.dump(pending, f)
        print('Outcome still unclear — saved as pending')

    # ── Update any pending outcome from previous candle ───────
    pending_file = 'data/pending_outcome.json'
    if os.path.exists(pending_file):
        try:
            with open(pending_file) as f:
                pending = json.load(f)

            # Read YES price of the pending 15m market
            prev_yes_id, _ = get_token_ids_for_market(
                pending['boundary_15'], '15m')
            prev_price   = get_yes_price(prev_yes_id)
            prev_outcome = price_to_direction(prev_price, 'prev 15m')

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
