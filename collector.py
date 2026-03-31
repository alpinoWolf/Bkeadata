"""
Polymarket BTC 15-min Data Collector
Continuous loop — watches clock itself.
Logs UP price, DOWN price and liquidity every second.
Token order fix: clobTokenIds[0]=DOWN, clobTokenIds[1]=UP
"""

import requests
import time
import csv
import os
import json
import subprocess
from datetime import datetime, timezone
import pytz

ET = pytz.timezone('America/New_York')

GAMMA_API = 'https://gamma-api.polymarket.com/markets'
CLOB_API  = 'https://clob.polymarket.com'

FIVE_MIN   = 300
FIFT_MIN   = 900
C1_CLOSE   = 300
C2_CLOSE   = 600
CANDLE_END = 900
LOG_END    = 840

UP_THRESHOLD   = 0.75
DOWN_THRESHOLD = 0.25

MAX_RUN_SECONDS = 6 * 60 * 60
START_TIME      = int(time.time())


def floor_to_boundary(ts, boundary):
    return ts - (ts % boundary)


def seconds_into_current_window():
    now      = int(time.time())
    boundary = floor_to_boundary(now, FIFT_MIN)
    return now - boundary, boundary


def wait_until(target_seconds, boundary_15):
    while True:
        secs = int(time.time()) - boundary_15
        if secs >= target_seconds:
            return secs
        time.sleep(0.5)


def get_token_ids_for_market(ts_start, interval='5m'):
    """
    Get UP and DOWN token IDs.
    IMPORTANT: clobTokenIds[0] = DOWN token, clobTokenIds[1] = UP token
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
            tokens = []

        if len(tokens) >= 2:
            # index 0 = DOWN token, index 1 = UP token
            down_id = tokens[0].get('token_id', tokens[0]) \
                      if isinstance(tokens[0], dict) else tokens[0]
            up_id   = tokens[1].get('token_id', tokens[1]) \
                      if isinstance(tokens[1], dict) else tokens[1]
            return up_id, down_id

        return None, None

    except Exception as e:
        print(f'  Error fetching token IDs: {e}')
        return None, None


def get_yes_price(token_id):
    """
    Read token price from order book midpoint.
    Falls back to last trade price if order book is empty.
    For UP token: near 1.0 = UP won, near 0.0 = DOWN won.
    """
    if not token_id:
        return None
    try:
        resp = requests.get(
            f'{CLOB_API}/book',
            params={'token_id': token_id},
            timeout=5
        )
        if resp.status_code == 200:
            book     = resp.json()
            bids     = book.get('bids', [])
            asks     = book.get('asks', [])
            best_bid = float(bids[0]['price']) if bids else None
            best_ask = float(asks[0]['price']) if asks else None

            if best_bid is not None and best_ask is not None:
                return round((best_bid + best_ask) / 2, 4)
            elif best_bid is not None:
                return best_bid
            elif best_ask is not None:
                return best_ask

        # Order book empty — fall back to last trade price
        print('  Order book empty — trying last trade price...')
        resp2 = requests.get(
            f'{CLOB_API}/last-trade-price',
            params={'token_id': token_id},
            timeout=5
        )
        if resp2.status_code == 200:
            price = resp2.json().get('price')
            if price:
                return float(price)

        return None

    except Exception as e:
        print(f'  Error reading price: {e}')
        return None


def price_to_direction(yes_price, label=''):
    if yes_price is None:
        print(f'  {label}: No price — skipping')
        return None
    print(f'  {label}: UP token price = {yes_price}')
    if yes_price >= UP_THRESHOLD:
        print(f'  {label}: Direction = UP')
        return 'UP'
    elif yes_price <= DOWN_THRESHOLD:
        print(f'  {label}: Direction = DOWN')
        return 'DOWN'
    else:
        print(f'  {label}: Unclear ({yes_price}) — skipping')
        return None


def get_order_book_simple(token_id):
    """
    Get best ask price and total ask liquidity (top 5 levels).
    Returns ask_price and ask_liquidity_in_dollars.
    """
    if not token_id:
        return 0.0, 0.0
    try:
        resp = requests.get(
            f'{CLOB_API}/book',
            params={'token_id': token_id},
            timeout=4
        )
        if resp.status_code != 200:
            return 0.0, 0.0
        book    = resp.json()
        asks    = book.get('asks', [])
        best_ask = float(asks[0]['price']) if asks else 0.0
        ask_liq  = sum(
            float(a['price']) * float(a['size'])
            for a in asks[:5]
        )
        return round(best_ask, 4), round(ask_liq, 2)
    except Exception:
        return 0.0, 0.0


def git_commit(message):
    try:
        subprocess.run(['git', 'add', 'data/'], check=True)
        result = subprocess.run(
            ['git', 'diff', '--staged', '--quiet'],
            capture_output=True
        )
        if result.returncode != 0:
            subprocess.run(
                ['git', 'commit', '-m', message], check=True)
            subprocess.run(['git', 'push'], check=True)
            print(f'Committed: {message}')
    except Exception as e:
        print(f'Git commit error: {e}')


def update_pending_outcome():
    """
    Called at start of every cycle.
    Reads previous candle's UP token price and updates CSV.
    """
    pending_file = 'data/pending_outcome.json'
    if not os.path.exists(pending_file):
        return

    print('Checking pending outcome from previous candle...')
    try:
        with open(pending_file) as f:
            pending = json.load(f)

        prev_boundary = pending['boundary_15']
        prev_filename = pending['filename']

        now = int(time.time())
        if now < prev_boundary + CANDLE_END + 10:
            print('Previous candle not closed yet. Will retry.')
            return

        prev_up_id, _ = get_token_ids_for_market(prev_boundary, '15m')
        if not prev_up_id:
            print('Cannot get token ID for previous candle.')
            return

        prev_price   = get_yes_price(prev_up_id)
        prev_outcome = price_to_direction(prev_price, 'prev 15m')

        if prev_outcome is None:
            print('Previous outcome unclear. Will retry.')
            return

        if not os.path.exists(prev_filename):
            os.remove(pending_file)
            return

        rows_updated = []
        fieldnames   = None
        with open(prev_filename, 'r') as f:
            reader     = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                row['candle_outcome'] = prev_outcome
                rows_updated.append(row)

        with open(prev_filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_updated)

        os.remove(pending_file)
        print(f'Previous candle outcome: {prev_outcome} '
              f'(UP token price: {prev_price})')
        print(f'Updated {len(rows_updated)} rows in {prev_filename}')
        git_commit(f'Update outcome {prev_outcome} {prev_filename}')

    except Exception as e:
        print(f'Error updating pending: {e}')


def process_one_window():
    """
    Handle one complete 15-min window.
    Returns True if data was logged.
    """
    secs, boundary_15 = seconds_into_current_window()
    c1_start = boundary_15
    c2_start = boundary_15 + FIVE_MIN

    now_et = datetime.fromtimestamp(
        boundary_15, tz=ET).strftime('%H:%M:%S ET')
    print(f'\n{"="*50}')
    print(f'Window: {now_et} | At {secs}s')
    print(f'{"="*50}')

    # ── Wait for C1 close at 5:00 ─────────────────────────────
    if secs < C1_CLOSE:
        print(f'Waiting {C1_CLOSE - secs}s for C1 to close...')
        wait_until(C1_CLOSE, boundary_15)
        time.sleep(2)

    # ── Read C1 direction ─────────────────────────────────────
    print('Reading C1...')
    c1_up_id, _ = get_token_ids_for_market(c1_start, '5m')
    if not c1_up_id:
        print('No C1 token. Skipping.')
        return False

    c1_price  = get_yes_price(c1_up_id)
    c1_result = price_to_direction(c1_price, 'C1')
    if c1_result is None:
        print('C1 unclear. Skipping.')
        return False

    # ── Wait for C2 close at 10:00 ────────────────────────────
    secs_now = int(time.time()) - boundary_15
    if secs_now < C2_CLOSE:
        print(f'Waiting {C2_CLOSE - secs_now}s for C2 to close...')
        wait_until(C2_CLOSE, boundary_15)
        time.sleep(2)

    # ── Read C2 direction ─────────────────────────────────────
    print('Reading C2...')
    c2_up_id, _ = get_token_ids_for_market(c2_start, '5m')
    if not c2_up_id:
        print('No C2 token. Skipping.')
        return False

    c2_price  = get_yes_price(c2_up_id)
    c2_result = price_to_direction(c2_price, 'C2')
    if c2_result is None:
        print('C2 unclear. Skipping.')
        return False

    # ── Check sequence ────────────────────────────────────────
    if   c1_result == 'UP'   and c2_result == 'UP':
        sequence = 'GG'
    elif c1_result == 'DOWN' and c2_result == 'DOWN':
        sequence = 'RR'
    else:
        print(f'Mixed ({c1_result},{c2_result}). Skipping.')
        return False

    print(f'✅ {sequence} confirmed!')

    # ── Get 15-min UP and DOWN token IDs ─────────────────────
    up_id, down_id = get_token_ids_for_market(boundary_15, '15m')
    if not up_id or not down_id:
        print('No 15-min tokens. Skipping.')
        return False

    # ── Prepare CSV ───────────────────────────────────────────
    os.makedirs('data', exist_ok=True)
    window_str = datetime.fromtimestamp(
        boundary_15, tz=ET).strftime('%Y%m%d_%H%M')
    filename = f'data/{window_str}_{sequence}.csv'

    # Clean simple columns — only what we need
    fieldnames = [
        'timestamp_et',
        'sequence_type',
        'seconds_elapsed',
        'up_ask',        # price to buy UP  (we want to buy NO/DOWN at low price)
        'up_liq_$',      # $ liquidity available at UP ask
        'down_ask',      # price to buy DOWN
        'down_liq_$',    # $ liquidity available at DOWN ask
        'candle_outcome' # UP or DOWN — filled after 15:00
    ]

    rows = []

    # ── Log every second until 14:00 ─────────────────────────
    print(f'Logging until {LOG_END}s...')

    while True:
        loop_now     = int(time.time())
        secs_into_15 = loop_now - boundary_15

        if secs_into_15 > LOG_END:
            print(f'Stopped at {secs_into_15}s.')
            break

        ts_et = datetime.fromtimestamp(
            loop_now, tz=ET).strftime('%Y-%m-%d %H:%M:%S')

        # Only 2 API calls per second — much faster
        up_ask,   up_liq   = get_order_book_simple(up_id)
        down_ask, down_liq = get_order_book_simple(down_id)

        row = {
            'timestamp_et'   : ts_et,
            'sequence_type'  : sequence,
            'seconds_elapsed': secs_into_15 - C2_CLOSE,
            'up_ask'         : up_ask,
            'up_liq_$'       : up_liq,
            'down_ask'       : down_ask,
            'down_liq_$'     : down_liq,
            'candle_outcome' : 'PENDING'
        }
        rows.append(row)
        print(f'  [{secs_into_15}s] '
              f'UP={up_ask} (${up_liq}) | '
              f'DOWN={down_ask} (${down_liq})')

        time.sleep(1)

    # ── Save CSV with PENDING outcome ─────────────────────────
    write_header = not os.path.exists(filename)
    with open(filename, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    print(f'Saved {len(rows)} rows → {filename}')

    # ── Save pending file ─────────────────────────────────────
    pending = {
        'boundary_15': boundary_15,
        'filename':    filename,
        'sequence':    sequence
    }
    with open('data/pending_outcome.json', 'w') as f:
        json.dump(pending, f)

    git_commit(f'Add {sequence} data {window_str}')

    # ── Wait for 15-min candle to close ──────────────────────
    secs_now = int(time.time()) - boundary_15
    if secs_now < CANDLE_END:
        wait = CANDLE_END - secs_now + 5
        print(f'Waiting {wait}s for 15-min candle to close...')
        time.sleep(wait)

    # ── Read 15-min UP token price for outcome ────────────────
    print('Reading 15-min outcome...')
    outcome_price = get_yes_price(up_id)
    outcome       = price_to_direction(outcome_price, '15m')
    print(f'Outcome: {outcome} (UP token price: {outcome_price})')

    if outcome:
        rows_updated = []
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['candle_outcome'] = outcome
                rows_updated.append(row)
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_updated)

        if os.path.exists('data/pending_outcome.json'):
            os.remove('data/pending_outcome.json')

        git_commit(f'Outcome {outcome} {window_str}')
        print(f'✅ Done. Outcome: {outcome}')
    else:
        print('Outcome unclear — pending for next cycle.')

    return True


def main():
    print(f'Started: '
          f'{datetime.fromtimestamp(START_TIME, tz=ET).strftime("%Y-%m-%d %H:%M:%S ET")}')

    window_count = 0

    while True:
        elapsed = int(time.time()) - START_TIME
        if elapsed > MAX_RUN_SECONDS - 120:
            print('Approaching 6-hour limit. Stopping.')
            break

        # Always check pending outcome first
        update_pending_outcome()

        # Process current window
        logged = process_one_window()
        if logged:
            window_count += 1
            print(f'Total logged this session: {window_count}')

        # Wait for next 15-min window
        secs, boundary_15 = seconds_into_current_window()
        next_boundary     = boundary_15 + FIFT_MIN
        wait_for_next     = next_boundary - int(time.time())

        if wait_for_next > 0:
            print(f'Next window in {wait_for_next}s. Waiting...')
            time.sleep(wait_for_next)
        else:
            time.sleep(1)


if __name__ == '__main__':
    main()
