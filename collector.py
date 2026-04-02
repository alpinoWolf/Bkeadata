"""
Polymarket BTC 15-min Data Collector
Continuous loop — watches clock itself.
Logs bid, ask, liquidity and last trade for UP and DOWN tokens.
Token order: clobTokenIds[0]=DOWN, clobTokenIds[1]=UP
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
    clobTokenIds[0] = DOWN token
    clobTokenIds[1] = UP token
    Returns (up_id, down_id)
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
    Read UP token price for direction detection.
    Uses midpoint if available, falls back to last trade.
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


def price_to_direction(up_price, label=''):
    if up_price is None:
        print(f'  {label}: No price — skipping')
        return None
    print(f'  {label}: UP token price = {up_price}')
    if up_price >= UP_THRESHOLD:
        print(f'  {label}: Direction = UP')
        return 'UP'
    elif up_price <= DOWN_THRESHOLD:
        print(f'  {label}: Direction = DOWN')
        return 'DOWN'
    else:
        print(f'  {label}: Unclear ({up_price}) — skipping')
        return None


def get_order_book_simple(token_id):
    """
    Returns bid, ask, bid_liq, ask_liq, last_trade for one token.
    Bid = real market price (what buyers are paying).
    Ask = what sellers are asking (often stale at 0.99).
    Last = most recent actual trade price.
    """
    if not token_id:
        return 0.0, 0.0, 0.0, 0.0, 0.0

    bid = ask = bid_liq = ask_liq = last = 0.0

    try:
        resp = requests.get(
            f'{CLOB_API}/book',
            params={'token_id': token_id},
            timeout=4
        )
        if resp.status_code == 200:
            book    = resp.json()
            bids    = book.get('bids', [])
            asks    = book.get('asks', [])
            bid     = float(bids[0]['price']) if bids else 0.0
            ask     = float(asks[0]['price']) if asks else 0.0
            bid_liq = round(sum(
                float(b['price']) * float(b['size'])
                for b in bids[:5]), 2)
            ask_liq = round(sum(
                float(a['price']) * float(a['size'])
                for a in asks[:5]), 2)
    except Exception:
        pass

    try:
        resp2 = requests.get(
            f'{CLOB_API}/last-trade-price',
            params={'token_id': token_id},
            timeout=4
        )
        if resp2.status_code == 200:
            price = resp2.json().get('price')
            if price:
                last = float(price)
    except Exception:
        pass

    return (round(bid, 4), round(ask, 4),
            round(bid_liq, 2), round(ask_liq, 2),
            round(last, 4))


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
    pending_file = 'data/pending_outcome.json'
    if not os.path.exists(pending_file):
        return

    print('Checking pending outcome...')
    try:
        with open(pending_file) as f:
            pending = json.load(f)

        prev_boundary = pending['boundary_15']
        prev_filename = pending['filename']

        now = int(time.time())
        if now < prev_boundary + CANDLE_END + 10:
            print('Previous candle not closed yet.')
            return

        prev_up_id, _ = get_token_ids_for_market(prev_boundary, '15m')
        if not prev_up_id:
            return

        prev_price   = get_yes_price(prev_up_id)
        prev_outcome = price_to_direction(prev_price, 'prev 15m')

        if prev_outcome is None:
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
        print(f'Previous outcome: {prev_outcome} (UP price: {prev_price})')
        git_commit(f'Update outcome {prev_outcome} {prev_filename}')

    except Exception as e:
        print(f'Error updating pending: {e}')


def process_one_window():
    secs, boundary_15 = seconds_into_current_window()
    c1_start = boundary_15
    c2_start = boundary_15 + FIVE_MIN

    now_et = datetime.fromtimestamp(
        boundary_15, tz=ET).strftime('%H:%M:%S ET')
    print(f'\n{"="*50}')
    print(f'Window: {now_et} | At {secs}s')
    print(f'{"="*50}')

    # ── Wait and read C1 ──────────────────────────────────────
    if secs < C1_CLOSE:
        print(f'Waiting {C1_CLOSE - secs}s for C1 to close...')
        wait_until(C1_CLOSE, boundary_15)
        time.sleep(2)

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

    # ── Wait and read C2 ──────────────────────────────────────
    secs_now = int(time.time()) - boundary_15
    if secs_now < C2_CLOSE:
        print(f'Waiting {C2_CLOSE - secs_now}s for C2 to close...')
        wait_until(C2_CLOSE, boundary_15)
        time.sleep(2)

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

    # ── Sequence check ────────────────────────────────────────
    if   c1_result == 'UP'   and c2_result == 'UP':
        sequence = 'GG'
    elif c1_result == 'DOWN' and c2_result == 'DOWN':
        sequence = 'RR'
    else:
        print(f'Mixed ({c1_result},{c2_result}). Skipping.')
        return False

    print(f'✅ {sequence} confirmed!')

    # ── Get 15-min tokens ─────────────────────────────────────
    up_id, down_id = get_token_ids_for_market(boundary_15, '15m')
    if not up_id or not down_id:
        print('No 15-min tokens. Skipping.')
        return False

    # ── Prepare CSV ───────────────────────────────────────────
    os.makedirs('data', exist_ok=True)
    window_str = datetime.fromtimestamp(
        boundary_15, tz=ET).strftime('%Y%m%d_%H%M')
    filename = f'data/{window_str}_{sequence}.csv'

    fieldnames = [
        'timestamp_et',
        'sequence_type',
        'seconds_elapsed',
        'up_bid',
        'up_ask',
        'up_bid_liq',
        'up_ask_liq',
        'up_last',
        'down_bid',
        'down_ask',
        'down_bid_liq',
        'down_ask_liq',
        'down_last',
        'candle_outcome'
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

        (up_bid, up_ask,
         up_bid_liq, up_ask_liq,
         up_last)   = get_order_book_simple(up_id)

        (down_bid, down_ask,
         down_bid_liq, down_ask_liq,
         down_last) = get_order_book_simple(down_id)

        row = {
            'timestamp_et'   : ts_et,
            'sequence_type'  : sequence,
            'seconds_elapsed': secs_into_15 - C2_CLOSE,
            'up_bid'         : up_bid,
            'up_ask'         : up_ask,
            'up_bid_liq'     : up_bid_liq,
            'up_ask_liq'     : up_ask_liq,
            'up_last'        : up_last,
            'down_bid'       : down_bid,
            'down_ask'       : down_ask,
            'down_bid_liq'   : down_bid_liq,
            'down_ask_liq'   : down_ask_liq,
            'down_last'      : down_last,
            'candle_outcome' : 'PENDING'
        }
        rows.append(row)
        print(f'  [{secs_into_15}s] '
              f'UP bid={up_bid} last={up_last} | '
              f'DOWN bid={down_bid} last={down_last}')

        time.sleep(1)

    # ── Write CSV ─────────────────────────────────────────────
    write_header = not os.path.exists(filename)
    with open(filename, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    print(f'Saved {len(rows)} rows → {filename}')

    # ── Save pending ──────────────────────────────────────────
    pending = {
        'boundary_15': boundary_15,
        'filename':    filename,
        'sequence':    sequence
    }
    with open('data/pending_outcome.json', 'w') as f:
        json.dump(pending, f)

    git_commit(f'Add {sequence} data {window_str}')

    # ── Wait for 15-min close then read outcome ───────────────
    secs_now = int(time.time()) - boundary_15
    if secs_now < CANDLE_END:
        wait = CANDLE_END - secs_now + 5
        print(f'Waiting {wait}s for 15-min candle to close...')
        time.sleep(wait)

    print('Reading 15-min outcome...')
    outcome_price = get_yes_price(up_id)
    outcome       = price_to_direction(outcome_price, '15m')
    print(f'Outcome: {outcome} (UP price: {outcome_price})')

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

        update_pending_outcome()

        logged = process_one_window()
        if logged:
            window_count += 1
            print(f'Total logged this session: {window_count}')

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
