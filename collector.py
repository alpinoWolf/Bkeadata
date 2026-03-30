"""
Polymarket BTC 15-min Data Collector
Continuous loop — watches clock itself, no external scheduler needed.
Every 15-min window: checks C1+C2, logs if GG/RR, records outcome.
Runs for up to 6 hours then exits cleanly.
Commits data after every logged candle.
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

# ── Polymarket API endpoints ──────────────────────────────────
GAMMA_API = 'https://gamma-api.polymarket.com/markets'
CLOB_API  = 'https://clob.polymarket.com'

# ── Timing constants (seconds within 15-min window) ───────────
FIVE_MIN   = 300   # 5 minutes
FIFT_MIN   = 900   # 15 minutes
C1_CLOSE   = 300   # C1 closes at 5:00
C2_CLOSE   = 600   # C2 closes at 10:00
CANDLE_END = 900   # 15-min candle closes at 15:00
LOG_END    = 840   # stop logging at 14:00

# ── Direction thresholds ──────────────────────────────────────
UP_THRESHOLD   = 0.75
DOWN_THRESHOLD = 0.25

# ── How long to run total (6 hours max for GitHub) ────────────
MAX_RUN_SECONDS = 6 * 60 * 60
START_TIME      = int(time.time())


def floor_to_boundary(ts, boundary):
    return ts - (ts % boundary)


def seconds_into_current_window():
    now = int(time.time())
    boundary = floor_to_boundary(now, FIFT_MIN)
    return now - boundary, boundary


def wait_until(target_seconds, boundary_15):
    """
    Wait until we reach target_seconds into the 15-min window.
    Checks every 0.5s so we don't overshoot.
    """
    while True:
        now  = int(time.time())
        secs = now - boundary_15
        if secs >= target_seconds:
            return secs
        time.sleep(0.5)


def get_token_ids_for_market(ts_start, interval='5m'):
    """
    Get YES and NO token IDs.
    clobTokenIds comes as stringified list — use json.loads.
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
    Read YES price from order book midpoint.
    Falls back to last trade price if order book is empty.
    Near 1.0 = UP won. Near 0.0 = DOWN won.
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
                print(f'  Last trade price: {price}')
                return float(price)

        return None

    except Exception as e:
        print(f'  Error reading YES price: {e}')
        return None


def price_to_direction(yes_price, label=''):
    """Convert YES price to UP, DOWN, or None."""
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
        book    = resp.json()
        bids    = book.get('bids', [])
        asks    = book.get('asks', [])
        best_bid = float(bids[0]['price']) if bids else 0.0
        best_ask = float(asks[0]['price']) if asks else 0.0
        bid_liq  = sum(float(b['price'])*float(b['size']) for b in bids[:5])
        ask_liq  = sum(float(a['price'])*float(a['size']) for a in asks[:5])
        spread   = round(best_ask - best_bid, 4) if best_bid and best_ask else 0
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


def git_commit(message):
    """Commit and push data files to repo."""
    try:
        subprocess.run(['git', 'add', 'data/'], check=True)
        result = subprocess.run(
            ['git', 'diff', '--staged', '--quiet'],
            capture_output=True
        )
        if result.returncode != 0:
            subprocess.run(
                ['git', 'commit', '-m', message],
                check=True
            )
            subprocess.run(['git', 'push'], check=True)
            print(f'✅ Committed: {message}')
        else:
            print('  No new data to commit.')
    except Exception as e:
        print(f'  Git commit error: {e}')


def update_pending_outcome():
    """
    Check for pending outcome from previous candle.
    If candle has now closed — read YES price and update CSV.
    Called at the start of every 15-min cycle.
    """
    pending_file = 'data/pending_outcome.json'
    if not os.path.exists(pending_file):
        return

    print('Found pending outcome from previous candle. Updating...')
    try:
        with open(pending_file) as f:
            pending = json.load(f)

        prev_boundary = pending['boundary_15']
        prev_filename = pending['filename']

        now = int(time.time())
        if now < prev_boundary + CANDLE_END + 10:
            print('Previous candle not yet fully closed. Will retry.')
            return

        prev_yes_id, _ = get_token_ids_for_market(prev_boundary, '15m')
        if not prev_yes_id:
            print('Cannot get token ID for previous candle.')
            return

        prev_price   = get_yes_price(prev_yes_id)
        prev_outcome = price_to_direction(prev_price, 'prev 15m candle')

        if prev_outcome is None:
            print('Previous outcome still unclear. Will retry.')
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
        print(f'✅ Previous candle outcome: {prev_outcome} '
              f'(YES price: {prev_price})')
        print(f'   Updated {len(rows_updated)} rows in {prev_filename}')

        git_commit(f'Update outcome {prev_outcome} for {prev_filename}')

    except Exception as e:
        print(f'Error updating pending outcome: {e}')


def process_one_window():
    """
    Handle one complete 15-min window.
    Waits for right moments, checks C1+C2, logs if GG/RR.
    Returns True if we logged data, False otherwise.
    """
    secs, boundary_15 = seconds_into_current_window()

    c1_start = boundary_15
    c2_start = boundary_15 + FIVE_MIN

    now_et = datetime.fromtimestamp(boundary_15, tz=ET).strftime('%H:%M:%S ET')
    print(f'\n{"="*55}')
    print(f'15-min window: {now_et} | '
          f'Currently at {secs}s into window')
    print(f'{"="*55}')

    # ── Step 1: Wait for C1 to close at 5:00 ─────────────────
    if secs < C1_CLOSE:
        print(f'Waiting {C1_CLOSE - secs}s for C1 to close...')
        wait_until(C1_CLOSE, boundary_15)
        time.sleep(2)  # tiny buffer after close

    # ── Step 2: Read C1 price ─────────────────────────────────
    print('Reading C1 price...')
    c1_yes_id, _ = get_token_ids_for_market(c1_start, '5m')
    if not c1_yes_id:
        print('Cannot get C1 token ID. Skipping window.')
        return False

    c1_price  = get_yes_price(c1_yes_id)
    c1_result = price_to_direction(c1_price, 'C1')
    if c1_result is None:
        print('C1 unclear. Skipping window.')
        return False

    # ── Step 3: Wait for C2 to close at 10:00 ────────────────
    secs = int(time.time()) - boundary_15
    if secs < C2_CLOSE:
        print(f'Waiting {C2_CLOSE - secs}s for C2 to close...')
        wait_until(C2_CLOSE, boundary_15)
        time.sleep(2)

    # ── Step 4: Read C2 price ─────────────────────────────────
    print('Reading C2 price...')
    c2_yes_id, _ = get_token_ids_for_market(c2_start, '5m')
    if not c2_yes_id:
        print('Cannot get C2 token ID. Skipping window.')
        return False

    c2_price  = get_yes_price(c2_yes_id)
    c2_result = price_to_direction(c2_price, 'C2')
    if c2_result is None:
        print('C2 unclear. Skipping window.')
        return False

    # ── Step 5: Determine sequence ────────────────────────────
    if   c1_result == 'UP'   and c2_result == 'UP':
        sequence = 'GG'
    elif c1_result == 'DOWN' and c2_result == 'DOWN':
        sequence = 'RR'
    else:
        print(f'Mixed ({c1_result},{c2_result}). Skipping window.')
        return False

    print(f'✅ {sequence} confirmed! Starting data collection...')

    # ── Step 6: Get 15-min token IDs ─────────────────────────
    yes_id, no_id = get_token_ids_for_market(boundary_15, '15m')
    if not yes_id or not no_id:
        print('Cannot get 15-min token IDs. Skipping.')
        return False

    # ── Step 7: Prepare CSV ───────────────────────────────────
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

    # ── Step 8: Log every second until 14:00 ─────────────────
    print(f'Logging from now until {LOG_END}s...')

    while True:
        loop_now     = int(time.time())
        secs_into_15 = loop_now - boundary_15

        if secs_into_15 > LOG_END:
            print(f'End of logging at {secs_into_15}s. Stopping.')
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
            'candle_outcome'   : 'PENDING'
        }
        rows.append(row)
        print(f'  [{secs_into_15}s] '
              f'YES={yes_book.get("best_ask","")} '
              f'NO={no_book.get("best_ask","")}')
        time.sleep(1)

    # ── Step 9: Write CSV with PENDING outcome ────────────────
    write_header = not os.path.exists(filename)
    with open(filename, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    print(f'✅ Saved {len(rows)} rows → {filename}')

    # ── Step 10: Save pending file ────────────────────────────
    pending = {
        'boundary_15': boundary_15,
        'filename':    filename,
        'sequence':    sequence
    }
    with open('data/pending_outcome.json', 'w') as f:
        json.dump(pending, f)

    # ── Step 11: Commit data ──────────────────────────────────
    git_commit(f'Add {sequence} data {window_str} PENDING outcome')

    # ── Step 12: Wait for 15-min candle to close ─────────────
    secs_now = int(time.time()) - boundary_15
    if secs_now < CANDLE_END:
        wait_secs = CANDLE_END - secs_now + 5
        print(f'Waiting {wait_secs}s for 15-min candle to close...')
        time.sleep(wait_secs)

    # ── Step 13: Read 15-min outcome from YES price ───────────
    print('Reading 15-min candle outcome...')
    outcome_price = get_yes_price(yes_id)
    outcome       = price_to_direction(outcome_price, '15m candle')
    print(f'15m outcome: {outcome} (YES price: {outcome_price})')

    if outcome:
        # Update all rows with outcome
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

        # Remove pending file since outcome is now known
        if os.path.exists('data/pending_outcome.json'):
            os.remove('data/pending_outcome.json')

        git_commit(f'Update outcome {outcome} for {window_str}')
        print(f'✅ Outcome {outcome} saved and committed.')
    else:
        print('Outcome unclear — will update next cycle via pending.')

    return True


def main():
    """
    Main continuous loop.
    Runs until 6-hour GitHub Actions limit approaches.
    """
    print(f'Collector started at '
          f'{datetime.fromtimestamp(START_TIME, tz=ET).strftime("%Y-%m-%d %H:%M:%S ET")}')
    print(f'Will run for up to {MAX_RUN_SECONDS // 3600} hours.')

    window_count = 0

    while True:
        # ── Stop before GitHub kills us ───────────────────────
        elapsed = int(time.time()) - START_TIME
        if elapsed > MAX_RUN_SECONDS - 120:
            print(f'Approaching 6-hour limit. Stopping cleanly.')
            break

        # ── Update any pending outcome from last cycle ────────
        update_pending_outcome()

        # ── Process current 15-min window ────────────────────
        logged = process_one_window()
        if logged:
            window_count += 1
            print(f'Total windows logged this session: {window_count}')

        # ── Wait for the next 15-min window to start ─────────
        secs, boundary_15 = seconds_into_current_window()
        next_boundary     = boundary_15 + FIFT_MIN
        wait_for_next     = next_boundary - int(time.time())

        if wait_for_next > 0:
            print(f'Current window done. '
                  f'Next window in {wait_for_next}s. Waiting...')
            time.sleep(wait_for_next)
        else:
            # Already in next window — loop immediately
            time.sleep(1)


if __name__ == '__main__':
    main()
