import os
import math
import json
import time
import gzip
import io
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from web3 import Web3

START = datetime(2025, 7, 1, 0, 0, 0, tzinfo=timezone.utc)
END = datetime(2025, 9, 30, 23, 59, 59, tzinfo=timezone.utc)
BAND_LOW = 0.999
BAND_HIGH = 1.001
POOL = '0x3416cf6c708da44db2624d63ea0aaef7113527c6'
# Graph removed: using RPC-only for Uniswap data
POOL_TOKENS = {
    '0x3416cf6c708da44db2624d63ea0aaef7113527c6': ('USDC','USDT')
}
BYBIT_BASE = 'https://public.bybit.com/'
BYBIT_SPOT_TRADES_ROOT = 'https://public.bybit.com/spot/public_trading/USDCUSDT/'
OUT_CSV = os.path.join('reports', 'DEX-CEX', 'usdc_peg_dex_cex.csv')
DATA_DIR = os.path.join('data', 'bybit_spot')
os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)










# RPC fallback config for Uniswap v3
ETH_RPC_URL = (os.getenv('ETH_RPC_URL') or '').strip().strip('`')
# USDC/USDT pool specifics for decimals and token order
POOL_DECIMALS = {
    '0x3416cf6c708da44db2624d63ea0aaef7113527c6': (6, 6)  # token0 USDC, token1 USDT
}
POOL_TOKEN_ORDER = {
    '0x3416cf6c708da44db2624d63ea0aaef7113527c6': ('USDC','USDT')
}
SWAP_EVENT_ABI = [{
    'anonymous': False,
    'inputs': [
        {'indexed': True, 'internalType': 'address', 'name': 'sender', 'type': 'address'},
        {'indexed': True, 'internalType': 'address', 'name': 'recipient', 'type': 'address'},
        {'indexed': False, 'internalType': 'int256', 'name': 'amount0', 'type': 'int256'},
        {'indexed': False, 'internalType': 'int256', 'name': 'amount1', 'type': 'int256'},
        {'indexed': False, 'internalType': 'uint160', 'name': 'sqrtPriceX96', 'type': 'uint160'},
        {'indexed': False, 'internalType': 'uint128', 'name': 'liquidity', 'type': 'uint128'},
        {'indexed': False, 'internalType': 'int24', 'name': 'tick', 'type': 'int24'},
    ],
    'name': 'Swap',
    'type': 'event'
}]

def _find_block_by_timestamp(web3, target_ts):
    # Get latest block; if target is in future, clamp to latest
    latest = web3.eth.get_block('latest')
    if target_ts >= latest.timestamp:
        return latest.number
    # Estimate block number by avg 12s/block
    avg_block_time = 12.0
    est = int(latest.number - max(0, (latest.timestamp - target_ts) / avg_block_time))
    # Search window around estimate to reduce provider pressure
    margin = 300000
    lo = max(1, est - margin)
    hi = min(latest.number, est + margin)
    res = latest.number
    # Safe getter with lightweight retries
    def _get_block(num):
        try:
            return web3.eth.get_block(num)
        except Exception:
            try:
                return web3.eth.get_block(num)
            except Exception:
                return None
    while lo <= hi:
        mid = (lo + hi) // 2
        blk = _get_block(mid)
        if blk is None:
            # Skip problematic midpoint
            hi = mid - 1
            continue
        ts = blk.timestamp
        if ts >= target_ts:
            res = mid
            hi = mid - 1
        else:
            lo = mid + 1
    return res


def uniswap_hourly_outside_band_rpc(pool_id, start_dt, end_dt):
    if not ETH_RPC_URL:
        raise RuntimeError('ETH_RPC_URL not set for RPC fallback')
    web3 = Web3(Web3.HTTPProvider(ETH_RPC_URL))
    connected_fn = getattr(web3, 'is_connected', None)
    ok = connected_fn() if callable(connected_fn) else web3.isConnected()
    if not ok:
        raise RuntimeError('Web3 cannot connect to ETH_RPC_URL')
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    from_block = _find_block_by_timestamp(web3, start_ts)
    to_block = _find_block_by_timestamp(web3, end_ts)
    pool = Web3.to_checksum_address(pool_id)
    swap_topic = web3.keccak(text='Swap(address,address,int256,int256,uint160,uint128,int24)').hex()
    # Dynamic step with backoff to mitigate provider internal errors
    step = int(os.getenv('ETH_LOGS_STEP', '1000'))
    min_step = 64
    step = 1000
    backoff_sleep = 0.25
    sleep_between_chunks = float(os.getenv('ETH_LOGS_SLEEP', '0.05'))
    rows = []
    dec0, dec1 = POOL_DECIMALS.get(pool_id.lower(), (6, 6))
    token_order = POOL_TOKEN_ORDER.get(pool_id.lower(), ('USDC','USDT'))
    from eth_abi import decode
    blk = from_block
    while blk <= to_block:
        end_blk = min(blk + step - 1, to_block)
        try:
            logs = web3.eth.get_logs({
                'address': pool,
                'fromBlock': blk,
                'toBlock': end_blk,
                'topics': [swap_topic]
            })
        except Exception as e:
            msg = str(e)
            # Handle rate limiting explicitly
            if '429' in msg or 'Too Many Requests' in msg:
                backoff_sleep = min(backoff_sleep * 2, 2.0)
                time.sleep(backoff_sleep)
            # Reduce range and retry same start block
            if step > min_step:
                step = max(min_step, step // 2)
                time.sleep(backoff_sleep)
                continue
            else:
                # If already at min_step, advance to avoid infinite loop
                blk = end_blk + 1
                time.sleep(backoff_sleep)
                continue
        # Proactively shrink step if result set is very large
        if isinstance(logs, list) and len(logs) >= 10000:
            step = max(min_step, step // 2)
        for log in logs:
            try:
                data_hex = log['data']
                data_bytes = data_hex if isinstance(data_hex, bytes) else bytes.fromhex(data_hex[2:])
                amount0, amount1, sqrtPriceX96, _liquidity, _tick = decode(['int256','int256','uint160','uint128','int24'], data_bytes)
            except Exception:
                continue
            blk_obj = web3.eth.get_block(log['blockNumber'])
            dt = datetime.fromtimestamp(blk_obj.timestamp, tz=timezone.utc)
            hour = dt.replace(minute=0, second=0, microsecond=0)
            price = (sqrtPriceX96 / (2**96))**2 * (10**(dec1 - dec0))
            if token_order[0].upper() == 'USDC':
                usdc_vol = abs(amount0) / (10**dec0)
            elif token_order[1].upper() == 'USDC':
                usdc_vol = abs(amount1) / (10**dec1)
            else:
                usdc_vol = float('nan')
            outside = (price < BAND_LOW) or (price > BAND_HIGH)
            rows.append({'hour': hour, 'price': price, 'usdc_vol': usdc_vol, 'outside': outside})
        # Polite sleep between chunks to avoid rate limits
        time.sleep(sleep_between_chunks)
        blk = end_blk + 1
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=['time','uniswap_volume','uniswap_min_price','uniswap_max_price'])
    g = df[df['outside']].groupby('hour').agg(
        uniswap_volume=('usdc_vol','sum'),
        uniswap_min_price=('price','min'),
        uniswap_max_price=('price','max')
    ).reset_index().rename(columns={'hour':'time'})
    return g


def list_public_dirs(base_url=BYBIT_BASE):
    r = requests.get(base_url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')
    links = [a.get('href') for a in soup.find_all('a') if a.get('href')]
    return links


def find_spot_trade_root():
    q = list_public_dirs(BYBIT_BASE)
    candidates = []
    for href in q:
        h = href.lower()
        if 'spot' in h:
            candidates.append(href)
    for c in candidates:
        url = BYBIT_BASE + c
        l2 = list_public_dirs(url)
        for h2 in l2:
            if ('trade' in h2.lower()) or ('trading' in h2.lower()):
                return url + h2
    return BYBIT_BASE + candidates[0] if candidates else None


def list_symbol_files(root_url, symbol='USDCUSDT'):
    # Prefer explicit spot trades root if available
    try:
        r = requests.get(BYBIT_SPOT_TRADES_ROOT, timeout=30)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            links = [a.get('href') for a in soup.find_all('a') if a.get('href')]
            files = []
            for fn in links:
                if not fn.endswith('.csv.gz'):
                    continue
                name = fn.lower()
                # match common patterns
                # USDCUSDT-YYYY-MM-DD.csv.gz or USDCUSDT_trades_YYYY-MM-DD.csv.gz
                parts = name.replace('-', '_').split('_')
                dt_candidate = None
                for i in range(len(parts)):
                    for fmt in ('%Y-%m-%d', '%Y%m%d'):
                        try:
                            dt_candidate = datetime.strptime(parts[i][:10], fmt).replace(tzinfo=timezone.utc)
                            break
                        except Exception:
                            pass
                    if dt_candidate:
                        break
                if dt_candidate and START <= dt_candidate <= END:
                    files.append(BYBIT_SPOT_TRADES_ROOT + fn)
            return sorted(set(files))
    except Exception:
        pass
    # Fallback to previous discovery logic
    r = requests.get(root_url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')
    links = [a.get('href') for a in soup.find_all('a') if a.get('href')]
    sym_dirs = [h for h in links if symbol in h]
    files = []
    for d in sym_dirs:
        url = root_url + d
        r2 = requests.get(url, timeout=30)
        r2.raise_for_status()
        soup2 = BeautifulSoup(r2.text, 'html.parser')
        f2 = [a.get('href') for a in soup2.find_all('a') if a.get('href') and a.get('href').endswith('.csv.gz')]
        for fn in f2:
            for fmt in ('%Y-%m-%d', '%Y%m%d'):
                for part in fn.split('_'):
                    try:
                        dt = datetime.strptime(part[:10], fmt).replace(tzinfo=timezone.utc)
                        if START <= dt <= END:
                            files.append(url + fn)
                        break
                    except Exception:
                        pass
    return sorted(set(files))


def download_and_parse_gz(url):
    name = url.split('/')[-1]
    local_gz = os.path.join(DATA_DIR, name)
    if not os.path.exists(local_gz):
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(local_gz, 'wb') as f:
            f.write(r.content)
    with gzip.open(local_gz, 'rb') as f:
        content = f.read()
    df = pd.read_csv(io.BytesIO(content))
    return df


def bybit_hourly_outside_band():
    # Try explicit spot trades root first
    files = []
    try:
        files = list_symbol_files(BYBIT_BASE, 'USDCUSDT')
    except Exception as e:
        print('Bybit list files failed:', e)
        files = []
    rows = []
    for u in files:
        try:
            df = download_and_parse_gz(u)
            cols = {c.lower(): c for c in df.columns}
            price_col = cols.get('price')
            size_col = cols.get('size') or cols.get('qty') or cols.get('quantity')
            symbol_col = cols.get('symbol')
            time_col = cols.get('time') or cols.get('timestamp')
            if not (price_col and size_col and time_col):
                continue
            ts_series = pd.to_numeric(df[time_col], errors='coerce')
            ts_sec = ts_series.apply(lambda x: x/1000.0 if x and x > 1e12 else x)
            dt = ts_sec.apply(lambda x: datetime.fromtimestamp(x, tz=timezone.utc))
            df['_hour'] = dt.apply(lambda d: d.replace(minute=0, second=0, microsecond=0))
            df['_price'] = pd.to_numeric(df[price_col], errors='coerce')
            df['_size'] = pd.to_numeric(df[size_col], errors='coerce')
            outside = df[(df['_price'] < BAND_LOW) | (df['_price'] > BAND_HIGH)]
            g = outside.groupby('_hour').agg(
                bybit_volume=('_size','sum'),
                bybit_min_price=('_price','min'),
                bybit_max_price=('_price','max')
            ).reset_index().rename(columns={'_hour':'time'})
            rows.append(g)
        except Exception as e:
            print('Error parsing', u, e)
    if rows:
        out = pd.concat(rows).groupby('time').agg(
            bybit_volume=('bybit_volume','sum'),
            bybit_min_price=('bybit_min_price','min'),
            bybit_max_price=('bybit_max_price','max'),
        ).reset_index()
        return out
    # Fallback: approximate with minute klines if no trade archives
    try:
        def fetch_kline(start_dt, end_dt):
            res_rows = []
            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)
            cursor = start_ms
            while cursor < end_ms:
                params = {
                    'category': 'spot',
                    'symbol': 'USDCUSDT',
                    'interval': '1',
                    'start': cursor,
                    'end': min(cursor + 1000*60*1000, end_ms),  # up to ~1000 minutes
                    'limit': 1000
                }
                # Fix URL: remove stray backticks and spaces
                r = requests.get('https://api.bybit.com/v5/market/kline', params=params, timeout=30)
                r.raise_for_status()
                resp = r.json()
                # Handle Bybit API-level errors (HTTP 200 but non-zero retCode)
                ret_code = resp.get('retCode')
                if ret_code and ret_code != 0:
                    print('Bybit kline error:', ret_code, resp.get('retMsg'))
                    break
                data = resp.get('result', {}).get('list', [])
                if not data:
                    break
                for row in data:
                    # row format: [start, open, high, low, close, volume, turnover]
                    ts = int(row[0])//1000
                    dtm = datetime.fromtimestamp(ts, tz=timezone.utc)
                    price_high = float(row[2])
                    price_low = float(row[3])
                    vol = float(row[5])
                    outside = (price_high > BAND_HIGH) or (price_low < BAND_LOW)
                    if outside:
                        hour = dtm.replace(minute=0, second=0, microsecond=0)
                        res_rows.append({'time': hour, 'bybit_volume': vol, 'bybit_min_price': price_low, 'bybit_max_price': price_high})
                cursor = int(data[-1][0]) + 60*1000
            if not res_rows:
                return pd.DataFrame(columns=['time','bybit_volume','bybit_min_price','bybit_max_price'])
            return pd.DataFrame(res_rows).groupby('time').agg(
                bybit_volume=('bybit_volume','sum'),
                bybit_min_price=('bybit_min_price','min'),
                bybit_max_price=('bybit_max_price','max')
            ).reset_index()
        return fetch_kline(START, END)
    except Exception as e:
        print('Bybit kline fallback failed:', e)
        return pd.DataFrame(columns=['time','bybit_volume','bybit_min_price','bybit_max_price'])


def uniswap_hourly_outside_band(pool_id, start_dt, end_dt):
    # RPC-only path for Uniswap v3 swaps
    return uniswap_hourly_outside_band_rpc(pool_id, start_dt, end_dt)


def main():
    # Allow overriding date range via environment variables (ISO date YYYY-MM-DD)
    s_env = os.getenv('START_DATE')
    e_env = os.getenv('END_DATE')
    if s_env and e_env:
        try:
            s_dt = datetime.strptime(s_env + ' 00:00:00+00:00', '%Y-%m-%d %H:%M:%S%z').astimezone(timezone.utc)
            e_dt = datetime.strptime(e_env + ' 23:59:59+00:00', '%Y-%m-%d %H:%M:%S%z').astimezone(timezone.utc)
            global START, END
            START, END = s_dt, e_dt
            print('Overriding date range:', START, 'to', END)
        except Exception as e:
            print('Env date parse failed:', e)
    # Optional peg band override in percent, e.g. PEG_BAND_PCT=0.5 for ±0.5%
    band_env = os.getenv('PEG_BAND_PCT')
    if band_env:
        try:
            pct = float(band_env)/100.0
            global BAND_LOW, BAND_HIGH
            BAND_LOW = 1.0 - pct
            BAND_HIGH = 1.0 + pct
            print(f'Overriding peg band to ±{band_env}% -> low={BAND_LOW}, high={BAND_HIGH}')
        except Exception as e:
            print('Env band parse failed:', e)
    try:
        dex_df = uniswap_hourly_outside_band(POOL, START, END)
    except Exception as e:
        print('Uniswap fetch failed:', e)
        dex_df = pd.DataFrame(columns=['time','uniswap_volume','uniswap_min_price','uniswap_max_price'])
    try:
        cex_df = bybit_hourly_outside_band()
    except Exception as e:
        print('Bybit fetch failed:', e)
        cex_df = pd.DataFrame(columns=['time','bybit_volume','bybit_min_price','bybit_max_price'])
    all_hours = pd.DataFrame({'time': pd.date_range(START.replace(minute=0, second=0, microsecond=0), END.replace(minute=0, second=0, microsecond=0), freq='H', tz=timezone.utc)})
    res = all_hours.merge(dex_df, on='time', how='left').merge(cex_df, on='time', how='left')
    for c in ['uniswap_volume','bybit_volume']:
        if c in res.columns:
            res[c] = res[c].fillna(0.0)
    res[['time','uniswap_volume','bybit_volume','uniswap_min_price','uniswap_max_price','bybit_min_price','bybit_max_price']].to_csv(OUT_CSV, index=False)
    print('Saved to', OUT_CSV)


if __name__ == '__main__':
    main()