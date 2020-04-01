#!/usr/bin/env python3

import datetime
import logging
import math
import os
import sys

import pandas as pd
from binance.client import Client
from binance.enums import *
from binance.websockets import BinanceSocketManager

# Add vendor directory to module search path
parent_dir = os.path.abspath(os.path.dirname(__file__))
vendor_dir = os.path.join(parent_dir, 'vendor')
sys.path.append(vendor_dir)

import pandas_ta as ta

MAIN_SYMBOL = 'BTC'
FOREIGN_SYMBOL = 'ZEC'
TICKER = 'ZECBTC'
public_key = 'NIVq1rngxerf1OpjY3CJsMCyM580ylkDbe0W833nWiSl3azstCCCB6v9orQMHd3v'
secret_key = 'MOjRytV4EPCImVp9uRZhoN1cTVA12iETbKUxx92JnoMFFRce97tAdAd2yeAginqc'

df = None

long_trade = False
long_loan = 0
long_price = 0
long_fill_price = 0

short_trade = False
short_loan = 0
short_price = 0
short_fill_price = 0

logging.basicConfig(filename='binance_trading.log', level=logging.DEBUG, format='[%(asctime)s] %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S')


def get_asset_balance(asset):
    account = client.get_margin_account()
    assets = account['userAssets']
    result = list(filter(lambda x: x['asset'] == asset, assets))
    if len(result) == 1:
        return float(result[0]['free'])
    else:
        return None


def get_order_avg_price(order):
    fills = order['fills']
    total_price = 0
    for fill in fills:
        total_price += float(fill['price'])
    return total_price / len(fills)


def process_message(msg):
    global df, long_trade, long_loan, long_price, long_fill_price, short_trade, short_loan, short_price, short_fill_price
    candle = msg['k']
    is_final = candle['x']
    open_time = datetime.datetime.fromtimestamp(int(candle['t']) / 1000)
    close_time = datetime.datetime.fromtimestamp(int(candle['T']) / 1000)

    if is_final and df['open_time'].iat[-1] != open_time:
        # Append candle to dataframe
        open_ = float(candle['o'])
        high = float(candle['h'])
        low = float(candle['l'])
        close = float(candle['c'])
        ha_close = (open_ + high + low + close) / 4
        ha_open = (df['ha_open'].iat[-1] + df['ha_close'].iat[-1]) / 2
        ha_high = max(ha_open, ha_close, high)
        ha_low = min(ha_open, ha_close, low)

        df = df.append({
            'open_time': open_time,
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'close_time': close_time,
            'ha_close': ha_close,
            'ha_open': ha_open,
            'ha_high': ha_high,
            'ha_low': ha_low
        }, ignore_index=True)

        # Add indicators
        df.ta.adx(high=df['ha_high'], low=df['ha_low'], close=df['ha_close'], length=20, append=True)
        df.ta.ao(high=df['ha_high'], low=df['ha_low'], append=True)
        df.ta.sma(close=df['AO_5_34'], length=5, append=True)
        df['AC'] = df['AO_5_34'] - df['SMA_5']
        plus = df['DMP_20'].iat[-1]
        plus_prev = df['DMP_20'].iat[-2]
        plus_change_pct = (plus - plus_prev) / abs(plus_prev) * 100
        minus = df['DMN_20'].iat[-1]
        minus_prev = df['DMN_20'].iat[-2]
        minus_change_pct = (minus - minus_prev) / abs(minus_prev) * 100
        adx = df['ADX_20'].iat[-1]
        ac = df['AC'].iat[-1]
        ac_prev = df['AC'].iat[-2]
        ac_change = ac - ac_prev
        ac_change_pct = ac_change / abs(ac_prev) * 100
        logging.info(
            '[%s] %s | Close: %0.8f | +DI: %0.8f | AC: %0.8f', open_time, TICKER, ha_close, plus, ac)

        # Strategy execution
        plus_lb = 8
        plus_ub = 24
        long_sl = 0.94

        # plus conditions
        long_cond_1 = (plus < plus_lb or plus_prev < plus_lb) and plus > 3 and plus_change_pct > 3 and adx > 20
        # ac conditions
        long_cond_2 = (ac <= 0 or (ac_prev <= 0 and ac < 0.0000005)) and ac_change_pct >= 12
        # universal conditions
        long_universal = not long_trade and not short_trade

        # plus and ac peak
        close_long_cond_1 = (plus > plus_ub or plus_prev > plus_ub) and ac > 0 and ac_change <= 0
        # stop loss
        close_long_cond_2 = ha_close < long_price * long_sl
        # universal conditions
        close_long_universal = long_trade

        long = long_cond_1 and long_cond_2 and long_universal
        close_long = (close_long_cond_1 or close_long_cond_2) and close_long_universal

        if long:
            long_loan = float(client.get_max_margin_loan(asset=MAIN_SYMBOL)['amount'])
            client.create_margin_loan(asset=MAIN_SYMBOL, amount=long_loan)
            logging.info('[Loan] Loaned %0.8f of %s', long_loan, MAIN_SYMBOL)

            trade_amount = round((get_asset_balance(MAIN_SYMBOL) * 0.99) / ha_close, 2)
            logging.info('[Alert] Long %s of %s at price %0.8f', trade_amount, TICKER, ha_close)
            order = client.create_margin_order(
                symbol=TICKER,
                side=SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=trade_amount
            )

            if order['status'] == 'FILLED':
                long_price = ha_close
                long_fill_price = get_order_avg_price(order)
                long_trade = True
                logging.info('[Order] Longed %s of %s at %0.8f', trade_amount, TICKER, long_fill_price)
            else:
                logging.info('[ERROR] Order to long %s of %s at %0.8f was not filled', trade_amount, TICKER, ha_close)

        if close_long:
            sell_amount = get_asset_balance(FOREIGN_SYMBOL) // 0.01 * 0.01  # round down to 2 decimals
            formatted_sell_amount = '{:0.0{}f}'.format(sell_amount, 2)
            logging.info('[Alert] Close long %s of %s at price %0.8f', formatted_sell_amount, TICKER, ha_close)
            order = client.create_margin_order(
                symbol=TICKER,
                side=SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=formatted_sell_amount
            )

            if order['status'] == 'FILLED':
                fill_price = get_order_avg_price(order)
                profit_pct = (fill_price - long_fill_price) / long_fill_price * 100
                logging.info('[Order] Closed long %s of %s at %0.8f. Profit: %0.2f%%', sell_amount, TICKER, fill_price,
                             profit_pct)
                client.repay_margin_loan(asset=MAIN_SYMBOL, amount=long_loan)
                logging.info('[Loan] Repaid %0.8f of %s', long_loan, MAIN_SYMBOL)
                long_trade = False
                long_loan = 0
                long_price = 0
                long_fill_price = 0
            else:
                logging.info('[ERROR] Order to close long %s of %s at %0.8f was not filled', sell_amount, TICKER,
                             ha_close)

        minus_lb = 10
        minus_ub = 23
        short_sl = 1.06

        # minus conditions
        short_cond_1 = (minus < minus_lb or minus_prev < minus_lb) and minus > 3 and minus_change_pct > 3 and adx > 20
        # ac conditions
        short_cond_2 = (ac >= 0 or (ac_prev >= 0 and ac > -0.0000005)) and ac_change_pct <= -12
        # universal conditions
        short_universal = not long_trade and not short_trade

        # minus peak and ac bottom
        close_short_cond_1 = (minus > minus_ub or minus_prev > minus_ub) and ac < 0 and ac_change >= 0
        # stop loss
        close_short_cond_2 = ha_close > short_price * short_sl
        # universal conditions
        close_short_universal = short_trade

        short = short_cond_1 and short_cond_2 and short_universal
        close_short = (close_short_cond_1 or close_short_cond_2) and close_short_universal

        if short:
            short_loan = float(client.get_max_margin_loan(asset=FOREIGN_SYMBOL)['amount'])
            client.create_margin_loan(asset=FOREIGN_SYMBOL, amount=short_loan)
            logging.info('[Loan] Loaned %0.8f %s', short_loan, FOREIGN_SYMBOL)

            trade_amount = round(get_asset_balance(FOREIGN_SYMBOL) * 0.99, 2)
            logging.info('[Alert] Short %s of %s at price %0.8f', trade_amount, TICKER, ha_close)
            order = client.create_margin_order(
                symbol=TICKER,
                side=SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=trade_amount
            )

            if order['status'] == 'FILLED':
                short_price = ha_close
                short_fill_price = get_order_avg_price(order)
                short_trade = True
                logging.info('[Order] Shorted %s of %s at %0.8f', trade_amount, TICKER, short_fill_price)
            else:
                logging.info('[ERROR] Order to short %s of %s at %0.8f was not filled', trade_amount, TICKER, ha_close)

        if close_short:
            trade_amount = math.ceil(short_loan)
            logging.info('[Alert] Close short %s of %s at price %0.8f', trade_amount, TICKER, ha_close)
            order = client.create_margin_order(
                symbol=TICKER,
                side=SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=trade_amount
            )

            if order['status'] == 'FILLED':
                fill_price = get_order_avg_price(order)
                profit_pct = (short_fill_price - fill_price) / fill_price * 100
                logging.info('[Order] Closed short %s of %s at %0.8f. Profit: %0.2f%%', trade_amount, TICKER,
                             fill_price, profit_pct)
                client.repay_margin_loan(asset=FOREIGN_SYMBOL, amount=short_loan)
                logging.info('[Loan] Repaid %0.8f of %s', short_loan, FOREIGN_SYMBOL)
                short_trade = False
                short_loan = 0
                short_price = 0
                short_fill_price = 0
            else:
                logging.info('[ERROR] Order to close short %s of %s at %0.8f was not filled', trade_amount, TICKER,
                             ha_close)


# Add heiken ashi candles to dataframe
def add_heiken_ashi():
    df['ha_close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

    for i in range(len(df)):
        if i == 0:
            df.at[0, 'ha_open'] = (df.at[0, 'open'] + df.at[0, 'close']) / 2
        else:
            df.at[i, 'ha_open'] = (df.at[i - 1, 'ha_open'] + df.at[i - 1, 'ha_close']) / 2

    df['ha_high'] = df[['ha_open', 'ha_close', 'high']].max(axis=1)
    df['ha_low'] = df[['ha_open', 'ha_close', 'low']].min(axis=1)


# Run program
logging.info('Starting Binance trading bot')
client = Client(public_key, secret_key)

# Load historical candles into dataframe
historical_klines = client.get_historical_klines(TICKER, Client.KLINE_INTERVAL_5MINUTE, '1 day ago UTC')
historical_candles = list(map(lambda kline: kline[:7], historical_klines))[:-1]
column_names = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time']
df = pd.DataFrame(data=historical_candles, columns=column_names)
df['open_time'] = df['open_time'].apply(lambda x: datetime.datetime.fromtimestamp(int(x) / 1000))
df['open'] = df['open'].astype(float)
df['high'] = df['high'].astype(float)
df['low'] = df['low'].astype(float)
df['close'] = df['close'].astype(float)
df['volume'] = df['volume'].astype(float)
df['close_time'] = df['close_time'].apply(lambda x: datetime.datetime.fromtimestamp(int(x) / 1000))
add_heiken_ashi()

# Start listening for live candles
bm = BinanceSocketManager(client, user_timeout=60)
conn_key = bm.start_kline_socket(TICKER, process_message, interval=KLINE_INTERVAL_5MINUTE)
logging.info('Starting websocket')
bm.start()
