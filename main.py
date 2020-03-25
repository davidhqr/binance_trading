import datetime

import pandas as pd
import pandas_ta as ta
from binance.client import Client
from binance.enums import *
from binance.websockets import BinanceSocketManager

MAIN_SYMBOL = 'BTC'
FOREIGN_SYMBOL = 'XTZ'
TICKER = 'XTZBTC'
public_key = 'NIVq1rngxerf1OpjY3CJsMCyM580ylkDbe0W833nWiSl3azstCCCB6v9orQMHd3v'
secret_key = 'MOjRytV4EPCImVp9uRZhoN1cTVA12iETbKUxx92JnoMFFRce97tAdAd2yeAginqc'

df = None
trade_executed = False
trade_enter_price = 0
trade_amount = 0


def get_asset_balance(asset):
    account = client.get_margin_account()
    assets = account['userAssets']
    result = list(filter(lambda x: x['asset'] == asset, assets))
    if len(result) == 1:
        return float(result[0]['free'])
    else:
        return None


def process_message(msg):
    global df, trade_executed, trade_enter_price, trade_amount
    candle = msg['k']
    is_final = candle['x']

    if is_final:
        # Append candle to dataframe
        open_time = candle['t']
        open_ = float(candle['o'])
        high = float(candle['h'])
        low = float(candle['l'])
        close = float(candle['c'])
        close_time = candle['T']
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

        df.ta.adx('ha_high', 'ha_low', 'ha_close', length=14, append=True)
        df.ta.ao('ha_high', 'ha_low', append=True)
        df.ta.sma('AO_5_34', 5, append=True)
        df['AC'] = df['AO_5_34'] - df['SMA_5']

        # Execute Strategy
        plus = df['DMP_14'].iat[-1]
        prev_plus = df['DMP_14'].iat[-2]
        ac = df['AC'].iat[-1]
        ac_change = ac - df['AC'].iat[-2]
        open_time_formatted = datetime.datetime.fromtimestamp(int(open_time) / 1000)
        print('[%s] %s | Close: %0.8f | +DI: %0.8f | AC: %0.8f' % (open_time_formatted, TICKER, close, plus, ac))

        buy = ((plus < 10 or prev_plus < 10) and ac < 0 and ac_change > 0) and not trade_executed
        sell = (((plus > 20 or prev_plus > 20) and ac > 0 and ac_change <= 0) or (
                close < trade_enter_price * 0.995)) and trade_executed

        if buy:
            trade_amount = round((get_asset_balance(MAIN_SYMBOL) * 0.98) / close, 2)
            print('[Alert] Buy %s of %s at price %0.8f' % (trade_amount, TICKER, close))
            order = client.create_margin_order(
                symbol=TICKER,
                side=SIDE_BUY,
                type=ORDER_TYPE_MARKET,
                quantity=50
            )
            if order['status'] == "FILLED":
                print('[Order] Bought %s of %s at %0.8f' % (trade_amount, TICKER, close))
                trade_executed = True
                trade_enter_price = close
            else:
                print('[ERROR] Order to buy %s of %s at %0.8f was not filled' % (trade_amount, TICKER, close))

        if sell:
            profit_pct = (close - trade_enter_price) / trade_enter_price * 100
            sell_amount = get_asset_balance(FOREIGN_SYMBOL) // 0.01 * 0.01  # round down to 2 decimals
            formatted_sell_amount = "{:0.0{}f}".format(sell_amount, 2)
            print('[Alert] Sell %s of %s at price %0.8f. Profit: %0.2f%%' % (
                formatted_sell_amount, TICKER, close, profit_pct))
            order = client.create_margin_order(
                symbol=TICKER,
                side=SIDE_SELL,
                type=ORDER_TYPE_MARKET,
                quantity=formatted_sell_amount
            )
            if order['status'] == "FILLED":
                print('[Order] Sold %s of %s at %0.8f' % (sell_amount, TICKER, close))
                trade_executed = False
                trade_enter_price = 0
                trade_amount = 0
            else:
                print('[ERROR] Order to sell %s of %s at %0.8f was not filled' % (trade_amount, TICKER, close))


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
client = Client(public_key, secret_key)

# Load historical candles into dataframe
historical_klines = client.get_historical_klines(TICKER, Client.KLINE_INTERVAL_5MINUTE, '1 day ago UTC')
historical_candles = map(lambda kline: kline[:7], historical_klines)
column_names = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time']
df = pd.DataFrame(data=historical_candles, columns=column_names)
df['open'] = df['open'].astype(float)
df['high'] = df['high'].astype(float)
df['low'] = df['low'].astype(float)
df['close'] = df['close'].astype(float)
df['volume'] = df['volume'].astype(float)
add_heiken_ashi()

# Start listening for live candles
bm = BinanceSocketManager(client, user_timeout=60)
conn_key = bm.start_kline_socket(TICKER, process_message, interval=KLINE_INTERVAL_5MINUTE)
bm.start()
