import time, random

import datetime as dt
from pybit import usdt_perpetual
import pandas as pd

# Based on article https://quantnomad.com/getting-historical-bars-from-bybit-api-with-python/

def get_bybit_candles(symbol, interval, limit, startTime):
    """
    symbol - string.
    interval - string. Data refresh interval: 1 3 5 15 30 60 120 240 360 720 "D" "M" "W"
    from - integer. From timestamp in seconds
    limit - integer	Limit for data size, max size is 200. Default as showing 200 pieces of data.
    For avoiding your IP ban, this script is using time.sleep(). You can play with this part.
    """
    session_unauth = usdt_perpetual.HTTP(endpoint="https://api.bybit.com")  

    startTime = str(int(startTime.timestamp()))
     
    data = session_unauth.query_kline(
        symbol=symbol,
        interval=interval,
        limit=limit,
        from_time=startTime
    )

    df = pd.DataFrame(data['result'])

    if df.empty == False:
          
        df.index = [dt.datetime.fromtimestamp(x) for x in df.open_time]
        df.index.name = 'DataTime' 
        return df
    else:
        return None


df_list = []

startTime = dt.datetime(2022, 1, 1)
while True:
    print(startTime)
    new_df = get_bybit_candles(symbol='MATICUSDT', interval=5, limit=200, startTime=startTime)
    time.sleep(random.uniform(3, 5)) # I wasn't banned last time with this values

    if new_df is None:
        print('end')
        break
    df_list.append(new_df)

    # Hours must be opposite to your UTC timezone.
    # Examples:
    # for UTC+3, "hours = -3"; 
    # for UTC-5, "hours = 5"
    startTime = max(new_df.index) + dt.timedelta(hours = -3, minutes = 5)

 
df = pd.concat(df_list)

df.to_excel("MATICUSDT.xlsx") 
df.to_csv('MATICUSDT.csv')







