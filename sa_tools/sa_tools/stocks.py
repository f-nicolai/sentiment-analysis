import pytz
from time import sleep
from pandas import DataFrame, concat, read_csv, to_datetime
from alpha_vantage.timeseries import TimeSeries
from itertools import product

from sa_tools.api_secrets import ALPHA_VANTAGE_API_KEY


def prices_for_ticker(ticker: str, interval: str, adjusted: bool, mode: str) -> DataFrame:
    print(f'Retrieving prices for {ticker} ..')
    if mode == 'extended_intraday':
        prices = DataFrame()
        for slice in product(['year1', 'year2'], [f'month{i}' for i in range(1, 13)]):
            print(f"\tslice: {''.join(slice)}")
            prices = concat([
                prices,
                request_alpha_vantage_with_retry(
                    ticker=ticker,
                    interval=interval,
                    adjusted=adjusted,
                    slice=''.join(slice))
            ])

        prices['time'] = to_datetime(prices['time']).dt.tz_localize(pytz.timezone('US/Eastern')).dt.tz_convert(pytz.utc)

    elif mode == 'intraday':
        ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
        prices, _ = ts.get_intraday_extended(symbol=ticker, interval=interval, outputsize='full', adjusted=adjusted)

    else:
        raise NotImplementedError

    return prices.reset_index(drop=True)


def request_alpha_vantage_with_retry(ticker: str, interval: str, adjusted: bool, slice: str) -> DataFrame:
    tmp = read_csv(
        f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={ticker}&adjusted={adjusted}&interval={interval}&slice={slice}&apikey={ALPHA_VANTAGE_API_KEY}")

    if 'time' not in tmp.columns:
        print("Waiting for ALPHA VANTAGE API 1mn limit")
        sleep(60)
        return request_alpha_vantage_with_retry(ticker=ticker, interval=interval, adjusted=adjusted, slice=slice)

    return tmp
