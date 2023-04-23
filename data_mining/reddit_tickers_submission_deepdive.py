import os

import seaborn as sns
import pandas as pd
from pathlib import Path
import argparse

from sa_tools.gcp import query_bq
from sa_tools.stocks import prices_for_ticker

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--ticker", help="ticker")

if __name__ =='__main__':
    args = parser.parse_args()

    print(f'Getting submissions for {args.ticker}')
    submissions = query_bq(
        query_or_file=Path(__file__).parent / 'sql/submissions_details_tickers.sql',
        replace={'{{ticker}}':args.ticker}
    )

    print(f'Getting comments for {args.ticker}')
    comments = query_bq(
        query_or_file=Path(__file__).parent / 'sql/comments_details_tickers.sql',
        replace={'{{ticker}}':args.ticker}
    )

    if f'{args.ticker}.feather' in os.listdir(Path(__file__).parent/f'stock_prices') :
        prices = pd.read_feather(Path(__file__).parent / f'stock_prices/{args.ticker}.feather')
    else:
        prices = prices_for_ticker(ticker = args.ticker,interval = '1min',adjusted=False,mode='extended_intraday')
        prices.to_feather(Path(__file__).parent / f'stock_prices/{args.ticker}.feather')

    prices = prices.loc[prices['time'].between(submissions['created_utc'].min(),submissions['created_utc'].max())]

    # TODO :
    #  - Compute  a cumulated sum of # ticker PER DAY
    #  - Plot it against price
    #  - Compute metrics on dynamic (ratio against other tickers)
    #  - KPI vs other days (or mean averages of this KPI)
    #  - Try with a global sum (over last month)

