import os

import seaborn as sns
import pandas as pd
from pathlib import Path
import argparse
import matplotlib.pyplot as plt

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

    # TEMP
    submissions.to_csv(Path(__file__).parent/'data/submissions.csv',index=False,sep=';')
    comments.to_csv(Path(__file__).parent/'data/comments.csv',index=False,sep=';')


    if f'{args.ticker}.feather' in os.listdir(Path(__file__).parent/f'stock_prices') :
        prices = pd.read_feather(Path(__file__).parent / f'stock_prices/{args.ticker}.feather')
    else:
        prices = prices_for_ticker(ticker = args.ticker,interval = '1min',adjusted=False,mode='extended_intraday')
        prices.to_feather(Path(__file__).parent / f'stock_prices/{args.ticker}.feather')

    submissions['created_utc'] = pd.to_datetime(submissions['created_utc'])
    comments['created_utc'] = pd.to_datetime(comments['created_utc'])

    print("done")
    prices = prices.loc[prices['time'].between(submissions['created_utc'].min(),submissions['created_utc'].max())]
    submissions = submissions.loc[submissions['created_utc'].between(prices['time'].min(),prices['time'].max())]

    # Order dataframes
    submissions = submissions.sort_values('created_utc',ascending=True)
    comments = comments.sort_values('created_utc',ascending=True)

    submissions['daily_mentions_cumsum'] = submissions.groupby('day')['focus_ticker'].cumsum()
    comments['daily_mentions_cumsum'] = comments.groupby('day')['focus_ticker'].cumsum()

    rolling_sum = submissions.set_index('created_utc') \
        .rolling('15D', closed='left') \
        .apply(lambda x: x.loc[x.index.dayofyear == x.index[-1].dayofyear, 'focus_ticker'].sum(), raw=False) \
        .reset_index(drop=True)


    fig, ax = plt.subplots()
    sns.lineplot(data=prices, x='time', y='close', ax=ax, color='royalblue')
    ax2 = ax.twinx()
    ax3 = ax.twinx()
    sns.lineplot(data=submissions.loc[lambda x: x['focus_ticker']!=0], x='created_utc', y='daily_mentions_cumsum', ax=ax2,  color='orange')
    sns.lineplot(data=comments.loc[lambda x: x['focus_ticker']!=0], x='created_utc', y='daily_mentions_cumsum', ax=ax3,  color='green')



    # TODO :
    #  - Compute  a cumulated sum of # ticker PER DAY
    #  - Plot it against price
    #  - Fame over last week
    #  - Popularity over last hour (volume et ratio against diversity)
    #  - Compute metrics on dynamic (ratio against other tickers)
    #  - Compute ration nb subs with ticker et nb comments with ticker
    #     - C'est la fin d'un trend quand les deux sont égaux ??
    #     - Nb commes du sub !
    #     - Faire des moyennes pondérées par volum de com ??
