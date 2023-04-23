import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

from sa_tools.gcp import query_bq

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

submissions_stats = query_bq(query_or_file=Path(__file__).parent / 'sql/submissions_stats_on_tickers.sql')
submissions_stats['day'] = pd.to_datetime(submissions_stats['day'])

submissions_stats = submissions_stats.apply(pd.Series).explode('top_5_most_frequent_ticker').reset_index(drop=True)
submissions_stats = pd.concat([
    submissions_stats.drop('top_5_most_frequent_ticker', axis=1),
    pd.json_normalize(submissions_stats['top_5_most_frequent_ticker']).rename({'value': 'ticker'}, axis=1)],
    axis=1)

###### PLOTS
fig, ax = plt.subplots()
sns.lineplot(data=submissions_stats[['day', 'ratio_distinct_total_tickers']].drop_duplicates(), x='day',
             y='ratio_distinct_total_tickers', ax=ax)
ax.axvline(x=pd.to_datetime('10/03/2023', dayfirst=True), color='r', linestyle='--')

ax2 = ax.twinx()
sns.lineplot(x='day', y='n_submissions', data=submissions_stats[['day', 'n_submissions']].drop_duplicates(), ax=ax2,color = 'green')

##### Tickers popularity

tickers_rank = submissions_stats.groupby('ticker',as_index=False)\
    .agg(n_days_top = ('day','nunique'),n_mentions=('count','sum'))\
    .sort_values('n_mentions',ascending=False)\
    .loc[lambda x: x['n_mentions']>=50]

tickers_rank['popularity_volatility'] =tickers_rank['n_mentions']/tickers_rank['n_days_top']

fig, ax = plt.subplots()
# sns.set_palette('Set2')
mentions = sns.barplot(data = tickers_rank, x='ticker',y='n_mentions',ax=ax,width=0.4,color='royalblue')
ax2 = ax.twinx()
vol = sns.barplot(data = tickers_rank, x='ticker',y='popularity_volatility',ax=ax2,width=0.4,color='orange')

for bar in mentions.containers:
    for b in bar:
        b.set_x(b.get_x() - 0.2)


for bar in vol.containers:
    for b in bar:
        b.set_x(b.get_x() + 0.2)



