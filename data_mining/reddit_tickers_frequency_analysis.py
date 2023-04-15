import seaborn as sns
import pandas as pd
from pathlib import Path

from utils.gcp import query_bq

tickers_to_names = pd.read_csv(Path(__file__).parent.parent/'utils/data/US_companies_and_tickers.csv',sep=';')
tickers_to_names = dict(zip(tickers_to_names['symbol'],tickers_to_names['short_name']))

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

submissions_stats = query_bq(query_or_file=Path(__file__).parent / 'sql/submissions_stats_on_tickers.sql')
submissions = query_bq(query_or_file=Path(__file__).parent / 'sql/submissions_with_tickers.sql')


