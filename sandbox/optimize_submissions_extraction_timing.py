from pathlib import Path
from google.cloud import bigquery
import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


with open(Path(__file__).parent/'sql/last_comment_age_per_subreddit.sql','r') as f:
    query = f.read()

client = bigquery.Client(project ='sentiment-analysis-379718')

data = client.query(query).to_dataframe()