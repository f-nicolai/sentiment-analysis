from utils.gcp import upload_file_to_gcs, create_bq_table_from_dataframe
from pathlib import Path
import pandas as pd


file_name = Path(__file__).parent.parent/'extracted_data/reddit_subreddits.csv'
data = pd.read_csv(file_name)


upload_file_to_gcs(
    filename = file_name,
    bucket_name = 'historical-data-extraction',
    file_name = 'reddit/tmp-2.csv',
)
