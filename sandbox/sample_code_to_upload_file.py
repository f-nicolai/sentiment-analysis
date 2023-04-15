from pathlib import Path
from datetime import  datetime
import sys
sys.path.append(f'{Path(__file__).parent.parent}')

from gcp import upload_file_to_gcs


if __name__ =='__main__':

    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(f'./{dt}.txt','w') as f:
        f.write(f'KOUKOU')

    upload_file_to_gcs(
        local_file_name=f'./{dt}.txt',
        bucket_name='intraday-data-extraction',
        project='sentiment-analysis-379718',
        file_name=f'test/{dt}.txt'
    )