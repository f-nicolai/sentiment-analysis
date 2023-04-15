from joblib import Parallel, delayed
from google.cloud import storage

from gcp import upload_file_to_gcs

client = storage.Client(project='sentiment-analysis-379718')
extraction_type = 'comments'
extraction_date = '2023/03/28'

files = [x.name for x in client.list_blobs('intraday-data-extraction', prefix=f'reddit/{extraction_type}/{extraction_date}')]
tbl_names = [f.split('/')[1] + '_' + f.split('/')[-1].split('_')[0] + '_' + ''.join(f.split('/')[2:5]) + '_' +
             f.split('/')[-1].split('_')[1].replace('-', '_') for f in files]

Parallel(n_jobs=100, prefer="threads")(
    delayed(upload_file_to_gcs)('trash', n, 'intraday-data-extraction', f,'sentiment-analysis-379718') for n, f in zip(tbl_names, files)
)


