from json import dumps
from google.cloud import bigquery
from google.cloud import storage
from typing import Union
from pathlib import Path
from datetime import datetime
from pandas import DataFrame

def upload_dataframe_to_gcs(dataframe: DataFrame, bucket_name: str, file_name: str, project: str = 'sentiment-analysis-379718', **kwargs) -> None:
    """
    Upload a file to GCS
    :param data: data to upload to GCS
    :param bucket_name: Name of the bucket to retrieve the file from
    :param file_name: Name of the file to retrieve
    :param project: name of the project
    :return: None
    """

    storage_client = storage.Client(project=project)

    output_destination_bucket = storage_client.get_bucket(bucket_name)

    output_destination_blob = output_destination_bucket.blob(file_name)
    output_destination_blob.upload_from_string(dataframe.to_csv(**kwargs),'text/csv')


def upload_file_to_gcs(local_file_name: Union[str,Path], bucket_name: str, file_name: str, project: str = 'sentiment-analysis-379718') -> None:
    """
    Upload a file to GCS
    :param data: data to upload to GCS
    :param bucket_name: Name of the bucket to retrieve the file from
    :param file_name: Name of the file to retrieve
    :param project: name of the project
    :return: None
    """

    storage_client = storage.Client(project=project)

    output_destination_bucket = storage_client.get_bucket(bucket_name)

    output_destination_blob = output_destination_bucket.blob(file_name)
    output_destination_blob.upload_from_filename(local_file_name)

def upload_dict_to_gcs(dictionary: dict, bucket_name: str, file_name: str, project: str = 'sentiment-analysis-379718') -> None:
    storage_client = storage.Client(project=project)

    output_destination_bucket = storage_client.get_bucket(bucket_name)

    output_destination_blob = output_destination_bucket.blob(file_name)
    output_destination_blob.upload_from_string(dumps(dictionary))



def create_bq_table_from_dataframe(
        dataframe: DataFrame,
        table_name: str,
        project:str='sentiment-analysis-379718',
        truncate: bool = False,
        expiration_date: datetime = None,
) -> None:
    """
    Creates a bigquery tables from a pandas DataFrame
    :param dataframe: The DataFrame used to create the table
    :param table_name: the name of the table to create
    :param truncate: Whether to truncate or not the existing table
    :param expiration_date: the expiration date of the table to create
    :return: None
    """
    client = bigquery.Client(project=project)

    if truncate:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    else:
        job_config = bigquery.LoadJobConfig()

    dataframe.columns = [col.replace('.','__') for col in dataframe.columns]

    job = client.load_table_from_dataframe(dataframe.reset_index(drop=True), table_name, job_config=job_config)
    job.result()

    if expiration_date:
        table = client.get_table(table_name)
        table.expires = expiration_date
        client.update_table(table, ["expires"])


def remove_previous_reddit_data_updates(table:str, project:str='sentiment-analysis-379718'):
    query = f"""
        create or replace table `{project}.reddit.{table}` as
            select * except(rank_update)
            from (
                select *, 
                row_number() over(partition by {'id' if table != 'authors' else 'fullname'} order by upload_datetime desc) as rank_update
                from `{project}.reddit.{table}`
            )
            where rank_update = 1
    """

    client = bigquery.Client(project=project)
    client.query(query).result()

def query_bq(query_or_file:Union[str,Path],project:str='sentiment-analysis-379718',replace:dict=None):
    if '.sql' in str(query_or_file):
        with open(query_or_file) as f:
            sql_query = f.read()
        if replace:
            for k,v in replace.items():
                sql_query = sql_query.replace(k,v)
    else:
        sql_query = query_or_file

    client = bigquery.Client(project=project)
    return client.query(sql_query).result().to_dataframe()




def download_file_as_string_from_gcs(storage_file_name: str, bucket_name: str, project: str = 'sentiment-analysis-379718') -> str:
    """
    Download a file from GCS
    :param storage_file_name: file to retrieve form GCS
    :param bucket_name: Name of the bucket to retrieve the file from
    :param project: name of the project
    :return: None
    """

    storage_client = storage.Client(project=project)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(storage_file_name)
    return blob.download_as_string()


