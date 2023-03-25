from google.cloud import bigquery
from google.cloud import storage
from typing import Union
from pathlib import Path
from datetime import datetime
from pandas import DataFrame

def upload_dataframe_to_gcs(dataframe: DataFrame, bucket_name: str, file_name: str, project: str = None, **kwargs) -> None:
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


def upload_file_to_gcs(filename: Union[str,Path], bucket_name: str, file_name: str, project: str = None) -> None:
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
    output_destination_blob.upload_from_filename(filename)



def create_bq_table_from_dataframe(
        dataframe: DataFrame,
        table_name: str,
        truncate: bool = False,
        expiration_date: datetime = None,
        project:str = None
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