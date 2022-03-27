from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import numpy as np


def save_local(data_url, temp_folder, data_file, path_to_local_home, date_col=None, limit_col=None):
    df = pd.read_csv(data_url)
    if limit_col:
        df = df.iloc[:, :limit_col]
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col])
    for col in df.columns:
        if df[col].dtypes == np.int64:
            df[col] = df[col].astype(float)
    df.to_parquet(f"{path_to_local_home}/{temp_folder}/{data_file}.parquet", index=False)


def dump_to_gcs(bucket, object_name, local_file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def blob_exists(bucket, object_name):
    storage_client = storage.Client()
    bucket_name = bucket
    bucket = storage_client.bucket(bucket_name)
    stats = storage.Blob(bucket=bucket, name=object_name).exists(storage_client)
    if not stats:
        return "save_local_population_task"
    else:
        return "skip_local_population_taks"

def weekday_branch(day_of_week, actual, dummy):
    if day_of_week=="7":
        return actual 
    else:
        return dummy

def table_exists(table, create_task, append_task):
    client = bigquery.Client()
    table_id = table
    try:
        client.get_table(table_id) 
        return append_task
    except NotFound:
        return create_task