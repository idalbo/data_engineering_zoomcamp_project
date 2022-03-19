from google.cloud import storage
import pandas as pd


def save_local(data_url, temp_folder, data_file, path_to_local_home, date_col=None, limit_col=None):
    df = pd.read_csv(data_url)
    if limit_col:
        df = df.iloc[:, :limit_col]
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col])
    df.to_parquet(f"{path_to_local_home}/{temp_folder}/{data_file}.parquet", index=False)


def dump_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

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