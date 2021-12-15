from google.cloud import storage

from src.logging import log_trace, log_error


def upload_file(upload_bucket, file_path, upload_file_key):
    """
    :type upload_client: storage.Client
    :type file_path: str
    """
    log_trace("Beginning Upload...")
    upload_blob = storage.blob.Blob(upload_file_key, upload_bucket)
    upload_blob.upload_from_filename(file_path, content_type="audio/aac")
    log_trace("Upload Completed...")


def connect_client(upload_bucket_name, transcode_bucket_name):
    log_trace("connecting client")
    client = storage.Client()
    log_trace("bucket connected")

    return client.get_bucket(upload_bucket_name), client.get_bucket(transcode_bucket_name)


def download_from_storage(bucket, key):
    """
    :type key: str
    :type client: storage.Client
    """
    log_trace(f"attempting to download key {key}")
    file_path = "/tmp/transcode-file"
    file_blob = storage.Blob(key, bucket)
    try:
        file_blob.download_to_filename(file_path)
    except Exception as e:
        log_error("Error Occured while Downloading file", e)

    return file_path