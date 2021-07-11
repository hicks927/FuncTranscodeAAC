from google.cloud import storage
from datetime import datetime
import ffmpeg
import os
import json

global_log_fields = {}


def log_trace (message, **data):
    log_entry = dict(
        severity="TRACE",
        message=message,
        component="transcoding",
        **data,
        **global_log_fields
    )

    print(json.dumps(log_entry))

def log_error (message, error):
    log_entry = dict(
        severity="ERROR",
        message=message,
        error=error,
        component="transcoding",
        file_system=os.listdir("/tmp")
        **global_log_fields
    )

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


def connect_client(upload_bucket_name, transcode_bucket_name):
    log_trace("connecting client")
    client = storage.Client()
    log_trace("bucket connected")

    return client.get_bucket(upload_bucket_name), client.get_bucket(transcode_bucket_name)


def upload_file(upload_bucket, file_path):
    """
    :type upload_client: storage.Client
    :type file_path: str
    """
    upload_filename = "snrnewsbulletin-" + datetime.now().isoformat() + ".aac"
    upload_blob = storage.blob.Blob(upload_filename, upload_bucket)
    upload_blob.upload_from_filename(file_path, content_type="audio/aac")


def run_transcode(input_filepath, output_filepath=""):
    try:
        log_trace(f"input file size {os.stat(input_filepath).st_size} bytes")
        if output_filepath == "":
            output_filepath = input_filepath + ".aac"

        stream = ffmpeg.input(input_filepath).output(output_filepath, **{'b:a': '32k'}).overwrite_output()
        print(stream.run(capture_stdout=True)[0])

        log_trace(f"transcoding complete, output file size {os.stat(output_filepath).st_size} bytes")

        return output_filepath

    except Exception as e:
        log_error("Transcoding Error", e)

def main(event, context):
    file = event
    log_trace(f"Entering transcoding of file {file['name']}")

    if 'metadata' in file and 'transcode_bucket_name' in file['metadata']:
        transcode_bucket_name = file['metadata']['transcode_bucket_name']
        log_trace("Result Bucket set in metadata, transcode bucket set to", transcode_bucket_name)
    elif os.getenv("TRANSCODE_BUCKET"):
        transcode_bucket_name = os.getenv("TRANSCODE_BUCKET")
        log_trace(f"No Bucket set in metadata, using environment var instead, transcode bucket: {transcode_bucket_name}")
    else:
        transcode_bucket_name = file['bucket']

    upload_bucket, transcode_bucket = connect_client(upload_bucket_name=file['bucket'], transcode_bucket_name=transcode_bucket_name)
    downloaded_file = download_from_storage(upload_bucket, file['name'])
    log_trace(f"Processing file: {file['name']}")

    output_file_path = run_transcode(downloaded_file)

    if not output_file_path:
        log_error("Error in outputting transcoded file", "Error: output_file_path is NoneType")
        quit(5)

    upload_file(transcode_bucket, output_file_path)

    return "Function Completed"

def main_function_wrapper(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    log_trace("Event: ", event=event)

    return main(event, context)
