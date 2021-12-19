import ffmpeg
import os

from src.logging import log_trace, log_error
from src.object_storage import upload_file, connect_client, download_from_storage

global_log_fields = {}


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
        log_trace(f"Result Bucket set in metadata, transcode bucket set to {transcode_bucket_name}")
    elif os.getenv("TRANSCODE_BUCKET"):
        transcode_bucket_name = os.getenv("TRANSCODE_BUCKET")
        log_trace(
            f"No Bucket set in metadata, using environment var instead, transcode bucket: {transcode_bucket_name}")
    else:
        transcode_bucket_name = file['bucket']

    upload_bucket, transcode_bucket = connect_client(upload_bucket_name=file['bucket'],
                                                     transcode_bucket_name=transcode_bucket_name)
    downloaded_file = download_from_storage(upload_bucket, file['name'])
    log_trace(f"Processing file: {file['name']}")

    output_file_path = run_transcode(downloaded_file)

    if not output_file_path:
        log_error("Error in outputting transcoded file", "Error: output_file_path is NoneType")
        quit(5)

    if 'output_file_key' in file['metadata']:
        output_file_key = file['metadata']['output_file_key']
    else:
        output_file_key = file['name']

    output_file_key = str.replace(output_file_key, ".mp3", ".aac")

    upload_file(transcode_bucket, output_file_path, output_file_key)

    return "Function Completed"


def main_function_wrapper(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    log_trace("Event: ", event=event)

    return main(event, context)
