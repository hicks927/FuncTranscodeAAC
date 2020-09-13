from google.cloud import storage
from datetime import datetime
import ffmpeg
import os

print("loaded dependencies")

def download_from_storage(bucket, key):
    """
    :type key: str
    :type client: storage.Client
    """
    print(f"attempting to download key '%s'", key)
    file_path = "/tmp/transcode-file"
    file_blob = storage.Blob(key, bucket)
    try:
        file_blob.download_to_filename(file_path)
    except Exception as e:
        print("Arse, error occurred like this: ")
        print(e)

    return file_path


def connect_client(upload_bucket_name, transcode_bucket_name):
    print("connecting client")
    client = storage.Client()
    print("bucket connected")

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
        print("Beginning Transcody bit?")
        print(f"input file size {os.stat(input_filepath).st_size}bytes")
        if output_filepath == "":
            output_filepath = input_filepath + ".aac"

        stream = ffmpeg.input(input_filepath).output(output_filepath, **{'b:a': '36k'}).overwrite_output()
        print(stream.run(capture_stdout=True)[0])

        print(f"transcody bit completed, output file size {os.stat(output_filepath).st_size}bytes")

        return output_filepath


    except Exception as e:
        print("Something went wrong while transcoding and that thing was:")
        print(e)

def main(event, context):
    file = event
    print(f"Entering transcoding of file {file['name']}")

    if 'transcode_bucket_name' in file['metadata']:
        transcode_bucket_name = file['metadata']['transcode_bucket_name']
    else:
        transcode_bucket_name = file['bucket']

    upload_bucket, transcode_bucket = connect_client(upload_bucket_name=file['bucket'], transcode_bucket_name=transcode_bucket_name)
    downloaded_file = download_from_storage(upload_bucket, file['name'])
    print(f"Processing file: {file['name']}.")

    output_file_path = run_transcode(downloaded_file)

    if not output_file_path:
        print("Dunno, something went wrong... output_file_path is NoneType")
        quit(5)

    upload_file(transcode_bucket, output_file_path)

    return "Function Completed"

def main_function_wrapper(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    print(type(event))
    try:
        return main(event, context)
    except KeyError as e:
        print("There was a KeyError apparently....")
        print(e)
