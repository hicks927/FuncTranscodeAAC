import datetime
import os
import sys
from urllib.parse import urlparse
from urllib.request import urlretrieve

from google.cloud import storage


def process_download_link(raw_link):
    t = urlparse(raw_link)

    if not t.netloc and not t.path:
        print("link is not valid")
        exit(2)

    if not raw_link.startswith("https://") and not raw_link.startswith("http://"):
        return "https://" + raw_link
    return raw_link


def connect_gcloud(bucket_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    return bucket


def upload_file(filepath, objectname, bucket):
    blob = bucket.blob(objectname)
    blob.metadata = {"output_file_key": objectname}
    blob.upload_from_filename(filepath)
    print("Successfully Uploaded to da cloud\n")
    return


def generate_objectname(prefix, postfix):
    timestring = datetime.date.today().strftime("%Y-%m-%dH%H%z")
    return prefix + timestring + postfix


def download_file(dl_link):
    temp_filepath, http_response = urlretrieve(dl_link)
    return temp_filepath


def pubsub_entry(event, context):
    dl_procedure()


def dl_procedure():
    bucket_name = "default"

    if len(sys.argv) > 1:
        download_link = sys.argv[1]
        if len(sys.argv) == 3:
            bucket_name = sys.argv[2]

    elif os.getenv("SCRIPT_DOWNLOAD_LINK"):
        download_link = sys.argv[1]

    else:
        print("No Valid download link provided")
        sys.exit(2)

    download_link = process_download_link(download_link)

    if bucket_name == "default" and os.getenv("SCRIPT_BUCKET_NAME"):
        bucket_name = os.getenv("SCRIPT_BUCKET_NAME")

    bucket = connect_gcloud(bucket_name)
    temp_file_location = download_file(download_link)
    upload_file(temp_file_location, generate_objectname("snrbulletin", ".mp3"), bucket)


if __name__ == '__main__':
    print("script entered")
    dl_procedure()
