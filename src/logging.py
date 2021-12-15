import json
import os

from src.main import global_log_fields


def log_trace(message, **data):
    log_entry = dict(
        severity="TRACE",
        message=message,
        component="transcoding",
        **data,
        **global_log_fields
    )

    print(json.dumps(log_entry))


def log_error(message, error):
    log_entry = dict(
        severity="ERROR",
        message=message,
        error=error,
        component="transcoding",
        file_system=os.listdir("/tmp")
                    ** global_log_fields
    )