from azure.storage.blob import BlobServiceClient
from io import StringIO
import pandas as pd
import json
import sys
import traceback

try:
    try:
        with open("config.json") as f:
            confi = json.load(f)
        conn_string = confi["conn_string"]
        container_name = confi["blob_container_name"]
    except Exception as e:
        raise Exception(f"Error loading config.json: {e}")

    try:
        payload = json.loads(sys.argv[1])
        filename = payload["filename"]
        blob_path = payload["blob_path"]
    except Exception as e:
        raise Exception(f"Invalid or missing payload argument: {e}")

    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_path)
    except Exception as e:
        raise Exception(f"Blob connection error: {e}")

    try:
        data = blob_client.download_blob().readall()
        df1 = pd.read_csv(StringIO(data.decode("utf-8")))
    except Exception as e:
        raise Exception(f"Error downloading or reading the blob file: {e}")

    try:
        with open("abc.txt", "w") as f:
            f.write(f"filename: {filename}\n")
            f.write(f"blob_path: {blob_path}\n")
            f.write(f"container: {container_name}\n")
            f.write(f"conn_str: {conn_string}\n")
    except Exception as e:
        raise Exception(f"Error writing abc.txt: {e}")

    print("SUCCESS: Script completed without errors.")

except Exception as e:
    print("ERROR OCCURRED:")
    print(str(e))
    print("\n--- Stack Trace ---")
    traceback.pri
