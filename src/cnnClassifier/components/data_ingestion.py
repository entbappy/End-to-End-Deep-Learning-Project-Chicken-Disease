import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import zipfile
from cnnClassifier import logger
from cnnClassifier.utils.common import get_size
import tempfile
from cnnClassifier.entity.config_entity import DataIngestionConfig
from pathlib import Path
load_dotenv()



class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def _make_s3(self, region_name, aws_access_key_id, aws_secret_access_key):
        return boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def _stream_download(self, s3, bucket_name, object_key, download_path):
        # Ensure target directory exists
        Path(download_path).parent.mkdir(parents=True, exist_ok=True)

        print(f"Downloading {object_key} from bucket {bucket_name} to {download_path}...")
        response = s3.get_object(
            Bucket=bucket_name,
            Key=object_key,
            RequestPayer="requester",
        )
        body = response["Body"]

        # Write to temp file, then atomic rename
        tmp_dir = Path(download_path).parent
        with tempfile.NamedTemporaryFile(dir=tmp_dir, delete=False) as tmp_f:
            tmp_path = tmp_f.name
            for chunk in body.iter_chunks(chunk_size=8 * 1024 * 1024):
                if chunk:
                    tmp_f.write(chunk)

        body.close()
        os.replace(tmp_path, download_path)
        print("Download complete.")

    def download_from_s3(
        self,
        bucket_name,
        object_key,
        download_path,
        aws_access_key_id,
        aws_secret_access_key,
        region_name="us-east-1",
    ):
        s3 = self._make_s3(region_name, aws_access_key_id, aws_secret_access_key)

        try:
            self._stream_download(s3, bucket_name, object_key, download_path)

        except ClientError as e:
            code = e.response["Error"].get("Code", "")
            msg = e.response["Error"].get("Message", str(e))
            print(f"Error ({code}): {msg}")

            if code in ["403", "AccessDenied"]:
                print("→ Check IAM permissions and ensure RequestPayer='requester' is allowed in the bucket policy.")
                raise

            elif code in ["PermanentRedirect", "301", "AuthorizationHeaderMalformed"]:
                try:
                    loc = s3.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
                    retry_region = loc or "us-east-1"
                    print(f"→ Detected bucket region: {retry_region}. Retrying download...")
                    s3_retry = self._make_s3(retry_region, aws_access_key_id, aws_secret_access_key)
                    self._stream_download(s3_retry, bucket_name, object_key, download_path)
                except ClientError as e2:
                    code2 = e2.response["Error"].get("Code", "")
                    msg2 = e2.response["Error"].get("Message", str(e2))
                    print(f"Retry failed ({code2}): {msg2}")
                    raise
            else:
                raise

    def extract_zip_file(self):
        """
        Extracts the zip file into the data directory specified by self.config.unzip_dir.
        """
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(self.config.local_data_file, "r") as zip_ref:
            zip_ref.extractall(unzip_path)


