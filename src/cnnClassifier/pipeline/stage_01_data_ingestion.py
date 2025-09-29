from cnnClassifier.config.configuration import ConfigurationManager
from cnnClassifier.components.data_ingestion import DataIngestion
from cnnClassifier import logger
import os 
from dotenv import load_dotenv

load_dotenv()



STAGE_NAME = "Data Ingestion stage"



class DataIngestionTrainingPipeline:
    def __init__(self):
        pass

    def main(self):
        try:
            print(f"access key: {os.getenv('AWS_ACCESS_KEY_ID')}")
            print(f"secret key: {os.getenv('AWS_SECRET_ACCESS_KEY')}")
            config = ConfigurationManager()
            data_ingestion_config = config.get_data_ingestion_config()
            data_ingestion = DataIngestion(config=data_ingestion_config)

            bucket_name = "chicken-data-2025"
            object_key = "data.zip"
            download_path = data_ingestion_config.local_data_file  # already set in config

            data_ingestion.download_from_s3(
                bucket_name,
                object_key,
                download_path,
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name="us-east-1",
            )

            data_ingestion.extract_zip_file()

        except Exception as e:
            raise e