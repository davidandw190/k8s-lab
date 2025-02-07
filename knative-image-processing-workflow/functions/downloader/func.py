from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
import os
import requests
import time
import logging
import json
import io 
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BUCKET_NAME = "raw-images"
MINIO_ENDPOINT = "minio.image-processing.svc.cluster.local:9000"

class MinioClientManager:
    @staticmethod
    def get_credentials() -> tuple[str, str]:
        access_key = os.getenv("MINIO_ROOT_USER")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        logger.info(f"MinIO credentials present: access_key={'YES' if access_key else 'NO'}, secret_key={'YES' if secret_key else 'NO'}")
        if not access_key or not secret_key:
            raise RuntimeError("MinIO credentials not available")
        return access_key, secret_key

    @classmethod
    def initialize_client(cls) -> Minio:
        try:
            access_key, secret_key = cls.get_credentials()
            
            logger.info(f"Initializing MinIO client with endpoint: {MINIO_ENDPOINT}")
            
            client = Minio(
                MINIO_ENDPOINT,
                access_key=access_key,
                secret_key=secret_key,
                secure=False 
            )
            
            client.list_buckets()
            logger.info("Successfully connected to MinIO")
            return client
            
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {str(e)}")
            raise

class ImageDownloader:
    def __init__(self, minio_client: Minio):
        self.minio_client = minio_client
    
    def ensure_bucket(self) -> None:
        try:
            if not self.minio_client.bucket_exists(BUCKET_NAME):
                logger.info(f"Creating bucket: {BUCKET_NAME}")
                self.minio_client.make_bucket(BUCKET_NAME)
            logger.info(f"Bucket {BUCKET_NAME} is ready")
        except Exception as e:
            logger.error(f"Failed to ensure bucket exists: {str(e)}")
            raise
    
    def download_and_store_image(self, image_url: str) -> dict:
        try:
            logger.info(f"Downloading image from URL: {image_url}")
            response = requests.get(image_url, stream=False)
            response.raise_for_status()
            
            image_data = response.content
            content_length = len(image_data)
            content_type = response.headers.get('content-type', 'image/jpeg')
            
            filename = f"image_{int(time.time())}_{os.urandom(4).hex()}.jpg"
            
            self.ensure_bucket()
            
            image_buffer = io.BytesIO(image_data)
            
            logger.info(f"Uploading image to MinIO: {filename}")
            self.minio_client.put_object(
                BUCKET_NAME,
                filename,
                image_buffer,
                length=content_length,
                content_type=content_type
            )
            logger.info(f"Successfully uploaded image: {filename}")
            
            return {
                "bucket": BUCKET_NAME,
                "filename": filename,
                "size": content_length,
                "content_type": content_type,
                "original_url": image_url
            }
            
        except Exception as e:
            logger.error(f"Error downloading/storing image: {str(e)}")
            raise

def create_cloud_event_response(event_type: str, data: dict) -> CloudEvent:
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": "knative/eventing/downloader",
        "id": f"download-{int(time.time())}",
        "time": datetime.now(timezone.utc).isoformat()
    }, data)

def main(context: Context) -> CloudEvent:
    logger.info(f"Processing event: {context.cloud_event}")
    image_url: Optional[str] = None
    
    try:
        minio_client = MinioClientManager.initialize_client()
        downloader = ImageDownloader(minio_client)
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        image_url = event_data.get("image_url")
        if not image_url:
            raise ValueError("No image URL provided in event data")
        
        result = downloader.download_and_store_image(image_url)
        
        return create_cloud_event_response(
            "dev.knative.samples.image.download.completed",
            result
        )
        
    except Exception as e:
        logger.error(f"Error in main handler: {str(e)}", exc_info=True)
        return create_cloud_event_response(
            "dev.knative.samples.image.error",
            {
                "error": str(e),
                "error_type": type(e).__name__,
                "original_url": image_url
            }
        )