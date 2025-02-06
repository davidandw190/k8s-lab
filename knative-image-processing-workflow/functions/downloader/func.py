from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
import os
import requests
import time
import logging
import json
from datetime import datetime, timezone
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BUCKET_NAME = "raw-images"
MINIO_ENDPOINT = "minwio.image-processing.svc.cluster.local:9000"

class MinioClientManager:
    @staticmethod
    def get_credentials() -> tuple[str, str]:
        access_key = os.getenv("MINIO_ROOT_USER")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        logger.info(f"Using MINIO_ROOT_USER: {access_key} and MINIO_ROOT_PASSWORD: {secret_key}")
        if not access_key or not secret_key:
            logger.error("Failed to get MinIO credentials from environment")
            raise RuntimeError("MinIO credentials not available")
        return access_key, secret_key

    
    @classmethod
    def initialize_client(cls) -> Minio:
        try:
            access_key = os.getenv("MINIO_ROOT_USER")
            secret_key = os.getenv("MINIO_ROOT_PASSWORD")
            
            logger.info("Credential Check:")
            logger.info(f"Access Key present: {bool(access_key)}")
            logger.info(f"Secret Key present: {bool(secret_key)}")
            logger.info(f"Endpoint being used: {MINIO_ENDPOINT}")
            
            client = Minio(
                MINIO_ENDPOINT,
                access_key=access_key,
                secret_key=secret_key,
                secure=False 
            )
            
            try:
                client.list_buckets()
                logger.info("Successfully listed buckets")
            except S3Error as se:
                logger.error(f"S3 Error during bucket listing: {se.code} - {se.message}")
                logger.error(f"Request ID: {se.request_id}")
                raise
                
            return client
            
        except Exception as e:
            logger.error(f"Client initialization error: {str(e)}")
            raise
        
class ImageDownloader:
    
    def __init__(self, minio_client: Minio):
        self.minio_client = minio_client
    
    def ensure_bucket(self) -> None:
        """Ensure the bucket exists; create if needed"""
        try:
            if not self.minio_client.bucket_exists(BUCKET_NAME):
                logger.info(f"Creating bucket: {BUCKET_NAME}")
                self.minio_client.make_bucket(BUCKET_NAME)
                logger.info(f"Successfully created bucket: {BUCKET_NAME}")
        except Exception as e:
            logger.error(f"Failed to ensure bucket exists: {str(e)}")
            raise
    
    def download_and_store_image(self, image_url: str) -> dict:
        try:
            logger.info(f"Downloading image from URL: {image_url}")
            response = requests.get(image_url, stream=True)
            response.raise_for_status()
            
            filename = f"image_{int(time.time())}_{os.urandom(4).hex()}.jpg"
            
            content_length = int(response.headers.get('content-length', 0))
            content_type = response.headers.get('content-type', 'image/jpeg')
            
            self.ensure_bucket()
            
            logger.info(f"Uploading image to MinIO: {filename}")
            self.minio_client.put_object(
                BUCKET_NAME,
                filename,
                response.raw,
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
            logger.error(f"Error processing image: {str(e)}")
            raise

def create_cloud_event_response(event_type: str, data: dict) -> CloudEvent:
    """Create and return a CloudEvent response"""
    return CloudEvent({
        "id": f"download-{int(time.time())}",
        "specversion": "1.0",
        "type": event_type,
        "source": "knative/eventing/downloader",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json"
    }, data)

def main(context: Context) -> CloudEvent:
    """Main function handler for downloading images"""
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