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
from typing import Optional, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config() -> Dict[str, Any]:
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.image-processing.svc.cluster.local:9000'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-images'),
        'event_source': os.getenv('EVENT_SOURCE', 'image-processing/storage'),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5')),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    }

class MinioClientManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.endpoint = config['minio_endpoint']
        self.max_retries = config['max_retries']
        self.secure = config['secure_connection']
        
    def get_credentials(self) -> tuple[str, str]:
        access_key = os.getenv("MINIO_ROOT_USER")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        
        logger.info("Checking for Vault-injected MinIO credentials")
        logger.debug(f"Using MinIO endpoint: {self.endpoint}")
        logger.info(f"MinIO credentials present: access_key={'YES' if access_key else 'NO'}, secret_key={'YES' if secret_key else 'NO'}")
        
        if not access_key or not secret_key:
            logger.error("MinIO credentials not found - ensure Vault injection is working")
            raise RuntimeError("MinIO credentials not available")
        
        return access_key, secret_key

    def initialize_client(self) -> Minio:
        for attempt in range(self.max_retries):
            try:
                access_key, secret_key = self.get_credentials()
                logger.info(f"Initializing MinIO client with endpoint: {self.endpoint}")
                
                client = Minio(
                    self.endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=self.secure
                )
                
                client.list_buckets()
                logger.info("Successfully connected to MinIO")
                return client
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to initialize MinIO client after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
                time.sleep(self.config['connection_timeout'])

class ImageDownloader:
    def __init__(self, minio_client: Minio, config: Dict[str, Any]):
        self.minio_client = minio_client
        self.bucket_name = config['raw_bucket']
    
    def ensure_bucket(self) -> None:
        try:
            if not self.minio_client.bucket_exists(self.bucket_name):
                logger.info(f"Creating bucket: {self.bucket_name}")
                self.minio_client.make_bucket(self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} is ready")
        except Exception as e:
            logger.error(f"Failed to ensure bucket exists: {str(e)}")
            raise
    
    def download_and_store_image(self, image_url: str) -> dict:
        try:
            logger.info(f"Downloading image from URL: {image_url}")
            response = requests.get(image_url, stream=False, timeout=self.config['connection_timeout'])
            response.raise_for_status()
            
            image_data = response.content
            content_length = len(image_data)
            content_type = response.headers.get('content-type', 'image/jpeg')
            
            filename = f"image_{int(time.time())}_{os.urandom(4).hex()}.jpg"
            
            self.ensure_bucket()
            
            image_buffer = io.BytesIO(image_data)
            
            logger.info(f"Uploading image to MinIO: {filename}")
            self.minio_client.put_object(
                self.bucket_name,
                filename,
                image_buffer,
                length=content_length,
                content_type=content_type
            )
            logger.info(f"Successfully uploaded image: {filename}")
            
            return {
                "bucket": self.bucket_name,
                "filename": filename,
                "size": content_length,
                "content_type": content_type,
                "original_url": image_url
            }
            
        except Exception as e:
            logger.error(f"Error downloading/storing image: {str(e)}")
            raise

def create_cloud_event_response(event_type: str, data: dict, config: Dict[str, Any]) -> CloudEvent:
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": config['event_source'],
        "id": f"storage-{int(time.time())}",
        "time": datetime.now(timezone.utc).isoformat(),
        "category": "storage" if event_type == "image.storage.completed" else "error",
        "datacontenttype": "application/json"
    }, data)

def main(context: Context) -> CloudEvent:
    logger.info(f"Processing event type: {context.cloud_event.type}")
    image_url: Optional[str] = None
    
    try:
        config = get_config()
        
        if context.cloud_event.type != "image.upload.requested":
            raise ValueError(f"Unexpected event type: {context.cloud_event.type}")
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        image_url = event_data.get("image_url")
        if not image_url:
            raise ValueError("No image URL provided in event data")
        
        minio_manager = MinioClientManager(config)
        minio_client = minio_manager.initialize_client()
        downloader = ImageDownloader(minio_client, config)
        
        result = downloader.download_and_store_image(image_url)
        
        return create_cloud_event_response(
            "image.storage.completed",
            result,
            config
        )
        
    except Exception as e:
        logger.error(f"Error in main handler: {str(e)}", exc_info=True)
        return create_cloud_event_response(
            "image.error",
            {
                "error": str(e),
                "error_type": type(e).__name__,
                "original_url": image_url,
                "component": "storage"
            },
            config if 'config' in locals() else get_config()
        )