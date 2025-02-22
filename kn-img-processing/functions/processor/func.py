from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
from minio.error import S3Error
from PIL import Image, ImageEnhance
from datetime import datetime, timezone
from typing import Any, Dict, Tuple
import os
import io
import time
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_config() -> Dict[str, Any]:
    max_size_str = os.getenv('MAX_IMAGE_SIZE', '800')
    max_size = int(max_size_str)
    max_size_tuple = (max_size, max_size)

    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.image-processing.svc.cluster.local:9000'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-images'),
        'processed_bucket': os.getenv('PROCESSED_BUCKET', 'processed-images'),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'jpeg_quality': int(os.getenv('JPEG_QUALITY', '85')),
        'max_image_size': max_size_tuple,
        'contrast_enhancement': float(os.getenv('CONTRAST_ENHANCEMENT', '1.2')),
        'event_source': 'image-processing/processor',
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    }

class MinioClientManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.endpoint = config['minio_endpoint']
        self.max_retries = config['max_retries']
        self.secure = config['secure_connection']

    def get_credentials(self) -> Tuple[str, str]:
        access_key = os.getenv("MINIO_ROOT_USER")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD")
        
        logger.info("Checking for Vault-injected MinIO credentials")
        logger.info(f"MinIO credentials present: access_key={'YES' if access_key else 'NO'}, secret_key={'YES' if secret_key else 'NO'}")
        
        if not access_key or not secret_key:
            logger.error("MinIO credentials not found - ensure Vault injection is working")
            raise RuntimeError("MinIO credentials not available")
        
        return access_key, secret_key

    def initialize_client(self) -> Minio:
        for attempt in range(self.max_retries):
            try:
                access_key, secret_key = self.get_credentials()
                
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
                time.sleep(1)

class ImageProcessor:
    def __init__(self, minio_client: Minio, config: Dict[str, Any]):
        self.minio_client = minio_client
        self.config = config
        self.raw_bucket = config['raw_bucket']
        self.processed_bucket = config['processed_bucket']

    def ensure_bucket(self, bucket: str) -> None:
        try:
            if not self.minio_client.bucket_exists(bucket):
                logger.info(f"Creating bucket: {bucket}")
                self.minio_client.make_bucket(bucket)
                logger.info(f"Successfully created bucket: {bucket}")
        except Exception as e:
            logger.error(f"Failed to ensure bucket '{bucket}' exists: {str(e)}")
            raise

    def process_image(self, image_data: bytes) -> Dict[str, Any]:
        if not image_data:
            raise ValueError("Received empty image data")
            
        image_buffer = io.BytesIO(image_data)
        image_buffer.seek(0)
        
        img = Image.open(image_buffer)
        img.verify()
        image_buffer.seek(0)
        
        img = Image.open(image_buffer)
        logger.info(f"Processing image of format {img.format} and size {img.size}")
        
        processed = img.convert("L")
        
        processed.thumbnail(self.config['max_image_size'], Image.LANCZOS)
        logger.info(f"Resized image to fit within {self.config['max_image_size']}")
        
        enhancer = ImageEnhance.Contrast(processed)
        processed = enhancer.enhance(self.config['contrast_enhancement'])
        logger.info("Enhanced image contrast")
        
        output_buffer = io.BytesIO()
        processed.save(output_buffer, format="JPEG", quality=self.config['jpeg_quality'])
        processed_bytes = output_buffer.getvalue()
        size_bytes = output_buffer.tell()
        
        return {
            "data": processed_bytes,
            "format": "JPEG",
            "size": size_bytes,
            "dimensions": processed.size
        }

    def handle_image(self, image_bucket: str, image_filename: str) -> Dict[str, Any]:
        for attempt in range(self.config['max_retries']):
            try:
                logger.info(f"Downloading image: {image_filename} from bucket: {image_bucket}")
                response = self.minio_client.get_object(image_bucket, image_filename)
                raw_image_data = response.read()
                response.close()
                response.release_conn()
                logger.info("Successfully downloaded image")
                break
            except Exception as e:
                if attempt == self.config['max_retries'] - 1:
                    raise
                logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}")
                time.sleep(1)

        processed_result = self.process_image(raw_image_data)
        
        self.ensure_bucket(self.processed_bucket)
        
        processed_filename = f"processed_{image_filename}"
        for attempt in range(self.config['max_retries']):
            try:
                logger.info(f"Uploading processed image: {processed_filename}")
                self.minio_client.put_object(
                    self.processed_bucket,
                    processed_filename,
                    io.BytesIO(processed_result["data"]),
                    length=processed_result["size"],
                    content_type="image/jpeg"
                )
                logger.info("Successfully uploaded processed image")
                break
            except Exception as e:
                if attempt == self.config['max_retries'] - 1:
                    raise
                logger.warning(f"Upload attempt {attempt + 1} failed: {str(e)}")
                time.sleep(1)

        return {
            "processed_bucket": self.processed_bucket,
            "processed_filename": processed_filename,
            "original_bucket": image_bucket,
            "original_filename": image_filename,
            "size": processed_result["size"],
            "dimensions": processed_result["dimensions"],
            "timestamp": int(time.time())
        }

def create_cloud_event_response(event_type: str, data: dict, config: Dict[str, Any]) -> CloudEvent:
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": config['event_source'],  
        "id": f"processor-{int(time.time())}",
        "time": datetime.now(timezone.utc).isoformat(),
        "category": "processing",  
        "datacontenttype": "application/json"
    }, data)

def main(context: Context) -> CloudEvent:
    logger.info(f"Received event type: {context.cloud_event.type}")
    config = get_config()
    
    try:
        if context.cloud_event.type != "image.storage.completed":
            raise ValueError(f"Unexpected event type: {context.cloud_event.type}")

        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        image_bucket = event_data.get("bucket")
        image_filename = event_data.get("filename")
        if not all([image_bucket, image_filename]):
            raise ValueError("Missing required file information")

        minio_manager = MinioClientManager(config)
        minio_client = minio_manager.initialize_client()
        processor = ImageProcessor(minio_client, config)
        
        result = processor.handle_image(image_bucket, image_filename)
        
        return create_cloud_event_response(
            "image.processing.completed",
            result,
            config
        )
        
    except ValueError as e:
        return create_cloud_event_response(
            "image.error.validation",
            {
                "error": str(e),
                "error_type": "ValidationError",
                "component": "processor",
                "timestamp": int(time.time())
            },
            config if 'config' in locals() else get_config()
        )
    except Exception as e:
        logger.error(f"Error in main handler: {str(e)}", exc_info=True)
        return create_cloud_event_response(
            "image.error.processing",
            {
                "error": str(e),
                "error_type": type(e).__name__,
                "original_event": context.cloud_event.data,
                "component": "processor",
                "timestamp": int(time.time())
            },
            config if 'config' in locals() else get_config()
        )