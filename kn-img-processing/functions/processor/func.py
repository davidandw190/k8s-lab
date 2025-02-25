from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
from PIL import Image, ImageEnhance
from datetime import datetime, timezone
from typing import Any, Dict, Tuple, Optional
import os
import io
import time
import json
import logging
import uuid
import threading
from functools import wraps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SECRETS_PATH = '/vault/secrets/minio-config'

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
        'event_source': os.getenv('EVENT_SOURCE_PROCESSOR', 'image-processing/processor'),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5'))
    }

def wait_for_vault_secrets() -> None:
    max_retries = 30
    retry_count = 0
    secrets_file = SECRETS_PATH
    
    while not os.path.exists(secrets_file):
        if retry_count >= max_retries:
            raise RuntimeError("Timed out waiting for Vault secrets")
        logger.info(f"Waiting for Vault secrets (attempt {retry_count + 1}/{max_retries})...")
        time.sleep(2)
        retry_count += 1
    
    with open(secrets_file, 'r') as f:
        for line in f:
            if line.startswith('export'):
                key, value = line.replace('export ', '').strip().split('=', 1)
                os.environ[key.strip()] = value.strip().strip('"\'')
    
    logger.info("Vault secrets loaded successfully")

def ensure_vault_secrets(func):
    initialization_complete = threading.Event()
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not initialization_complete.is_set():
            try:
                wait_for_vault_secrets()
                initialization_complete.set()
            except Exception as e:
                logger.error(f"Failed to initialize Vault secrets: {str(e)}")
                raise
        return func(*args, **kwargs)
    return wrapper

class MinioClientManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.endpoint = config['minio_endpoint']
        self.max_retries = config['max_retries']
        self.secure = config['secure_connection']
        self.connection_timeout = config.get('connection_timeout', 5)

    def get_credentials(self) -> Tuple[str, str]:
        try:
            secrets_dir = '/vault/secrets'
            if os.path.exists(secrets_dir):
                logger.info(f"Contents of {secrets_dir}: {os.listdir(secrets_dir)}")
            
            access_key = os.getenv("MINIO_ROOT_USER")
            secret_key = os.getenv("MINIO_ROOT_PASSWORD")
            
            logger.info("Checking for Vault-injected MinIO credentials")
            logger.info(f"MinIO credentials present: access_key={'YES' if access_key else 'NO'}, secret_key={'YES' if secret_key else 'NO'}")
            
            if not access_key or not secret_key:
                logger.error("MinIO credentials not found - ensure Vault injection is working")
                raise RuntimeError("MinIO credentials not available")
            
            return access_key, secret_key
            
        except Exception as e:
            logger.error(f"Error retrieving credentials: {str(e)}")
            raise RuntimeError(f"Failed to get MinIO credentials: {str(e)}")

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
                time.sleep(self.connection_timeout)

class ImageProcessor:
    def __init__(self, minio_client: Minio, config: Dict[str, Any]):
        self.minio_client = minio_client
        self.config = config
        self.raw_bucket = config['raw_bucket']
        self.processed_bucket = config['processed_bucket']
        self.max_retries = config['max_retries']
        self.connection_timeout = config.get('connection_timeout', 5)

    def ensure_bucket(self, bucket: str) -> None:
        try:
            if not self.minio_client.bucket_exists(bucket):
                logger.info(f"Creating bucket: {bucket}")
                self.minio_client.make_bucket(bucket)
                logger.info(f"Successfully created bucket: {bucket}")
            else:
                logger.info(f"Bucket {bucket} already exists and is ready")
        except Exception as e:
            logger.error(f"Failed to ensure bucket '{bucket}' exists: {str(e)}")
            raise

    def process_image(self, image_data: bytes) -> Dict[str, Any]:
        if not image_data:
            raise ValueError("Received empty image data")
            
        image_buffer = io.BytesIO(image_data)
        image_buffer.seek(0)
        
        try:
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
        except Exception as e:
            logger.error(f"Error processing image: {str(e)}")
            raise ValueError(f"Image processing failed: {str(e)}")

    def handle_image(self, image_bucket: str, image_filename: str) -> Dict[str, Any]:
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Downloading image: {image_filename} from bucket: {image_bucket}")
                response = self.minio_client.get_object(image_bucket, image_filename)
                raw_image_data = response.read()
                response.close()
                response.release_conn()
                logger.info("Successfully downloaded image")
                break
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to download image after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.connection_timeout)

        processed_result = self.process_image(raw_image_data)
        
        self.ensure_bucket(self.processed_bucket)
        
        processed_filename = f"processed_{image_filename}"
        
        for attempt in range(self.max_retries):
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
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to upload processed image after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Upload attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.connection_timeout)

        return {
            "processed_bucket": self.processed_bucket,
            "processed_filename": processed_filename,
            "original_bucket": image_bucket,
            "original_filename": image_filename,
            "size": processed_result["size"],
            "dimensions": processed_result["dimensions"],
            "timestamp": int(time.time())
        }

def create_cloud_event_response(
    event_type: str,
    data: dict,
    config: Dict[str, Any],
    source_override: Optional[str] = None,
    category_override: Optional[str] = None
) -> CloudEvent:
    source = source_override if source_override is not None else config['event_source']
    category = category_override if category_override is not None else "processing"
    trace_id = str(uuid.uuid4())
    
    logger.info(f"Creating CloudEvent: type={event_type}, source={source}, category={category}")
    
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"processor-{int(time.time())}",
        "time": datetime.now(timezone.utc).isoformat(),
        "category": category,
        "datacontenttype": "application/json",
        "trace_id": trace_id
    }, data)

@ensure_vault_secrets
def main(context: Context) -> CloudEvent:
    try:
        logger.info("Processor function activated")
        
        if not hasattr(context, 'cloud_event'):
            logger.error("No cloud_event in context")
            return CloudEvent({
                "specversion": "1.0",
                "id": f"image.error.validation-{int(time.time())}",
                "type": "image.error.validation",
                "source": "image-processing/processor",
                "time": datetime.now(timezone.utc).isoformat(),
                "datacontenttype": "application/json",
                "category": "processing"
            }, {
                "error": "No cloud event in context",
                "error_type": "ValidationError",
                "component": "processor",
                "timestamp": int(time.time())
            })
        
        logger.info(f"CloudEvent type: {type(context.cloud_event).__name__}")
        
        event_type = context.cloud_event['type'] if hasattr(context.cloud_event, '__getitem__') and 'type' in context.cloud_event else "unknown"
        event_source = context.cloud_event['source'] if hasattr(context.cloud_event, '__getitem__') and 'source' in context.cloud_event else "unknown"
        event_id = context.cloud_event['id'] if hasattr(context.cloud_event, '__getitem__') and 'id' in context.cloud_event else "unknown"
        
        logger.info(f"Processing event: id={event_id}, type={event_type}, source={event_source}")
        
        config = get_config()
        
        try:
            secrets_contents = os.listdir('/vault/secrets')
            logger.info(f"Available secrets: {secrets_contents}")
        except Exception as e:
            logger.warning(f"Could not list secrets: {str(e)}")
        
        if event_type != "image.storage.completed":
            logger.warning(f"Unexpected event type: {event_type}, expected: image.storage.completed")
            raise ValueError(f"Unexpected event type: {event_type}")

        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            logger.info("Parsing string event data as JSON")
            event_data = json.loads(event_data)
        
        logger.info(f"Processing event data: {json.dumps(event_data, indent=2)}")
        
        image_bucket = event_data.get("bucket")
        image_filename = event_data.get("filename")
        if not all([image_bucket, image_filename]):
            logger.error(f"Missing required file information. bucket={image_bucket}, filename={image_filename}")
            raise ValueError(f"Missing required file information")

        logger.info("Initializing MinIO client")
        minio_manager = MinioClientManager(config)
        minio_client = minio_manager.initialize_client()
        
        logger.info(f"Processing image {image_filename} from bucket {image_bucket}")
        processor = ImageProcessor(minio_client, config)
        result = processor.handle_image(image_bucket, image_filename)
        
        logger.info(f"Successfully processed image {image_filename} from bucket {image_bucket}")

        response_event = CloudEvent({
            "specversion": "1.0",
            "id": f"image.processing.completed-{int(time.time())}",
            "type": "image.processing.completed",
            "source": "image-processing/processor",
            "time": datetime.now(timezone.utc).isoformat(),
            "datacontenttype": "application/json",
            "category": "processing"
        }, result)
        
        logger.info(f"Returning CloudEvent with id={response_event['id']}, type={response_event['type']}")
        logger.info(f"CloudEvent data: {json.dumps(result)}")
        return response_event
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return CloudEvent({
            "specversion": "1.0",
            "id": f"image.error.validation-{int(time.time())}",
            "type": "image.error.validation",
            "source": "image-processing/processor",
            "time": datetime.now(timezone.utc).isoformat(),
            "datacontenttype": "application/json",
            "category": "processing"
        }, {
            "error": str(e),
            "error_type": "ValidationError",
            "component": "processor",
            "timestamp": int(time.time())
        })
    except Exception as e:
        logger.error(f"Error in main handler: {str(e)}", exc_info=True)
        return CloudEvent({
            "specversion": "1.0",
            "id": f"image.error.processing-{int(time.time())}",
            "type": "image.error.processing",
            "source": "image-processing/processor",
            "time": datetime.now(timezone.utc).isoformat(),
            "datacontenttype": "application/json",
            "category": "processing"
        }, {
            "error": str(e),
            "error_type": type(e).__name__,
            "component": "processor",
            "timestamp": int(time.time())
        })