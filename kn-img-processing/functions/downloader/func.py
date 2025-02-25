from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
import os
import requests
import time
import logging
import json
import io
import uuid
import threading
from functools import wraps
from datetime import datetime, timezone
from typing import Optional, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SECRETS_PATH = '/vault/secrets/minio-config'
CONFIG_PATH = '/vault/secrets/minio-config'

def get_config() -> Dict[str, Any]:
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.image-processing.svc.cluster.local:9000'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-images'),
        'event_source': os.getenv('EVENT_SOURCE_STORAGE', 'image-processing/storage'),
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
        try:
            config_file = CONFIG_PATH
            logger.info(f"Attempting to read credentials from {config_file}")
            
            if not os.path.exists(config_file):
                logger.error(f"Secrets file not found at {config_file}")
                parent_dir = os.path.dirname(config_file)
                if os.path.exists(parent_dir):
                    logger.info(f"Contents of {parent_dir}: {os.listdir(parent_dir)}")
                else:
                    logger.error(f"Parent directory {parent_dir} does not exist")
                raise FileNotFoundError(f"Secrets file not found at {config_file}")

            with open(config_file, 'r') as f:
                config_content = f.read()
                logger.debug(f"Successfully read config file (length: {len(config_content)})")

            credentials = {}
            for line in config_content.split('\n'):
                if line.startswith('export'):
                    key, value = line.replace('export ', '').split('=', 1)
                    value = value.strip().strip('"\'')
                    credentials[key.strip()] = value

            access_key = credentials.get('MINIO_ROOT_USER')
            secret_key = credentials.get('MINIO_ROOT_PASSWORD')

            logger.info("Checking for Vault-injected MinIO credentials")
            logger.info(f"MinIO credentials present: access_key={'YES' if access_key else 'NO'}, secret_key={'YES' if secret_key else 'NO'}")

            if not access_key or not secret_key:
                logger.error("Failed to extract credentials from config file")
                raise RuntimeError("MinIO credentials not available in config file")

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
                time.sleep(self.config['connection_timeout'])

class ImageDownloader:
    def __init__(self, minio_client: Minio, config: Dict[str, Any]):
        self.minio_client = minio_client
        self.bucket_name = config['raw_bucket']
        self.config = config
    
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
        
def wait_for_vault_secrets():
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

def create_cloud_event_response(
    event_type: str,
    data: dict,
    config: Dict[str, Any],
    source_override: Optional[str] = None,
    category_override: Optional[str] = None
) -> CloudEvent:
    source = source_override if source_override is not None else config['event_source']
    category = category_override if category_override is not None else "storage"
    trace_id = str(uuid.uuid4())
    
    logger.info(f"Creating CloudEvent response: type={event_type}, source={source}, category={category}")
    
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}",
        "time": datetime.now(timezone.utc).isoformat(),
        "category": category,
        "datacontenttype": "application/json",
        "trace_id": trace_id
    }, data)

@ensure_vault_secrets
def main(context: Context) -> CloudEvent:
    logger.info(f"Processing event type: {context.cloud_event['type']}")
    image_url: Optional[str] = None
    
    try:
        config = get_config()
        
        try:
            secrets_contents = os.listdir('/vault/secrets')
            logger.info(f"Contents of /vault/secrets: {secrets_contents}")
        except Exception as e:
            logger.error(f"Error listing /vault/secrets: {str(e)}")
        
        if context.cloud_event["type"] != "image.storage.requested":
            raise ValueError(f"Unexpected event type: {context.cloud_event['type']}")
        
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
        
        event_id = f"image.storage.completed-{int(time.time())}"
        event_time = datetime.now(timezone.utc).isoformat()
        
        response_event = CloudEvent({
            "specversion": "1.0",
            "id": event_id,
            "type": "image.storage.completed",
            "source": "image-processing/storage",
            "time": event_time,
            "datacontenttype": "application/json",
            "category": "processing"
        }, result)
        
        logger.info(f"Returning CloudEvent with id={event_id}, type=image.storage.completed")
        logger.info(f"CloudEvent data: {json.dumps(result)}")
        
        return response_event
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error downloading image: {str(e)}")
        error_data = {
            "error": str(e),
            "error_type": "NetworkError",
            "original_url": image_url,
            "component": "storage",
            "timestamp": int(time.time())
        }
        return CloudEvent({
            "specversion": "1.0",
            "id": f"image.error-{int(time.time())}",
            "type": "image.error",
            "source": "image-processing/storage",
            "time": datetime.now(timezone.utc).isoformat(),
            "datacontenttype": "application/json",
            "category": "storage"
        }, error_data)
        
    except Exception as e:
        logger.error(f"Error in main handler: {str(e)}", exc_info=True)
        error_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "original_url": image_url,
            "component": "storage",
            "timestamp": int(time.time())
        }
        return CloudEvent({
            "specversion": "1.0",
            "id": f"image.error-{int(time.time())}",
            "type": "image.error",
            "source": "image-processing/storage",
            "time": datetime.now(timezone.utc).isoformat(),
            "datacontenttype": "application/json",
            "category": "storage"
        }, error_data)