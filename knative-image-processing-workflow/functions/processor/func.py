from cloudevents.http import CloudEvent
from typing import Any, Dict
import functions_framework
from minio import Minio
import os
from PIL import Image, ImageEnhance
import io
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

minio_client = Minio(
    "minio.image-processing:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

RAW_BUCKET = "raw-images"
PROCESSED_BUCKET = "processed-images"
MAX_RETRIES = 3
MAX_IMAGE_SIZE = (800, 800)
JPEG_QUALITY = 85

def ensure_bucket() -> None:
    """Ensure processed bucket exists with error handling"""
    try:
        if not minio_client.bucket_exists(PROCESSED_BUCKET):
            logger.info(f"Creating bucket: {PROCESSED_BUCKET}")
            minio_client.make_bucket(PROCESSED_BUCKET)
    except Exception as e:
        logger.error(f"Failed to ensure bucket exists: {str(e)}")
        raise

def process_image(image_data: bytes) -> Dict[str, Any]:
    try:
        img = Image.open(io.BytesIO(image_data))
        
        processed = img.convert('L')
        
        processed.thumbnail(MAX_IMAGE_SIZE, Image.LANCZOS)
        
        enhancer = ImageEnhance.Contrast(processed)
        processed = enhancer.enhance(1.2)
        
        output = io.BytesIO()
        processed.save(output, format='JPEG', quality=JPEG_QUALITY)
        
        return {
            'data': output.getvalue(),
            'format': 'JPEG',
            'size': output.tell(),
            'dimensions': processed.size
        }
    except Exception as e:
        logger.error(f"Image processing failed: {str(e)}")
        raise

@functions_framework.cloud_event
def handle_event(cloud_event: CloudEvent) -> dict[str, Any]:
    """Handle incoming CloudEvents for image processing"""
    try:
        logger.info(f"Received event: {cloud_event.type} from {cloud_event.source}")

        event_data = cloud_event.data
        bucket = event_data.get("bucket")
        filename = event_data.get("filename")
        
        if not all([bucket, filename]):
            raise ValueError("Missing required file information in event data")
        
        for attempt in range(MAX_RETRIES):
            try:
                data = minio_client.get_object(bucket, filename).read()
                break
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}")
                time.sleep(1)

        processed_result = process_image(data)
        
        ensure_bucket()
        
        processed_filename = f"processed_{filename}"
        
        for attempt in range(MAX_RETRIES):
            try:
                minio_client.put_object(
                    PROCESSED_BUCKET,
                    processed_filename,
                    io.BytesIO(processed_result['data']),
                    length=processed_result['size'],
                    content_type=f"image/{processed_result['format'].lower()}"
                )
                break
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                logger.warning(f"Upload attempt {attempt + 1} failed: {str(e)}")
                time.sleep(1)

        logger.info(f"Successfully processed image: {processed_filename}")
        return {
            "type": "dev.knative.samples.image.processing.completed",
            "source": "image-processor",
            "data": {
                "bucket": PROCESSED_BUCKET,
                "filename": processed_filename,
                "original_filename": filename,
                "original_bucket": bucket,
                "size": processed_result['size'],
                "dimensions": processed_result['dimensions'],
                "timestamp": int(time.time())
            }
        }

    except Exception as e:
        logger.error(f"Error processing image: {str(e)}")
        return {
            "type": "dev.knative.samples.image.error",
            "source": "image-processor",
            "data": {
                "error": str(e),
                "error_type": type(e).__name__,
                "original_event": cloud_event.data,
                "timestamp": int(time.time())
            }
        }