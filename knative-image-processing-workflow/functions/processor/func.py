from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
from minio.error import S3Error
from PIL import Image, ImageEnhance
from datetime import datetime, timezone
from typing import Any, Dict
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

RAW_BUCKET = "raw-images"
PROCESSED_BUCKET = "processed-images"
MAX_RETRIES = 3
MAX_IMAGE_SIZE = (800, 800)
JPEG_QUALITY = 85
MINIO_ENDPOINT = "minio.image-processing.svc.cluster.local:9000"

def initialize_minio_client() -> Minio:
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    if not access_key or not secret_key:
        logger.error("MinIO credentials not available in environment.")
        raise RuntimeError("MinIO credentials not available")
    
    client = Minio(
        MINIO_ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    
    try:
        client.list_buckets()
        logger.info("Successfully connected to MinIO and listed buckets.")
    except S3Error as se:
        logger.error(f"S3 Error: {se.code} - {se.message} (Request ID: {se.request_id})")
        raise
    except Exception as e:
        logger.error(f"Error connecting to MinIO: {str(e)}")
        raise
    
    return client

minio_client = initialize_minio_client()

def ensure_bucket(bucket: str) -> None:
    try:
        if not minio_client.bucket_exists(bucket):
            logger.info(f"Creating bucket: {bucket}")
            minio_client.make_bucket(bucket)
            logger.info(f"Successfully created bucket: {bucket}")
    except Exception as e:
        logger.error(f"Failed to ensure bucket '{bucket}' exists: {str(e)}")
        raise

def process_image(image_data: bytes) -> Dict[str, Any]:
    try:
        if not image_data:
            raise ValueError("Received empty image data")
            
        image_buffer = io.BytesIO(image_data)
        image_buffer.seek(0) 
        
        img = Image.open(image_buffer)
        img.verify()  
        image_buffer.seek(0)  
        
        img = Image.open(image_buffer)
        logger.info(f"Successfully opened image of format {img.format} and size {img.size}")
        
        processed = img.convert("L")
        
        processed.thumbnail(MAX_IMAGE_SIZE, Image.LANCZOS)
        logger.info(f"Resized image to fit within {MAX_IMAGE_SIZE}.")
        
        enhancer = ImageEnhance.Contrast(processed)
        processed = enhancer.enhance(1.2)
        logger.info("Enhanced image contrast.")
        
        output_buffer = io.BytesIO()
        processed.save(output_buffer, format="JPEG", quality=JPEG_QUALITY)
        processed_bytes = output_buffer.getvalue()
        size_bytes = output_buffer.tell()
        
        return {
            "data": processed_bytes,
            "format": "JPEG",
            "size": size_bytes,
            "dimensions": processed.size
        }
    except Exception as e:
        logger.error(f"Image processing failed: {str(e)}")
        raise
    
def create_cloud_event_response(event_type: str, data: dict, source: str) -> CloudEvent:
    attributes = {
        "id": f"processor-{int(time.time())}",
        "specversion": "1.0",
        "type": event_type,
        "source": f"image-processing/{source}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
        "category": "processing"
    }
    return CloudEvent(attributes, data)

def main(context: Context) -> CloudEvent:
    logger.info("Received event for image processing: %s", context.cloud_event)
    image_bucket = None
    image_filename = None

    try:
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        logger.info("Parsed event data: %s", event_data)
        
        image_bucket = event_data.get("bucket")
        image_filename = event_data.get("filename")
        if not all([image_bucket, image_filename]):
            raise ValueError("Missing required file information in event data (bucket and/or filename).")
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Attempt {attempt} to download image: {image_filename} from bucket: {image_bucket}")
                response = minio_client.get_object(image_bucket, image_filename)
                raw_image_data = response.read()
                response.close()
                response.release_conn()
                logger.info("Successfully downloaded image from MinIO.")
                break
            except Exception as download_error:
                logger.warning(f"Download attempt {attempt} failed: {str(download_error)}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(1)
        
        processed_result = process_image(raw_image_data)
        
        ensure_bucket(PROCESSED_BUCKET)
        
        processed_filename = f"processed_{image_filename}"
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Attempt {attempt} to upload processed image: {processed_filename} to bucket: {PROCESSED_BUCKET}")
                processed_data = processed_result["data"]
                minio_client.put_object(
                    PROCESSED_BUCKET,
                    processed_filename,
                    io.BytesIO(processed_data),
                    length=processed_result["size"],
                    content_type="image/jpeg"
                )
                logger.info("Successfully uploaded processed image to MinIO.")
                break
            except Exception as upload_error:
                logger.warning(f"Upload attempt {attempt} failed: {str(upload_error)}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(1)
        
        response_data = {
            "processed_bucket": PROCESSED_BUCKET,
            "processed_filename": processed_filename,
            "original_bucket": image_bucket,
            "original_filename": image_filename,
            "size": processed_result["size"],
            "dimensions": processed_result["dimensions"],
            "timestamp": int(time.time())
        }
        return create_cloud_event_response(
            "image.processing.completed",  
            response_data,
            source="processor"
        )
        
    except Exception as e:
        logger.error("Error processing image: %s", str(e), exc_info=True)
        return create_cloud_event_response(
            "image.error", 
            {
                "error": str(e),
                "error_type": type(e).__name__,
                "original_event": context.cloud_event.data,
                "component": "processor",
                "timestamp": int(time.time())
            },
            source="processor"
        )
