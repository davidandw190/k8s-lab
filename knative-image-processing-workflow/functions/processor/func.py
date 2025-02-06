from cloudevents.http import CloudEvent
from typing import Any
import functions_framework
from minio import Minio
import os
from PIL import Image
import io

# Initialize MinIO client
minio_client = Minio(
    "minio.image-processing:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

RAW_BUCKET = "raw-images"
PROCESSED_BUCKET = "processed-images"

def ensure_bucket():
    """Ensure processed bucket exists"""
    if not minio_client.bucket_exists(PROCESSED_BUCKET):
        minio_client.make_bucket(PROCESSED_BUCKET)

def process_image(image_data: bytes) -> bytes:
    """Apply image processing operations"""
    img = Image.open(io.BytesIO(image_data))
    
    # converting to grayscale
    processed = img.convert('L')
    
    # resizing maintaining aspect ratio
    processed.thumbnail((800, 800), Image.LANCZOS)
    
    # enhancing contrast
    from PIL import ImageEnhance
    enhancer = ImageEnhance.Contrast(processed)
    processed = enhancer.enhance(1.2)
    
    # converting back to bytes
    output = io.BytesIO()
    processed.save(output, format='JPEG', quality=85)
    return output.getvalue()

@functions_framework.cloud_event
def handle_event(cloud_event: CloudEvent) -> dict[str, Any]:
    """Handle incoming CloudEvents for image processing"""
    try:
        # extracting file information
        event_data = cloud_event.data
        bucket = event_data.get("bucket")
        filename = event_data.get("filename")
        
        if not all([bucket, filename]):
            raise ValueError("Missing file information")
        
        # getting image from MinIO
        data = minio_client.get_object(bucket, filename).read()
        
        # processing image
        processed_data = process_image(data)
        
        # ensuring processed bucket exists
        ensure_bucket()
        
        # storing processed image
        processed_filename = f"processed_{filename}"
        minio_client.put_object(
            PROCESSED_BUCKET,
            processed_filename,
            io.BytesIO(processed_data),
            length=len(processed_data),
            content_type="image/jpeg"
        )
        
        return {
            "type": "dev.knative.samples.image.processed",
            "source": "image-processor",
            "data": {
                "bucket": PROCESSED_BUCKET,
                "filename": processed_filename,
                "original_filename": filename,
                "original_bucket": bucket,
                "size": len(processed_data)
            }
        }
    except Exception as e:
        return {
            "type": "dev.knative.samples.image.error",
            "source": "image-processor",
            "data": {
                "error": str(e),
                "original_event": cloud_event.data
            }
        }