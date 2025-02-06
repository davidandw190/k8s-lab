from cloudevents.http import CloudEvent
from typing import Any
import functions_framework
from minio import Minio
import os
import requests
import time

minio_client = Minio(
    "minio.image-processing:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

BUCKET_NAME = "raw-images"

def ensure_bucket():
    """Ensure bucket exists, create if it doesn't"""
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)

@functions_framework.cloud_event
def handle_event(cloud_event: CloudEvent) -> dict[str, Any]:
    """Handle incoming CloudEvents for image downloading"""
    try:
        # extracting image URL from event data
        image_url = cloud_event.data.get("image_url")
        if not image_url:
            raise ValueError("No image URL provided")

        # downloading image
        response = requests.get(image_url, stream=True)
        response.raise_for_status()
        
        # generating unique filename
        filename = f"image_{int(time.time())}_{os.urandom(4).hex()}.jpg"
        
        # ensuring bucket exists
        ensure_bucket()
        
        # uploading to MinIO
        minio_client.put_object(
            BUCKET_NAME,
            filename,
            response.raw,
            length=int(response.headers.get('content-length', 0)),
            content_type=response.headers.get('content-type', 'image/jpeg')
        )
        
        # returning success event
        return {
            "type": "dev.knative.samples.image.downloaded",
            "source": "image-downloader",
            "data": {
                "bucket": BUCKET_NAME,
                "filename": filename,
                "size": int(response.headers.get('content-length', 0)),
                "content_type": response.headers.get('content-type', 'image/jpeg')
            }
        }
    except Exception as e:
        return {
            "type": "dev.knative.samples.image.error",
            "source": "image-downloader",
            "data": {
                "error": str(e),
                "original_url": image_url if 'image_url' in locals() else None
            }
        }