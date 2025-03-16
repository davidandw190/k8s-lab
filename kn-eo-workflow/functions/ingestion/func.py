from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
import planetary_computer
from pystac_client import Client
import os
import time
import logging
import json
import uuid
import io
import requests
import threading
import http.client
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SECRETS_PATH = '/vault/secrets/minio-config'
PC_CATALOG_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"

def get_config():
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-assets'),
        'catalog_url': os.getenv('STAC_CATALOG_URL', PC_CATALOG_URL),
        'collection': os.getenv('STAC_COLLECTION', 'sentinel-2-l2a'),
        'max_cloud_cover': float(os.getenv('MAX_CLOUD_COVER', '20')),
        'max_items': int(os.getenv('MAX_ITEMS', '3')),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5')),
        'max_workers': int(os.getenv('MAX_WORKERS', '4')),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/stac-ingestion'),
        'broker_url': os.getenv('BROKER_URL', 'http://broker-ingress.knative-eventing.svc.cluster.local/eo-workflow/eo-event-broker')
    }

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

class MinioClientManager:
    def __init__(self, config):
        self.config = config
        self.endpoint = config['minio_endpoint']
        self.max_retries = config['max_retries']
        self.secure = config['secure_connection']

    def get_credentials(self):
        try:
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

    def initialize_client(self):
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

def create_cloud_event(event_type, data, source):
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

def send_cloud_event_to_broker(cloud_event, broker_url):
    try:
        if broker_url.startswith("http://"):
            broker_url = broker_url[7:]
        elif broker_url.startswith("https://"):
            broker_url = broker_url[8:]
            
        host, path = broker_url.split("/", 1)
        path = "/" + path
        
        headers = {
            "Content-Type": "application/cloudevents+json",
        }
        
        ce_json = json.dumps(cloud_event.serialize())
        
        conn = http.client.HTTPConnection(host)
        conn.request("POST", path, body=ce_json, headers=headers)
        
        response = conn.getresponse()
        response_body = response.read().decode()
        
        if response.status >= 200 and response.status < 300:
            logger.info(f"Successfully sent event {cloud_event['id']} to broker")
            return True
        else:
            logger.error(f"Failed to send event to broker. Status: {response.status}, Response: {response_body}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending event to broker: {str(e)}")
        return False

class STACIngestionManager:
    def __init__(self, minio_client, config):
        self.minio_client = minio_client
        self.config = config
        self.raw_bucket = config['raw_bucket']
        self.connection_timeout = config.get('connection_timeout', 5)
        self.stac_client = Client.open(
            config['catalog_url'],
            modifier=planetary_computer.sign_inplace
        )
        self.max_workers = config['max_workers']

    def ensure_bucket(self):
        try:
            if not self.minio_client.bucket_exists(self.raw_bucket):
                logger.info(f"Creating bucket: {self.raw_bucket}")
                self.minio_client.make_bucket(self.raw_bucket)
                logger.info(f"Successfully created bucket: {self.raw_bucket}")
            else:
                logger.info(f"Bucket {self.raw_bucket} already exists and is ready")
        except Exception as e:
            logger.error(f"Failed to ensure bucket '{self.raw_bucket}' exists: {str(e)}")
            raise

    def search_scenes(self, bbox, time_range, cloud_cover=None, max_items=None):
        logger.info(f"Searching for {self.config['collection']} scenes in bbox: {bbox}, timerange: {time_range}")
        
        if cloud_cover is None:
            cloud_cover = self.config['max_cloud_cover']
        
        if max_items is None:
            max_items = self.config['max_items']
        
        # Search for items
        search = self.stac_client.search(
            collections=[self.config['collection']],
            bbox=bbox,
            datetime=time_range,
            query={
                "eo:cloud_cover": {"lt": cloud_cover},
                "s2:degraded_msi_data_percentage": {"lt": 5},
                "s2:nodata_pixel_percentage": {"lt": 10}
            },
            limit=max_items
        )
        
        # Retrieve items
        items = list(search.get_items())
        
        if not items:
            logger.warning("No scenes found for the specified parameters.")
            return []
        
        logger.info(f"Found {len(items)} scene(s)")
        return items

    def download_asset(self, item, asset_id):
        """Download a specific asset from a STAC item"""
        if asset_id not in item.assets:
            raise ValueError(f"Asset {asset_id} not found in item {item.id}")
        
        asset = item.assets[asset_id]
        
        asset_href = planetary_computer.sign(asset.href)
        
        for attempt in range(self.config['max_retries']):
            try:
                response = requests.get(asset_href, stream=True, timeout=self.connection_timeout)
                response.raise_for_status()
                
                metadata = {
                    "item_id": item.id,
                    "collection": item.collection_id,
                    "asset_id": asset_id,
                    "content_type": asset.media_type,
                    "original_href": asset.href,
                    "timestamp": int(time.time()),
                    "bbox": json.dumps(item.bbox),
                    "datetime": item.datetime.isoformat() if item.datetime else None
                }
                
                return response.content, metadata
            except requests.exceptions.RequestException as e:
                if attempt == self.config['max_retries'] - 1:
                    logger.error(f"Failed to download asset {asset_id} after {self.config['max_retries']} attempts: {str(e)}")
                    raise
                logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.connection_timeout)

    def store_asset(self, item_id, asset_id, data, metadata):
        collection = metadata.get("collection", "unknown")
        object_name = f"{collection}/{item_id}/{asset_id}"
        
        self.ensure_bucket()
        
        content_type = metadata.get("content_type", "application/octet-stream")
        
        for attempt in range(self.config['max_retries']):
            try:
                self.minio_client.put_object(
                    self.raw_bucket,
                    object_name,
                    io.BytesIO(data),
                    length=len(data),
                    content_type=content_type,
                    metadata=metadata
                )
                
                logger.info(f"Stored asset {asset_id} from item {item_id} in bucket {self.raw_bucket}")
                
                return {
                    "bucket": self.raw_bucket,
                    "object_name": object_name,
                    "item_id": item_id,
                    "asset_id": asset_id,
                    "collection": collection,
                    "size": len(data),
                    "content_type": content_type
                }
            except Exception as e:
                if attempt == self.config['max_retries'] - 1:
                    logger.error(f"Failed to store asset {asset_id} after {self.config['max_retries']} attempts: {str(e)}")
                    raise
                logger.warning(f"Storage attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.connection_timeout)

    def process_single_asset(self, item, asset_id, request_id):
        try:
            logger.info(f"Processing asset {asset_id} for item {item.id}")
            
            asset_data, asset_metadata = self.download_asset(item, asset_id)
            
            result = self.store_asset(item.id, asset_id, asset_data, asset_metadata)
            
            result["request_id"] = request_id
            
            return result
        except Exception as e:
            logger.error(f"Error processing asset {asset_id}: {str(e)}")
            raise

    def process_scene(self, item, request_id, bbox):
        scene_id = item.id
        collection_id = item.collection_id
        acquisition_date = item.datetime.strftime('%Y-%m-%d') if item.datetime else None
        cloud_cover = item.properties.get('eo:cloud_cover', 'N/A')
        
        logger.info(f"Processing scene: {scene_id} from {collection_id}, date: {acquisition_date}, cloud cover: {cloud_cover}%")
        
        band_assets = [f"B{i:02d}" for i in range(1, 13)] + ["B8A", "SCL"]
        assets_to_ingest = [band for band in band_assets if band in item.assets]
        
        if not assets_to_ingest:
            logger.warning(f"No assets to ingest for scene {scene_id}")
            return None, []
        
        # Create registration event data
        registration_data = {
            "request_id": request_id,
            "item_id": scene_id,
            "collection": collection_id,
            "assets": assets_to_ingest,
            "bbox": bbox,
            "acquisition_date": acquisition_date,
            "cloud_cover": cloud_cover
        }
        
        # Create registration event
        registration_event = create_cloud_event(
            "eo.scene.assets.registered",
            registration_data,
            self.config['event_source']
        )
        
        # Process assets in parallel
        processed_assets = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all asset processing tasks
            future_to_asset = {
                executor.submit(self.process_single_asset, item, asset_id, request_id): asset_id
                for asset_id in assets_to_ingest
            }
            
            # Process results as they complete
            for future in as_completed(future_to_asset):
                asset_id = future_to_asset[future]
                try:
                    result = future.result()
                    processed_assets.append(result)
                    logger.info(f"Successfully processed asset {asset_id} for scene {scene_id}")
                except Exception as e:
                    logger.error(f"Failed to process asset {asset_id} for scene {scene_id}: {str(e)}")
        
        return registration_event, processed_assets

    def process_scenes(self, items, request_id, bbox):
        """Process multiple scenes and manage event emission"""
        results = []
        
        for item in items:
            try:
                # Process scene - returns registration event and processed assets
                registration_event, processed_assets = self.process_scene(item, request_id, bbox)
                
                if registration_event:
                    # Create asset ingestion events for each processed asset
                    asset_events = []
                    for asset_data in processed_assets:
                        asset_event = create_cloud_event(
                            "eo.asset.ingested",
                            asset_data,
                            self.config['event_source']
                        )
                        asset_events.append(asset_event)
                    
                    results.append({
                        "scene_id": item.id,
                        "registration_event": registration_event,
                        "asset_events": asset_events,
                        "processed_count": len(processed_assets)
                    })
            except Exception as e:
                logger.error(f"Error processing scene {item.id}: {str(e)}")
        
        return results

# @ensure_vault_secrets
def main(context: Context):
    try:
        logger.info("STAC Ingestion function activated")
        config = get_config()
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        # extract search parameters
        bbox = event_data.get('bbox')
        time_range = event_data.get('time_range')
        cloud_cover = event_data.get('cloud_cover', config['max_cloud_cover'])
        max_items = event_data.get('max_items', config['max_items'])
        request_id = event_data.get('request_id', str(uuid.uuid4()))
        
        if not bbox or not time_range:
            error_msg = "Missing required parameters (bbox, time_range)"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        minio_manager = MinioClientManager(config)
        minio_client = minio_manager.initialize_client()
        
        ingestion_manager = STACIngestionManager(minio_client, config)
        
        items = ingestion_manager.search_scenes(bbox, time_range, cloud_cover, max_items)
        
        if not items:
            logger.warning("No scenes found matching the search criteria")
            return {
                "status": "no_scenes_found",
                "request_id": request_id,
                "parameters": {
                    "bbox": bbox,
                    "time_range": time_range,
                    "cloud_cover": cloud_cover,
                    "max_items": max_items
                }
            }, 200
        
        scene_results = ingestion_manager.process_scenes(items, request_id, bbox)
        
        if not scene_results:
            logger.warning("No scenes were successfully processed")
            return {
                "status": "processing_failed",
                "request_id": request_id,
                "message": "No scenes were successfully processed"
            }, 200
        
        stats = {
            "scenes_processed": len(scene_results),
            "total_assets_processed": sum(result["processed_count"] for result in scene_results),
            "request_id": request_id
        }
        
        logger.info(f"Processed {stats['scenes_processed']} scenes with {stats['total_assets_processed']} total assets")
        
        # Strategy for event emission:
        # 1. For first scene, return the registration event as the main response
        # 2. Send all asset events and other registration events directly to broker
        
        # Get first scene's registration event to return
        first_result = scene_results[0]
        primary_event = first_result["registration_event"]
        
        # For all scenes, send asset events to broker directly
        total_events_sent = 0
        for result in scene_results:
            scene_id = result["scene_id"]
            # Skip first scene's registration as we're returning it
            if scene_id != first_result["scene_id"]:
                # Send registration event
                if send_cloud_event_to_broker(result["registration_event"], config["broker_url"]):
                    total_events_sent += 1
            
            # Send all asset events for all scenes
            for asset_event in result["asset_events"]:
                if send_cloud_event_to_broker(asset_event, config["broker_url"]):
                    total_events_sent += 1
        
        # Log event sending statistics
        logger.info(f"Directly sent {total_events_sent} events to the broker")
        logger.info(f"Returning registration event for scene {first_result['scene_id']}")
        
        # Return the first registration event as the main response
        # This triggers the completion tracker while other events are sent directly
        return primary_event
        
    except Exception as e:
        logger.exception(f"Error in STAC ingestion function: {str(e)}")
        error_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "request_id": event_data.get("request_id", str(uuid.uuid4())) if "event_data" in locals() else str(uuid.uuid4())
        }
        error_event = create_cloud_event(
            "eo.processing.error",
            error_data,
            config['event_source']
        )
        return error_event