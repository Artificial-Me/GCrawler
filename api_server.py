"""
REST API Server for GhostCrawler

Provides a FastAPI-based REST API for remote control of the GhostCrawler web crawler.
Features:
- Crawl management (start, stop, status)
- Queue operations
- Statistics and monitoring
- Data export
- Webhook management
- WebSocket for real-time updates
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union, Tuple

import uvicorn
from fastapi import (
    BackgroundTasks, Depends, FastAPI, HTTPException, Query, 
    Request, Response, WebSocket, WebSocketDisconnect, status
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, HttpUrl, validator
import aiofiles

# Import GhostCrawler components
from ghostcrawler import GhostCrawler, CrawlerConfig
from queue_manager import TaskQueue, TaskStatus, TaskPriority
from data_exporter import DataExporter, ExportFormat, ExportOptions, CompressionType
from webhook_notifier import WebhookNotifier, WebhookEndpointConfig, WebhookEventType
from rate_limiter import AdaptiveRateLimiter, DomainConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("api_server")

# API Key authentication
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

# Load API keys from environment or config file
API_KEYS = set(os.environ.get("GHOSTCRAWLER_API_KEYS", "").split(","))
if not API_KEYS or (len(API_KEYS) == 1 and not API_KEYS[0]):
    # Try to load from file
    try:
        api_keys_file = Path("api_keys.json")
        if api_keys_file.exists():
            with open(api_keys_file, "r") as f:
                API_KEYS = set(json.load(f))
            logger.info(f"Loaded {len(API_KEYS)} API keys from file")
    except Exception as e:
        logger.warning(f"Failed to load API keys from file: {e}")
        # Add a default key for development
        default_key = str(uuid.uuid4())
        API_KEYS = {default_key}
        logger.warning(f"Using default API key for development: {default_key}")

# Rate limiting configuration
RATE_LIMIT_ENABLED = os.environ.get("RATE_LIMIT_ENABLED", "true").lower() == "true"
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", "60"))  # seconds
RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "100"))

# Global rate limiter for API endpoints
api_rate_limiter = {}  # ip -> {window_start, request_count}

# Global instances of components
task_queue = None
data_exporter = None
webhook_notifier = None
adaptive_rate_limiter = None

# Active crawl tasks
active_crawls = {}  # crawl_id -> {task, config, status, stats}

# WebSocket connections for real-time updates
websocket_connections = set()

#
# Pydantic Models
#

class CrawlStatus(str, Enum):
    """Crawl status enum"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"


class ExportFormatEnum(str, Enum):
    """Export format enum"""
    CSV = "csv"
    JSON = "json"
    EXCEL = "excel"
    HTML = "html"


class CompressionTypeEnum(str, Enum):
    """Compression type enum"""
    NONE = "none"
    GZIP = "gzip"
    ZIP = "zip"


class WebhookAuthTypeEnum(str, Enum):
    """Webhook authentication type enum"""
    NONE = "none"
    BEARER_TOKEN = "bearer_token"
    API_KEY = "api_key"
    HMAC = "hmac"
    BASIC = "basic"


class WebhookEventTypeEnum(str, Enum):
    """Webhook event type enum"""
    CRAWL_STARTED = "crawl_started"
    URL_PROCESSED = "url_processed"
    URL_FAILED = "url_failed"
    BATCH_COMPLETED = "batch_completed"
    CRAWL_COMPLETED = "crawl_completed"
    ERROR_THRESHOLD = "error_threshold"
    RATE_LIMITED = "rate_limited"
    MEMORY_WARNING = "memory_warning"
    CIRCUIT_BREAKER_TRIGGERED = "circuit_breaker_triggered"
    PROGRESS_UPDATE = "progress_update"


class CrawlerConfigModel(BaseModel):
    """Crawler configuration model"""
    max_browsers: int = Field(2, ge=1, le=20, description="Maximum number of concurrent browsers")
    headless: bool = Field(True, description="Run browsers in headless mode")
    batch_size: int = Field(10, ge=1, le=100, description="Batch size for URL processing")
    url_delay: float = Field(1.0, ge=0, description="Delay between URLs in seconds")
    max_retries: int = Field(1, ge=0, le=10, description="Maximum number of retries for failed URLs")
    memory_threshold_mb: int = Field(49152, ge=1000, description="Memory threshold in MB")
    max_total_urls: int = Field(1000000, ge=1, description="Maximum total URLs to process")
    output_dir: str = Field("Specs", description="Output directory")
    
    # Optional proxy configuration
    proxy_server: Optional[str] = Field(None, description="Proxy server URL")
    proxy_username: Optional[str] = Field(None, description="Proxy username")
    proxy_password: Optional[str] = Field(None, description="Proxy password")
    
    # Advanced options
    humanize: bool = Field(True, description="Use browser humanization")
    geoip: bool = Field(True, description="Use GeoIP for browser location")
    block_resources: List[str] = Field(
        ["stylesheet", "image", "media", "font", "other"],
        description="Resource types to block"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "max_browsers": 2,
                "headless": True,
                "batch_size": 10,
                "url_delay": 1.0,
                "max_retries": 1,
                "memory_threshold_mb": 49152,
                "max_total_urls": 1000000,
                "output_dir": "Specs",
                "humanize": True,
                "geoip": True
            }
        }


class StartCrawlRequest(BaseModel):
    """Start crawl request model"""
    urls: List[str] = Field(..., min_items=1, description="URLs to crawl")
    config: CrawlerConfigModel = Field(default_factory=CrawlerConfigModel, description="Crawler configuration")
    crawl_id: Optional[str] = Field(None, description="Custom crawl ID (generated if not provided)")
    
    @validator("urls")
    def validate_urls(cls, urls):
        """Validate URLs"""
        if not urls:
            raise ValueError("At least one URL is required")
        
        for url in urls:
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"Invalid URL: {url}")
        
        return urls


class AddUrlsRequest(BaseModel):
    """Add URLs to queue request model"""
    urls: List[str] = Field(..., min_items=1, description="URLs to add")
    priority: Optional[str] = Field("medium", description="Priority (high, medium, low)")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator("priority")
    def validate_priority(cls, priority):
        """Validate priority"""
        if priority not in ("high", "medium", "low"):
            raise ValueError("Priority must be one of: high, medium, low")
        return priority


class ExportRequest(BaseModel):
    """Export request model"""
    crawl_id: str = Field(..., description="Crawl ID to export")
    format: ExportFormatEnum = Field(ExportFormatEnum.JSON, description="Export format")
    compression: CompressionTypeEnum = Field(CompressionTypeEnum.NONE, description="Compression type")
    include_fields: Optional[List[str]] = Field(None, description="Fields to include")
    exclude_fields: Optional[List[str]] = Field(None, description="Fields to exclude")
    pretty_print: bool = Field(True, description="Pretty print output (for JSON)")
    filename: Optional[str] = Field(None, description="Custom filename")


class WebhookEndpointRequest(BaseModel):
    """Webhook endpoint request model"""
    url: HttpUrl = Field(..., description="Webhook URL")
    event_types: List[WebhookEventTypeEnum] = Field(..., description="Event types to trigger webhook")
    auth_type: WebhookAuthTypeEnum = Field(WebhookAuthTypeEnum.NONE, description="Authentication type")
    auth_credentials: Optional[Dict[str, str]] = Field({}, description="Authentication credentials")
    custom_headers: Optional[Dict[str, str]] = Field({}, description="Custom headers")
    retry_count: int = Field(3, ge=0, le=10, description="Number of retries")
    retry_delay: float = Field(5.0, ge=1.0, description="Delay between retries in seconds")
    timeout: float = Field(10.0, ge=1.0, le=60.0, description="Request timeout in seconds")
    enabled: bool = Field(True, description="Whether the webhook is enabled")
    transform_template: Optional[str] = Field(None, description="Template for payload transformation")
    filter_expression: Optional[str] = Field(None, description="Filter expression")
    batch_size: int = Field(1, ge=1, description="Batch size (1 means no batching)")
    batch_interval: float = Field(0.0, ge=0.0, description="Batch interval in seconds (0 means no time-based batching)")


class HealthResponse(BaseModel):
    """Health check response model"""
    status: str = "ok"
    version: str = "1.0.0"
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    components: Dict[str, str] = {}


class ErrorResponse(BaseModel):
    """Error response model"""
    error: str
    detail: Optional[str] = None
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


#
# Authentication and Rate Limiting
#

async def get_api_key(api_key_header: str = Depends(api_key_header)) -> str:
    """Validate API key"""
    if not API_KEYS:
        # No API keys configured, allow all requests
        return None
    
    if api_key_header not in API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "APIKey"},
        )
    
    return api_key_header


async def rate_limit_middleware(request: Request, call_next):
    """Rate limiting middleware"""
    if not RATE_LIMIT_ENABLED:
        return await call_next(request)
    
    # Get client IP
    client_ip = request.client.host
    
    # Check if client is rate limited
    now = time.time()
    
    if client_ip in api_rate_limiter:
        window_start, request_count = api_rate_limiter[client_ip]
        
        # Check if window has expired
        if now - window_start > RATE_LIMIT_WINDOW:
            # Reset window
            api_rate_limiter[client_ip] = (now, 1)
        else:
            # Increment request count
            request_count += 1
            
            # Check if rate limit exceeded
            if request_count > RATE_LIMIT_MAX_REQUESTS:
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={"error": "Rate limit exceeded", "retry_after": int(window_start + RATE_LIMIT_WINDOW - now)}
                )
            
            # Update request count
            api_rate_limiter[client_ip] = (window_start, request_count)
    else:
        # First request from this client
        api_rate_limiter[client_ip] = (now, 1)
    
    # Process request
    return await call_next(request)


#
# FastAPI App
#

app = FastAPI(
    title="GhostCrawler API",
    description="REST API for GhostCrawler web crawler",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Add rate limiting middleware
app.middleware("http")(rate_limit_middleware)


#
# API Endpoints
#

@app.on_event("startup")
async def startup_event():
    """Initialize components on startup"""
    global task_queue, data_exporter, webhook_notifier, adaptive_rate_limiter
    
    logger.info("Initializing API server components...")
    
    # Initialize task queue
    task_queue = TaskQueue(db_path="api_queue.db")
    
    # Initialize data exporter
    data_exporter = DataExporter(output_dir="exports")
    
    # Initialize webhook notifier
    webhook_notifier = WebhookNotifier(queue_path="webhook_events.db")
    await webhook_notifier.start()
    
    # Initialize adaptive rate limiter
    adaptive_rate_limiter = AdaptiveRateLimiter()
    
    logger.info("API server components initialized")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    logger.info("Shutting down API server...")
    
    # Stop webhook notifier
    if webhook_notifier:
        await webhook_notifier.stop()
    
    # Stop active crawls
    for crawl_id, crawl_data in active_crawls.items():
        if crawl_data.get("crawler"):
            logger.info(f"Stopping crawl {crawl_id}")
            await crawl_data["crawler"].cleanup()
    
    logger.info("API server shutdown complete")


@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Health check endpoint"""
    components = {
        "task_queue": "ok" if task_queue else "not_initialized",
        "data_exporter": "ok" if data_exporter else "not_initialized",
        "webhook_notifier": "ok" if webhook_notifier else "not_initialized",
        "adaptive_rate_limiter": "ok" if adaptive_rate_limiter else "not_initialized"
    }
    
    return HealthResponse(components=components)


@app.post("/crawl/start", tags=["Crawl Management"])
async def start_crawl(
    request: StartCrawlRequest,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(get_api_key)
):
    """Start a new crawl"""
    # Generate crawl ID if not provided
    crawl_id = request.crawl_id or str(uuid.uuid4())
    
    # Check if crawl ID already exists
    if crawl_id in active_crawls:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Crawl with ID {crawl_id} already exists"
        )
    
    # Convert Pydantic model to CrawlerConfig
    config_dict = request.config.dict()
    config = CrawlerConfig(**config_dict)
    
    # Create crawler instance
    crawler = GhostCrawler(config)
    
    # Initialize crawl status
    active_crawls[crawl_id] = {
        "crawler": crawler,
        "config": config,
        "status": CrawlStatus.PENDING,
        "start_time": time.time(),
        "stats": {
            "total_urls": len(request.urls),
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "in_progress": 0
        }
    }
    
    # Start crawl in background
    background_tasks.add_task(
        run_crawl_task,
        crawl_id,
        crawler,
        request.urls
    )
    
    # Notify webhook subscribers
    if webhook_notifier:
        await webhook_notifier.notify(
            WebhookEventType.CRAWL_STARTED,
            {
                "crawl_id": crawl_id,
                "urls_count": len(request.urls),
                "config": config_dict
            }
        )
    
    return {
        "crawl_id": crawl_id,
        "status": CrawlStatus.PENDING,
        "message": f"Crawl started with {len(request.urls)} URLs"
    }


async def run_crawl_task(crawl_id: str, crawler: GhostCrawler, urls: List[str]):
    """Run crawl task in background"""
    try:
        # Update status
        active_crawls[crawl_id]["status"] = CrawlStatus.RUNNING
        
        # Initialize crawler
        await crawler.initialize()
        
        # Update websocket clients
        await broadcast_status_update(crawl_id, "started")
        
        # Start crawling
        await crawler.crawl(urls)
        
        # Update status
        active_crawls[crawl_id]["status"] = CrawlStatus.COMPLETED
        active_crawls[crawl_id]["end_time"] = time.time()
        active_crawls[crawl_id]["stats"]["processed"] = crawler.urls_processed + crawler.urls_failed
        active_crawls[crawl_id]["stats"]["successful"] = crawler.urls_processed
        active_crawls[crawl_id]["stats"]["failed"] = crawler.urls_failed
        
        # Notify webhook subscribers
        if webhook_notifier:
            await webhook_notifier.notify(
                WebhookEventType.CRAWL_COMPLETED,
                {
                    "crawl_id": crawl_id,
                    "status": CrawlStatus.COMPLETED,
                    "stats": active_crawls[crawl_id]["stats"],
                    "elapsed_time": active_crawls[crawl_id]["end_time"] - active_crawls[crawl_id]["start_time"]
                }
            )
        
        # Update websocket clients
        await broadcast_status_update(crawl_id, "completed")
        
    except Exception as e:
        logger.error(f"Error in crawl task {crawl_id}: {e}")
        
        # Update status
        active_crawls[crawl_id]["status"] = CrawlStatus.FAILED
        active_crawls[crawl_id]["end_time"] = time.time()
        active_crawls[crawl_id]["error"] = str(e)
        
        # Notify webhook subscribers
        if webhook_notifier:
            await webhook_notifier.notify(
                WebhookEventType.ERROR_THRESHOLD,
                {
                    "crawl_id": crawl_id,
                    "status": CrawlStatus.FAILED,
                    "error": str(e)
                }
            )
        
        # Update websocket clients
        await broadcast_status_update(crawl_id, "failed", error=str(e))
        
    finally:
        # Clean up crawler resources
        await crawler.cleanup()


@app.get("/crawl/{crawl_id}/status", tags=["Crawl Management"])
async def get_crawl_status(
    crawl_id: str,
    api_key: str = Depends(get_api_key)
):
    """Get crawl status"""
    # Check if crawl exists
    if crawl_id not in active_crawls:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl with ID {crawl_id} not found"
        )
    
    crawl_data = active_crawls[crawl_id]
    
    # Calculate progress
    total_urls = crawl_data["stats"]["total_urls"]
    processed = crawl_data["stats"]["processed"]
    progress = (processed / total_urls * 100) if total_urls > 0 else 0
    
    # Calculate elapsed time
    start_time = crawl_data["start_time"]
    end_time = crawl_data.get("end_time", time.time())
    elapsed_time = end_time - start_time
    
    # Calculate ETA
    eta = 0
    if crawl_data["status"] == CrawlStatus.RUNNING and processed > 0:
        urls_per_second = processed / elapsed_time if elapsed_time > 0 else 0
        remaining_urls = total_urls - processed
        eta = remaining_urls / urls_per_second if urls_per_second > 0 else 0
    
    return {
        "crawl_id": crawl_id,
        "status": crawl_data["status"],
        "stats": crawl_data["stats"],
        "progress": progress,
        "elapsed_time": elapsed_time,
        "eta": eta,
        "start_time": datetime.fromtimestamp(start_time).isoformat(),
        "end_time": datetime.fromtimestamp(end_time).isoformat() if "end_time" in crawl_data else None,
        "error": crawl_data.get("error")
    }


@app.post("/crawl/{crawl_id}/stop", tags=["Crawl Management"])
async def stop_crawl(
    crawl_id: str,
    api_key: str = Depends(get_api_key)
):
    """Stop a running crawl"""
    # Check if crawl exists
    if crawl_id not in active_crawls:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl with ID {crawl_id} not found"
        )
    
    crawl_data = active_crawls[crawl_id]
    
    # Check if crawl is running
    if crawl_data["status"] != CrawlStatus.RUNNING:
        return {
            "crawl_id": crawl_id,
            "status": crawl_data["status"],
            "message": f"Crawl is not running (current status: {crawl_data['status']})"
        }
    
    # Stop crawler
    crawler = crawl_data["crawler"]
    if crawler:
        await crawler.cleanup()
    
    # Update status
    crawl_data["status"] = CrawlStatus.STOPPED
    crawl_data["end_time"] = time.time()
    
    # Notify webhook subscribers
    if webhook_notifier:
        await webhook_notifier.notify(
            WebhookEventType.CRAWL_COMPLETED,
            {
                "crawl_id": crawl_id,
                "status": CrawlStatus.STOPPED,
                "stats": crawl_data["stats"],
                "elapsed_time": crawl_data["end_time"] - crawl_data["start_time"]
            }
        )
    
    # Update websocket clients
    await broadcast_status_update(crawl_id, "stopped")
    
    return {
        "crawl_id": crawl_id,
        "status": CrawlStatus.STOPPED,
        "message": "Crawl stopped successfully"
    }


@app.get("/crawl/{crawl_id}/results", tags=["Crawl Management"])
async def get_crawl_results(
    crawl_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    api_key: str = Depends(get_api_key)
):
    """Get crawl results"""
    # Check if crawl exists
    if crawl_id not in active_crawls:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl with ID {crawl_id} not found"
        )
    
    crawl_data = active_crawls[crawl_id]
    
    # Get output directory
    output_dir = Path(crawl_data["config"].output_dir)
    
    # Collect result files
    result_files = []
    if output_dir.exists():
        for manufacturer_dir in output_dir.iterdir():
            if manufacturer_dir.is_dir():
                raw_html_dir = manufacturer_dir / f"{manufacturer_dir.name.upper()}_RAW_HTML"
                if raw_html_dir.exists():
                    for file_path in raw_html_dir.glob("*.html"):
                        result_files.append({
                            "file": str(file_path),
                            "manufacturer": manufacturer_dir.name,
                            "filename": file_path.name,
                            "size": file_path.stat().st_size,
                            "created": datetime.fromtimestamp(file_path.stat().st_ctime).isoformat()
                        })
    
    # Apply pagination
    total = len(result_files)
    paginated_results = result_files[offset:offset+limit]
    
    return {
        "crawl_id": crawl_id,
        "status": crawl_data["status"],
        "total_results": total,
        "limit": limit,
        "offset": offset,
        "results": paginated_results
    }


@app.get("/queue/status", tags=["Queue Management"])
async def get_queue_status(
    api_key: str = Depends(get_api_key)
):
    """Get queue statistics"""
    if not task_queue:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Task queue not initialized"
        )
    
    stats = await task_queue.get_queue_stats()
    
    return {
        "stats": stats,
        "timestamp": datetime.now().isoformat()
    }


@app.post("/queue/add", tags=["Queue Management"])
async def add_urls_to_queue(
    request: AddUrlsRequest,
    api_key: str = Depends(get_api_key)
):
    """Add URLs to the queue"""
    if not task_queue:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Task queue not initialized"
        )
    
    # Convert priority string to enum
    priority_map = {
        "high": TaskPriority.HIGH,
        "medium": TaskPriority.MEDIUM,
        "low": TaskPriority.LOW
    }
    priority = priority_map.get(request.priority, TaskPriority.MEDIUM)
    
    # Add tasks to queue
    tasks = []
    for url in request.urls:
        tasks.append({
            "url": url,
            "priority": priority,
            "metadata": request.metadata
        })
    
    task_ids = await task_queue.add_tasks_bulk(tasks)
    
    return {
        "message": f"Added {len(task_ids)} URLs to queue",
        "task_ids": task_ids
    }


@app.delete("/queue/clear", tags=["Queue Management"])
async def clear_queue(
    status: Optional[str] = None,
    api_key: str = Depends(get_api_key)
):
    """Clear the queue"""
    if not task_queue:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Task queue not initialized"
        )
    
    # Convert status string to enum if provided
    task_status = None
    if status:
        try:
            task_status = TaskStatus(status)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status: {status}"
            )
    
    # Clear queue
    cleared = await task_queue.clear_queue(task_status)
    
    return {
        "message": f"Cleared {cleared} tasks from queue",
        "status": status
    }


@app.get("/stats", tags=["Statistics"])
async def get_crawler_stats(
    api_key: str = Depends(get_api_key)
):
    """Get crawler statistics"""
    # Collect stats from active crawls
    crawl_stats = {}
    for crawl_id, crawl_data in active_crawls.items():
        crawl_stats[crawl_id] = {
            "status": crawl_data["status"],
            "stats": crawl_data["stats"],
            "start_time": datetime.fromtimestamp(crawl_data["start_time"]).isoformat(),
            "end_time": datetime.fromtimestamp(crawl_data["end_time"]).isoformat() if "end_time" in crawl_data else None,
            "elapsed_time": time.time() - crawl_data["start_time"] if crawl_data["status"] == CrawlStatus.RUNNING else (
                crawl_data["end_time"] - crawl_data["start_time"] if "end_time" in crawl_data else 0
            )
        }
    
    # Get queue stats if available
    queue_stats = await task_queue.get_queue_stats() if task_queue else {}
    
    # Get webhook stats if available
    webhook_stats = webhook_notifier.get_stats() if webhook_notifier else {}
    
    # System stats
    import psutil
    system_stats = {
        "cpu_percent": psutil.cpu_percent(),
        "memory_percent": psutil.virtual_memory().percent,
        "memory_used_mb": psutil.virtual_memory().used / (1024 * 1024),
        "disk_percent": psutil.disk_usage('/').percent
    }
    
    return {
        "crawls": crawl_stats,
        "queue": queue_stats,
        "webhooks": webhook_stats,
        "system": system_stats,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/stats/domains", tags=["Statistics"])
async def get_domain_stats(
    api_key: str = Depends(get_api_key)
):
    """Get per-domain statistics"""
    if not adaptive_rate_limiter:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Rate limiter not initialized"
        )
    
    domain_stats = adaptive_rate_limiter.get_all_stats()
    
    return {
        "domains": domain_stats,
        "total_domains": len(domain_stats),
        "timestamp": datetime.now().isoformat()
    }


@app.post("/export", tags=["Data Export"])
async def export_data(
    request: ExportRequest,
    api_key: str = Depends(get_api_key)
):
    """Export crawled data"""
    if not data_exporter:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Data exporter not initialized"
        )
    
    # Check if crawl exists
    if request.crawl_id not in active_crawls:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl with ID {request.crawl_id} not found"
        )
    
    crawl_data = active_crawls[request.crawl_id]
    
    # Get output directory
    output_dir = Path(crawl_data["config"].output_dir)
    
    # Check if output directory exists
    if not output_dir.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Output directory {output_dir} not found"
        )
    
    # Collect data files
    data_dict = {}
    for manufacturer_dir in output_dir.iterdir():
        if manufacturer_dir.is_dir():
            raw_html_dir = manufacturer_dir / f"{manufacturer_dir.name.upper()}_RAW_HTML"
            if raw_html_dir.exists():
                for file_path in raw_html_dir.glob("*.html"):
                    try:
                        # Read HTML file
                        async with aiofiles.open(file_path, "r") as f:
                            content = await f.read()
                        
                        # Extract data (simplified - in real implementation, parse HTML)
                        url = f"https://example.com/car-specs/{manufacturer_dir.name}/{file_path.stem}"
                        data_dict[url] = {
                            "content": content[:1000],  # Truncate for demo
                            "file": str(file_path),
                            "manufacturer": manufacturer_dir.name,
                            "filename": file_path.name
                        }
                    except Exception as e:
                        logger.error(f"Error reading file {file_path}: {e}")
    
    # Prepare export options
    export_options = ExportOptions(
        include_fields=request.include_fields,
        exclude_fields=request.exclude_fields,
        pretty_print=request.pretty_print,
        compression=CompressionType[request.compression.upper()],
    )
    
    # Generate filename
    filename = request.filename or f"export_{request.crawl_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Convert format enum
    format_map = {
        ExportFormatEnum.CSV: ExportFormat.CSV,
        ExportFormatEnum.JSON: ExportFormat.JSON,
        ExportFormatEnum.EXCEL: ExportFormat.EXCEL,
        ExportFormatEnum.HTML: ExportFormat.HTML
    }
    export_format = format_map[request.format]
    
    # Export data
    try:
        export_path = data_exporter.create_combined_export(
            data_dict,
            export_format,
            filename,
            export_options
        )
        
        return {
            "message": f"Data exported successfully to {export_path}",
            "format": request.format,
            "compression": request.compression,
            "file_path": export_path,
            "record_count": len(data_dict)
        }
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error exporting data: {str(e)}"
        )


@app.get("/webhooks", tags=["Webhooks"])
async def list_webhooks(
    api_key: str = Depends(get_api_key)
):
    """List configured webhooks"""
    if not webhook_notifier:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook notifier not initialized"
        )
    
    webhooks = []
    for endpoint_id, endpoint in webhook_notifier.endpoints.items():
        webhooks.append({
            "id": endpoint_id,
            "url": endpoint.url,
            "event_types": [et.value for et in endpoint.event_types],
            "auth_type": endpoint.auth_type.value,
            "enabled": endpoint.enabled,
            "retry_count": endpoint.retry_count,
            "batch_size": endpoint.batch_size
        })
    
    return {
        "webhooks": webhooks,
        "total": len(webhooks)
    }


@app.post("/webhooks", tags=["Webhooks"])
async def add_webhook(
    request: WebhookEndpointRequest,
    api_key: str = Depends(get_api_key)
):
    """Add a new webhook"""
    if not webhook_notifier:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook notifier not initialized"
        )
    
    # Convert event types from string to enum
    event_types = []
    for event_type in request.event_types:
        event_types.append(WebhookEventType[event_type.upper()])
    
    # Create webhook config
    config = WebhookEndpointConfig(
        url=str(request.url),
        event_types=event_types,
        auth_type=WebhookAuthType[request.auth_type.upper()],
        auth_credentials=request.auth_credentials or {},
        custom_headers=request.custom_headers or {},
        retry_count=request.retry_count,
        retry_delay=request.retry_delay,
        timeout=request.timeout,
        enabled=request.enabled,
        transform_template=request.transform_template,
        filter_expression=request.filter_expression,
        batch_size=request.batch_size,
        batch_interval=request.batch_interval
    )
    
    # Add webhook
    webhook_id = webhook_notifier.add_endpoint(config)
    
    return {
        "message": "Webhook added successfully",
        "webhook_id": webhook_id
    }


@app.delete("/webhooks/{webhook_id}", tags=["Webhooks"])
async def remove_webhook(
    webhook_id: str,
    api_key: str = Depends(get_api_key)
):
    """Remove a webhook"""
    if not webhook_notifier:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Webhook notifier not initialized"
        )
    
    # Check if webhook exists
    if webhook_id not in webhook_notifier.endpoints:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Webhook with ID {webhook_id} not found"
        )
    
    # Remove webhook
    success = webhook_notifier.remove_endpoint(webhook_id)
    
    if success:
        return {
            "message": f"Webhook {webhook_id} removed successfully"
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to remove webhook {webhook_id}"
        )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await websocket.accept()
    
    # Add to connections
    websocket_connections.add(websocket)
    
    try:
        while True:
            # Keep connection alive
            data = await websocket.receive_text()
            
            # Handle commands
            try:
                command = json.loads(data)
                if "type" in command:
                    if command["type"] == "ping":
                        await websocket.send_json({"type": "pong", "timestamp": time.time()})
                    elif command["type"] == "subscribe" and "crawl_id" in command:
                        # Could implement subscription filtering here
                        await websocket.send_json({
                            "type": "subscribed",
                            "crawl_id": command["crawl_id"]
                        })
            except json.JSONDecodeError:
                # Not JSON, ignore
                pass
            
    except WebSocketDisconnect:
        # Remove from connections
        websocket_connections.remove(websocket)


async def broadcast_status_update(crawl_id: str, event: str, error: str = None):
    """Broadcast status update to all WebSocket clients"""
    if not websocket_connections:
        return
    
    # Prepare message
    message = {
        "type": "status_update",
        "crawl_id": crawl_id,
        "event": event,
        "timestamp": time.time()
    }
    
    if error:
        message["error"] = error
    
    # Add crawl data if available
    if crawl_id in active_crawls:
        crawl_data = active_crawls[crawl_id]
        message["status"] = crawl_data["status"]
        message["stats"] = crawl_data["stats"]
    
    # Broadcast to all connections
    disconnected = set()
    for connection in websocket_connections:
        try:
            await connection.send_json(message)
        except Exception:
            # Connection probably closed
            disconnected.add(connection)
    
    # Remove disconnected clients
    for connection in disconnected:
        if connection in websocket_connections:
            websocket_connections.remove(connection)


#
# Main Function
#

def main():
    """Run the API server"""
    host = os.environ.get("API_HOST", "0.0.0.0")
    port = int(os.environ.get("API_PORT", "8000"))
    
    # Configure Uvicorn logging
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_config["formatters"]["default"]["fmt"] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Start server
    uvicorn.run(
        "api_server:app",
        host=host,
        port=port,
        log_config=log_config,
        reload=os.environ.get("API_RELOAD", "false").lower() == "true"
    )


if __name__ == "__main__":
    main()
