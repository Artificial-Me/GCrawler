"""
Webhook Notification System for GhostCrawler

Provides a robust webhook notification system for crawl events with features like:
- Multiple webhook endpoints
- Event filtering
- Retry logic
- Authentication support
- Payload transformation
- Circuit breaker pattern
- Event queue with persistence
"""

import asyncio
import aiohttp
import json
import logging
import time
import hmac
import hashlib
import base64
import uuid
from enum import Enum, auto
from typing import Dict, List, Any, Optional, Set, Union, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import sqlite3
from pathlib import Path
import os
import re
import gzip
import traceback
from collections import defaultdict, deque
import threading

logger = logging.getLogger(__name__)

class WebhookEventType(Enum):
    """Types of events that can trigger webhooks"""
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


class WebhookAuthType(Enum):
    """Authentication types for webhooks"""
    NONE = "none"
    BEARER_TOKEN = "bearer_token"
    API_KEY = "api_key"
    HMAC = "hmac"
    BASIC = "basic"


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = auto()  # Normal operation
    OPEN = auto()    # Stopping requests
    HALF_OPEN = auto()  # Testing if endpoint recovered


@dataclass
class WebhookEndpointConfig:
    """Configuration for a webhook endpoint"""
    url: str
    event_types: List[WebhookEventType] = field(default_factory=list)
    auth_type: WebhookAuthType = WebhookAuthType.NONE
    auth_credentials: Dict[str, str] = field(default_factory=dict)
    custom_headers: Dict[str, str] = field(default_factory=dict)
    retry_count: int = 3
    retry_delay: float = 5.0
    timeout: float = 10.0
    enabled: bool = True
    transform_template: Optional[str] = None
    filter_expression: Optional[str] = None
    batch_size: int = 1  # 1 means no batching
    batch_interval: float = 0.0  # 0 means no time-based batching
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def __post_init__(self):
        # Convert event types to enum if they are strings
        event_types = []
        for event_type in self.event_types:
            if isinstance(event_type, str):
                try:
                    event_types.append(WebhookEventType[event_type.upper()])
                except KeyError:
                    logger.warning(f"Invalid event type: {event_type}")
            else:
                event_types.append(event_type)
        self.event_types = event_types
        
        # Convert auth type to enum if it's a string
        if isinstance(self.auth_type, str):
            try:
                self.auth_type = WebhookAuthType[self.auth_type.upper()]
            except KeyError:
                logger.warning(f"Invalid auth type: {self.auth_type}, defaulting to NONE")
                self.auth_type = WebhookAuthType.NONE


@dataclass
class WebhookEvent:
    """Webhook event data"""
    event_type: WebhookEventType
    payload: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    endpoint_ids: List[str] = field(default_factory=list)
    retry_count: int = 0
    next_retry: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for serialization"""
        return {
            "id": self.id,
            "event_type": self.event_type.value,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "endpoint_ids": self.endpoint_ids,
            "retry_count": self.retry_count,
            "next_retry": self.next_retry
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WebhookEvent':
        """Create event from dictionary"""
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            event_type=WebhookEventType(data["event_type"]),
            payload=data["payload"],
            timestamp=data.get("timestamp", time.time()),
            endpoint_ids=data.get("endpoint_ids", []),
            retry_count=data.get("retry_count", 0),
            next_retry=data.get("next_retry", 0.0)
        )


@dataclass
class EndpointStats:
    """Statistics for a webhook endpoint"""
    total_sent: int = 0
    successful: int = 0
    failed: int = 0
    last_success: float = 0.0
    last_failure: float = 0.0
    last_error: Optional[str] = None
    consecutive_failures: int = 0
    circuit_state: CircuitState = CircuitState.CLOSED
    circuit_open_until: float = 0.0
    avg_response_time: float = 0.0
    response_times: deque = field(default_factory=lambda: deque(maxlen=10))


class WebhookEventQueue:
    """Persistent queue for webhook events"""
    
    def __init__(self, db_path: str = "webhook_events.db"):
        """
        Initialize the event queue
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._setup_database()
        self._lock = asyncio.Lock()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection with row factory"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _setup_database(self) -> None:
        """Set up the database schema if it doesn't exist"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Create events table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS webhook_events (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp REAL NOT NULL,
                endpoint_ids TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                next_retry REAL DEFAULT 0,
                status TEXT DEFAULT 'pending'
            )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_status ON webhook_events(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_next_retry ON webhook_events(next_retry)')
            
            conn.commit()
            logger.debug("Webhook event queue database setup complete")
        except sqlite3.Error as e:
            logger.error(f"Database setup error: {e}")
        finally:
            if conn:
                conn.close()
    
    async def add_event(self, event: WebhookEvent) -> bool:
        """
        Add an event to the queue
        
        Args:
            event: Webhook event to add
            
        Returns:
            Success status
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                # Serialize endpoint_ids to JSON
                endpoint_ids_json = json.dumps(event.endpoint_ids)
                
                # Serialize payload to JSON
                payload_json = json.dumps(event.payload)
                
                cursor.execute('''
                INSERT OR REPLACE INTO webhook_events
                (id, event_type, payload, timestamp, endpoint_ids, retry_count, next_retry, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    event.id,
                    event.event_type.value,
                    payload_json,
                    event.timestamp,
                    endpoint_ids_json,
                    event.retry_count,
                    event.next_retry,
                    'pending'
                ))
                
                conn.commit()
                logger.debug(f"Added webhook event {event.id} to queue")
                return True
            except sqlite3.Error as e:
                logger.error(f"Error adding event to queue: {e}")
                return False
            finally:
                if conn:
                    conn.close()
    
    async def get_pending_events(self, limit: int = 100) -> List[WebhookEvent]:
        """
        Get pending events from the queue
        
        Args:
            limit: Maximum number of events to retrieve
            
        Returns:
            List of pending webhook events
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            now = time.time()
            
            cursor.execute('''
            SELECT * FROM webhook_events
            WHERE status = 'pending' AND next_retry <= ?
            ORDER BY timestamp ASC
            LIMIT ?
            ''', (now, limit))
            
            rows = cursor.fetchall()
            
            events = []
            for row in rows:
                # Parse endpoint_ids from JSON
                endpoint_ids = json.loads(row['endpoint_ids'])
                
                # Parse payload from JSON
                payload = json.loads(row['payload'])
                
                event = WebhookEvent(
                    id=row['id'],
                    event_type=WebhookEventType(row['event_type']),
                    payload=payload,
                    timestamp=row['timestamp'],
                    endpoint_ids=endpoint_ids,
                    retry_count=row['retry_count'],
                    next_retry=row['next_retry']
                )
                events.append(event)
            
            return events
        except sqlite3.Error as e:
            logger.error(f"Error getting pending events: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    async def mark_event_processed(self, event_id: str) -> bool:
        """
        Mark an event as processed
        
        Args:
            event_id: Event ID
            
        Returns:
            Success status
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cursor.execute('''
                UPDATE webhook_events
                SET status = 'processed'
                WHERE id = ?
                ''', (event_id,))
                
                conn.commit()
                logger.debug(f"Marked webhook event {event_id} as processed")
                return True
            except sqlite3.Error as e:
                logger.error(f"Error marking event as processed: {e}")
                return False
            finally:
                if conn:
                    conn.close()
    
    async def update_event_retry(self, event_id: str, retry_count: int, next_retry: float) -> bool:
        """
        Update event retry information
        
        Args:
            event_id: Event ID
            retry_count: New retry count
            next_retry: Timestamp for next retry
            
        Returns:
            Success status
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cursor.execute('''
                UPDATE webhook_events
                SET retry_count = ?, next_retry = ?
                WHERE id = ?
                ''', (retry_count, next_retry, event_id))
                
                conn.commit()
                logger.debug(f"Updated retry info for webhook event {event_id}")
                return True
            except sqlite3.Error as e:
                logger.error(f"Error updating event retry info: {e}")
                return False
            finally:
                if conn:
                    conn.close()
    
    async def purge_processed_events(self, older_than_hours: int = 24) -> int:
        """
        Purge processed events older than specified hours
        
        Args:
            older_than_hours: Remove processed events older than this many hours
            
        Returns:
            Number of events purged
        """
        async with self._lock:
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                
                cutoff_time = time.time() - (older_than_hours * 3600)
                
                cursor.execute('''
                DELETE FROM webhook_events
                WHERE status = 'processed' AND timestamp < ?
                ''', (cutoff_time,))
                
                purged = cursor.rowcount
                conn.commit()
                
                logger.info(f"Purged {purged} processed webhook events")
                return purged
            except sqlite3.Error as e:
                logger.error(f"Error purging processed events: {e}")
                return 0
            finally:
                if conn:
                    conn.close()


class WebhookNotifier:
    """
    Webhook notification system for GhostCrawler
    
    Features:
    - Multiple webhook endpoints
    - Event filtering
    - Retry logic
    - Authentication support
    - Payload transformation
    - Circuit breaker pattern
    - Event queue with persistence
    """
    
    def __init__(self, queue_path: str = "webhook_events.db"):
        """
        Initialize the webhook notifier
        
        Args:
            queue_path: Path to event queue database file
        """
        self.endpoints: Dict[str, WebhookEndpointConfig] = {}
        self.event_queue = WebhookEventQueue(queue_path)
        self.endpoint_stats: Dict[str, EndpointStats] = defaultdict(EndpointStats)
        self.batch_queues: Dict[str, List[WebhookEvent]] = defaultdict(list)
        self.batch_timers: Dict[str, asyncio.Task] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()
        self._batch_lock = asyncio.Lock()
        logger.info("WebhookNotifier initialized")
    
    async def start(self) -> None:
        """Start the webhook notifier"""
        if self._running:
            return
        
        self._running = True
        self._session = aiohttp.ClientSession()
        self._worker_task = asyncio.create_task(self._process_queue_worker())
        logger.info("WebhookNotifier started")
    
    async def stop(self) -> None:
        """Stop the webhook notifier"""
        if not self._running:
            return
        
        self._running = False
        
        # Cancel worker task
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
        
        # Cancel batch timers
        for timer in self.batch_timers.values():
            timer.cancel()
        self.batch_timers.clear()
        
        # Close HTTP session
        if self._session:
            await self._session.close()
            self._session = None
        
        logger.info("WebhookNotifier stopped")
    
    def add_endpoint(self, config: WebhookEndpointConfig) -> str:
        """
        Add a webhook endpoint
        
        Args:
            config: Webhook endpoint configuration
            
        Returns:
            Endpoint ID
        """
        self.endpoints[config.id] = config
        logger.info(f"Added webhook endpoint {config.id}: {config.url}")
        return config.id
    
    def remove_endpoint(self, endpoint_id: str) -> bool:
        """
        Remove a webhook endpoint
        
        Args:
            endpoint_id: Endpoint ID
            
        Returns:
            Success status
        """
        if endpoint_id in self.endpoints:
            del self.endpoints[endpoint_id]
            logger.info(f"Removed webhook endpoint {endpoint_id}")
            return True
        return False
    
    def get_endpoint(self, endpoint_id: str) -> Optional[WebhookEndpointConfig]:
        """
        Get webhook endpoint configuration
        
        Args:
            endpoint_id: Endpoint ID
            
        Returns:
            Webhook endpoint configuration or None if not found
        """
        return self.endpoints.get(endpoint_id)
    
    def get_endpoints_for_event(self, event_type: WebhookEventType) -> List[WebhookEndpointConfig]:
        """
        Get endpoints that should receive an event
        
        Args:
            event_type: Event type
            
        Returns:
            List of endpoint configurations
        """
        return [
            endpoint for endpoint in self.endpoints.values()
            if endpoint.enabled and (not endpoint.event_types or event_type in endpoint.event_types)
        ]
    
    async def notify(self, event_type: WebhookEventType, payload: Dict[str, Any]) -> List[str]:
        """
        Send a notification for an event
        
        Args:
            event_type: Event type
            payload: Event payload
            
        Returns:
            List of queued event IDs
        """
        # Get endpoints for this event type
        endpoints = self.get_endpoints_for_event(event_type)
        if not endpoints:
            logger.debug(f"No endpoints configured for event type {event_type.value}")
            return []
        
        # Create event
        event = WebhookEvent(
            event_type=event_type,
            payload=payload,
            endpoint_ids=[endpoint.id for endpoint in endpoints]
        )
        
        # Add timestamp to payload if not present
        if "timestamp" not in payload:
            payload["timestamp"] = datetime.now().isoformat()
        
        # Add event type to payload if not present
        if "event_type" not in payload:
            payload["event_type"] = event_type.value
        
        # Process event for each endpoint
        event_ids = []
        for endpoint in endpoints:
            # Apply filter if specified
            if endpoint.filter_expression and not self._evaluate_filter(endpoint.filter_expression, payload):
                logger.debug(f"Event filtered out for endpoint {endpoint.id} by expression: {endpoint.filter_expression}")
                continue
            
            # Check if endpoint uses batching
            if endpoint.batch_size > 1:
                await self._add_to_batch(endpoint.id, event)
            else:
                # Add to queue for immediate processing
                event_copy = WebhookEvent(
                    event_type=event_type,
                    payload=payload.copy(),
                    endpoint_ids=[endpoint.id]
                )
                if await self.event_queue.add_event(event_copy):
                    event_ids.append(event_copy.id)
        
        return event_ids
    
    async def _add_to_batch(self, endpoint_id: str, event: WebhookEvent) -> None:
        """Add event to batch queue for an endpoint"""
        endpoint = self.endpoints.get(endpoint_id)
        if not endpoint:
            return
        
        async with self._batch_lock:
            # Add to batch queue
            self.batch_queues[endpoint_id].append(event)
            
            # Check if we should flush the batch
            if len(self.batch_queues[endpoint_id]) >= endpoint.batch_size:
                await self._flush_batch(endpoint_id)
            elif endpoint.batch_interval > 0 and endpoint_id not in self.batch_timers:
                # Start timer for time-based batching
                self.batch_timers[endpoint_id] = asyncio.create_task(self._batch_timer(endpoint_id, endpoint.batch_interval))
    
    async def _batch_timer(self, endpoint_id: str, interval: float) -> None:
        """Timer for flushing batches after an interval"""
        try:
            await asyncio.sleep(interval)
            async with self._batch_lock:
                if endpoint_id in self.batch_queues and self.batch_queues[endpoint_id]:
                    await self._flush_batch(endpoint_id)
        except asyncio.CancelledError:
            pass
        finally:
            async with self._batch_lock:
                if endpoint_id in self.batch_timers:
                    del self.batch_timers[endpoint_id]
    
    async def _flush_batch(self, endpoint_id: str) -> None:
        """Flush a batch queue to the event queue"""
        if endpoint_id not in self.batch_queues or not self.batch_queues[endpoint_id]:
            return
        
        endpoint = self.endpoints.get(endpoint_id)
        if not endpoint:
            return
        
        # Create a combined event
        events = self.batch_queues[endpoint_id]
        
        # Combine payloads
        combined_payload = {
            "event_type": WebhookEventType.BATCH_COMPLETED.value,
            "timestamp": datetime.now().isoformat(),
            "batch_size": len(events),
            "events": [
                {
                    "event_type": event.event_type.value,
                    "payload": event.payload,
                    "timestamp": datetime.fromtimestamp(event.timestamp).isoformat()
                }
                for event in events
            ]
        }
        
        # Create batch event
        batch_event = WebhookEvent(
            event_type=WebhookEventType.BATCH_COMPLETED,
            payload=combined_payload,
            endpoint_ids=[endpoint_id]
        )
        
        # Add to queue
        await self.event_queue.add_event(batch_event)
        
        # Clear batch queue
        self.batch_queues[endpoint_id] = []
        
        # Cancel timer if active
        if endpoint_id in self.batch_timers:
            self.batch_timers[endpoint_id].cancel()
            del self.batch_timers[endpoint_id]
    
    def _evaluate_filter(self, expression: str, payload: Dict[str, Any]) -> bool:
        """
        Evaluate a filter expression against a payload
        
        Args:
            expression: Filter expression (e.g., "status == 'success'" or "error_count > 5")
            payload: Event payload
            
        Returns:
            True if the payload passes the filter, False otherwise
        """
        try:
            # Simple expression evaluator
            # This is a basic implementation - in production, consider using a proper expression parser
            
            # Replace payload fields with their values
            expr = expression
            for key, value in payload.items():
                if isinstance(value, str):
                    # Replace string values with quoted strings
                    expr = expr.replace(f"{key}", f"'{value}'")
                elif isinstance(value, (int, float, bool)):
                    # Replace numeric values directly
                    expr = expr.replace(f"{key}", str(value))
            
            # Evaluate the expression
            # Note: This is a security risk if not properly sanitized
            # In production, use a safer evaluation method
            result = eval(expr)
            return bool(result)
        except Exception as e:
            logger.error(f"Error evaluating filter expression '{expression}': {e}")
            return False
    
    async def _process_queue_worker(self) -> None:
        """Worker task to process the event queue"""
        while self._running:
            try:
                # Get pending events
                events = await self.event_queue.get_pending_events(limit=50)
                
                if not events:
                    # No events to process, sleep and try again
                    await asyncio.sleep(1.0)
                    continue
                
                # Process events
                for event in events:
                    for endpoint_id in event.endpoint_ids:
                        endpoint = self.endpoints.get(endpoint_id)
                        if not endpoint or not endpoint.enabled:
                            continue
                        
                        # Check circuit breaker
                        stats = self.endpoint_stats[endpoint_id]
                        if stats.circuit_state == CircuitState.OPEN:
                            # Check if recovery time has elapsed
                            if time.time() >= stats.circuit_open_until:
                                logger.info(f"Circuit half-open for endpoint {endpoint_id}, testing recovery")
                                stats.circuit_state = CircuitState.HALF_OPEN
                            else:
                                logger.debug(f"Circuit open for endpoint {endpoint_id}, skipping event {event.id}")
                                continue
                        
                        # Send webhook
                        success, error = await self._send_webhook(endpoint, event)
                        
                        if success:
                            # Mark event as processed
                            await self.event_queue.mark_event_processed(event.id)
                            
                            # Update circuit breaker if in half-open state
                            if stats.circuit_state == CircuitState.HALF_OPEN:
                                stats.circuit_state = CircuitState.CLOSED
                                stats.consecutive_failures = 0
                                logger.info(f"Circuit closed for endpoint {endpoint_id} after successful test")
                        else:
                            # Update retry information
                            retry_count = event.retry_count + 1
                            if retry_count < endpoint.retry_count:
                                # Calculate next retry time with exponential backoff
                                backoff = endpoint.retry_delay * (2 ** (retry_count - 1))
                                next_retry = time.time() + backoff
                                
                                await self.event_queue.update_event_retry(event.id, retry_count, next_retry)
                                logger.debug(f"Scheduled retry #{retry_count} for event {event.id} at {datetime.fromtimestamp(next_retry).isoformat()}")
                            else:
                                # Max retries reached, mark as processed
                                await self.event_queue.mark_event_processed(event.id)
                                logger.warning(f"Max retries reached for event {event.id} to endpoint {endpoint_id}: {error}")
                
                # Purge old processed events periodically
                if random.random() < 0.01:  # ~1% chance each iteration
                    await self.event_queue.purge_processed_events()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in webhook queue worker: {e}")
                await asyncio.sleep(5.0)  # Sleep longer on error
    
    async def _send_webhook(self, endpoint: WebhookEndpointConfig, 
                           event: WebhookEvent) -> Tuple[bool, Optional[str]]:
        """
        Send a webhook to an endpoint
        
        Args:
            endpoint: Webhook endpoint configuration
            event: Event to send
            
        Returns:
            Tuple of (success, error_message)
        """
        if not self._session:
            return False, "HTTP session not initialized"
        
        stats = self.endpoint_stats[endpoint.id]
        stats.total_sent += 1
        
        # Prepare payload
        payload = event.payload.copy()
        
        # Apply template transformation if specified
        if endpoint.transform_template:
            try:
                payload = self._transform_payload(payload, endpoint.transform_template)
            except Exception as e:
                logger.error(f"Error transforming payload: {e}")
                # Continue with original payload
        
        # Prepare headers
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "GhostCrawler-Webhook/1.0",
            "X-Webhook-ID": endpoint.id,
            "X-Event-Type": event.event_type.value,
            "X-Event-ID": event.id
        }
        
        # Add custom headers
        if endpoint.custom_headers:
            headers.update(endpoint.custom_headers)
        
        # Add authentication
        if endpoint.auth_type != WebhookAuthType.NONE:
            self._add_authentication(headers, payload, endpoint)
        
        start_time = time.time()
        
        try:
            async with self._session.post(
                endpoint.url,
                json=payload,
                headers=headers,
                timeout=endpoint.timeout
            ) as response:
                response_time = time.time() - start_time
                stats.response_times.append(response_time)
                stats.avg_response_time = sum(stats.response_times) / len(stats.response_times)
                
                # Check if request was successful
                if 200 <= response.status < 300:
                    stats.successful += 1
                    stats.last_success = time.time()
                    stats.consecutive_failures = 0
                    logger.debug(f"Webhook sent successfully to {endpoint.url} in {response_time:.2f}s")
                    return True, None
                else:
                    # Handle failure
                    error_text = await response.text()
                    error_message = f"HTTP {response.status}: {error_text[:100]}"
                    stats.failed += 1
                    stats.last_failure = time.time()
                    stats.last_error = error_message
                    stats.consecutive_failures += 1
                    
                    # Update circuit breaker
                    await self._update_circuit_breaker(endpoint.id)
                    
                    logger.warning(f"Webhook to {endpoint.url} failed: {error_message}")
                    return False, error_message
                    
        except asyncio.TimeoutError:
            response_time = time.time() - start_time
            error_message = f"Request timed out after {response_time:.2f}s"
            stats.failed += 1
            stats.last_failure = time.time()
            stats.last_error = error_message
            stats.consecutive_failures += 1
            
            # Update circuit breaker
            await self._update_circuit_breaker(endpoint.id)
            
            logger.warning(f"Webhook to {endpoint.url} timed out")
            return False, error_message
            
        except Exception as e:
            error_message = str(e)
            stats.failed += 1
            stats.last_failure = time.time()
            stats.last_error = error_message
            stats.consecutive_failures += 1
            
            # Update circuit breaker
            await self._update_circuit_breaker(endpoint.id)
            
            logger.error(f"Error sending webhook to {endpoint.url}: {e}")
            return False, error_message
    
    async def _update_circuit_breaker(self, endpoint_id: str) -> None:
        """Update circuit breaker state based on failures"""
        stats = self.endpoint_stats[endpoint_id]
        endpoint = self.endpoints.get(endpoint_id)
        
        if not endpoint:
            return
        
        # Default threshold
        failure_threshold = 5
        recovery_time = 60.0
        
        if stats.circuit_state == CircuitState.CLOSED:
            # Check if we need to open the circuit
            if stats.consecutive_failures >= failure_threshold:
                # Open the circuit
                stats.circuit_state = CircuitState.OPEN
                stats.circuit_open_until = time.time() + recovery_time
                
                logger.warning(f"Opening circuit breaker for endpoint {endpoint_id} after {stats.consecutive_failures} consecutive failures")
                
        elif stats.circuit_state == CircuitState.HALF_OPEN:
            # Test request failed, reopen the circuit
            stats.circuit_state = CircuitState.OPEN
            stats.circuit_open_until = time.time() + recovery_time
            logger.warning(f"Reopening circuit breaker for endpoint {endpoint_id} after failed test")
    
    def _add_authentication(self, headers: Dict[str, str], payload: Dict[str, Any], 
                           endpoint: WebhookEndpointConfig) -> None:
        """Add authentication to request headers"""
        auth_type = endpoint.auth_type
        credentials = endpoint.auth_credentials
        
        if auth_type == WebhookAuthType.BEARER_TOKEN:
            token = credentials.get("token")
            if token:
                headers["Authorization"] = f"Bearer {token}"
            else:
                logger.warning(f"Bearer token authentication configured but no token provided for endpoint {endpoint.id}")
                
        elif auth_type == WebhookAuthType.API_KEY:
            key = credentials.get("key")
            key_name = credentials.get("key_name", "X-API-Key")
            if key:
                headers[key_name] = key
            else:
                logger.warning(f"API key authentication configured but no key provided for endpoint {endpoint.id}")
                
        elif auth_type == WebhookAuthType.HMAC:
            secret = credentials.get("secret")
            algorithm = credentials.get("algorithm", "sha256")
            header_name = credentials.get("header_name", "X-Signature")
            
            if secret:
                # Create signature from payload
                payload_str = json.dumps(payload, separators=(',', ':'))
                
                # Get appropriate hash algorithm
                hash_func = getattr(hashlib, algorithm, hashlib.sha256)
                
                # Create HMAC signature
                signature = hmac.new(
                    secret.encode('utf-8'),
                    payload_str.encode('utf-8'),
                    hash_func
                ).digest()
                
                # Add signature to headers
                headers[header_name] = base64.b64encode(signature).decode('ascii')
            else:
                logger.warning(f"HMAC authentication configured but no secret provided for endpoint {endpoint.id}")
                
        elif auth_type == WebhookAuthType.BASIC:
            username = credentials.get("username")
            password = credentials.get("password")
            
            if username and password:
                auth_str = f"{username}:{password}"
                encoded = base64.b64encode(auth_str.encode('utf-8')).decode('ascii')
                headers["Authorization"] = f"Basic {encoded}"
            else:
                logger.warning(f"Basic authentication configured but credentials not provided for endpoint {endpoint.id}")
    
    def _transform_payload(self, payload: Dict[str, Any], template: str) -> Dict[str, Any]:
        """
        Transform payload using a template
        
        Args:
            payload: Original payload
            template: Template string with placeholders (e.g., "{{ field }}")
            
        Returns:
            Transformed payload
        """
        # Simple template rendering
        # In production, consider using a proper template engine like Jinja2
        
        result = template
        
        # Replace placeholders
        for key, value in payload.items():
            placeholder = f"{{{{ {key} }}}}"
            if placeholder in result:
                if isinstance(value, (dict, list)):
                    value_str = json.dumps(value)
                else:
                    value_str = str(value)
                result = result.replace(placeholder, value_str)
        
        # Parse result as JSON
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            # Return as string if not valid JSON
            return {"content": result}
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics for all endpoints
        
        Returns:
            Dictionary of endpoint statistics
        """
        result = {}
        
        for endpoint_id, stats in self.endpoint_stats.items():
            endpoint = self.endpoints.get(endpoint_id)
            if not endpoint:
                continue
                
            result[endpoint_id] = {
                "url": endpoint.url,
                "total_sent": stats.total_sent,
                "successful": stats.successful,
                "failed": stats.failed,
                "success_rate": (stats.successful / stats.total_sent if stats.total_sent > 0 else 0),
                "avg_response_time": stats.avg_response_time,
                "last_success": datetime.fromtimestamp(stats.last_success).isoformat() if stats.last_success > 0 else None,
                "last_failure": datetime.fromtimestamp(stats.last_failure).isoformat() if stats.last_failure > 0 else None,
                "last_error": stats.last_error,
                "consecutive_failures": stats.consecutive_failures,
                "circuit_state": stats.circuit_state.name
            }
        
        return result
    
    def load_endpoints_from_file(self, file_path: str) -> int:
        """
        Load endpoint configurations from a JSON file
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Number of endpoints loaded
        """
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            count = 0
            for endpoint_data in data.get("endpoints", []):
                config = WebhookEndpointConfig(**endpoint_data)
                self.add_endpoint(config)
                count += 1
            
            logger.info(f"Loaded {count} webhook endpoints from {file_path}")
            return count
        except Exception as e:
            logger.error(f"Error loading webhook endpoints from {file_path}: {e}")
            return 0
    
    def save_endpoints_to_file(self, file_path: str) -> bool:
        """
        Save endpoint configurations to a JSON file
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Success status
        """
        try:
            data = {
                "endpoints": [
                    {
                        "id": endpoint.id,
                        "url": endpoint.url,
                        "event_types": [et.value for et in endpoint.event_types],
                        "auth_type": endpoint.auth_type.value,
                        "auth_credentials": endpoint.auth_credentials,
                        "custom_headers": endpoint.custom_headers,
                        "retry_count": endpoint.retry_count,
                        "retry_delay": endpoint.retry_delay,
                        "timeout": endpoint.timeout,
                        "enabled": endpoint.enabled,
                        "transform_template": endpoint.transform_template,
                        "filter_expression": endpoint.filter_expression,
                        "batch_size": endpoint.batch_size,
                        "batch_interval": endpoint.batch_interval
                    }
                    for endpoint in self.endpoints.values()
                ]
            }
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Saved {len(self.endpoints)} webhook endpoints to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving webhook endpoints to {file_path}: {e}")
            return False


# Helper functions for creating common event payloads

def create_crawl_started_payload(urls: List[str], config: Dict[str, Any]) -> Dict[str, Any]:
    """Create payload for CRAWL_STARTED event"""
    return {
        "timestamp": datetime.now().isoformat(),
        "urls_count": len(urls),
        "sample_urls": urls[:5],
        "config": config
    }

def create_url_processed_payload(url: str, success: bool, data: Optional[Dict[str, Any]] = None,
                                elapsed_time: float = 0.0) -> Dict[str, Any]:
    """Create payload for URL_PROCESSED event"""
    return {
        "timestamp": datetime.now().isoformat(),
        "url": url,
        "success": success,
        "elapsed_time": elapsed_time,
        "data_size": len(json.dumps(data)) if data else 0,
        "data_sample": str(data)[:200] if data else None
    }

def create_url_failed_payload(url: str, error: str, retry_count: int = 0,
                             elapsed_time: float = 0.0) -> Dict[str, Any]:
    """Create payload for URL_FAILED event"""
    return {
        "timestamp": datetime.now().isoformat(),
        "url": url,
        "error": error,
        "retry_count": retry_count,
        "elapsed_time": elapsed_time,
        "traceback": traceback.format_exc()
    }

def create_batch_completed_payload(batch_size: int, success_count: int, 
                                  failure_count: int, elapsed_time: float) -> Dict[str, Any]:
    """Create payload for BATCH_COMPLETED event"""
    return {
        "timestamp": datetime.now().isoformat(),
        "batch_size": batch_size,
        "success_count": success_count,
        "failure_count": failure_count,
        "success_rate": success_count / batch_size if batch_size > 0 else 0,
        "elapsed_time": elapsed_time,
        "urls_per_second": batch_size / elapsed_time if elapsed_time > 0 else 0
    }

def create_crawl_completed_payload(total_urls: int, success_count: int,
                                  failure_count: int, elapsed_time: float,
                                  stats: Dict[str, Any]) -> Dict[str, Any]:
    """Create payload for CRAWL_COMPLETED event"""
    return {
        "timestamp": datetime.now().isoformat(),
        "total_urls": total_urls,
        "success_count": success_count,
        "failure_count": failure_count,
        "success_rate": success_count / total_urls if total_urls > 0 else 0,
        "elapsed_time": elapsed_time,
        "urls_per_second": total_urls / elapsed_time if elapsed_time > 0 else 0,
        "stats": stats
    }

def create_error_threshold_payload(error_rate: float, error_count: int,
                                  recent_errors: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create payload for ERROR_THRESHOLD event"""
    return {
        "timestamp": datetime.now().isoformat(),
        "error_rate": error_rate,
        "error_count": error_count,
        "recent_errors": recent_errors
    }

def create_rate_limited_payload(domain: str, status_code: int,
                               retry_after: Optional[int] = None) -> Dict[str, Any]:
    """Create payload for RATE_LIMITED event"""
    return {
        "timestamp": datetime.now().isoformat(),
        "domain": domain,
        "status_code": status_code,
        "retry_after": retry_after,
        "retry_at": (datetime.now() + timedelta(seconds=retry_after)).isoformat() if retry_after else None
    }

def create_progress_update_payload(total_urls: int, processed: int,
                                  success_count: int, failure_count: int,
                                  elapsed_time: float, eta: float) -> Dict[str, Any]:
    """Create payload for PROGRESS_UPDATE event"""
    return {
        "timestamp": datetime.now().isoformat(),
        "total_urls": total_urls,
        "processed": processed,
        "remaining": total_urls - processed,
        "progress_percent": (processed / total_urls * 100) if total_urls > 0 else 0,
        "success_count": success_count,
        "failure_count": failure_count,
        "success_rate": success_count / processed if processed > 0 else 0,
        "elapsed_time": elapsed_time,
        "eta_seconds": eta,
        "eta_time": (datetime.now() + timedelta(seconds=eta)).isoformat() if eta > 0 else None,
        "urls_per_second": processed / elapsed_time if elapsed_time > 0 else 0
    }
