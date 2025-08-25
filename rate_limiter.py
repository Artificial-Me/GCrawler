"""
Adaptive Rate Limiter for GhostCrawler

Provides intelligent rate limiting with dynamic adjustment based on server responses.
Features:
- Per-domain rate limiting
- Adaptive delay adjustment based on response metrics
- Detection of rate limiting signals (429, slow responses)
- Circuit breaker pattern for overloaded servers
- Support for rate limit headers
- Comprehensive statistics tracking
"""

import asyncio
import logging
import time
import re
import math
import json
import traceback
import threading
import os
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple, Set, Union, Any, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import deque, defaultdict
import statistics
from urllib.parse import urlparse
from pathlib import Path

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = auto()  # Normal operation
    OPEN = auto()    # Stopping requests
    HALF_OPEN = auto()  # Testing if server recovered


@dataclass
class ResponseMetrics:
    """Metrics for a single response"""
    status_code: int
    response_time: float  # in seconds
    timestamp: float = field(default_factory=time.time)
    error: Optional[str] = None
    retry_after: Optional[int] = None  # seconds to wait if specified by server
    rate_limit_remaining: Optional[int] = None
    rate_limit_reset: Optional[int] = None  # timestamp or seconds


@dataclass
class DomainConfig:
    """Configuration for a specific domain"""
    min_delay: float = 1.0  # Minimum delay between requests in seconds
    max_delay: float = 60.0  # Maximum delay between requests in seconds
    target_response_time: float = 0.8  # Target response time in seconds
    backoff_factor: float = 1.5  # Multiplier for exponential backoff
    recovery_factor: float = 0.9  # Multiplier for recovery (reducing delay)
    max_consecutive_errors: int = 5  # Errors before circuit breaker opens
    circuit_recovery_time: float = 30.0  # Seconds before testing recovery
    response_time_weight: float = 0.3  # Weight for response time in delay calculation
    status_code_weight: float = 0.5  # Weight for status code in delay calculation
    error_weight: float = 0.7  # Weight for errors in delay calculation
    window_size: int = 10  # Number of responses to keep for statistics
    respect_retry_after: bool = True  # Whether to respect Retry-After header
    respect_rate_limit_headers: bool = True  # Whether to respect rate limit headers
    
    def __post_init__(self):
        """Validate configuration values"""
        if self.min_delay <= 0:
            logger.warning("min_delay must be positive, setting to 0.1")
            self.min_delay = 0.1
            
        if self.max_delay < self.min_delay:
            logger.warning(f"max_delay ({self.max_delay}) cannot be less than min_delay ({self.min_delay}), adjusting")
            self.max_delay = max(self.min_delay * 2, self.max_delay)
            
        if self.window_size < 1:
            logger.warning("window_size must be at least 1, setting to 1")
            self.window_size = 1


@dataclass
class DomainStats:
    """Statistics for a specific domain"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rate_limited_requests: int = 0
    current_delay: float = 1.0
    last_request_time: float = 0.0
    consecutive_errors: int = 0
    circuit_state: CircuitState = CircuitState.CLOSED
    circuit_open_until: float = 0.0
    response_metrics: deque = field(default_factory=lambda: deque(maxlen=10))
    avg_response_time: float = 0.0
    min_response_time: float = float('inf')
    max_response_time: float = 0.0
    last_optimized: float = 0.0  # Timestamp of last optimization
    
    def update_response_time_stats(self) -> None:
        """Update response time statistics based on stored metrics"""
        if not self.response_metrics:
            return
        
        response_times = [m.response_time for m in self.response_metrics 
                         if m.response_time is not None]
        
        if response_times:
            self.avg_response_time = sum(response_times) / len(response_times)
            self.min_response_time = min(response_times)
            self.max_response_time = max(response_times)
    
    def update_window_size(self, size: int) -> None:
        """Update the window size for response metrics"""
        if size < 1:
            size = 1
        
        # Create new deque with desired size
        new_metrics = deque(maxlen=size)
        
        # Copy existing metrics to new deque
        for metric in self.response_metrics:
            new_metrics.append(metric)
            
        self.response_metrics = new_metrics


class RateLimitDetector:
    """Detects rate limiting based on various signals"""
    
    @staticmethod
    def is_rate_limited(status_code: int, response_time: float, 
                       headers: Optional[Dict[str, str]] = None) -> Tuple[bool, Optional[int]]:
        """
        Check if a response indicates rate limiting
        
        Args:
            status_code: HTTP status code
            response_time: Response time in seconds
            headers: Response headers
            
        Returns:
            Tuple of (is_rate_limited, retry_after_seconds)
        """
        # Check explicit rate limit status code
        if status_code == 429:
            retry_after = RateLimitDetector._parse_retry_after(headers)
            return True, retry_after
        
        # Check other status codes that might indicate rate limiting
        if status_code in (403, 503):
            retry_after = RateLimitDetector._parse_retry_after(headers)
            return True, retry_after
            
        # Check for rate limit headers
        if headers:
            remaining = RateLimitDetector._parse_rate_limit_remaining(headers)
            if remaining is not None and remaining <= 0:
                reset = RateLimitDetector._parse_rate_limit_reset(headers)
                return True, reset
        
        # Check for very slow responses as potential rate limiting
        if response_time > 5.0:  # 5 seconds is quite slow
            return True, None
            
        return False, None
    
    @staticmethod
    def _parse_retry_after(headers: Optional[Dict[str, str]]) -> Optional[int]:
        """Parse Retry-After header"""
        if not headers:
            return None
            
        retry_after = headers.get('Retry-After') or headers.get('retry-after')
        if not retry_after:
            return None
            
        try:
            # Could be seconds or HTTP date
            if re.match(r'^\d+$', retry_after):
                return int(retry_after)
            else:
                # Try to parse as HTTP date
                try:
                    from email.utils import parsedate_to_datetime
                    date = parsedate_to_datetime(retry_after)
                    now = datetime.now(date.tzinfo)
                    return max(0, int((date - now).total_seconds()))
                except Exception as e:
                    logger.debug(f"Failed to parse Retry-After date: {retry_after}, error: {e}")
                    return None
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse Retry-After: {retry_after}, error: {e}")
            return None
    
    @staticmethod
    def _parse_rate_limit_remaining(headers: Optional[Dict[str, str]]) -> Optional[int]:
        """Parse rate limit remaining header"""
        if not headers:
            return None
            
        # Check various header formats
        for header in ('X-RateLimit-Remaining', 'X-Rate-Limit-Remaining', 
                      'RateLimit-Remaining', 'X-API-Rate-Limit-Remaining'):
            value = headers.get(header) or headers.get(header.lower())
            if value:
                try:
                    return int(value)
                except (ValueError, TypeError):
                    pass
        
        return None
    
    @staticmethod
    def _parse_rate_limit_reset(headers: Optional[Dict[str, str]]) -> Optional[int]:
        """Parse rate limit reset header"""
        if not headers:
            return None
            
        # Check various header formats
        for header in ('X-RateLimit-Reset', 'X-Rate-Limit-Reset', 
                      'RateLimit-Reset', 'X-API-Rate-Limit-Reset'):
            value = headers.get(header) or headers.get(header.lower())
            if value:
                try:
                    reset_time = int(value)
                    # If it's a timestamp, convert to seconds from now
                    if reset_time > time.time() - 86400:  # More than a day ago, it's a timestamp
                        return max(0, int(reset_time - time.time()))
                    return reset_time  # Already in seconds
                except (ValueError, TypeError):
                    pass
        
        return None


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter that dynamically adjusts delays based on server responses
    
    Features:
    - Per-domain rate limiting
    - Dynamic delay adjustment based on response metrics
    - Circuit breaker pattern for overloaded servers
    - Support for rate limit headers
    - Comprehensive statistics tracking
    """
    
    def __init__(self, default_config: Optional[DomainConfig] = None):
        """
        Initialize the rate limiter
        
        Args:
            default_config: Default configuration for all domains
        """
        self.default_config = default_config or DomainConfig()
        self.domain_configs: Dict[str, DomainConfig] = {}
        self.domain_stats: Dict[str, DomainStats] = defaultdict(DomainStats)
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._global_lock = threading.RLock()  # For thread-safe operations
        logger.info("AdaptiveRateLimiter initialized with default config: "
                   f"min_delay={self.default_config.min_delay}, "
                   f"max_delay={self.default_config.max_delay}")
    
    def extract_domain(self, url: str) -> str:
        """Extract domain from URL for rate limiting purposes"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            # Remove port if present
            if ':' in domain:
                domain = domain.split(':', 1)[0]
            return domain.lower()
        except Exception as e:
            logger.warning(f"Error extracting domain from {url}: {e}")
            # Fallback to using the whole URL as the key
            return url
    
    def set_domain_config(self, domain: str, config: DomainConfig) -> None:
        """
        Set configuration for a specific domain
        
        Args:
            domain: Domain name
            config: Domain-specific configuration
        """
        with self._global_lock:
            domain = domain.lower()
            self.domain_configs[domain] = config
            
            # Update window size in stats if needed
            if domain in self.domain_stats:
                self.domain_stats[domain].update_window_size(config.window_size)
                
            logger.info(f"Set custom rate limit config for domain: {domain}, "
                       f"min_delay={config.min_delay}, max_delay={config.max_delay}")
    
    def get_domain_config(self, domain: str) -> DomainConfig:
        """
        Get configuration for a domain
        
        Args:
            domain: Domain name
            
        Returns:
            Domain configuration (or default if not set)
        """
        with self._global_lock:
            return self.domain_configs.get(domain.lower(), self.default_config)
    
    def get_domain_stats(self, domain: str) -> DomainStats:
        """
        Get statistics for a domain
        
        Args:
            domain: Domain name
            
        Returns:
            Domain statistics
        """
        with self._global_lock:
            domain = domain.lower()
            if domain not in self.domain_stats:
                # Initialize with correct window size
                stats = DomainStats()
                config = self.get_domain_config(domain)
                stats.update_window_size(config.window_size)
                self.domain_stats[domain] = stats
            return self.domain_stats[domain]
    
    async def wait_before_request(self, url: str) -> bool:
        """
        Wait before making a request to respect rate limits
        
        Args:
            url: Target URL
            
        Returns:
            True if request should proceed, False if circuit is open
        """
        domain = self.extract_domain(url)
        stats = self.domain_stats[domain]
        config = self.get_domain_config(domain)
        
        try:
            async with self._locks[domain]:
                # Check circuit breaker state
                if stats.circuit_state == CircuitState.OPEN:
                    # Check if recovery time has elapsed
                    if time.time() >= stats.circuit_open_until:
                        logger.info(f"Circuit half-open for {domain}, testing recovery")
                        stats.circuit_state = CircuitState.HALF_OPEN
                    else:
                        logger.debug(f"Circuit open for {domain}, blocking request")
                        return False
                
                # Calculate time to wait
                now = time.time()
                time_since_last_request = now - stats.last_request_time
                
                # Ensure minimum delay between requests
                wait_time = max(0, stats.current_delay - time_since_last_request)
                
                if wait_time > 0:
                    logger.debug(f"Waiting {wait_time:.2f}s before request to {domain}")
                    await asyncio.sleep(wait_time)
                
                # Update last request time after waiting
                stats.last_request_time = time.time()
                stats.total_requests += 1
                
                return True
        except Exception as e:
            logger.error(f"Error in wait_before_request for {domain}: {e}")
            logger.debug(traceback.format_exc())
            # In case of error, allow the request to proceed
            return True
    
    async def update_from_response(self, url: str, status_code: int, 
                                  response_time: float, 
                                  headers: Optional[Dict[str, str]] = None,
                                  error: Optional[str] = None) -> None:
        """
        Update rate limiter based on response
        
        Args:
            url: Target URL
            status_code: HTTP status code
            response_time: Response time in seconds
            headers: Response headers
            error: Error message if request failed
        """
        try:
            domain = self.extract_domain(url)
            stats = self.domain_stats[domain]
            config = self.get_domain_config(domain)
            
            # Create response metrics
            metrics = ResponseMetrics(
                status_code=status_code,
                response_time=response_time,
                error=error
            )
            
            # Check for rate limiting
            is_rate_limited, retry_after = RateLimitDetector.is_rate_limited(
                status_code, response_time, headers
            )
            
            if is_rate_limited:
                metrics.retry_after = retry_after
                stats.rate_limited_requests += 1
                logger.warning(f"Rate limit detected for {domain}: status={status_code}, retry_after={retry_after}")
            
            # Parse rate limit headers if available
            if headers and config.respect_rate_limit_headers:
                metrics.rate_limit_remaining = RateLimitDetector._parse_rate_limit_remaining(headers)
                metrics.rate_limit_reset = RateLimitDetector._parse_rate_limit_reset(headers)
            
            async with self._locks[domain]:
                # Add metrics to history
                stats.response_metrics.append(metrics)
                stats.update_response_time_stats()
                
                # Update success/failure counts
                if 200 <= status_code < 400 and not error:
                    stats.successful_requests += 1
                    stats.consecutive_errors = 0
                else:
                    stats.failed_requests += 1
                    stats.consecutive_errors += 1
                
                # Handle circuit breaker logic
                await self._update_circuit_breaker(domain, is_rate_limited, error is not None)
                
                # Update delay based on response
                await self._update_delay(domain, metrics, is_rate_limited)
        except Exception as e:
            logger.error(f"Error updating rate limiter from response for {url}: {e}")
            logger.debug(traceback.format_exc())
    
    async def _update_circuit_breaker(self, domain: str, is_rate_limited: bool, 
                                     is_error: bool) -> None:
        """Update circuit breaker state based on response"""
        try:
            stats = self.domain_stats[domain]
            config = self.get_domain_config(domain)
            
            if stats.circuit_state == CircuitState.CLOSED:
                # Check if we need to open the circuit
                if (is_rate_limited or 
                    stats.consecutive_errors >= config.max_consecutive_errors):
                    
                    # Open the circuit
                    stats.circuit_state = CircuitState.OPEN
                    stats.circuit_open_until = time.time() + config.circuit_recovery_time
                    
                    logger.warning(
                        f"Opening circuit breaker for {domain} due to "
                        f"{'rate limiting' if is_rate_limited else 'consecutive errors'} "
                        f"(consecutive errors: {stats.consecutive_errors})"
                    )
                    
            elif stats.circuit_state == CircuitState.HALF_OPEN:
                # Check if test request was successful
                if not is_rate_limited and not is_error:
                    # Success - close the circuit
                    stats.circuit_state = CircuitState.CLOSED
                    stats.consecutive_errors = 0
                    logger.info(f"Closing circuit breaker for {domain} after successful test")
                else:
                    # Failed test - reopen the circuit
                    stats.circuit_state = CircuitState.OPEN
                    stats.circuit_open_until = time.time() + config.circuit_recovery_time
                    logger.warning(f"Reopening circuit breaker for {domain} after failed test")
        except Exception as e:
            logger.error(f"Error updating circuit breaker for {domain}: {e}")
            logger.debug(traceback.format_exc())
    
    async def _update_delay(self, domain: str, metrics: ResponseMetrics, 
                           is_rate_limited: bool) -> None:
        """Update delay based on response metrics"""
        try:
            stats = self.domain_stats[domain]
            config = self.get_domain_config(domain)
            
            current_delay = stats.current_delay
            
            # If rate limited with Retry-After, respect it
            if is_rate_limited and metrics.retry_after and config.respect_retry_after:
                new_delay = max(current_delay, metrics.retry_after)
                logger.info(f"Setting delay for {domain} to {new_delay}s based on Retry-After header")
                stats.current_delay = min(new_delay, config.max_delay)
                return
            
            # Calculate new delay based on various factors
            adjustment_factor = 1.0
            
            # Factor 1: Response time vs target
            if metrics.response_time > config.target_response_time:
                rt_factor = metrics.response_time / config.target_response_time
                # Apply diminishing returns for very slow responses
                rt_adjustment = min(2.0, 1.0 + math.log10(rt_factor) * config.response_time_weight)
                adjustment_factor *= rt_adjustment
            elif metrics.response_time < config.target_response_time * 0.5:
                # Response is much faster than target, we can speed up a bit
                adjustment_factor *= config.recovery_factor
            
            # Factor 2: Status code
            if is_rate_limited or metrics.status_code >= 500:
                # Severe: Apply full backoff
                adjustment_factor *= config.backoff_factor
            elif 400 <= metrics.status_code < 500:
                # Client error: Apply partial backoff
                adjustment_factor *= (1.0 + (config.backoff_factor - 1.0) * config.status_code_weight)
            elif 200 <= metrics.status_code < 300:
                # Success: No adjustment needed
                pass
            else:
                # Other status codes: Slight backoff
                adjustment_factor *= 1.1
            
            # Factor 3: Error presence
            if metrics.error:
                adjustment_factor *= (1.0 + (config.backoff_factor - 1.0) * config.error_weight)
            
            # Calculate new delay
            new_delay = current_delay * adjustment_factor
            
            # Ensure within bounds
            new_delay = max(config.min_delay, min(new_delay, config.max_delay))
            
            # Apply change
            if abs(new_delay - current_delay) > 0.1:  # Only log significant changes
                logger.debug(
                    f"Adjusting delay for {domain}: {current_delay:.2f}s -> {new_delay:.2f}s "
                    f"(factor: {adjustment_factor:.2f})"
                )
            
            stats.current_delay = new_delay
        except Exception as e:
            logger.error(f"Error updating delay for {domain}: {e}")
            logger.debug(traceback.format_exc())
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics for all domains
        
        Returns:
            Dictionary of domain statistics
        """
        result = {}
        
        with self._global_lock:
            for domain, stats in self.domain_stats.items():
                # Update response time stats before returning
                stats.update_response_time_stats()
                
                result[domain] = {
                    "total_requests": stats.total_requests,
                    "successful_requests": stats.successful_requests,
                    "failed_requests": stats.failed_requests,
                    "rate_limited_requests": stats.rate_limited_requests,
                    "success_rate": (stats.successful_requests / stats.total_requests 
                                    if stats.total_requests > 0 else 0),
                    "current_delay": stats.current_delay,
                    "avg_response_time": stats.avg_response_time,
                    "min_response_time": stats.min_response_time if stats.min_response_time != float('inf') else 0,
                    "max_response_time": stats.max_response_time,
                    "circuit_state": stats.circuit_state.name,
                    "consecutive_errors": stats.consecutive_errors,
                    "last_optimized": datetime.fromtimestamp(stats.last_optimized).isoformat() 
                                      if stats.last_optimized > 0 else None
                }
        
        return result
    
    def reset_stats(self, domain: Optional[str] = None) -> None:
        """
        Reset statistics for a domain or all domains
        
        Args:
            domain: Domain to reset (or all if None)
        """
        with self._global_lock:
            if domain:
                domain = domain.lower()
                if domain in self.domain_stats:
                    config = self.get_domain_config(domain)
                    stats = DomainStats()
                    stats.update_window_size(config.window_size)
                    self.domain_stats[domain] = stats
                    logger.info(f"Reset statistics for domain: {domain}")
            else:
                # Reset all domains but preserve window sizes
                for domain in list(self.domain_stats.keys()):
                    config = self.get_domain_config(domain)
                    stats = DomainStats()
                    stats.update_window_size(config.window_size)
                    self.domain_stats[domain] = stats
                logger.info("Reset statistics for all domains")
    
    def export_stats_json(self) -> str:
        """
        Export statistics as JSON string
        
        Returns:
            JSON string of statistics
        """
        stats = self.get_all_stats()
        
        # Add metadata
        result = {
            "timestamp": datetime.now().isoformat(),
            "domains": stats,
            "total_domains": len(stats),
            "total_requests": sum(s["total_requests"] for s in stats.values()),
            "total_successful": sum(s["successful_requests"] for s in stats.values()),
            "total_failed": sum(s["failed_requests"] for s in stats.values()),
            "total_rate_limited": sum(s["rate_limited_requests"] for s in stats.values()),
            "overall_success_rate": (
                sum(s["successful_requests"] for s in stats.values()) / 
                sum(s["total_requests"] for s in stats.values())
                if sum(s["total_requests"] for s in stats.values()) > 0 else 0
            )
        }
        
        return json.dumps(result, indent=2)
    
    async def detect_optimal_rate(self, url: str, 
                                 max_requests: int = 20,
                                 start_delay: float = 0.5,
                                 timeout: float = 10.0) -> float:
        """
        Detect optimal request rate for a domain
        
        Args:
            url: URL to test
            max_requests: Maximum number of test requests
            start_delay: Initial delay between requests
            timeout: Request timeout in seconds
            
        Returns:
            Optimal delay in seconds
        """
        domain = self.extract_domain(url)
        logger.info(f"Detecting optimal rate for {domain} (max {max_requests} requests)")
        
        # Save original config and stats
        original_config = self.get_domain_config(domain)
        original_stats = None
        with self._global_lock:
            if domain in self.domain_stats:
                original_stats = self.domain_stats[domain]
        
        # Create test config
        test_config = DomainConfig(
            min_delay=0.1,
            max_delay=10.0,
            target_response_time=0.8,
            backoff_factor=1.5,
            window_size=max_requests
        )
        self.set_domain_config(domain, test_config)
        
        # Reset stats for this domain
        with self._global_lock:
            stats = DomainStats()
            stats.update_window_size(test_config.window_size)
            stats.current_delay = start_delay
            self.domain_stats[domain] = stats
        
        # Make test requests with increasing concurrency
        try:
            # Try to import aiohttp
            try:
                import aiohttp
                http_client_available = True
            except ImportError:
                logger.warning("aiohttp not available, using fallback HTTP client")
                http_client_available = False
            
            if http_client_available:
                optimal_delay = await self._detect_with_aiohttp(url, domain, max_requests, timeout)
            else:
                optimal_delay = await self._detect_with_fallback(url, domain, max_requests, timeout)
            
            # Ensure within reasonable bounds
            optimal_delay = max(original_config.min_delay, 
                               min(optimal_delay, original_config.max_delay))
            
            logger.info(
                f"Detected optimal delay for {domain}: {optimal_delay:.2f}s "
                f"(avg response: {self.domain_stats[domain].avg_response_time:.2f}s)"
            )
            
            # Update last optimized timestamp
            with self._global_lock:
                self.domain_stats[domain].last_optimized = time.time()
            
            return optimal_delay
                
        except Exception as e:
            logger.error(f"Error during optimal rate detection for {domain}: {e}")
            logger.error(traceback.format_exc())
            return original_config.min_delay
        finally:
            # Restore original config
            self.set_domain_config(domain, original_config)
            
            # Restore original stats if they existed
            if original_stats:
                with self._global_lock:
                    self.domain_stats[domain] = original_stats
    
    async def _detect_with_aiohttp(self, url: str, domain: str, 
                                  max_requests: int, timeout: float) -> float:
        """Detect optimal rate using aiohttp"""
        import aiohttp
        
        async with aiohttp.ClientSession() as session:
            for i in range(max_requests):
                # Wait according to current delay
                proceed = await self.wait_before_request(url)
                if not proceed:
                    logger.warning(f"Circuit breaker opened during rate detection for {domain}")
                    break
                
                start_time = time.time()
                status_code = 0
                error = None
                headers = {}
                
                try:
                    async with session.get(url, timeout=timeout, 
                                          allow_redirects=True) as response:
                        status_code = response.status
                        headers = dict(response.headers)
                        # Read response to complete the request
                        await response.text()
                except asyncio.TimeoutError:
                    error = "Request timeout"
                    status_code = 408
                except Exception as e:
                    error = str(e)
                    status_code = 500
                
                response_time = time.time() - start_time
                
                # Update rate limiter
                await self.update_from_response(
                    url, status_code, response_time, headers, error
                )
                
                # If we hit rate limits or errors, stop testing
                stats = self.domain_stats[domain]
                if (status_code == 429 or 
                    stats.consecutive_errors >= 3 or
                    stats.circuit_state != CircuitState.CLOSED):
                    logger.warning(
                        f"Stopping rate detection for {domain} due to "
                        f"{'rate limiting' if status_code == 429 else 'errors'}"
                    )
                    break
                
                # Short pause between tests
                await asyncio.sleep(0.1)
        
        # Calculate optimal delay based on test results
        stats = self.domain_stats[domain]
        stats.update_response_time_stats()
        
        if stats.avg_response_time > 0:
            # Start with current delay
            optimal_delay = stats.current_delay
            
            # Adjust based on response time trend
            if stats.avg_response_time > test_config.target_response_time * 1.2:
                # Too slow, increase delay
                optimal_delay *= 1.2
            elif stats.avg_response_time < test_config.target_response_time * 0.8:
                # Fast enough, can decrease delay slightly
                optimal_delay *= 0.9
            
            return optimal_delay
        else:
            logger.warning(f"Could not determine optimal rate for {domain}")
            return self.get_domain_config(domain).min_delay
    
    async def _detect_with_fallback(self, url: str, domain: str, 
                                   max_requests: int, timeout: float) -> float:
        """Fallback method when aiohttp is not available"""
        import urllib.request
        from urllib.error import URLError, HTTPError
        
        for i in range(max_requests):
            # Wait according to current delay
            proceed = await self.wait_before_request(url)
            if not proceed:
                logger.warning(f"Circuit breaker opened during rate detection for {domain}")
                break
            
            start_time = time.time()
            status_code = 0
            error = None
            headers = {}
            
            try:
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    status_code = response.status
                    headers = dict(response.headers)
                    # Read response to complete the request
                    response.read()
            except HTTPError as e:
                status_code = e.code
                headers = dict(e.headers)
                error = f"HTTP Error: {e.reason}"
            except URLError as e:
                error = f"URL Error: {e.reason}"
                status_code = 500
            except Exception as e:
                error = str(e)
                status_code = 500
            
            response_time = time.time() - start_time
            
            # Update rate limiter
            await self.update_from_response(
                url, status_code, response_time, headers, error
            )
            
            # If we hit rate limits or errors, stop testing
            stats = self.domain_stats[domain]
            if (status_code == 429 or 
                stats.consecutive_errors >= 3 or
                stats.circuit_state != CircuitState.CLOSED):
                logger.warning(
                    f"Stopping rate detection for {domain} due to "
                    f"{'rate limiting' if status_code == 429 else 'errors'}"
                )
                break
            
            # Short pause between tests
            await asyncio.sleep(0.1)
        
        # Calculate optimal delay based on test results
        stats = self.domain_stats[domain]
        stats.update_response_time_stats()
        
        if stats.avg_response_time > 0:
            # Start with current delay
            optimal_delay = stats.current_delay
            
            # Adjust based on response time trend
            config = self.get_domain_config(domain)
            if stats.avg_response_time > config.target_response_time * 1.2:
                # Too slow, increase delay
                optimal_delay *= 1.2
            elif stats.avg_response_time < config.target_response_time * 0.8:
                # Fast enough, can decrease delay slightly
                optimal_delay *= 0.9
            
            return optimal_delay
        else:
            logger.warning(f"Could not determine optimal rate for {domain}")
            return self.get_domain_config(domain).min_delay
    
    async def optimize_all_domains(self, test_url_template: str = None,
                                  min_requests: int = 10,
                                  max_age_hours: int = 24) -> Dict[str, float]:
        """
        Optimize rate limits for all domains with activity
        
        Args:
            test_url_template: URL template for testing (e.g., "https://{domain}/")
            min_requests: Minimum requests before optimizing
            max_age_hours: Only optimize domains not optimized in this many hours
            
        Returns:
            Dictionary of domain to optimal delay
        """
        results = {}
        
        # Only optimize domains with enough data
        domains_to_optimize = []
        now = time.time()
        max_age_seconds = max_age_hours * 3600
        
        with self._global_lock:
            for domain, stats in self.domain_stats.items():
                # Check if domain has enough data and hasn't been optimized recently
                if (stats.total_requests >= min_requests and 
                    (stats.last_optimized == 0 or 
                     now - stats.last_optimized > max_age_seconds)):
                    domains_to_optimize.append(domain)
        
        logger.info(f"Optimizing rate limits for {len(domains_to_optimize)} domains")
        
        # Process each domain
        for domain in domains_to_optimize:
            try:
                # Determine test URL
                test_url = None
                if test_url_template:
                    test_url = test_url_template.format(domain=domain)
                else:
                    # Try to find a recent URL for this domain
                    recent_urls = self._find_recent_urls(domain)
                    if recent_urls:
                        test_url = recent_urls[0]
                
                if not test_url:
                    logger.warning(f"No test URL available for {domain}, skipping optimization")
                    continue
                
                # Run optimization
                optimal_delay = await self.detect_optimal_rate(test_url)
                results[domain] = optimal_delay
                
                # Apply the optimal delay
                with self._global_lock:
                    if domain in self.domain_stats:
                        self.domain_stats[domain].current_delay = optimal_delay
                        self.domain_stats[domain].last_optimized = time.time()
                
                logger.info(f"Optimized rate for {domain}: {optimal_delay:.2f}s")
                
                # Small pause between domains to avoid overloading
                await asyncio.sleep(1.0)
                
            except Exception as e:
                logger.error(f"Error optimizing rate for {domain}: {e}")
                logger.error(traceback.format_exc())
        
        return results
    
    def _find_recent_urls(self, domain: str) -> List[str]:
        """Find recent URLs for a domain from metrics history"""
        # This is a placeholder implementation
        # In a real implementation, you would store URLs in metrics
        # or have a separate URL history
        return []

    def save_to_file(self, file_path: str) -> bool:
        """
        Save rate limiter state to file
        
        Args:
            file_path: Path to save state
            
        Returns:
            Success status
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
            
            with self._global_lock:
                # Prepare data to save
                data = {
                    "timestamp": datetime.now().isoformat(),
                    "default_config": self._config_to_dict(self.default_config),
                    "domain_configs": {
                        domain: self._config_to_dict(config)
                        for domain, config in self.domain_configs.items()
                    },
                    "domain_stats": {
                        domain: {
                            "total_requests": stats.total_requests,
                            "successful_requests": stats.successful_requests,
                            "failed_requests": stats.failed_requests,
                            "rate_limited_requests": stats.rate_limited_requests,
                            "current_delay": stats.current_delay,
                            "last_request_time": stats.last_request_time,
                            "consecutive_errors": stats.consecutive_errors,
                            "circuit_state": stats.circuit_state.name,
                            "circuit_open_until": stats.circuit_open_until,
                            "avg_response_time": stats.avg_response_time,
                            "min_response_time": stats.min_response_time if stats.min_response_time != float('inf') else 0,
                            "max_response_time": stats.max_response_time,
                            "last_optimized": stats.last_optimized
                        }
                        for domain, stats in self.domain_stats.items()
                    }
                }
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
                
            logger.info(f"Saved rate limiter state to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving rate limiter state: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def load_from_file(self, file_path: str) -> bool:
        """
        Load rate limiter state from file
        
        Args:
            file_path: Path to load state from
            
        Returns:
            Success status
        """
        try:
            if not os.path.exists(file_path):
                logger.warning(f"State file not found: {file_path}")
                return False
                
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            with self._global_lock:
                # Load default config
                if "default_config" in data:
                    self.default_config = DomainConfig(**data["default_config"])
                
                # Load domain configs
                if "domain_configs" in data:
                    for domain, config_dict in data["domain_configs"].items():
                        self.domain_configs[domain] = DomainConfig(**config_dict)
                
                # Load domain stats
                if "domain_stats" in data:
                    for domain, stats_dict in data["domain_stats"].items():
                        stats = DomainStats()
                        
                        # Copy simple attributes
                        for key, value in stats_dict.items():
                            if hasattr(stats, key) and key != "response_metrics" and key != "circuit_state":
                                setattr(stats, key, value)
                        
                        # Handle circuit state
                        if "circuit_state" in stats_dict:
                            try:
                                stats.circuit_state = CircuitState[stats_dict["circuit_state"]]
                            except (KeyError, ValueError):
                                stats.circuit_state = CircuitState.CLOSED
                        
                        # Set window size based on config
                        config = self.get_domain_config(domain)
                        stats.update_window_size(config.window_size)
                        
                        self.domain_stats[domain] = stats
            
            logger.info(f"Loaded rate limiter state from {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading rate limiter state: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def _config_to_dict(self, config: DomainConfig) -> Dict[str, Any]:
        """Convert DomainConfig to dictionary"""
        return {
            "min_delay": config.min_delay,
            "max_delay": config.max_delay,
            "target_response_time": config.target_response_time,
            "backoff_factor": config.backoff_factor,
            "recovery_factor": config.recovery_factor,
            "max_consecutive_errors": config.max_consecutive_errors,
            "circuit_recovery_time": config.circuit_recovery_time,
            "response_time_weight": config.response_time_weight,
            "status_code_weight": config.status_code_weight,
            "error_weight": config.error_weight,
            "window_size": config.window_size,
            "respect_retry_after": config.respect_retry_after,
            "respect_rate_limit_headers": config.respect_rate_limit_headers
        }


class DomainRateLimitManager:
    """
    Manager for domain-specific rate limiting configurations
    
    Allows loading and saving configurations from/to files
    """
    
    def __init__(self, rate_limiter: AdaptiveRateLimiter):
        """
        Initialize the manager
        
        Args:
            rate_limiter: AdaptiveRateLimiter instance
        """
        self.rate_limiter = rate_limiter
    
    def load_configs(self, config_file: str) -> int:
        """
        Load domain configurations from file
        
        Args:
            config_file: Path to configuration file
            
        Returns:
            Number of configurations loaded
        """
        try:
            if not os.path.exists(config_file):
                logger.warning(f"Configuration file not found: {config_file}")
                return 0
                
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            
            count = 0
            for domain, config_dict in config_data.items():
                try:
                    # Skip non-dictionary values
                    if not isinstance(config_dict, dict):
                        logger.warning(f"Invalid configuration for domain {domain}: not a dictionary")
                        continue
                        
                    # Handle default config specially
                    if domain == "_default":
                        self.rate_limiter.default_config = DomainConfig(**config_dict)
                        logger.info("Updated default domain configuration")
                        continue
                    
                    # Convert dict to DomainConfig
                    config = DomainConfig(**config_dict)
                    self.rate_limiter.set_domain_config(domain, config)
                    count += 1
                except Exception as e:
                    logger.error(f"Error loading configuration for domain {domain}: {e}")
            
            logger.info(f"Loaded {count} domain configurations from {config_file}")
            return count
        except Exception as e:
            logger.error(f"Error loading domain configurations: {e}")
            logger.error(traceback.format_exc())
            return 0
    
    def save_configs(self, config_file: str) -> bool:
        """
        Save domain configurations to file
        
        Args:
            config_file: Path to configuration file
            
        Returns:
            Success status
        """
        try:
            config_data = {}
            
            # Save default config
            config_data["_default"] = self._config_to_dict(self.rate_limiter.default_config)
            
            # Save domain-specific configs
            for domain, config in self.rate_limiter.domain_configs.items():
                config_data[domain] = self._config_to_dict(config)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(config_file)), exist_ok=True)
            
            with open(config_file, 'w') as f:
                json.dump(config_data, f, indent=2)
            
            logger.info(f"Saved {len(config_data)} domain configurations to {config_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving domain configurations: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def _config_to_dict(self, config: DomainConfig) -> Dict[str, Any]:
        """Convert DomainConfig to dictionary"""
        return {
            "min_delay": config.min_delay,
            "max_delay": config.max_delay,
            "target_response_time": config.target_response_time,
            "backoff_factor": config.backoff_factor,
            "recovery_factor": config.recovery_factor,
            "max_consecutive_errors": config.max_consecutive_errors,
            "circuit_recovery_time": config.circuit_recovery_time,
            "response_time_weight": config.response_time_weight,
            "status_code_weight": config.status_code_weight,
            "error_weight": config.error_weight,
            "window_size": config.window_size,
            "respect_retry_after": config.respect_retry_after,
            "respect_rate_limit_headers": config.respect_rate_limit_headers
        }
    
    async def optimize_all_domains(self, test_url_template: str = None,
                                  min_requests: int = 10,
                                  max_age_hours: int = 24) -> Dict[str, float]:
        """
        Optimize rate limits for all domains with activity
        
        Args:
            test_url_template: URL template for testing (e.g., "https://{domain}/")
            min_requests: Minimum requests before optimizing
            max_age_hours: Only optimize domains not optimized in this many hours
            
        Returns:
            Dictionary of domain to optimal delay
        """
        return await self.rate_limiter.optimize_all_domains(
            test_url_template, min_requests, max_age_hours
        )


# Example usage
async def example_usage():
    # Create rate limiter with default configuration
    rate_limiter = AdaptiveRateLimiter()
    
    # Set custom configuration for a specific domain
    custom_config = DomainConfig(
        min_delay=2.0,
        max_delay=30.0,
        target_response_time=1.0,
        backoff_factor=2.0
    )
    rate_limiter.set_domain_config("example.com", custom_config)
    
    # Wait before making a request
    url = "https://example.com/page1"
    proceed = await rate_limiter.wait_before_request(url)
    
    if proceed:
        # Make the request (example)
        start_time = time.time()
        status_code = 200  # This would be the actual response status
        response_time = time.time() - start_time
        headers = {"X-RateLimit-Remaining": "98"}
        
        # Update rate limiter with response information
        await rate_limiter.update_from_response(
            url, status_code, response_time, headers
        )
    
    # Get statistics
    stats = rate_limiter.get_all_stats()
    print(json.dumps(stats, indent=2))
    
    # Export statistics
    stats_json = rate_limiter.export_stats_json()
    
    # Detect optimal rate
    optimal_delay = await rate_limiter.detect_optimal_rate("https://example.com/test")
    print(f"Optimal delay: {optimal_delay}s")
    
    # Save and load configurations
    manager = DomainRateLimitManager(rate_limiter)
    manager.save_configs("rate_limits.json")
    manager.load_configs("rate_limits.json")
    
    # Optimize all domains
    results = await manager.optimize_all_domains("https://{domain}/")
    print(f"Optimized {len(results)} domains")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # This would run the example in an async environment
    # asyncio.run(example_usage())
    pass
