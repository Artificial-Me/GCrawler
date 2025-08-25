#!/usr/bin/env python
"""
GhostCrawler Startup Script

This script serves as the main entry point for the GhostCrawler application.
It handles configuration loading, component initialization, and provides
different operation modes.

Usage:
    python start.py [options]

Options:
    --mode {api,crawler,combined}  Operation mode (default: combined)
    --config PATH                  Path to config file (default: config.json)
    --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}  Log level (default: from config)
    --no-api                       Disable API server
    --no-webhooks                  Disable webhook notifications
    --headless                     Run browsers in headless mode
    --help                         Show this help message and exit
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# Try to import dotenv for environment variable loading
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

# Component imports with fallbacks
try:
    from queue_manager import TaskQueue, TaskStatus, TaskPriority
    TASK_QUEUE_AVAILABLE = True
except ImportError:
    TASK_QUEUE_AVAILABLE = False
    print("WARNING: TaskQueue module not available")

try:
    from data_exporter import DataExporter, ExportFormat, ExportOptions, CompressionType
    DATA_EXPORTER_AVAILABLE = True
except ImportError:
    DATA_EXPORTER_AVAILABLE = False
    print("WARNING: DataExporter module not available")

try:
    from webhook_notifier import WebhookNotifier, WebhookEndpointConfig, WebhookEventType
    WEBHOOK_NOTIFIER_AVAILABLE = True
except ImportError:
    WEBHOOK_NOTIFIER_AVAILABLE = False
    print("WARNING: WebhookNotifier module not available")

try:
    from rate_limiter import AdaptiveRateLimiter, DomainConfig
    RATE_LIMITER_AVAILABLE = True
except ImportError:
    RATE_LIMITER_AVAILABLE = False
    print("WARNING: AdaptiveRateLimiter module not available")

try:
    import uvicorn
    UVICORN_AVAILABLE = True
except ImportError:
    UVICORN_AVAILABLE = False
    print("WARNING: uvicorn not available, API server cannot be started")

# Global variables
config = {}
logger = logging.getLogger("ghostcrawler")
components = {}
shutdown_event = asyncio.Event()


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> None:
    """
    Set up logging configuration
    
    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (None for console only)
    """
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Add file handler if log file specified
    if log_file:
        try:
            # Ensure log directory exists
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
                
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logging.getLogger().addHandler(file_handler)
            logger.info(f"Logging to file: {log_file}")
        except Exception as e:
            logger.error(f"Failed to set up file logging: {e}")
    
    # Set level for specific loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    logger.info(f"Logging initialized with level: {log_level}")


def load_config(config_path: str = "config.json") -> Dict[str, Any]:
    """
    Load configuration from config file and environment variables
    
    Args:
        config_path: Path to config file
        
    Returns:
        Merged configuration dictionary
    """
    # Default configuration
    default_config = {
        "application": {
            "name": "GhostCrawler",
            "log_level": "INFO",
            "log_file": "ghostcrawler.log",
            "data_dir": "./data",
            "temp_dir": "./temp"
        },
        "crawler": {
            "max_browsers": 2,
            "headless": True,
            "batch_size": 10,
            "url_delay": 1.0,
            "max_retries": 3,
            "timeout": 30,
            "output_dir": "Specs"
        },
        "queue_manager": {
            "db_path": "queue.db",
            "max_retries": 3,
            "retry_delay": 60
        },
        "data_exporter": {
            "output_dir": "exports",
            "default_format": "json",
            "compression": "none",
            "pretty_print": True
        },
        "webhook_notifier": {
            "queue_path": "webhook_events.db",
            "enabled": True
        },
        "rate_limiter": {
            "enabled": True
        },
        "api_server": {
            "enabled": True,
            "host": "0.0.0.0",
            "port": 8000
        }
    }
    
    # Load configuration from file
    file_config = {}
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                file_config = json.load(f)
            logger.info(f"Loaded configuration from {config_path}")
        else:
            logger.warning(f"Config file {config_path} not found, using defaults")
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
    
    # Load environment variables
    if DOTENV_AVAILABLE:
        # Try to load from .env file
        env_file = os.environ.get("ENV_FILE", ".env")
        if os.path.exists(env_file):
            load_dotenv(env_file)
            logger.info(f"Loaded environment variables from {env_file}")
    
    # Create merged configuration
    merged_config = deep_merge(default_config, file_config)
    
    # Override with environment variables
    env_overrides = {
        "application.log_level": os.environ.get("LOG_LEVEL"),
        "application.log_file": os.environ.get("LOG_FILE"),
        "crawler.max_browsers": int_env("MAX_BROWSERS"),
        "crawler.headless": bool_env("HEADLESS"),
        "crawler.batch_size": int_env("BATCH_SIZE"),
        "crawler.url_delay": float_env("URL_DELAY"),
        "crawler.max_retries": int_env("MAX_RETRIES"),
        "crawler.output_dir": os.environ.get("OUTPUT_DIR"),
        "queue_manager.db_path": os.environ.get("QUEUE_DB_PATH"),
        "data_exporter.output_dir": os.environ.get("EXPORT_OUTPUT_DIR"),
        "webhook_notifier.queue_path": os.environ.get("WEBHOOK_DB_PATH"),
        "webhook_notifier.enabled": bool_env("WEBHOOKS_ENABLED"),
        "rate_limiter.enabled": bool_env("RATE_LIMIT_ENABLED"),
        "api_server.host": os.environ.get("API_HOST"),
        "api_server.port": int_env("API_PORT"),
        "api_server.workers": int_env("API_WORKERS"),
        "api_server.reload": bool_env("API_RELOAD")
    }
    
    # Apply environment overrides
    for path, value in env_overrides.items():
        if value is not None:  # Only override if environment variable is set
            set_nested_value(merged_config, path, value)
    
    return merged_config


def deep_merge(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries
    
    Args:
        dict1: Base dictionary
        dict2: Dictionary to merge (overrides dict1)
        
    Returns:
        Merged dictionary
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


def set_nested_value(d: Dict[str, Any], path: str, value: Any) -> None:
    """
    Set a value in a nested dictionary using a dot-separated path
    
    Args:
        d: Dictionary to modify
        path: Dot-separated path (e.g., "api_server.host")
        value: Value to set
    """
    parts = path.split('.')
    for part in parts[:-1]:
        if part not in d:
            d[part] = {}
        d = d[part]
    d[parts[-1]] = value


def int_env(name: str) -> Optional[int]:
    """
    Get integer from environment variable
    
    Args:
        name: Environment variable name
        
    Returns:
        Integer value or None if not set or invalid
    """
    value = os.environ.get(name)
    if value is not None:
        try:
            return int(value)
        except ValueError:
            logger.warning(f"Invalid integer value for {name}: {value}")
    return None


def float_env(name: str) -> Optional[float]:
    """
    Get float from environment variable
    
    Args:
        name: Environment variable name
        
    Returns:
        Float value or None if not set or invalid
    """
    value = os.environ.get(name)
    if value is not None:
        try:
            return float(value)
        except ValueError:
            logger.warning(f"Invalid float value for {name}: {value}")
    return None


def bool_env(name: str) -> Optional[bool]:
    """
    Get boolean from environment variable
    
    Args:
        name: Environment variable name
        
    Returns:
        Boolean value or None if not set
    """
    value = os.environ.get(name)
    if value is not None:
        return value.lower() in ('true', 'yes', '1', 'y', 't')
    return None


def validate_config(config: Dict[str, Any]) -> List[str]:
    """
    Validate configuration
    
    Args:
        config: Configuration dictionary
        
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    # Check required sections
    required_sections = ["application", "crawler", "queue_manager", "data_exporter", 
                        "webhook_notifier", "rate_limiter", "api_server"]
    for section in required_sections:
        if section not in config:
            errors.append(f"Missing required section: {section}")
    
    # Check required paths
    data_dir = config.get("application", {}).get("data_dir")
    if data_dir and not os.path.exists(data_dir):
        try:
            os.makedirs(data_dir)
            logger.info(f"Created data directory: {data_dir}")
        except Exception as e:
            errors.append(f"Failed to create data directory {data_dir}: {e}")
    
    temp_dir = config.get("application", {}).get("temp_dir")
    if temp_dir and not os.path.exists(temp_dir):
        try:
            os.makedirs(temp_dir)
            logger.info(f"Created temp directory: {temp_dir}")
        except Exception as e:
            errors.append(f"Failed to create temp directory {temp_dir}: {e}")
    
    # Check API server configuration if enabled
    if config.get("api_server", {}).get("enabled", True):
        if not UVICORN_AVAILABLE:
            errors.append("API server is enabled but uvicorn is not installed")
    
    # Check component availability
    if not TASK_QUEUE_AVAILABLE:
        errors.append("TaskQueue module not available")
    
    if not DATA_EXPORTER_AVAILABLE:
        errors.append("DataExporter module not available")
    
    if config.get("webhook_notifier", {}).get("enabled", True) and not WEBHOOK_NOTIFIER_AVAILABLE:
        errors.append("WebhookNotifier is enabled but module not available")
    
    if config.get("rate_limiter", {}).get("enabled", True) and not RATE_LIMITER_AVAILABLE:
        errors.append("RateLimiter is enabled but module not available")
    
    return errors


async def init_queue_manager() -> Optional[TaskQueue]:
    """
    Initialize task queue manager
    
    Returns:
        TaskQueue instance or None if initialization failed
    """
    if not TASK_QUEUE_AVAILABLE:
        logger.warning("TaskQueue module not available, skipping initialization")
        return None
    
    try:
        queue_config = config.get("queue_manager", {})
        db_path = queue_config.get("db_path", "queue.db")
        
        # Ensure data directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        
        task_queue = TaskQueue(db_path=db_path)
        logger.info(f"TaskQueue initialized with database: {db_path}")
        return task_queue
    except Exception as e:
        logger.error(f"Failed to initialize TaskQueue: {e}")
        return None


def init_data_exporter() -> Optional[DataExporter]:
    """
    Initialize data exporter
    
    Returns:
        DataExporter instance or None if initialization failed
    """
    if not DATA_EXPORTER_AVAILABLE:
        logger.warning("DataExporter module not available, skipping initialization")
        return None
    
    try:
        exporter_config = config.get("data_exporter", {})
        output_dir = exporter_config.get("output_dir", "exports")
        
        # Ensure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        data_exporter = DataExporter(output_dir=output_dir)
        logger.info(f"DataExporter initialized with output directory: {output_dir}")
        return data_exporter
    except Exception as e:
        logger.error(f"Failed to initialize DataExporter: {e}")
        return None


async def init_webhook_notifier() -> Optional[WebhookNotifier]:
    """
    Initialize webhook notifier
    
    Returns:
        WebhookNotifier instance or None if initialization failed or disabled
    """
    webhook_config = config.get("webhook_notifier", {})
    if not webhook_config.get("enabled", True):
        logger.info("WebhookNotifier disabled in configuration")
        return None
    
    if not WEBHOOK_NOTIFIER_AVAILABLE:
        logger.warning("WebhookNotifier module not available, skipping initialization")
        return None
    
    try:
        queue_path = webhook_config.get("queue_path", "webhook_events.db")
        
        # Ensure data directory exists
        db_dir = os.path.dirname(queue_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        
        webhook_notifier = WebhookNotifier(queue_path=queue_path)
        await webhook_notifier.start()
        
        # Add configured endpoints
        endpoints = webhook_config.get("endpoints", [])
        for endpoint_config in endpoints:
            if endpoint_config.get("enabled", True):
                try:
                    # Convert event types from string to enum
                    event_types = []
                    for event_type in endpoint_config.get("event_types", []):
                        event_types.append(WebhookEventType[event_type.upper()])
                    
                    # Create config
                    config_obj = WebhookEndpointConfig(
                        url=endpoint_config["url"],
                        event_types=event_types,
                        auth_type=getattr(WebhookAuthType, endpoint_config.get("auth_type", "NONE").upper()),
                        auth_credentials=endpoint_config.get("auth_credentials", {}),
                        custom_headers=endpoint_config.get("custom_headers", {}),
                        retry_count=endpoint_config.get("retry_count", 3),
                        retry_delay=endpoint_config.get("retry_delay", 5.0),
                        timeout=endpoint_config.get("timeout", 10.0),
                        enabled=True
                    )
                    
                    webhook_notifier.add_endpoint(config_obj)
                    logger.info(f"Added webhook endpoint: {endpoint_config['url']}")
                except Exception as e:
                    logger.error(f"Failed to add webhook endpoint {endpoint_config.get('url')}: {e}")
        
        logger.info(f"WebhookNotifier initialized with queue: {queue_path}")
        return webhook_notifier
    except Exception as e:
        logger.error(f"Failed to initialize WebhookNotifier: {e}")
        return None


def init_rate_limiter() -> Optional[AdaptiveRateLimiter]:
    """
    Initialize adaptive rate limiter
    
    Returns:
        AdaptiveRateLimiter instance or None if initialization failed or disabled
    """
    rate_limiter_config = config.get("rate_limiter", {})
    if not rate_limiter_config.get("enabled", True):
        logger.info("AdaptiveRateLimiter disabled in configuration")
        return None
    
    if not RATE_LIMITER_AVAILABLE:
        logger.warning("AdaptiveRateLimiter module not available, skipping initialization")
        return None
    
    try:
        # Create default config
        default_config_dict = rate_limiter_config.get("default_config", {})
        default_config = DomainConfig(
            min_delay=default_config_dict.get("min_delay", 1.0),
            max_delay=default_config_dict.get("max_delay", 60.0),
            target_response_time=default_config_dict.get("target_response_time", 0.8),
            backoff_factor=default_config_dict.get("backoff_factor", 1.5),
            recovery_factor=default_config_dict.get("recovery_factor", 0.9),
            max_consecutive_errors=default_config_dict.get("max_consecutive_errors", 5),
            circuit_recovery_time=default_config_dict.get("circuit_recovery_time", 30.0),
            response_time_weight=default_config_dict.get("response_time_weight", 0.3),
            status_code_weight=default_config_dict.get("status_code_weight", 0.5),
            error_weight=default_config_dict.get("error_weight", 0.7),
            window_size=default_config_dict.get("window_size", 10),
            respect_retry_after=default_config_dict.get("respect_retry_after", True),
            respect_rate_limit_headers=default_config_dict.get("respect_rate_limit_headers", True)
        )
        
        # Initialize rate limiter
        rate_limiter = AdaptiveRateLimiter(default_config=default_config)
        
        # Add domain-specific configurations
        domain_configs = rate_limiter_config.get("domain_configs", {})
        for domain, domain_config_dict in domain_configs.items():
            # Create domain config by merging with default config values
            merged_config = default_config_dict.copy()
            merged_config.update(domain_config_dict)
            
            domain_config = DomainConfig(
                min_delay=merged_config.get("min_delay", default_config.min_delay),
                max_delay=merged_config.get("max_delay", default_config.max_delay),
                target_response_time=merged_config.get("target_response_time", default_config.target_response_time),
                backoff_factor=merged_config.get("backoff_factor", default_config.backoff_factor),
                recovery_factor=merged_config.get("recovery_factor", default_config.recovery_factor),
                max_consecutive_errors=merged_config.get("max_consecutive_errors", default_config.max_consecutive_errors),
                circuit_recovery_time=merged_config.get("circuit_recovery_time", default_config.circuit_recovery_time),
                window_size=merged_config.get("window_size", default_config.window_size)
            )
            
            rate_limiter.set_domain_config(domain, domain_config)
            logger.info(f"Added rate limit configuration for domain: {domain}")
        
        logger.info("AdaptiveRateLimiter initialized")
        return rate_limiter
    except Exception as e:
        logger.error(f"Failed to initialize AdaptiveRateLimiter: {e}")
        return None


async def init_components(mode: str) -> Dict[str, Any]:
    """
    Initialize all components based on mode
    
    Args:
        mode: Operation mode (api, crawler, combined)
        
    Returns:
        Dictionary of initialized components
    """
    components = {}
    
    # Initialize task queue (needed for all modes)
    components["task_queue"] = await init_queue_manager()
    
    # Initialize data exporter (needed for all modes)
    components["data_exporter"] = init_data_exporter()
    
    # Initialize webhook notifier (if enabled)
    if mode in ["api", "combined"]:
        components["webhook_notifier"] = await init_webhook_notifier()
    
    # Initialize rate limiter (needed for all modes)
    components["rate_limiter"] = init_rate_limiter()
    
    return components


async def start_api_server(components: Dict[str, Any]) -> None:
    """
    Start the API server
    
    Args:
        components: Dictionary of initialized components
    """
    if not UVICORN_AVAILABLE:
        logger.error("Cannot start API server: uvicorn not installed")
        return
    
    api_config = config.get("api_server", {})
    host = api_config.get("host", "0.0.0.0")
    port = api_config.get("port", 8000)
    workers = api_config.get("workers", 1)
    reload = api_config.get("reload", False)
    
    # Import API server module
    try:
        import api_server
        
        # Set components in API server
        api_server.task_queue = components.get("task_queue")
        api_server.data_exporter = components.get("data_exporter")
        api_server.webhook_notifier = components.get("webhook_notifier")
        api_server.adaptive_rate_limiter = components.get("rate_limiter")
        
        logger.info(f"Starting API server on {host}:{port}")
        
        # Configure Uvicorn logging
        log_config = uvicorn.config.LOGGING_CONFIG
        log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        log_config["formatters"]["default"]["fmt"] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        
        # Start server
        server = uvicorn.Server(uvicorn.Config(
            "api_server:app",
            host=host,
            port=port,
            workers=workers,
            log_config=log_config,
            reload=reload
        ))
        
        # Run server and handle shutdown
        await server.serve()
        
    except ImportError:
        logger.error("Failed to import api_server module")
    except Exception as e:
        logger.error(f"Error starting API server: {e}")


async def shutdown_components() -> None:
    """Gracefully shut down all components"""
    logger.info("Shutting down components...")
    
    # Shutdown webhook notifier
    if "webhook_notifier" in components and components["webhook_notifier"]:
        try:
            await components["webhook_notifier"].stop()
            logger.info("Webhook notifier stopped")
        except Exception as e:
            logger.error(f"Error stopping webhook notifier: {e}")
    
    # Close any other resources
    logger.info("All components shut down")


def setup_signal_handlers() -> None:
    """Set up signal handlers for graceful shutdown"""
    def signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()
    
    # Handle SIGINT (Ctrl+C) and SIGTERM
    for sig in [signal.SIGINT, signal.SIGTERM]:
        asyncio.get_event_loop().add_signal_handler(sig, signal_handler)


async def run_crawler_mode() -> None:
    """Run in crawler-only mode"""
    logger.info("Starting in crawler mode")
    
    # TODO: Implement crawler-only mode
    # This would initialize a crawler instance and run it directly
    # without starting the API server
    
    logger.warning("Crawler-only mode not fully implemented yet")
    
    # Wait for shutdown signal
    await shutdown_event.wait()


async def run_api_mode(components: Dict[str, Any]) -> None:
    """Run in API-only mode"""
    logger.info("Starting in API mode")
    
    # Start API server
    await start_api_server(components)


async def run_combined_mode(components: Dict[str, Any]) -> None:
    """Run in combined mode (API + crawler)"""
    logger.info("Starting in combined mode")
    
    # Start API server
    await start_api_server(components)


async def main_async() -> None:
    """Main async function"""
    global config, components
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="GhostCrawler")
    parser.add_argument("--mode", choices=["api", "crawler", "combined"], default="combined",
                      help="Operation mode (default: combined)")
    parser.add_argument("--config", default="config.json", help="Path to config file")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                      help="Log level")
    parser.add_argument("--no-api", action="store_true", help="Disable API server")
    parser.add_argument("--no-webhooks", action="store_true", help="Disable webhook notifications")
    parser.add_argument("--headless", action="store_true", help="Run browsers in headless mode")
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Override config with command line arguments
    if args.log_level:
        config["application"]["log_level"] = args.log_level
    if args.no_api:
        config["api_server"]["enabled"] = False
    if args.no_webhooks:
        config["webhook_notifier"]["enabled"] = False
    if args.headless:
        config["crawler"]["headless"] = True
    
    # Set up logging
    setup_logging(
        config["application"]["log_level"],
        config["application"]["log_file"]
    )
    
    # Validate configuration
    errors = validate_config(config)
    if errors:
        for error in errors:
            logger.error(f"Configuration error: {error}")
        if any(error.startswith("Missing required section") for error in errors):
            logger.error("Fatal configuration errors, exiting")
            return
    
    # Set up signal handlers
    setup_signal_handlers()
    
    # Initialize components
    components = await init_components(args.mode)
    
    # Determine actual mode based on config and arguments
    actual_mode = args.mode
    if args.no_api and actual_mode != "crawler":
        logger.info("API server disabled, switching to crawler mode")
        actual_mode = "crawler"
    
    # Run in selected mode
    try:
        if actual_mode == "api":
            await run_api_mode(components)
        elif actual_mode == "crawler":
            await run_crawler_mode()
        else:  # combined
            await run_combined_mode(components)
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        # Shutdown components
        await shutdown_components()


def main() -> None:
    """Main entry point"""
    try:
        if sys.platform == "win32":
            # Set event loop policy for Windows
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        # Run async main
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Unhandled exception: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
