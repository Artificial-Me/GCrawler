#!/usr/bin/env python3
"""
GCrawler API Server Startup Script

This script initializes and starts the GCrawler API server with proper configuration,
logging, error handling, and component initialization. It supports different environments
(development, production, testing) and handles graceful shutdown.

Usage:
    python start_api.py [--env development|production|testing] [--config path/to/config.yaml]

Example:
    python start_api.py --env production --config /etc/gcrawler/config.yaml
"""

import argparse
import asyncio
import atexit
import json
import logging
import os
import platform
import signal
import socket
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    import uvicorn
    UVICORN_AVAILABLE = True
except ImportError:
    UVICORN_AVAILABLE = False

try:
    from fastapi import FastAPI
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# Configure basic logging until proper logging is set up
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("gcrawler.startup")

# Global variables
CONFIG = {}
ENVIRONMENT = "development"
CONFIG_PATH = "config.yaml"
REQUIRED_DIRS = [
    "data",
    "logs",
    "exports",
    "Specs"
]
REQUIRED_PACKAGES = {
    "fastapi": "FastAPI framework",
    "uvicorn": "ASGI server",
    "pydantic": "Data validation",
    "yaml": "YAML configuration",
    "aiofiles": "Async file operations",
    "tenacity": "Retry logic",
    "psutil": "System monitoring"
}
STARTUP_TIME = datetime.now()
SHUTDOWN_REQUESTED = False
API_SERVER_PROCESS = None


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Start the GCrawler API server")
    parser.add_argument(
        "--env",
        choices=["development", "production", "testing"],
        default="development",
        help="Environment to run in (default: development)"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode"
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only check configuration and system requirements, don't start server"
    )
    return parser.parse_args()


def load_config(config_path: str, environment: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file and apply environment-specific overrides.
    
    Args:
        config_path: Path to the configuration file
        environment: Environment to use (development, production, testing)
        
    Returns:
        Dict containing the configuration
        
    Raises:
        FileNotFoundError: If the configuration file is not found
        ValueError: If the configuration file is invalid
    """
    if not YAML_AVAILABLE:
        logger.error("PyYAML is not installed. Install it with: pip install pyyaml")
        sys.exit(1)
    
    config_file = Path(config_path)
    if not config_file.exists():
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        
        # Validate basic structure
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")
        
        if "environments" not in config:
            logger.warning("No environments section found in configuration")
        elif environment in config["environments"]:
            # Apply environment-specific overrides
            env_config = config["environments"][environment]
            
            # Deep merge environment config into base config
            deep_merge(config, env_config)
        
        # Remove environments section to avoid confusion
        if "environments" in config:
            del config["environments"]
        
        # Apply environment variables (format: GCRAWLER_SECTION_KEY)
        apply_env_overrides(config)
        
        return config
    
    except yaml.YAMLError as e:
        logger.error(f"Error parsing configuration file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge override dict into base dict.
    
    Args:
        base: Base dictionary to merge into
        override: Dictionary with overrides
        
    Returns:
        Merged dictionary
    """
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    
    return base


def apply_env_overrides(config: Dict[str, Any], prefix: str = "GCRAWLER") -> None:
    """
    Apply environment variable overrides to configuration.
    
    Environment variables should be in the format:
    GCRAWLER_SECTION_KEY=value
    
    For example:
    GCRAWLER_API_SERVER_PORT=9000
    
    Args:
        config: Configuration dictionary to update
        prefix: Prefix for environment variables
    """
    for env_var, value in os.environ.items():
        if env_var.startswith(f"{prefix}_"):
            # Remove prefix and split into parts
            parts = env_var[len(prefix) + 1:].lower().split("_")
            
            # Navigate to the correct section
            current = config
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            # Set the value, converting to appropriate type
            key = parts[-1]
            
            # Try to determine the type
            if value.lower() in ("true", "yes", "on", "1"):
                value = True
            elif value.lower() in ("false", "no", "off", "0"):
                value = False
            elif value.isdigit():
                value = int(value)
            elif value.replace(".", "", 1).isdigit() and value.count(".") == 1:
                value = float(value)
            
            current[key] = value
            logger.debug(f"Applied environment override: {env_var}={value}")


def setup_logging(config: Dict[str, Any]) -> None:
    """
    Set up logging based on configuration.
    
    Args:
        config: Configuration dictionary
    """
    if "logging" not in config:
        logger.warning("No logging configuration found, using defaults")
        return
    
    log_config = config["logging"]
    log_level = getattr(logging, log_config.get("level", "info").upper())
    
    # Reset root logger
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Configure root logger
    root_logger.setLevel(log_level)
    
    # Console logging
    if log_config.get("console", {}).get("enabled", True):
        console_level = getattr(
            logging,
            log_config.get("console", {}).get("level", "info").upper()
        )
        console_format = log_config.get("console", {}).get(
            "format",
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(logging.Formatter(console_format))
        
        # Add colored output if requested and colorama is available
        if log_config.get("console", {}).get("colored", False):
            try:
                import colorama
                from colorama import Fore, Style
                
                colorama.init()
                
                class ColoredFormatter(logging.Formatter):
                    COLORS = {
                        'DEBUG': Fore.CYAN,
                        'INFO': Fore.GREEN,
                        'WARNING': Fore.YELLOW,
                        'ERROR': Fore.RED,
                        'CRITICAL': Fore.RED + Style.BRIGHT
                    }
                    
                    def format(self, record):
                        levelname = record.levelname
                        if levelname in self.COLORS:
                            levelname_color = self.COLORS[levelname] + levelname + Style.RESET_ALL
                            record.levelname = levelname_color
                        return super().format(record)
                
                console_handler.setFormatter(ColoredFormatter(console_format))
            except ImportError:
                logger.warning("colorama not installed, using plain console output")
        
        root_logger.addHandler(console_handler)
    
    # File logging
    if log_config.get("file", {}).get("enabled", True):
        file_level = getattr(
            logging,
            log_config.get("file", {}).get("level", "debug").upper()
        )
        file_format = log_config.get("file", {}).get(
            "format",
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_path = log_config.get("file", {}).get("path", "logs/gcrawler.log")
        
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(file_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # Use RotatingFileHandler if backup_count is specified
        backup_count = log_config.get("file", {}).get("backup_count", 0)
        max_size = log_config.get("file", {}).get("max_size_mb", 10) * 1024 * 1024
        
        if backup_count > 0:
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                file_path,
                maxBytes=max_size,
                backupCount=backup_count
            )
        else:
            file_handler = logging.FileHandler(file_path)
        
        file_handler.setLevel(file_level)
        file_handler.setFormatter(logging.Formatter(file_format))
        root_logger.addHandler(file_handler)
    
    # Component-specific logging
    if "components" in log_config:
        for component, comp_config in log_config["components"].items():
            component_logger = logging.getLogger(f"gcrawler.{component}")
            component_level = getattr(logging, comp_config.get("level", "info").upper())
            component_logger.setLevel(component_level)
            
            # Add file handler if specified
            if "file" in comp_config:
                comp_file_path = comp_config["file"]
                
                # Create directory if it doesn't exist
                comp_log_dir = os.path.dirname(comp_file_path)
                if comp_log_dir and not os.path.exists(comp_log_dir):
                    os.makedirs(comp_log_dir, exist_ok=True)
                
                comp_file_handler = logging.FileHandler(comp_file_path)
                comp_file_handler.setLevel(component_level)
                comp_file_handler.setFormatter(logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                ))
                component_logger.addHandler(comp_file_handler)
    
    logger.debug("Logging configured successfully")


def create_directories(config: Dict[str, Any]) -> None:
    """
    Create necessary directories based on configuration.
    
    Args:
        config: Configuration dictionary
    """
    dirs_to_create = REQUIRED_DIRS.copy()
    
    # Add directories from config
    if "api_server" in config and "output_dir" in config["api_server"]:
        dirs_to_create.append(config["api_server"]["output_dir"])
    
    if "export" in config and "output_dir" in config["export"]:
        dirs_to_create.append(config["export"]["output_dir"])
    
    if "crawler" in config and "output_dir" in config["crawler"]:
        dirs_to_create.append(config["crawler"]["output_dir"])
    
    if "logging" in config and "file" in config["logging"] and "path" in config["logging"]["file"]:
        log_dir = os.path.dirname(config["logging"]["file"]["path"])
        if log_dir:
            dirs_to_create.append(log_dir)
    
    # Create directories
    for directory in dirs_to_create:
        try:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {directory}")
        except Exception as e:
            logger.error(f"Failed to create directory {directory}: {e}")
            logger.debug(traceback.format_exc())


def check_system_requirements(config: Dict[str, Any]) -> bool:
    """
    Check if the system meets the requirements.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if all requirements are met, False otherwise
    """
    logger.info("Checking system requirements...")
    all_requirements_met = True
    
    # Check Python version
    python_version = platform.python_version()
    if tuple(map(int, python_version.split("."))) < (3, 7):
        logger.error(f"Python 3.7+ required, found {python_version}")
        all_requirements_met = False
    else:
        logger.info(f"Python version: {python_version} ✓")
    
    # Check required packages
    missing_packages = []
    for package, description in REQUIRED_PACKAGES.items():
        try:
            __import__(package)
            logger.info(f"Package {package} ({description}) ✓")
        except ImportError:
            logger.warning(f"Package {package} ({description}) not found ✗")
            missing_packages.append(package)
            if package in ("fastapi", "uvicorn", "pydantic", "yaml"):
                all_requirements_met = False
    
    if missing_packages:
        logger.warning(f"Missing packages: {', '.join(missing_packages)}")
        logger.info("Install missing packages with: pip install " + " ".join(missing_packages))
    
    # Check memory
    if PSUTIL_AVAILABLE:
        memory = psutil.virtual_memory()
        memory_gb = memory.total / (1024 * 1024 * 1024)
        memory_threshold_gb = config.get("crawler", {}).get("memory_threshold_mb", 49152) / 1024
        
        if memory_gb < memory_threshold_gb:
            logger.warning(
                f"System memory ({memory_gb:.1f} GB) is less than the configured threshold "
                f"({memory_threshold_gb:.1f} GB)"
            )
        else:
            logger.info(f"System memory: {memory_gb:.1f} GB ✓")
    else:
        logger.warning("psutil not available, skipping memory check")
    
    # Check disk space
    if PSUTIL_AVAILABLE:
        disk = psutil.disk_usage(".")
        disk_gb = disk.free / (1024 * 1024 * 1024)
        
        if disk_gb < 1:
            logger.warning(f"Low disk space: {disk_gb:.1f} GB free")
        else:
            logger.info(f"Disk space: {disk_gb:.1f} GB free ✓")
    
    # Check if ports are available
    api_port = config.get("api_server", {}).get("port", 8000)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", api_port))
        sock.close()
        logger.info(f"Port {api_port} is available ✓")
    except socket.error:
        logger.warning(f"Port {api_port} is already in use ✗")
        all_requirements_met = False
    
    # Check write permissions
    for directory in REQUIRED_DIRS:
        if os.path.exists(directory):
            if os.access(directory, os.W_OK):
                logger.info(f"Write permission for {directory} ✓")
            else:
                logger.warning(f"No write permission for {directory} ✗")
                all_requirements_met = False
    
    return all_requirements_met


def generate_api_keys_file(config: Dict[str, Any]) -> None:
    """
    Generate API keys file if it doesn't exist.
    
    Args:
        config: Configuration dictionary
    """
    api_keys_file = Path("api_keys.json")
    
    # If file already exists, don't overwrite it
    if api_keys_file.exists():
        logger.info(f"API keys file already exists: {api_keys_file}")
        return
    
    # Get API keys from config
    api_keys = config.get("api_server", {}).get("api_keys", [])
    
    # If no API keys in config, generate a default one
    if not api_keys:
        import uuid
        api_keys = [str(uuid.uuid4())]
        logger.warning(f"No API keys found in config, generated default key: {api_keys[0]}")
    
    # Write API keys to file
    try:
        with open(api_keys_file, "w") as f:
            json.dump(api_keys, f, indent=2)
        
        logger.info(f"API keys file created: {api_keys_file}")
        
        # Set proper permissions
        os.chmod(api_keys_file, 0o600)  # Only owner can read/write
        
        # Print API keys in development mode
        if ENVIRONMENT == "development":
            logger.info("API Keys (development mode):")
            for i, key in enumerate(api_keys):
                logger.info(f"  {i+1}. {key}")
    except Exception as e:
        logger.error(f"Failed to create API keys file: {e}")
        logger.debug(traceback.format_exc())


def setup_signal_handlers() -> None:
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(sig, frame):
        global SHUTDOWN_REQUESTED
        if SHUTDOWN_REQUESTED:
            logger.warning("Forced shutdown requested, exiting immediately")
            sys.exit(1)
        
        logger.info("Shutdown requested, gracefully shutting down...")
        SHUTDOWN_REQUESTED = True
        
        # Stop API server if running
        if API_SERVER_PROCESS:
            logger.info("Stopping API server...")
            API_SERVER_PROCESS.terminate()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Register atexit handler
    atexit.register(cleanup)


def cleanup() -> None:
    """Clean up resources before exit."""
    logger.info("Cleaning up resources...")
    
    # Calculate uptime
    uptime = datetime.now() - STARTUP_TIME
    hours, remainder = divmod(uptime.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    logger.info(f"Server uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    logger.info("Shutdown complete")


def run_health_check(config: Dict[str, Any]) -> bool:
    """
    Run a health check to ensure all components are working.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if all components are healthy, False otherwise
    """
    logger.info("Running health check...")
    all_healthy = True
    
    # Check if API server is available
    api_host = config.get("api_server", {}).get("host", "0.0.0.0")
    api_port = config.get("api_server", {}).get("port", 8000)
    
    # Only check localhost even if binding to all interfaces
    if api_host == "0.0.0.0":
        api_host = "127.0.0.1"
    
    # Wait for server to start
    max_retries = 5
    retry_delay = 1
    
    for i in range(max_retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((api_host, api_port))
            sock.close()
            
            if result == 0:
                logger.info(f"API server is running on {api_host}:{api_port} ✓")
                break
            else:
                logger.debug(f"API server not responding (attempt {i+1}/{max_retries})")
                time.sleep(retry_delay)
        except Exception as e:
            logger.debug(f"Error checking API server: {e}")
            time.sleep(retry_delay)
    else:
        logger.error(f"API server not responding on {api_host}:{api_port} ✗")
        all_healthy = False
    
    # Try to make a request to the health endpoint
    if all_healthy:
        try:
            import requests
            response = requests.get(f"http://{api_host}:{api_port}/health", timeout=5)
            
            if response.status_code == 200:
                health_data = response.json()
                logger.info(f"API server health check: {health_data.get('status', 'unknown')} ✓")
                
                # Check component status
                components = health_data.get("components", {})
                for component, status in components.items():
                    if status == "ok":
                        logger.info(f"Component {component}: {status} ✓")
                    else:
                        logger.warning(f"Component {component}: {status} ✗")
                        all_healthy = False
            else:
                logger.error(f"API server health check failed: {response.status_code} ✗")
                all_healthy = False
        except ImportError:
            logger.warning("requests not installed, skipping API health check")
        except Exception as e:
            logger.error(f"Error checking API health: {e}")
            logger.debug(traceback.format_exc())
            all_healthy = False
    
    return all_healthy


def start_api_server(config: Dict[str, Any]) -> None:
    """
    Start the API server.
    
    Args:
        config: Configuration dictionary
    """
    if not UVICORN_AVAILABLE:
        logger.error("uvicorn is not installed. Install it with: pip install uvicorn")
        sys.exit(1)
    
    if not FASTAPI_AVAILABLE:
        logger.error("fastapi is not installed. Install it with: pip install fastapi")
        sys.exit(1)
    
    # Get API server configuration
    api_config = config.get("api_server", {})
    host = api_config.get("host", "0.0.0.0")
    port = api_config.get("port", 8000)
    workers = api_config.get("workers", 1)
    reload = api_config.get("reload", False)
    
    # Configure Uvicorn logging
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_config["formatters"]["default"]["fmt"] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Set log level based on environment
    if ENVIRONMENT == "development":
        log_level = "debug"
    else:
        log_level = "info"
    
    logger.info(f"Starting API server on {host}:{port} (environment: {ENVIRONMENT})")
    
    # Start server
    global API_SERVER_PROCESS
    
    if reload:
        # In reload mode, run in the current process
        logger.info("Running in reload mode (development)")
        uvicorn.run(
            "api_server:app",
            host=host,
            port=port,
            log_config=log_config,
            log_level=log_level,
            reload=True
        )
    else:
        # In production mode, use subprocess for better isolation
        import subprocess
        
        cmd = [
            sys.executable, "-m", "uvicorn",
            "api_server:app",
            "--host", host,
            "--port", str(port),
            "--log-level", log_level
        ]
        
        if workers > 1:
            cmd.extend(["--workers", str(workers)])
        
        logger.info(f"Running with {workers} worker(s) (production)")
        
        try:
            API_SERVER_PROCESS = subprocess.Popen(cmd)
            API_SERVER_PROCESS.wait()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
        except Exception as e:
            logger.error(f"Error starting API server: {e}")
            logger.debug(traceback.format_exc())
            sys.exit(1)


def main() -> None:
    """Main function."""
    global CONFIG, ENVIRONMENT, CONFIG_PATH
    
    # Parse command line arguments
    args = parse_arguments()
    ENVIRONMENT = args.env
    CONFIG_PATH = args.config
    
    # Print banner
    print("\n" + "=" * 80)
    print(f"  GCrawler API Server - {ENVIRONMENT.capitalize()} Environment")
    print("=" * 80)
    
    # Load configuration
    logger.info(f"Loading configuration from {CONFIG_PATH}...")
    CONFIG = load_config(CONFIG_PATH, ENVIRONMENT)
    
    # Set up logging
    setup_logging(CONFIG)
    
    # Create directories
    create_directories(CONFIG)
    
    # Generate API keys file
    generate_api_keys_file(CONFIG)
    
    # Check system requirements
    if not check_system_requirements(CONFIG):
        if args.check_only:
            logger.error("System requirements check failed")
            sys.exit(1)
        else:
            logger.warning("System requirements check failed, but continuing anyway")
    
    # If check-only mode, exit here
    if args.check_only:
        logger.info("Check-only mode, not starting server")
        sys.exit(0)
    
    # Set up signal handlers
    setup_signal_handlers()
    
    # Start API server
    try:
        start_api_server(CONFIG)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.error(f"Error starting API server: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)
    
    # Run health check
    if not SHUTDOWN_REQUESTED:
        health_status = run_health_check(CONFIG)
        if health_status:
            logger.info("Health check passed, server is ready")
        else:
            logger.warning("Health check failed, server may not be fully operational")
    
    # Print access information
    api_host = CONFIG.get("api_server", {}).get("host", "0.0.0.0")
    api_port = CONFIG.get("api_server", {}).get("port", 8000)
    
    if api_host == "0.0.0.0":
        # Get all network interfaces
        import socket
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        
        print("\n" + "=" * 80)
        print("  GCrawler API Server is running!")
        print("=" * 80)
        print(f"  Local URL:     http://localhost:{api_port}")
        print(f"  Network URL:   http://{local_ip}:{api_port}")
        print(f"  API Docs URL:  http://localhost:{api_port}/docs")
        print(f"  ReDoc URL:     http://localhost:{api_port}/redoc")
        print("=" * 80 + "\n")
    else:
        print("\n" + "=" * 80)
        print("  GCrawler API Server is running!")
        print("=" * 80)
        print(f"  URL:          http://{api_host}:{api_port}")
        print(f"  API Docs URL: http://{api_host}:{api_port}/docs")
        print(f"  ReDoc URL:    http://{api_host}:{api_port}/redoc")
        print("=" * 80 + "\n")
    
    # Keep main thread alive to handle signals
    try:
        while not SHUTDOWN_REQUESTED and (API_SERVER_PROCESS is None or API_SERVER_PROCESS.poll() is None):
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
