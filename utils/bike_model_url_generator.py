r"""
GhostCrawler - Motorcycle Model URL Generator
This script takes "specs\input\motorcycle\ultimatespecs_motorcycle_manufacturers.txt" as 
input and iteratively goes through each manufacturer URL to extract motorcycle model lists.
It downloads the RAW HTML file after waiting for DOM content to load and bypassing any 
Cloudflare Turnstiles, then extracts:
1. JSON-LD breadcrumb data 
2. Model links organized by year
3. Manufacturer information
"""

import asyncio
import os
import json
import logging
import time
import gc
import re
from datetime import datetime
from urllib.parse import urlparse, urljoin
from typing import List, Dict, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from pathlib import Path
import aiofiles
from tenacity import retry, stop_after_attempt, wait_exponential
from playwright.async_api import Page, Browser, Route, async_playwright
from camoufox import AsyncNewBrowser
import psutil
import tkinter as tk
from tkinter import filedialog, messagebox
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bike_model_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_proxies_from_file(proxy_file: str = ".config/proxy.json") -> List[Dict[str, Any]]:
    """Load proxy configurations from JSON file"""
    try:
        if not os.path.isabs(proxy_file):
            # Try current directory first, then parent directory
            if os.path.exists(proxy_file):
                proxy_file = os.path.abspath(proxy_file)
            else:
                # Look in parent directory (for specs subfolder)
                parent_proxy = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), proxy_file)
                if os.path.exists(parent_proxy):
                    proxy_file = parent_proxy
                else:
                    proxy_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), proxy_file)
        
        if not os.path.exists(proxy_file):
            logger.warning(f"Proxy file {proxy_file} not found")
            return []
        with open(proxy_file, 'r') as f:
            return json.load(f).get('proxies', [])
    except Exception as e:
        logger.error(f"Error loading proxies from {proxy_file}: {e}")
        return []


def parse_proxy_url(proxy_url: str) -> Dict[str, str]:
    """Parse proxy URL into components"""
    try:
        parsed = urlparse(proxy_url)
        return {
            'server': f"{parsed.scheme}://{parsed.hostname}:{parsed.port}",
            'username': parsed.username,
            'password': parsed.password
        }
    except Exception as e:
        logger.error(f"Error parsing proxy URL: {e}")
        return {}


@dataclass
class CrawlerConfig:
    """Configuration for the motorcycle model crawler"""
    max_browsers: int = 2
    headless: bool = True
    humanize: bool = True
    geoip: bool = True
    block_webrtc: bool = False
    
    # Timeouts
    request_timeout: int = 120000
    navigation_timeout: int = 60000
    wait_until: str = 'domcontentloaded'
    turnstile_timeout: int = 20000
    post_turnstile_wait: int = 4000
    element_timeout: int = 10000
    network_idle_timeout: int = 3000
    stability_wait: int = 2000
    
    # Resource blocking
    block_resources: List[str] = field(default_factory=lambda: [
        'stylesheet', 'image', 'media', 'font', 'other'
    ])
    block_patterns: List[str] = field(default_factory=lambda: [
        '.css', '.webp', '.jpg', '.jpeg', '.png', '.svg', '.gif', 
        '.woff', '.woff2', '.php', '.pdf', '.zip'
    ])
    allow_patterns: List[str] = field(default_factory=lambda: ['/motorcycles-specs/'])
    
    # Proxy settings
    proxy_server: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None
    
    # Processing settings
    batch_size: int = 20
    url_delay: float = 1
    max_retries: int = 1
    memory_threshold_mb: int = 48000
    
    # Output settings
    output_dir: str = "specs/output/motorcycle"
    save_screenshots: bool = False


class ResourceBlocker:
    """Handles request blocking and resource management"""
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.blocked_count = 0
        self.allowed_count = 0
        
    async def setup_blocking(self, page: Page) -> None:
        """Setup request interception and blocking"""
        await page.route('**/*', self._handle_route)
        
    async def _handle_route(self, route: Route) -> None:
        """Handle each request and decide whether to block"""
        url = route.request.url
        url_lower = url.lower()
        
        # Always allow Cloudflare and essential requests
        if ('challenges.cloudflare.com' in url or 
            'turnstile' in url_lower or 
            any(p in url for p in self.config.allow_patterns)):
            await route.continue_()
            self.allowed_count += 1
            return
        
        # Block non-essential resources
        if (route.request.resource_type in self.config.block_resources or
            any(p in url_lower for p in self.config.block_patterns)):
            await route.abort()
            self.blocked_count += 1
        else:
            await route.continue_()
            self.allowed_count += 1


class TurnstileHandler:
    """Handles Cloudflare Turnstile challenges"""
    
    @staticmethod
    async def detect_turnstile(page: Page) -> bool:
        """Detect if Turnstile challenge is present"""
        try:
            count = await page.locator('div.cf-turnstile').count()
            if count:
                print(f"      └─ Turnstile widget found ({count} instance{'s' if count > 1 else ''})")
            return count > 0
        except Exception as e:
            logger.debug(f"Error detecting Turnstile: {e}")
            return False
    
    @staticmethod
    async def wait_for_turnstile(page: Page, timeout: int = 45000) -> bool:
        """Wait for Turnstile to be solved"""
        try:
            print(f"      └─ Waiting for Turnstile solution (max {timeout/1000}s)...")
            logger.info("Turnstile detected, waiting for solution...")
            
            await page.wait_for_selector('div.cf-turnstile', state='visible', timeout=timeout//2)
            print(f"         └─ Turnstile widget is visible")
            
            max_attempts = 15
            check_interval = 2000
            
            for attempt in range(max_attempts):
                print(f"         └─ Checking solution attempt {attempt + 1}/{max_attempts}...")
                
                # Check for token
                token = await page.locator('input[name="cf-turnstile-response"]').get_attribute('value')
                if token:
                    print(f"         [OK] Turnstile solved! (Token received)")
                    try:
                        await page.wait_for_load_state('networkidle', timeout=5000)
                    except:
                        pass
                    return True
                
                # Check if widget is hidden
                if not await page.locator('div.cf-turnstile').is_visible():
                    print(f"         [OK] Turnstile widget hidden (Challenge passed)")
                    try:
                        await page.wait_for_load_state('networkidle', timeout=5000)
                    except:
                        pass
                    return True
                
                # Check if redirected away from challenge
                if not any(x in page.url.lower() for x in ['challenge', 'turnstile']):
                    print(f"         [OK] Redirected away from challenge page")
                    return True
                
                # Increase check interval after several attempts
                if attempt > 5:
                    check_interval = 3000
                
                print(f"         └─ Waiting {check_interval/1000}s before next check...")
                await page.wait_for_timeout(check_interval)
            
            print(f"         [ERROR] Turnstile solution timeout after {max_attempts} attempts")
            logger.warning("Turnstile solution timeout")
            return False
            
        except Exception as e:
            logger.error(f"Error handling Turnstile: {e}")
            return False


class MotorcycleModelExtractor:
    """Extracts motorcycle model data from manufacturer pages"""
    
    @staticmethod
    def _extract_manufacturer_from_url(url: str) -> str:
        """Extract manufacturer name from URL structure"""
        try:
            # Parse URL pattern: /motorcycles-specs/{manufacturer}/
            # Example: https://www.ultimatespecs.com/motorcycles-specs/acabion/
            parsed_url = urlparse(url)
            path_parts = [part for part in parsed_url.path.split('/') if part]
            
            if len(path_parts) >= 2 and path_parts[0] == 'motorcycles-specs':
                manufacturer = path_parts[1]
                # Capitalize first letter for display
                return manufacturer.capitalize()
            
            return ''
        except Exception as e:
            logger.debug(f"Error extracting manufacturer from URL: {e}")
            return ''
    
    @staticmethod
    async def extract_model_data(page: Page, manufacturer_url: str) -> Dict[str, Any]:
        """Extract motorcycle model data from the page"""
        data = {
            'url': manufacturer_url,
            'timestamp': datetime.now().isoformat(),
            'manufacturer': '',
            'jsonld_data': {},
            'model_links': [],
            'years_available': []
        }
        
        try:
            # Extract manufacturer from URL structure first (most reliable)
            url_manufacturer = MotorcycleModelExtractor._extract_manufacturer_from_url(manufacturer_url)
            if url_manufacturer:
                data['manufacturer'] = url_manufacturer
                print(f"      [OK] Manufacturer extracted from URL: {url_manufacturer}")
            
            # Extract JSON-LD breadcrumb data
            print(f"      [INFO] Extracting JSON-LD breadcrumb data...")
            jsonld_scripts = await page.locator('script[type="application/ld+json"]').all()
            print(f"         Found {len(jsonld_scripts)} JSON-LD script(s)")
            
            for i, script in enumerate(jsonld_scripts):
                try:
                    content = await script.inner_text()
                    if content:
                        content = content.strip()
                        parsed = json.loads(content)
                        
                        if isinstance(parsed, dict) and parsed.get('@type') == 'BreadcrumbList':
                            data['jsonld_data'] = parsed
                            print(f"         [OK] Found BreadcrumbList in script {i+1}")
                            
                            # Use JSON-LD manufacturer name if URL extraction failed
                            if not data['manufacturer']:
                                items = parsed.get('itemListElement', [])
                                for item in items:
                                    if item.get('position') == 2:  # Manufacturer is position 2
                                        data['manufacturer'] = item.get('name', '')
                                        break
                except json.JSONDecodeError as e:
                    print(f"         [WARNING] Script {i+1} has JSON error: {e}")
                    continue
                except Exception as e:
                    print(f"         [WARNING] Script {i+1} error: {str(e)[:50]}")
                    continue
            
            # Extract manufacturer name from breadcrumb div as fallback
            if not data['manufacturer']:
                print(f"      [INFO] Extracting manufacturer from breadcrumb div...")
                breadcrumb_selector = 'div.col-md-12 > span'
                breadcrumb_elem = page.locator(breadcrumb_selector).first
                if await breadcrumb_elem.count() > 0:
                    # Get the parent div that contains the manufacturer name
                    parent_div = breadcrumb_elem.locator('..').first
                    text_content = await parent_div.text_content()
                    # Extract manufacturer name (text after the ">" symbol)
                    lines = text_content.strip().split('\n')
                    for line in lines:
                        line = line.strip()
                        if '>' in line and not line.startswith('<'):
                            data['manufacturer'] = line.split('>')[-1].strip()
                            break
                
                if data['manufacturer']:
                    print(f"         [OK] Manufacturer extracted from page: {data['manufacturer']}")
                else:
                    print(f"         [WARNING] Could not extract manufacturer name")
            
            # Extract year links
            print(f"      [INFO] Extracting available years...")
            year_links = await page.locator('div[style*="line-height:30px"] a[href^="#"]').all()
            years = []
            for link in year_links:
                href = await link.get_attribute('href')
                if href and href.startswith('#'):
                    year = href[1:]  # Remove the #
                    if year.isdigit():
                        years.append(int(year))
            
            data['years_available'] = sorted(set(years), reverse=True)
            print(f"         [OK] Found {len(data['years_available'])} years: {data['years_available']}")
            
            # Extract model links for each year
            print(f"      [INFO] Extracting motorcycle model links...")
            model_links = []
            
            # Find all motorcycle spec links
            spec_links = await page.locator('a[href*="/motorcycles-specs/"]').all()
            
            for link in spec_links:
                try:
                    href = await link.get_attribute('href')
                    text = await link.text_content()
                    
                    if href and text and 'Specs' in text:
                        # Clean up the text
                        model_name = text.replace('Specs', '').strip()
                        
                        # Extract year from the URL or surrounding context
                        year_match = re.search(r'-(\d{4})$', href.rstrip('/'))
                        model_year = year_match.group(1) if year_match else None
                        
                        # Create full URL
                        full_url = urljoin(manufacturer_url, href)
                        
                        model_data = {
                            'name': model_name,
                            'url': full_url,
                            'year': model_year,
                            'relative_url': href
                        }
                        
                        model_links.append(model_data)
                
                except Exception as e:
                    logger.debug(f"Error extracting link data: {e}")
                    continue
            
            # Remove duplicates and sort
            seen_urls = set()
            unique_models = []
            for model in model_links:
                if model['url'] not in seen_urls:
                    seen_urls.add(model['url'])
                    unique_models.append(model)
            
            # Sort by year (newest first) then by name
            data['model_links'] = sorted(
                unique_models,
                key=lambda x: (-(int(x['year']) if x['year'] else 0), x['name'])
            )
            
            print(f"         [OK] Extracted {len(data['model_links'])} unique motorcycle models")
            
            # Group models by year for summary
            year_counts = {}
            for model in data['model_links']:
                year = model['year'] or 'Unknown'
                year_counts[year] = year_counts.get(year, 0) + 1
            
            print(f"         └─ Models per year: {dict(sorted(year_counts.items(), reverse=True))}")
            
        except Exception as e:
            logger.error(f"Error extracting model data: {e}")
        
        return data


class MotorcycleModelSaver:
    """Saves extracted motorcycle model data"""
    
    def __init__(self, base_dir: str = "specs/output/motorcycle"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = Path("logs")
        self.logs_dir.mkdir(exist_ok=True)
        self.files_saved = 0
    
    async def save_model_data(self, data: Dict[str, Any], manufacturer_url: str) -> Optional[str]:
        """Save the extracted model data to JSON file"""
        try:
            # Extract manufacturer from URL if not in data
            manufacturer = data.get('manufacturer', '')
            if not manufacturer:
                # Fallback: extract from URL
                from urllib.parse import urlparse
                parsed_url = urlparse(manufacturer_url)
                path_parts = [part for part in parsed_url.path.split('/') if part]
                if len(path_parts) >= 2 and path_parts[0] == 'motorcycles-specs':
                    manufacturer = path_parts[1].capitalize()
                else:
                    manufacturer = 'unknown'
            
            # Clean manufacturer name for folder creation
            manufacturer_folder = manufacturer.lower().replace(' ', '_').replace('-', '_')
            
            # Create manufacturer directory
            manufacturer_dir = self.base_dir / manufacturer_folder
            manufacturer_dir.mkdir(parents=True, exist_ok=True)
            
            # Save main data file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = manufacturer_dir / f"{manufacturer_folder}_models_{timestamp}.json"
            
            async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
            
            self.files_saved += 1
            logger.info(f"Saved model data {self.files_saved}: {output_path.name}")
            
            # Save URL list for easy processing
            url_list_path = manufacturer_dir / f"{manufacturer_folder}_model_urls.txt"
            async with aiofiles.open(url_list_path, 'w', encoding='utf-8') as f:
                for model in data['model_links']:
                    await f.write(f"{model['url']}\n")
            
            print(f"   [OK] Files saved:")
            print(f"      └─ Model data: {output_path.name}")
            print(f"      └─ URL list: {url_list_path.name}")
            print(f"   [STATS] Statistics:")
            print(f"      └─ Manufacturer: {data['manufacturer']}")
            print(f"      └─ Models found: {len(data['model_links'])}")
            print(f"      └─ Years covered: {len(data['years_available'])}")
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error saving model data: {e}")
            return None


class BrowserPool:
    """Manages browser instances"""
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.playwright = None
        self._lock = asyncio.Lock()
        self.active_browsers = 0
        self.total_browsers_created = 0
        self.max_concurrent_browsers = config.max_browsers
        
    async def initialize(self) -> None:
        """Initialize the browser pool"""
        print(f"\n[INIT] INITIALIZING MOTORCYCLE MODEL EXTRACTOR")
        print(f"   Max concurrent browsers: {self.max_concurrent_browsers}")
        logger.info(f"Initializing browser pool with max {self.max_concurrent_browsers} browsers")
        
        self.playwright = await async_playwright().start()
        print(f"   [OK] Playwright started")
        logger.info("Browser pool initialized")
    
    async def _create_browser(self, os_type: str = 'windows') -> Browser:
        """Create a new browser instance"""
        print(f"      [CONFIG] Configuring Camoufox browser...")
        args = {
            'headless': self.config.headless,
            'os': os_type,
            'geoip': self.config.geoip,
            'locale': 'en-US',
            'humanize': self.config.humanize,
            'block_webrtc': self.config.block_webrtc
        }
        
        if self.config.proxy_server:
            print(f"      [PROXY] Adding proxy configuration")
            args['proxy'] = {'server': self.config.proxy_server}
            if self.config.proxy_username and self.config.proxy_password:
                args['proxy'].update({
                    'username': self.config.proxy_username,
                    'password': self.config.proxy_password
                })
        
        print(f"      [LAUNCH] Launching browser...")
        try:
            browser = await asyncio.wait_for(
                AsyncNewBrowser(self.playwright, **args),
                timeout=30.0
            )
            print(f"      [OK] Browser ready")
            return browser
        except asyncio.TimeoutError:
            print(f"      [ERROR] Browser creation timeout")
            raise RuntimeError("Browser creation timed out")
        except Exception as e:
            print(f"      [ERROR] Browser creation failed: {e}")
            raise
    
    async def acquire_browser_for_crawl(self) -> Optional[Tuple[Browser, Page, int]]:
        """Acquire a browser for crawling"""
        browser_id = None
        start_time = time.time()
        
        # Wait for available slot
        while time.time() - start_time < 60:
            async with self._lock:
                if self.active_browsers < self.max_concurrent_browsers:
                    self.active_browsers += 1
                    self.total_browsers_created += 1
                    browser_id = self.total_browsers_created
                    break
            await asyncio.sleep(0.1)
        
        if browser_id is None:
            logger.warning("Timeout waiting for browser slot")
            return None
            
        try:
            os_choice = ['windows', 'macos', 'linux'][browser_id % 3]
            print(f"   [CREATE] Creating browser #{browser_id} (OS: {os_choice})")
            browser = await self._create_browser(os_choice)
            page = await browser.new_page()
            logger.info(f"Created browser #{browser_id}")
            return browser, page, browser_id
        except Exception as e:
            async with self._lock:
                self.active_browsers -= 1
            logger.error(f"Failed to create browser: {e}")
            return None
    
    async def destroy_browser_after_crawl(self, browser: Browser, page: Page, browser_id: int = None) -> None:
        """Clean up browser resources"""
        try:
            if page:
                await asyncio.wait_for(page.close(), timeout=5.0)
            if browser:
                await asyncio.wait_for(browser.close(), timeout=10.0)
            print(f"   [DESTROY] Browser #{browser_id} destroyed")
            logger.info(f"Browser #{browser_id} destroyed successfully")
        except Exception as e:
            logger.warning(f"Error destroying browser #{browser_id}: {e}")
        finally:
            async with self._lock:
                self.active_browsers = max(0, self.active_browsers - 1)
            gc.collect()
    
    async def cleanup(self) -> None:
        """Clean up the browser pool"""
        logger.info("Cleaning up browser pool...")
        if self.playwright:
            try:
                await self.playwright.stop()
                logger.info("Playwright stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Playwright: {e}")


class MotorcycleModelGenerator:
    """Main class for generating motorcycle model URLs"""
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.browser_pool = BrowserPool(config)
        self.model_saver = MotorcycleModelSaver(config.output_dir)
        self.turnstile_handler = TurnstileHandler()
        self.extractor = MotorcycleModelExtractor()
        self.urls_processed = 0
        self.urls_failed = 0
        self.start_time = None
    
    async def initialize(self) -> None:
        """Initialize the generator"""
        print(f"\n{'='*80}\n[MOTOR] MOTORCYCLE MODEL URL GENERATOR\n{'='*80}")
        print(f"\nConfiguration:")
        print(f"   Browsers: {self.config.max_browsers}")
        print(f"   Output dir: {self.config.output_dir}")
        print(f"   Headless: {self.config.headless}")
        logger.info("Initializing MotorcycleModelGenerator...")
        
        await self.browser_pool.initialize()
        self.start_time = time.time()
        print(f"\n[READY] MOTORCYCLE MODEL GENERATOR READY\n{'='*80}\n")
        logger.info("MotorcycleModelGenerator initialized successfully")
    
    async def _wait_for_page_stability(self, page: Page, context: str = "general") -> None:
        """Wait for page to stabilize"""
        try:
            logger.debug(f"Waiting for page stability ({context})")
            await page.wait_for_load_state('networkidle', timeout=self.config.network_idle_timeout)
            logger.debug(f"Network idle achieved for {context}")
        except:
            logger.debug(f"Network didn't idle for {context}, continuing anyway")
        
        await page.wait_for_timeout(self.config.stability_wait)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def process_manufacturer_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Process a single manufacturer URL to extract model data"""
        browser = None
        page = None
        browser_id = None
        resource_blocker = None
        
        try:
            print(f"\n[PROCESS] PROCESSING: {url}")
            
            # Acquire browser
            browser_page_tuple = await self.browser_pool.acquire_browser_for_crawl()
            if not browser_page_tuple:
                raise RuntimeError("Failed to acquire browser")
            
            browser, page, browser_id = browser_page_tuple
            
            # Setup resource blocking
            resource_blocker = ResourceBlocker(self.config)
            await resource_blocker.setup_blocking(page)
            
            # Navigate to page
            print(f"   [NAV] Navigating to manufacturer page...")
            response = await page.goto(
                url,
                wait_until=self.config.wait_until,
                timeout=self.config.navigation_timeout
            )
            
            if not response or response.status >= 400:
                raise RuntimeError(f"Failed to load page: {response.status if response else 'No response'}")
            
            print(f"   [OK] Page loaded (Status: {response.status})")
            
            # Wait for page stability
            print(f"   [WAIT] Waiting for page stability...")
            await self._wait_for_page_stability(page, "initial")
            
            # Check for Turnstile
            print(f"   [CHECK] Checking for Cloudflare Turnstile...")
            if await self.turnstile_handler.detect_turnstile(page):
                print(f"   [TURNSTILE] Turnstile detected! Solving...")
                success = await self.turnstile_handler.wait_for_turnstile(page, self.config.turnstile_timeout)
                if not success:
                    raise RuntimeError("Failed to solve Turnstile challenge")
                
                print(f"   [SUCCESS] Turnstile solved successfully")
                await page.wait_for_timeout(self.config.post_turnstile_wait)
                await self._wait_for_page_stability(page, "post-Turnstile")
            else:
                print(f"   [OK] No Turnstile detected")
            
            # Extract model data
            print(f"   [EXTRACT] Extracting motorcycle model data...")
            data = await self.extractor.extract_model_data(page, url)
            
            # Validate extracted data
            if not data['manufacturer']:
                print(f"   [WARNING] Warning: No manufacturer name extracted")
            
            if not data['model_links']:
                print(f"   [WARNING] Warning: No model links found")
                logger.warning(f"No model links found for {url}")
            
            # Save data
            print(f"   [SAVE] Saving model data...")
            output_path = await self.model_saver.save_model_data(data, url)
            
            if output_path:
                print(f"   [SUCCESS] SUCCESS: Data saved successfully")
                self.urls_processed += 1
            else:
                print(f"   [ERROR] Failed to save data")
                self.urls_failed += 1
            
            if resource_blocker:
                logger.info(f"Blocked {resource_blocker.blocked_count} resources, "
                          f"allowed {resource_blocker.allowed_count}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            self.urls_failed += 1
            return None
            
        finally:
            if browser and page:
                await self.browser_pool.destroy_browser_after_crawl(browser, page, browser_id)
    
    async def process_manufacturer_urls(self, urls: List[str]) -> List[Optional[Dict[str, Any]]]:
        """Process multiple manufacturer URLs"""
        logger.info(f"Processing {len(urls)} manufacturer URLs")
        
        results = []
        for url in urls:
            if self.config.url_delay > 0:
                await asyncio.sleep(self.config.url_delay)
            
            result = await self.process_manufacturer_url(url)
            results.append(result)
            
            # Memory management
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 75:
                logger.debug(f"Memory at {memory_percent}%, running GC")
                gc.collect()
        
        return results
    
    async def cleanup(self) -> None:
        """Clean up resources"""
        logger.info("Cleaning up MotorcycleModelGenerator...")
        await self.browser_pool.cleanup()
        
        if self.start_time:
            elapsed = time.time() - self.start_time
            total_urls = self.urls_processed + self.urls_failed
            success_rate = self.urls_processed / total_urls if total_urls > 0 else 0
            
            print(f"\n{'='*80}")
            print(f"[STATS] FINAL STATISTICS")
            print(f"{'='*80}")
            print(f"Total URLs processed: {total_urls}")
            print(f"Successful: {self.urls_processed}")
            print(f"Failed: {self.urls_failed}")
            print(f"Success Rate: {success_rate:.2%}")
            print(f"Total Time: {elapsed/60:.1f} minutes")
            print(f"Files Saved: {self.model_saver.files_saved}")
            print(f"{'='*80}")
            
            logger.info(f"Final stats - Success: {self.urls_processed}, "
                       f"Failed: {self.urls_failed}, Rate: {success_rate:.2%}")


def load_manufacturer_urls(file_path: str) -> List[str]:
    """Load manufacturer URLs from text file"""
    try:
        urls = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith('#') and url.startswith('http'):
                    urls.append(url)
        
        logger.info(f"Loaded {len(urls)} manufacturer URLs from {file_path}")
        return urls
    except Exception as e:
        logger.error(f"Error loading URLs from {file_path}: {e}")
        return []


class InteractiveCLI:
    """Interactive command line interface for bike model URL generator"""
    
    def __init__(self):
        self.config = CrawlerConfig()
        self.input_file = ""
        self.proxies = self.load_proxies()
        load_dotenv()
        
    def load_proxies(self) -> List[Dict[str, Any]]:
        """Load available proxies from .config/proxy.json"""
        return load_proxies_from_file()
    
    def clear_screen(self):
        """Clear the terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def display_header(self):
        """Display the application header"""
        print("=" * 80)
        print("🏍️  MOTORCYCLE MODEL URL GENERATOR - INTERACTIVE MODE")
        print("=" * 80)
        print()
    
    def display_current_config(self):
        """Display current configuration"""
        print("📋 CURRENT CONFIGURATION:")
        print(f"   Input File: {self.input_file or 'Not selected'}")
        print(f"   Output Directory: {self.config.output_dir}")
        print(f"   Max Browsers: {self.config.max_browsers}")
        print(f"   Headless Mode: {self.config.headless}")
        print(f"   Stealth Mode: {self.config.humanize}")
        print(f"   URL Delay: {self.config.url_delay}s")
        print(f"   Batch Size: {self.config.batch_size}")
        print(f"   Max Retries: {self.config.max_retries}")
        
        proxy_info = "None"
        if self.config.proxy_server:
            proxy_info = f"{self.config.proxy_server}"
            if hasattr(self, 'selected_proxy_name'):
                proxy_info = f"{self.selected_proxy_name} ({self.config.proxy_server})"
        print(f"   Proxy: {proxy_info}")
        print()
    
    def browse_input_file(self):
        """Browse for input file"""
        print("📁 Select input file containing manufacturer URLs...")
        try:
            root = tk.Tk()
            root.withdraw()
            root.attributes('-topmost', True)  # Bring to front
            
            initial_dir = os.path.join(os.getcwd(), "specs", "input", "motorcycle")
            if not os.path.exists(initial_dir):
                initial_dir = os.getcwd()
            
            file_path = filedialog.askopenfilename(
                title="Select Manufacturer URLs File",
                filetypes=[
                    ("Text files", "*.txt"),
                    ("All files", "*.*")
                ],
                initialdir=initial_dir
            )
            
            root.destroy()
        except Exception as e:
            print(f"❌ Error opening file browser: {e}")
            print("💡 Fallback: Please enter the file path manually:")
            file_path = input("File path: ").strip().strip('"')
        
        if file_path:
            self.input_file = file_path
            print(f"✅ Selected: {file_path}")
            
            # Preview first few URLs
            try:
                with open(file_path, 'r') as f:
                    lines = f.readlines()[:5]
                    url_count = sum(1 for line in lines if line.strip() and not line.startswith('#'))
                    print(f"📊 Preview (showing first 5 lines, ~{url_count} URLs detected):")
                    for line in lines:
                        if line.strip():
                            print(f"   {line.strip()}")
            except Exception as e:
                print(f"⚠️  Error reading file: {e}")
        else:
            print("❌ No file selected")
        
        input("\nPress Enter to continue...")
    
    def browse_output_directory(self):
        """Browse for output directory"""
        print("📁 Select output directory...")
        try:
            root = tk.Tk()
            root.withdraw()
            root.attributes('-topmost', True)  # Bring to front
            
            initial_dir = self.config.output_dir
            if not os.path.exists(initial_dir):
                initial_dir = os.getcwd()
            
            directory = filedialog.askdirectory(
                title="Select Output Directory",
                initialdir=initial_dir
            )
            
            root.destroy()
        except Exception as e:
            print(f"❌ Error opening directory browser: {e}")
            print("💡 Fallback: Please enter the directory path manually:")
            directory = input("Directory path: ").strip().strip('"')
        
        if directory:
            self.config.output_dir = directory
            print(f"✅ Selected: {directory}")
        else:
            print("❌ No directory selected")
        
        input("\nPress Enter to continue...")
    
    def configure_browser_settings(self):
        """Configure browser-related settings"""
        while True:
            self.clear_screen()
            self.display_header()
            print("🌐 BROWSER CONFIGURATION")
            print("=" * 40)
            print("1. Number of browsers:", self.config.max_browsers)
            print("2. Headless mode:", "Enabled" if self.config.headless else "Disabled")
            print("3. Stealth/Humanization:", "Enabled" if self.config.humanize else "Disabled")
            print("4. GeoIP:", "Enabled" if self.config.geoip else "Disabled")
            print("5. Block WebRTC:", "Enabled" if self.config.block_webrtc else "Disabled")
            print("0. Back to main menu")
            print()
            
            choice = input("Select option: ").strip()
            
            if choice == '1':
                try:
                    browsers = int(input(f"Enter number of browsers (current: {self.config.max_browsers}): "))
                    if 1 <= browsers <= 10:
                        self.config.max_browsers = browsers
                        print(f"✅ Set to {browsers} browsers")
                    else:
                        print("❌ Please enter a number between 1 and 10")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '2':
                self.config.headless = not self.config.headless
                print(f"✅ Headless mode {'enabled' if self.config.headless else 'disabled'}")
                input("Press Enter to continue...")
            
            elif choice == '3':
                self.config.humanize = not self.config.humanize
                print(f"✅ Stealth mode {'enabled' if self.config.humanize else 'disabled'}")
                input("Press Enter to continue...")
            
            elif choice == '4':
                self.config.geoip = not self.config.geoip
                print(f"✅ GeoIP {'enabled' if self.config.geoip else 'disabled'}")
                input("Press Enter to continue...")
            
            elif choice == '5':
                self.config.block_webrtc = not self.config.block_webrtc
                print(f"✅ WebRTC blocking {'enabled' if self.config.block_webrtc else 'disabled'}")
                input("Press Enter to continue...")
            
            elif choice == '0':
                break
            else:
                print("❌ Invalid option")
                input("Press Enter to continue...")
    
    def configure_timing_settings(self):
        """Configure timing and timeout settings"""
        while True:
            self.clear_screen()
            self.display_header()
            print("⏱️  TIMING CONFIGURATION")
            print("=" * 40)
            print("1. URL delay:", f"{self.config.url_delay}s")
            print("2. Request timeout:", f"{self.config.request_timeout/1000}s")
            print("3. Navigation timeout:", f"{self.config.navigation_timeout/1000}s")
            print("4. Turnstile timeout:", f"{self.config.turnstile_timeout/1000}s")
            print("5. Element timeout:", f"{self.config.element_timeout/1000}s")
            print("0. Back to main menu")
            print()
            
            choice = input("Select option: ").strip()
            
            if choice == '1':
                try:
                    delay = float(input(f"Enter URL delay in seconds (current: {self.config.url_delay}): "))
                    if 0 <= delay <= 10:
                        self.config.url_delay = delay
                        print(f"✅ Set to {delay}s")
                    else:
                        print("❌ Please enter a value between 0 and 10")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '2':
                try:
                    timeout = int(input(f"Enter request timeout in seconds (current: {self.config.request_timeout/1000}): "))
                    if 30 <= timeout <= 300:
                        self.config.request_timeout = timeout * 1000
                        print(f"✅ Set to {timeout}s")
                    else:
                        print("❌ Please enter a value between 30 and 300 seconds")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '3':
                try:
                    timeout = int(input(f"Enter navigation timeout in seconds (current: {self.config.navigation_timeout/1000}): "))
                    if 15 <= timeout <= 120:
                        self.config.navigation_timeout = timeout * 1000
                        print(f"✅ Set to {timeout}s")
                    else:
                        print("❌ Please enter a value between 15 and 120 seconds")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '4':
                try:
                    timeout = int(input(f"Enter Turnstile timeout in seconds (current: {self.config.turnstile_timeout/1000}): "))
                    if 10 <= timeout <= 60:
                        self.config.turnstile_timeout = timeout * 1000
                        print(f"✅ Set to {timeout}s")
                    else:
                        print("❌ Please enter a value between 10 and 60 seconds")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '5':
                try:
                    timeout = int(input(f"Enter element timeout in seconds (current: {self.config.element_timeout/1000}): "))
                    if 5 <= timeout <= 30:
                        self.config.element_timeout = timeout * 1000
                        print(f"✅ Set to {timeout}s")
                    else:
                        print("❌ Please enter a value between 5 and 30 seconds")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '0':
                break
            else:
                print("❌ Invalid option")
                input("Press Enter to continue...")
    
    def configure_processing_settings(self):
        """Configure processing settings"""
        while True:
            self.clear_screen()
            self.display_header()
            print("⚙️  PROCESSING CONFIGURATION")
            print("=" * 40)
            print("1. Batch size:", self.config.batch_size)
            print("2. Max retries:", self.config.max_retries)
            print("3. Memory threshold:", f"{self.config.memory_threshold_mb}MB")
            print("4. Save screenshots:", "Enabled" if self.config.save_screenshots else "Disabled")
            print("0. Back to main menu")
            print()
            
            choice = input("Select option: ").strip()
            
            if choice == '1':
                try:
                    batch_size = int(input(f"Enter batch size (current: {self.config.batch_size}): "))
                    if 1 <= batch_size <= 100:
                        self.config.batch_size = batch_size
                        print(f"✅ Set to {batch_size}")
                    else:
                        print("❌ Please enter a value between 1 and 100")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '2':
                try:
                    retries = int(input(f"Enter max retries (current: {self.config.max_retries}): "))
                    if 0 <= retries <= 5:
                        self.config.max_retries = retries
                        print(f"✅ Set to {retries}")
                    else:
                        print("❌ Please enter a value between 0 and 5")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '3':
                try:
                    memory = int(input(f"Enter memory threshold in MB (current: {self.config.memory_threshold_mb}): "))
                    if 1000 <= memory <= 100000:
                        self.config.memory_threshold_mb = memory
                        print(f"✅ Set to {memory}MB")
                    else:
                        print("❌ Please enter a value between 1000 and 100000")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '4':
                self.config.save_screenshots = not self.config.save_screenshots
                print(f"✅ Screenshots {'enabled' if self.config.save_screenshots else 'disabled'}")
                input("Press Enter to continue...")
            
            elif choice == '0':
                break
            else:
                print("❌ Invalid option")
                input("Press Enter to continue...")
    
    def configure_proxy_settings(self):
        """Configure proxy settings"""
        while True:
            self.clear_screen()
            self.display_header()
            print("🔒 PROXY CONFIGURATION")
            print("=" * 40)
            
            current_proxy = "None"
            if self.config.proxy_server:
                current_proxy = getattr(self, 'selected_proxy_name', self.config.proxy_server)
            print(f"Current proxy: {current_proxy}")
            print()
            
            print("1. No proxy")
            print("2. Select from available proxies")
            if os.getenv('BRD_SERVER'):
                print("3. Use .env proxy settings")
            print("0. Back to main menu")
            print()
            
            choice = input("Select option: ").strip()
            
            if choice == '1':
                self.config.proxy_server = None
                self.config.proxy_username = None
                self.config.proxy_password = None
                if hasattr(self, 'selected_proxy_name'):
                    delattr(self, 'selected_proxy_name')
                print("✅ Proxy disabled")
                input("Press Enter to continue...")
            
            elif choice == '2':
                if not self.proxies:
                    print("❌ No proxies available in .config/proxy.json")
                    input("Press Enter to continue...")
                    continue
                
                print("\nAvailable proxies:")
                for i, proxy in enumerate(self.proxies, 1):
                    proxy_name = proxy.get('name', f"Proxy {proxy.get('id', i)}")
                    print(f"{i}. {proxy_name}")
                
                try:
                    proxy_choice = int(input("\nSelect proxy number: ")) - 1
                    if 0 <= proxy_choice < len(self.proxies):
                        selected_proxy = self.proxies[proxy_choice]
                        proxy_config = parse_proxy_url(selected_proxy['url'])
                        
                        if proxy_config:
                            self.config.proxy_server = proxy_config.get('server')
                            self.config.proxy_username = proxy_config.get('username')
                            self.config.proxy_password = proxy_config.get('password')
                            self.selected_proxy_name = selected_proxy.get('name', f'Proxy {proxy_choice + 1}')
                            print(f"✅ Selected proxy: {self.selected_proxy_name}")
                        else:
                            print("❌ Error parsing proxy URL")
                    else:
                        print("❌ Invalid proxy selection")
                except ValueError:
                    print("❌ Please enter a valid number")
                input("Press Enter to continue...")
            
            elif choice == '3' and os.getenv('BRD_SERVER'):
                self.config.proxy_server = os.getenv('BRD_SERVER')
                self.config.proxy_username = os.getenv('BRD_USERNAME')
                self.config.proxy_password = os.getenv('BRD_PASSWORD')
                self.selected_proxy_name = "Environment proxy"
                print("✅ Using .env proxy settings")
                input("Press Enter to continue...")
            
            elif choice == '0':
                break
            else:
                print("❌ Invalid option")
                input("Press Enter to continue...")
    
    def save_configuration(self):
        """Save current configuration to file"""
        print("💾 Save current configuration...")
        
        try:
            root = tk.Tk()
            root.withdraw()
            root.attributes('-topmost', True)
            
            file_path = filedialog.asksaveasfilename(
                title="Save Configuration",
                defaultextension=".json",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
                initialdir=os.getcwd()
            )
            
            root.destroy()
        except Exception as e:
            print(f"❌ Error opening save dialog: {e}")
            print("💡 Fallback: Please enter the save path manually:")
            file_path = input("Save path (without .json extension): ").strip().strip('"')
            if file_path and not file_path.endswith('.json'):
                file_path += '.json'
        
        if file_path:
            try:
                config_data = {
                    'input_file': self.input_file,
                    'max_browsers': self.config.max_browsers,
                    'headless': self.config.headless,
                    'humanize': self.config.humanize,
                    'geoip': self.config.geoip,
                    'block_webrtc': self.config.block_webrtc,
                    'request_timeout': self.config.request_timeout,
                    'navigation_timeout': self.config.navigation_timeout,
                    'turnstile_timeout': self.config.turnstile_timeout,
                    'element_timeout': self.config.element_timeout,
                    'url_delay': self.config.url_delay,
                    'batch_size': self.config.batch_size,
                    'max_retries': self.config.max_retries,
                    'memory_threshold_mb': self.config.memory_threshold_mb,
                    'output_dir': self.config.output_dir,
                    'save_screenshots': self.config.save_screenshots,
                    'proxy_server': self.config.proxy_server,
                    'proxy_username': self.config.proxy_username,
                    'proxy_password': self.config.proxy_password
                }
                
                with open(file_path, 'w') as f:
                    json.dump(config_data, f, indent=2)
                
                print(f"✅ Configuration saved to: {file_path}")
            except Exception as e:
                print(f"❌ Error saving configuration: {e}")
        else:
            print("❌ Save cancelled")
        
        input("\nPress Enter to continue...")
    
    def load_configuration(self):
        """Load configuration from file"""
        print("📂 Load configuration...")
        
        try:
            root = tk.Tk()
            root.withdraw()
            root.attributes('-topmost', True)
            
            file_path = filedialog.askopenfilename(
                title="Load Configuration",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
                initialdir=os.getcwd()
            )
            
            root.destroy()
        except Exception as e:
            print(f"❌ Error opening load dialog: {e}")
            print("💡 Fallback: Please enter the file path manually:")
            file_path = input("Configuration file path: ").strip().strip('"')
        
        if file_path:
            try:
                with open(file_path, 'r') as f:
                    config_data = json.load(f)
                
                # Apply loaded configuration
                self.input_file = config_data.get('input_file', 'specs/input/motorcycle/ultimatespecs_motorcycle_manufacturers.txt')
                self.config.max_browsers = config_data.get('max_browsers', 2)
                self.config.headless = config_data.get('headless', True)
                self.config.humanize = config_data.get('humanize', True)
                self.config.geoip = config_data.get('geoip', True)
                self.config.block_webrtc = config_data.get('block_webrtc', False)
                self.config.request_timeout = config_data.get('request_timeout', 120000)
                self.config.navigation_timeout = config_data.get('navigation_timeout', 60000)
                self.config.turnstile_timeout = config_data.get('turnstile_timeout', 20000)
                self.config.element_timeout = config_data.get('element_timeout', 10000)
                self.config.url_delay = config_data.get('url_delay', 1)
                self.config.batch_size = config_data.get('batch_size', 20)
                self.config.max_retries = config_data.get('max_retries', 1)
                self.config.memory_threshold_mb = config_data.get('memory_threshold_mb', 48000)
                self.config.output_dir = config_data.get('output_dir', 'specs/output/motorcycle')
                self.config.save_screenshots = config_data.get('save_screenshots', False)
                self.config.proxy_server = config_data.get('proxy_server')
                self.config.proxy_username = config_data.get('proxy_username')
                self.config.proxy_password = config_data.get('proxy_password')
                
                print(f"✅ Configuration loaded from: {file_path}")
            except Exception as e:
                print(f"❌ Error loading configuration: {e}")
        else:
            print("❌ Load cancelled")
        
        input("\nPress Enter to continue...")
    
    def run_interactive_menu(self):
        """Run the main interactive menu"""
        while True:
            self.clear_screen()
            self.display_header()
            self.display_current_config()
            
            print("🎛️  MAIN MENU")
            print("=" * 40)
            print("1. 📁 Select input file")
            print("2. 📂 Select output directory")
            print("3. 🌐 Configure browser settings")
            print("4. ⏱️  Configure timing settings")
            print("5. ⚙️  Configure processing settings")
            print("6. 🔒 Configure proxy settings")
            print("7. 💾 Save configuration")
            print("8. 📂 Load configuration")
            print("9. 🚀 Start crawling")
            print("0. ❌ Exit")
            print()
            
            choice = input("Select option: ").strip()
            
            if choice == '1':
                self.browse_input_file()
            elif choice == '2':
                self.browse_output_directory()
            elif choice == '3':
                self.configure_browser_settings()
            elif choice == '4':
                self.configure_timing_settings()
            elif choice == '5':
                self.configure_processing_settings()
            elif choice == '6':
                self.configure_proxy_settings()
            elif choice == '7':
                self.save_configuration()
            elif choice == '8':
                self.load_configuration()
            elif choice == '9':
                if not self.input_file:
                    print("❌ Please select an input file first!")
                    input("Press Enter to continue...")
                    continue
                
                # Confirm start
                self.clear_screen()
                self.display_header()
                print("🚀 READY TO START CRAWLING")
                print("=" * 40)
                self.display_current_config()
                
                # Load and preview URLs
                urls = load_manufacturer_urls(self.input_file)
                if not urls:
                    print("❌ No valid URLs found in the input file!")
                    input("Press Enter to continue...")
                    continue
                
                print(f"📊 Found {len(urls)} URLs to process")
                print("\nFirst 3 URLs to be processed:")
                for i, url in enumerate(urls[:3], 1):
                    print(f"   {i}. {url}")
                
                if len(urls) > 3:
                    print(f"   ... and {len(urls) - 3} more")
                print()
                
                confirm = input("Start crawling? (y/N): ").strip().lower()
                if confirm == 'y':
                    return self.config, urls
                
            elif choice == '0':
                print("👋 Goodbye!")
                return None, None
            else:
                print("❌ Invalid option")
                input("Press Enter to continue...")


async def main():
    """Main entry point"""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description='Motorcycle Model URL Generator')
    parser.add_argument('input_file', nargs='?', 
                       help='Path to manufacturer URLs file (if not provided, interactive mode starts)')
    parser.add_argument('-o', '--output', default='specs/output/motorcycle',
                       help='Output directory')
    parser.add_argument('--browsers', type=int, default=2,
                       help='Number of browsers')
    parser.add_argument('--delay', type=float, default=2.0,
                       help='Delay between URLs')
    parser.add_argument('--headless', action='store_true',
                       help='Run in headless mode')
    parser.add_argument('--no-stealth', action='store_true',
                       help='Disable stealth mode')
    parser.add_argument('--proxy', type=int,
                       help='Use proxy by ID from .config/proxy.json')
    parser.add_argument('--list-proxies', action='store_true',
                       help='List available proxies and exit')
    parser.add_argument('-i', '--interactive', action='store_true',
                       help='Force interactive mode')
    
    args = parser.parse_args()
    
    # Handle proxy listing
    if args.list_proxies:
        proxies = load_proxies_from_file()
        if not proxies:
            print("[INFO] No proxies found in .config/proxy.json")
            return
        
        print("[INFO] Available proxies:")
        for proxy in proxies:
            print(f"  ID: {proxy.get('id', 'N/A')} - Name: {proxy.get('name', 'Unnamed')}")
        return
    
    config = None
    manufacturer_urls = None
    
    # Determine if we should run in interactive mode
    if args.interactive or (not args.input_file and len(sys.argv) == 1):
        # Interactive mode
        print("🏍️  Starting Interactive Mode...")
        print("   Use command line arguments to run in batch mode")
        print("   Use --help for command line options\n")
        
        try:
            cli = InteractiveCLI()
            config, manufacturer_urls = cli.run_interactive_menu()
            
            if config is None or manufacturer_urls is None:
                return  # User cancelled
        except ImportError as e:
            print(f"❌ Error: {e}")
            print("   Interactive mode requires tkinter. Install with: pip install tkinter")
            print("   Or use command line arguments instead.")
            return
        except Exception as e:
            print(f"❌ Error starting interactive mode: {e}")
            return
    else:
        # Command line mode
        input_file = args.input_file or 'specs/input/motorcycle/ultimatespecs_motorcycle_manufacturers.txt'
        
        # Load manufacturer URLs
        manufacturer_urls = load_manufacturer_urls(input_file)
        if not manufacturer_urls:
            print(f"[ERROR] No URLs found in {input_file}")
            return
        
        print(f"[INFO] Found {len(manufacturer_urls)} manufacturer URLs to process")
        
        # Configure crawler
        config = CrawlerConfig(
            max_browsers=args.browsers,
            headless=args.headless,
            humanize=not args.no_stealth,
            geoip=True,
            url_delay=args.delay,
            output_dir=args.output
        )
        
        # Handle proxy selection
        if args.proxy:
            proxies = load_proxies_from_file()
            selected_proxy = None
            for proxy in proxies:
                if proxy.get('id') == args.proxy:
                    selected_proxy = proxy
                    break
            
            if selected_proxy:
                proxy_config = parse_proxy_url(selected_proxy['url'])
                if proxy_config:
                    config.proxy_server = proxy_config.get('server')
                    config.proxy_username = proxy_config.get('username')
                    config.proxy_password = proxy_config.get('password')
                    print(f"[INFO] Using proxy: {selected_proxy.get('name', 'Unknown')}")
                else:
                    print(f"[ERROR] Invalid proxy URL for ID {args.proxy}")
                    return
            else:
                print(f"[ERROR] Proxy ID {args.proxy} not found")
                return
        
        # Check for .env proxy settings
        load_dotenv()
        if not config.proxy_server and os.getenv('BRD_SERVER'):
            config.proxy_server = os.getenv('BRD_SERVER')
            config.proxy_username = os.getenv('BRD_USERNAME')
            config.proxy_password = os.getenv('BRD_PASSWORD')
            print("[INFO] Using proxy from .env file")
    
    # Initialize and run generator
    generator = MotorcycleModelGenerator(config)
    
    try:
        await generator.initialize()
        await generator.process_manufacturer_urls(manufacturer_urls)
    except KeyboardInterrupt:
        print(f"\n[INTERRUPT] Process interrupted by user")
        logger.info("Process interrupted by user")
    except Exception as e:
        print(f"[FATAL] Fatal error: {e}")
        logger.error(f"Fatal error: {e}")
    finally:
        await generator.cleanup()


if __name__ == "__main__":
    asyncio.run(main())