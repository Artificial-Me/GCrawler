"""
GhostCrawler - High-Performance Stealth Web Crawler and Scraper
"""

import asyncio, os, json, logging, time, gc, sys
from datetime import datetime
from urllib.parse import urlparse
from typing import List, Dict, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from collections import deque
from pathlib import Path
import aiofiles
from tenacity import retry, stop_after_attempt, wait_exponential
from playwright.async_api import Page, Browser, Route, async_playwright
from camoufox import AsyncNewBrowser
import psutil

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.url_processor import URLProcessor, check_and_filter_urls, filter_urls_from_file

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ghostcrawler.log')
    ]
)
logger = logging.getLogger(__name__)


def load_proxies_from_file(proxy_file: str = ".config/proxy.json") -> List[Dict[str, Any]]:
    try:
        if not os.path.isabs(proxy_file):
            # Look for .config/proxy.json in the parent directory (project root)
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            proxy_file = os.path.join(project_root, proxy_file)
        if not os.path.exists(proxy_file):
            logger.warning(f"Proxy file {proxy_file} not found")
            return []
        with open(proxy_file, 'r') as f:
            return json.load(f).get('proxies', [])
    except Exception as e:
        logger.error(f"Error loading proxies from {proxy_file}: {e}")
        return []


def parse_proxy_url(proxy_url: str) -> Dict[str, str]:
    try:
        parsed = urlparse(proxy_url)
        return {'server': f"{parsed.scheme}://{parsed.hostname}:{parsed.port}",
                'username': parsed.username, 'password': parsed.password}
    except Exception as e:
        logger.error(f"Error parsing proxy URL: {e}")
        return {}


def resolve_proxy_config(interactive_config: Optional[Dict[str, str]] = None, cmd_config: Optional[Dict[str, str]] = None) -> Optional[Dict[str, str]]:
    if interactive_config:
        logger.info("Using proxy from interactive selection")
        return interactive_config
    if cmd_config:
        logger.info("Using proxy from command line")
        return cmd_config
    proxy_server = os.getenv('BRD_SERVER')
    if proxy_server:
        logger.info("Using proxy from environment variables")
        return {'server': proxy_server, 'username': os.getenv('BRD_USERNAME'), 
                'password': os.getenv('BRD_PASSWORD')}
    return None

def validate_and_deduplicate_urls(urls: List[str]) -> List[str]:
    valid_urls, seen_urls = [], set()
    for url in urls:
        if not url or not isinstance(url, str): 
            logger.warning(f"Skipping invalid URL: {url}"); continue
        url = url.strip()
        if not url or not url.startswith(('http://', 'https://')):
            logger.warning(f"Skipping URL without proper scheme: {url}") if url else None; continue
        try:
            parsed = urlparse(url)
            if not parsed.netloc or not parsed.scheme:
                logger.warning(f"Skipping malformed URL: {url}"); continue
        except Exception as e:
            logger.warning(f"Skipping URL with parsing error: {url} - {e}"); continue
        if url not in seen_urls:
            seen_urls.add(url); valid_urls.append(url)
        else:
            logger.debug(f"Skipping duplicate URL: {url}")
    logger.info(f"URL validation: {len(urls)} input -> {len(valid_urls)} valid unique URLs")
    return valid_urls

def select_proxy_interactive(proxies: List[Dict[str, Any]]) -> Optional[Dict[str, str]]:
    if not proxies:
        print("No proxies available.")
        return None
    
    print("\nAvailable Proxies:")
    print("0. No proxy (direct connection)")
    for i, proxy in enumerate(proxies, 1):
        print(f"{i}. {proxy['name']} (ID: {proxy['id']})")
    
    while True:
        try:
            choice = input(f"\nSelect proxy (0-{len(proxies)}): ").strip()
            choice_num = int(choice)
            
            if choice_num == 0:
                return None  # No proxy
            elif 1 <= choice_num <= len(proxies):
                selected_proxy = proxies[choice_num - 1]
                proxy_config = parse_proxy_url(selected_proxy['url'])
                if proxy_config:
                    print(f"Selected: {selected_proxy['name']}")
                    return proxy_config
                else:
                    print("Error parsing selected proxy. Please try again.")
            else:
                print(f"Invalid choice. Please enter 0-{len(proxies)}")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            return None


@dataclass
class CrawlerConfig:
    max_browsers: int = 20
    max_contexts_per_browser: int = 1
    max_pages_per_context: int = 2
    headless: bool = True
    
    def __post_init__(self):
        self.max_browsers = max(1, min(self.max_browsers, 20))
        if self.max_browsers != getattr(self, '_orig_max_browsers', self.max_browsers):
            logger.warning(f"max_browsers adjusted to {self.max_browsers}")
        self.batch_size = max(1, min(self.batch_size, 100))
        if self.batch_size != getattr(self, '_orig_batch_size', self.batch_size):
            logger.warning(f"batch_size adjusted to {self.batch_size}")
        self.url_delay = max(0, self.url_delay)
        if self.url_delay < 0: logger.warning(f"url_delay adjusted to {self.url_delay}")
        self.memory_threshold_mb = max(1000, min(self.memory_threshold_mb, 65536))
        if self.memory_threshold_mb != getattr(self, '_orig_memory', self.memory_threshold_mb):
            logger.warning(f"memory_threshold_mb adjusted to {self.memory_threshold_mb}")
        self.request_timeout = self.request_timeout if self.request_timeout > 0 else 120000
        self.navigation_timeout = self.navigation_timeout if self.navigation_timeout > 0 else 60000
        self.turnstile_timeout = self.turnstile_timeout if self.turnstile_timeout > 0 else 20000
        self.min_content_length = max(1, self.min_content_length) if self.min_content_length >= 1 else 50
    
    humanize: bool = True
    geoip: bool = True
    block_webrtc: bool = False
    
    request_timeout: int = 120000
    navigation_timeout: int = 60000
    wait_until: str = 'domcontentloaded'
    
    turnstile_timeout: int = 20000
    post_turnstile_wait: int = 4000
    element_timeout: int = 10000
    network_idle_timeout: int = 3000
    stability_wait: int = 2000
    
    block_resources: List[str] = field(default_factory=lambda: [
        'stylesheet', 'image', 'media', 'font', 'other'
    ])
    block_patterns: List[str] = field(default_factory=lambda: [
        '.css', '.webp', '.jpg', '.jpeg', '.png', '.svg', '.gif', 
        '.woff', '.woff2', '.php', '.pdf', '.zip'
    ])
    allow_patterns: List[str] = field(default_factory=lambda: ['/cargallery/'])
    
    proxy_server: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None
    
    batch_size: int = 20
    url_delay: float = 1.0
    max_retries: int = 1
    memory_threshold_mb: int = 49152
    max_total_urls: int = 50000
    
    retry_incomplete_data: bool = False
    save_partial_data: bool = False
    aggressive_wait_mode: bool = False
    min_content_length: int = 50
    
    output_dir: str = "specs/output/motorcycle"
    save_screenshots: bool = False
    save_har: bool = True


class ResourceBlocker:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.blocked_count = 0
        self.allowed_count = 0
        
    async def setup_blocking(self, page: Page) -> None:
        """Setup request interception and blocking"""
        await page.route('**/*', self._handle_route)
        
    async def _handle_route(self, route: Route) -> None:
        """Handle each request and decide whether to block"""
        url, url_lower = route.request.url, route.request.url.lower()
        if 'challenges.cloudflare.com' in url or 'turnstile' in url_lower or \
           any(p in url for p in self.config.allow_patterns):
            await route.continue_(); self.allowed_count += 1; return
        if route.request.resource_type in self.config.block_resources or \
           any(p in url_lower for p in self.config.block_patterns):
            await route.abort(); self.blocked_count += 1
        else:
            await route.continue_(); self.allowed_count += 1


class TurnstileHandler:
    @staticmethod
    async def detect_turnstile(page: Page) -> bool:
        try:
            count = await page.locator('div.cf-turnstile').count()
            if count: print(f"      └─ Turnstile widget found ({count} instance{'s' if count > 1 else ''})")
            return count > 0
        except Exception as e:
            logger.debug(f"Error detecting Turnstile: {e}"); return False
    
    @staticmethod
    async def wait_for_turnstile(page: Page, timeout: int = 45000) -> bool:
        try:
            print(f"      └─ Waiting for Turnstile solution (max {timeout/1000}s)...")
            logger.info("Turnstile detected, waiting for solution...")
            
            await page.wait_for_selector('div.cf-turnstile', state='visible', timeout=timeout//2)
            print(f"         └─ Turnstile widget is visible")
            
            max_attempts = 15
            check_interval = 2000
            
            for attempt in range(max_attempts):
                print(f"         └─ Checking solution attempt {attempt + 1}/{max_attempts}...")
                
                token = await page.locator('input[name="cf-turnstile-response"]').get_attribute('value')
                if token:
                    print(f"         ✓ Turnstile solved! (Token received)\n         └─ Waiting for page to stabilize...")
                    try: await page.wait_for_load_state('networkidle', timeout=5000)
                    except: pass
                    return True
                if not await page.locator('div.cf-turnstile').is_visible():
                    print(f"         ✓ Turnstile widget hidden (Challenge passed)\n         └─ Waiting for page to stabilize...")
                    try: await page.wait_for_load_state('networkidle', timeout=5000)
                    except: pass
                    return True
                
                if not any(x in page.url.lower() for x in ['challenge', 'turnstile']):
                    print(f"         ✓ Redirected away from challenge page"); return True
                if attempt > 5: check_interval = 3000
                
                print(f"         └─ Waiting {check_interval/1000}s before next check...")
                await page.wait_for_timeout(check_interval)
            
            print(f"         ❌ Turnstile solution timeout after {max_attempts} attempts")
            logger.warning("Turnstile solution timeout")
            return False
            
        except Exception as e:
            logger.error(f"Error handling Turnstile: {e}")
            return False


class BrowserPool:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.playwright = None
        self._lock = asyncio.Lock()
        self.active_browsers = 0
        self.total_browsers_created = 0
        self.max_concurrent_browsers = config.max_browsers
        
    async def initialize(self) -> None:
        print(f"\nDYNAMIC BROWSER POOL INITIALIZATION")
        print(f"   Max concurrent browsers: {self.max_concurrent_browsers}")
        print(f"   Lifecycle: create->crawl->save->destroy->replace")
        logger.info(f"Initializing dynamic browser pool with max {self.max_concurrent_browsers} browsers")
        
        print(f"\nStarting Playwright...")
        self.playwright = await async_playwright().start()
        print(f"   Playwright started")
        
        print(f"\nDYNAMIC BROWSER POOL READY")
        print(f"   Browsers will be created on-demand for each crawl task")
        logger.info("Dynamic browser pool initialized - browsers created on-demand")
    
    async def _create_browser(self, os_type: str = 'windows') -> Browser:
        print(f"      Configuring Camoufox...")
        args = {'headless': self.config.headless, 'os': os_type, 'geoip': self.config.geoip,
                'locale': 'en-US', 'humanize': self.config.humanize, 'block_webrtc': self.config.block_webrtc}
        if self.config.proxy_server:
            print(f"      Adding proxy configuration")
            args['proxy'] = {'server': self.config.proxy_server}
            if self.config.proxy_username and self.config.proxy_password:
                args['proxy'].update({'username': self.config.proxy_username, 'password': self.config.proxy_password})
        print(f"      Launching Camoufox browser...")
        try:
            browser = await asyncio.wait_for(AsyncNewBrowser(self.playwright, **args), timeout=30.0)
            print(f"      Browser instance ready")
            return browser
        except asyncio.TimeoutError:
            print(f"      Browser creation timeout (30s)"); raise RuntimeError("Browser creation timed out")
        except Exception as e:
            print(f"      Browser creation failed: {e}"); raise
    
    async def acquire_browser_for_crawl(self) -> Optional[Tuple[Browser, Page, int]]:
        browser_id, start_time = None, time.time()
        while time.time() - start_time < 60:
            async with self._lock:
                if self.active_browsers < self.max_concurrent_browsers:
                    self.active_browsers += 1; self.total_browsers_created += 1
                    browser_id = self.total_browsers_created; break
            await asyncio.sleep(0.1)
        if browser_id is None:
            logger.warning(f"Timeout waiting for browser slot after 60s"); return None
            
        try:
            os_choice = ['windows', 'macos', 'linux'][browser_id % 3]
            print(f"   Creating dedicated browser #{browser_id} (OS: {os_choice})")
            browser = await self._create_browser(os_choice)
            page = await browser.new_page()
            logger.info(f"Created dedicated browser #{browser_id} for crawl task")
            return browser, page, browser_id
        except Exception as e:
            async with self._lock: self.active_browsers -= 1
            logger.error(f"Failed to create browser for crawl: {e}"); return None
    
    async def _cleanup_browser_resources(self, browser: Browser, page: Page, browser_id: int = None) -> None:
        cleanup_errors = []
        if page:
            try: await asyncio.wait_for(page.close(), timeout=5.0); logger.debug(f"Page closed for browser #{browser_id}")
            except Exception as e: cleanup_errors.append(f"page: {e}"); logger.warning(f"Error closing page for browser #{browser_id}: {e}")
        if browser:
            try: await asyncio.wait_for(browser.close(), timeout=10.0); logger.debug(f"Browser #{browser_id} closed successfully")
            except Exception as e: cleanup_errors.append(f"browser: {e}"); logger.warning(f"Error closing browser #{browser_id}: {e}")
        if cleanup_errors:
            logger.error(f"Browser #{browser_id} cleanup had errors: {'; '.join(cleanup_errors)}")
            print(f"   ⚠️ Browser #{browser_id} cleanup completed with warnings")
        else:
            logger.info(f"Browser #{browser_id} destroyed successfully after crawl")
            print(f"   🔥 Browser #{browser_id} destroyed successfully")
    
    async def destroy_browser_after_crawl(self, browser: Browser, page: Page, browser_id: int = None) -> None:
        try: await self._cleanup_browser_resources(browser, page, browser_id)
        finally:
            async with self._lock: self.active_browsers = max(0, self.active_browsers - 1)
            gc.collect()
    
    async def cleanup(self) -> None:
        logger.info("Cleaning up dynamic browser pool...")
        if self.playwright:
            try: await self.playwright.stop(); logger.info("Playwright stopped successfully")
            except Exception as e: logger.error(f"Error stopping Playwright: {e}")
        async with self._lock: self.active_browsers = self.total_browsers_created = 0


class DataExtractor:
    @staticmethod
    async def extract_motorcycle_specs(page: Page) -> Dict[str, Any]:
        data = {}
        
        try:
            print(f"      🔍 Extracting page title and meta information...")
            try:
                title_elem = page.locator('title')
                if await title_elem.count() > 0:
                    data['page_title'] = await title_elem.inner_text()
                    print(f"         ✓ Page title extracted")
                
                meta_description = page.locator('meta[name="description"]')
                if await meta_description.count() > 0:
                    data['meta_description'] = await meta_description.get_attribute('content')
                    print(f"         ✓ Meta description extracted")
                
                meta_keywords = page.locator('meta[name="keywords"]')
                if await meta_keywords.count() > 0:
                    data['meta_keywords'] = await meta_keywords.get_attribute('content')
                    print(f"         ✓ Meta keywords extracted")
                
                canonical_link = page.locator('link[rel="canonical"]')
                if await canonical_link.count() > 0:
                    data['canonical_url'] = await canonical_link.get_attribute('href')
                    print(f"         ✓ Canonical URL extracted")
                    
            except Exception as e:
                print(f"         ⚠️ Error extracting meta information: {e}")
            
            print(f"      🔍 Extracting JSON-LD data...")
            jsonld_scripts = await page.locator('script[type="application/ld+json"]').all()
            print(f"         Found {len(jsonld_scripts)} JSON-LD script(s)")
            
            if jsonld_scripts:
                jsonld_data = {}
                for i, script in enumerate(jsonld_scripts):
                    try:
                        content = await script.inner_text()
                        if content:
                            content = content.strip()
                            
                            try:
                                parsed = json.loads(content)
                            except json.JSONDecodeError as e:
                                print(f"         ⚠️ Script {i+1} has JSON error, attempting to fix...")
                                
                                import re
                                
                                content = re.sub(r'"@type"\s*,', '"@type": "ListItem",', content)
                                content = re.sub(r',\s*}', '}', content)
                                content = re.sub(r',\s*]', ']', content)
                                content = re.sub(r'\\/', '/', content)
                                
                                try:
                                    parsed = json.loads(content)
                                    print(f"         ✓ Fixed JSON error in script {i+1}")
                                except json.JSONDecodeError as e2:
                                    print(f"         ❌ Could not fix JSON in script {i+1}: {e2}")
                                    jsonld_data[f'error_script_{i+1}'] = {
                                        'error': str(e2),
                                        'content_preview': content[:200]
                                    }
                                    continue
                            
                            if isinstance(parsed, dict):
                                data_type = parsed.get('@type')
                                if data_type == 'BreadcrumbList':
                                    jsonld_data['breadcrumbs'] = parsed
                                    print(f"         ✓ Found Breadcrumb data in script {i+1}")
                                elif data_type == 'FAQPage':
                                    jsonld_data['faq'] = parsed
                                    print(f"         ✓ Found FAQ data in script {i+1}")
                                elif data_type == 'Motorcycle' or data_type == 'Vehicle':
                                    jsonld_data['motorcycle'] = parsed
                                    print(f"         ✓ Found Motorcycle data in script {i+1}")
                                else:
                                    jsonld_data[f'other_{data_type.lower()}'] = parsed
                                    print(f"         ℹ️ Script {i+1} has type: {data_type}")
                    except Exception as e:
                        print(f"         ⚠️ Script {i+1} error: {str(e)[:50]}")
                        continue
                
                if jsonld_data:
                    data['jsonld'] = jsonld_data
                    print(f"         ✓ JSON-LD data extracted successfully")
                else:
                    print(f"         ⚠️ No relevant JSON-LD data found")
            
            print(f"      🔍 Extracting motorcycle page title...")
            try:
                page_title_elem = page.locator('div.page_ficha_title')
                if await page_title_elem.count() > 0:
                    data['motorcycle_page_title'] = await page_title_elem.inner_html()
                    print(f"         ✓ Motorcycle page title found")
                else:
                    print(f"         ⚠️ Motorcycle page title not found")
            except Exception as e:
                print(f"         ⚠️ Error extracting motorcycle page title: {e}")
            
            print(f"      🔍 Extracting motorcycle image...")
            try:
                image_selectors = [
                    'img.left_column_top_model_image',
                    'div.resumo_ficha img',
                    'div.col-md-6 img'
                ]
                
                for selector in image_selectors:
                    image_elem = page.locator(selector)
                    if await image_elem.count() > 0:
                        src = await image_elem.first.get_attribute('src')
                        if src and not src.endswith('moto-bg.png'):
                            data['image_url'] = src
                            print(f"         ✓ Motorcycle image found: {src}")
                            break
                        elif src:
                            data['placeholder_image'] = src
                            print(f"         ℹ️ Placeholder image found: {src}")
                
                if 'image_url' not in data and 'placeholder_image' not in data:
                    print(f"         ⚠️ No motorcycle image found")
            except Exception as e:
                print(f"         ⚠️ Error extracting motorcycle image: {e}")
            
            print(f"      🔍 Extracting key specs...")
            try:
                key_specs_selectors = [
                    'div.col-12 > h3.posts_title:has-text("Key Specs")+div.col-12',
                    'h3.posts_title:has-text("Key Specs")+div',
                    'div:has(> h3.posts_title:has-text("Key Specs")) div.col-12',
                    'div.col-md-6:has(h3:has-text("Key Specs"))'
                ]
                
                for selector in key_specs_selectors:
                    try:
                        key_specs_elem = page.locator(selector)
                        if await key_specs_elem.count() > 0:
                            data['key_specs'] = await key_specs_elem.first.inner_html()
                            print(f"         ✓ Key specs found with selector: {selector[:50]}")
                            break
                    except:
                        continue
                
                if 'key_specs' not in data:
                    print(f"         ⚠️ Key specs not found with any selector")
            except Exception as e:
                print(f"         ⚠️ Error extracting key specs: {e}")
            
            print(f"      🔍 Extracting detailed motorcycle specifications...")
            try:
                detailed_specs_elem = page.locator('div.ficha_specs_main')
                if await detailed_specs_elem.count() > 0:
                    data['detailed_specs'] = await detailed_specs_elem.first.inner_html()
                    print(f"         ✓ Detailed specs found")
                else:
                    print(f"         ⚠️ Detailed specs not found")
            except Exception as e:
                print(f"         ⚠️ Error extracting detailed specs: {e}")
            
            print(f"      🔍 Extracting FAQ section...")
            try:
                faq_elem = page.locator('div.div_faqs')
                if await faq_elem.count() > 0:
                    data['faq_section'] = await faq_elem.first.inner_html()
                    print(f"         ✓ FAQ section found")
                else:
                    print(f"         ℹ️ FAQ section not found (optional)")
            except Exception as e:
                print(f"         ⚠️ Error extracting FAQ section: {e}")
            
        except Exception as e:
            logger.error(f"Error extracting motorcycle data: {e}")
        
        return data


class HTMLSaver:
    def __init__(self, base_dir: str = "output"):
        self.base_dir = Path(base_dir); self.base_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = Path("logs"); self.logs_dir.mkdir(exist_ok=True)
        self.files_saved = 0
    
    def _parse_url(self, url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        try:
            parts = [p for p in urlparse(url).path.split('/') if p]
            if len(parts) >= 3 and parts[0] == 'motorcycles-specs':
                manufacturer = parts[1]
                # For motorcycle URLs, use the full model name as filename
                if len(parts) == 3:
                    filename = parts[2]
                    # Create a simple ID from the beginning of the filename
                    id_part = filename.split('-')[0] if '-' in filename else filename[:10]
                else:
                    # Handle URLs with more parts (join them)
                    filename = '-'.join(parts[2:])
                    id_part = parts[2]
                return manufacturer, id_part, filename
            elif len(parts) >= 4 and parts[0] == 'car-specs':
                return parts[1], parts[2], parts[3]
            return None, None, None
        except Exception as e:
            logger.error(f"Error parsing URL {url}: {e}"); return None, None, None
    
    async def save_html(self, url: str, data: Dict[str, Any]) -> Optional[str]:
        manufacturer, id_part, filename = self._parse_url(url)
        if not all([manufacturer, id_part, filename]):
            async with aiofiles.open(self.logs_dir / "URL_Pattern_ERROR.txt", 'a') as f:
                await f.write(f"Invalid URL pattern: {url}\n")
            return None
        
        # Create manufacturer folder directly in base directory
        manufacturer_dir = self.base_dir / manufacturer
        manufacturer_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as HTML file with .html extension
        output_path = manufacturer_dir / f"{filename}.html"
        async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
            await f.write(self._build_html_content(data))
        
        self.files_saved += 1
        logger.info(f"Saved file {self.files_saved}: {output_path}")
        print(f"   ✓ File saved: {output_path}")
        return str(output_path)
    
    def _build_html_content(self, data: Dict[str, Any]) -> str:
        html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Motorcycle Specifications</title>
</head>
<body>
"""
        
        if 'page_title' in data:
            html += f'<div id="page_title">\n<h1>Page Title</h1>\n<p>{data["page_title"]}</p>\n</div>\n'
        
        if 'meta_description' in data:
            html += f'<div id="meta_description">\n<h2>Meta Description</h2>\n<p>{data["meta_description"]}</p>\n</div>\n'
        
        if 'meta_keywords' in data:
            html += f'<div id="meta_keywords">\n<h2>Meta Keywords</h2>\n<p>{data["meta_keywords"]}</p>\n</div>\n'
        
        if 'canonical_url' in data:
            html += f'<div id="canonical_url">\n<h2>Canonical URL</h2>\n<p>{data["canonical_url"]}</p>\n</div>\n'
        
        if 'motorcycle_page_title' in data:
            html += f'<div id="motorcycle_page_title">\n<h2>Motorcycle Page Title</h2>\n{data["motorcycle_page_title"]}\n</div>\n'
        
        if 'jsonld' in data:
            html += '<div id="jsonld">\n<h2>JSON-LD Data</h2>\n'
            
            if 'motorcycle' in data['jsonld']:
                html += '<div id="motorcycle_jsonld">\n<h3>Motorcycle Data</h3>\n'
                html += f'<pre>{json.dumps(data["jsonld"]["motorcycle"], indent=2)}</pre>\n'
                html += '</div>\n'
            
            if 'breadcrumbs' in data['jsonld']:
                html += '<div id="breadcrumbs_jsonld">\n<h3>Breadcrumbs</h3>\n'
                html += f'<pre>{json.dumps(data["jsonld"]["breadcrumbs"], indent=2)}</pre>\n'
                html += '</div>\n'
            
            if 'faq' in data['jsonld']:
                html += '<div id="faq_jsonld">\n<h3>FAQ Data</h3>\n'
                html += f'<pre>{json.dumps(data["jsonld"]["faq"], indent=2)}</pre>\n'
                html += '</div>\n'
            
            for key, value in data['jsonld'].items():
                if key not in ['motorcycle', 'breadcrumbs', 'faq']:
                    html += f'<div id="{key}_jsonld">\n<h3>{key.title()} Data</h3>\n'
                    html += f'<pre>{json.dumps(value, indent=2)}</pre>\n'
                    html += '</div>\n'
            
            html += '</div>\n'
        
        if 'image_url' in data:
            html += f'<div id="image_url">\n<h2>Motorcycle Image</h2>\n<p>{data["image_url"]}</p>\n</div>\n'
        elif 'placeholder_image' in data:
            html += f'<div id="placeholder_image">\n<h2>Placeholder Image</h2>\n<p>{data["placeholder_image"]}</p>\n</div>\n'
        
        if 'key_specs' in data:
            html += f'<div id="key_specs">\n<h2>Key Specs</h2>\n{data["key_specs"]}\n</div>\n'
        
        if 'detailed_specs' in data:
            html += f'<div id="detailed_specs">\n<h2>Detailed Specs</h2>\n{data["detailed_specs"]}\n</div>\n'
        
        if 'faq_section' in data:
            html += f'<div id="faq_section">\n<h2>FAQ Section</h2>\n{data["faq_section"]}\n</div>\n'
        
        html += """</body>
</html>"""
        
        return html


class GhostCrawler:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.browser_pool = BrowserPool(config)
        self.html_saver = HTMLSaver(config.output_dir)
        self.turnstile_handler = TurnstileHandler()
        self.urls_processed = self.urls_failed = 0
        self.start_time = None
        self.last_gc_time, self.gc_interval = time.time(), 60
    
    async def _wait_for_page_stability(self, page: Page, context: str = "general") -> None:
        try:
            logger.debug(f"Waiting for page stability ({context})")
            await page.wait_for_load_state('networkidle', timeout=self.config.network_idle_timeout)
            logger.debug(f"Network idle achieved for {context}")
        except: logger.debug(f"Network didn't idle for {context}, continuing anyway")
        if self.config.aggressive_wait_mode:
            logger.debug(f"Aggressive mode: additional stability wait for {context}")
            await page.wait_for_timeout(self.config.stability_wait)
    
    def _log_failure_reason(self, url: str, reason: str, details: str = None) -> None:
        try:
            failure_log_path = Path("logs/failure_reasons.json")
            failure_log_path.parent.mkdir(exist_ok=True)
            failures = json.load(open(failure_log_path, 'r')) if failure_log_path.exists() else []
            failures.append({'url': url, 'reason': reason, 'details': details, 
                           'timestamp': datetime.now().isoformat()})
            json.dump(failures, open(failure_log_path, 'w'), indent=2)
        except Exception as e: logger.error(f"Failed to log failure reason: {e}")
    
    async def initialize(self) -> None:
        print(f"\n{'='*60}\nGHOSTCRAWLER INITIALIZATION\n{'='*60}\n\nConfiguration Summary:")
        print(f"   Max browsers: {self.config.max_browsers}\n   Batch size: {self.config.batch_size}")
        print(f"   Output dir: {self.config.output_dir}\n   Aggressive mode: {self.config.aggressive_wait_mode}")
        logger.info("Initializing GhostCrawler...")
        try:
            await self.browser_pool.initialize(); self.start_time = time.time()
            print(f"\nGHOSTCRAWLER READY TO CRAWL\n{'='*60}\n")
            logger.info("GhostCrawler initialized successfully")
        except Exception as e:
            print(f"\nINITIALIZATION FAILED: {e}"); logger.error(f"GhostCrawler initialization failed: {e}"); raise
    
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def crawl_url(self, url: str) -> Optional[Dict[str, Any]]:
        browser = None
        page = None
        browser_id = None
        resource_blocker = None
        
        try:
            browser_page_tuple = await self.browser_pool.acquire_browser_for_crawl()
            if not browser_page_tuple:
                raise RuntimeError("Failed to acquire browser for crawl")
            
            browser, page, browser_id = browser_page_tuple
            
            resource_blocker = ResourceBlocker(self.config)
            await resource_blocker.setup_blocking(page)
            
            response = await page.goto(
                url,
                wait_until=self.config.wait_until,
                timeout=self.config.navigation_timeout
            )
            
            if not response or response.status >= 400:
                raise RuntimeError(f"Failed to load page: {response.status if response else 'No response'}")
            
            print(f"   ✓ Page navigated successfully (Status: {response.status})")
            
            print(f"   ⏳ Waiting for page stability...")
            await self._wait_for_page_stability(page, "initial page")
            
            print(f"   🔍 Checking for Cloudflare Turnstile...")
            if await self.turnstile_handler.detect_turnstile(page):
                print(f"   ⚠️ Turnstile detected! Attempting to solve...")
                success = await self.turnstile_handler.wait_for_turnstile(page, timeout=self.config.turnstile_timeout)
                if not success:
                    print(f"   ❌ Failed to solve Turnstile challenge")
                    raise RuntimeError("Failed to solve Turnstile challenge")
                
                print(f"   ✓ Turnstile solved successfully")
                print(f"   ⏳ Waiting for post-Turnstile page stability...")
                await page.wait_for_timeout(self.config.post_turnstile_wait)
                await self._wait_for_page_stability(page, "post-Turnstile")
            else:
                print(f"   ✓ No Turnstile detected")
            
            print(f"   ⏳ Waiting for critical page elements (timeout: {self.config.element_timeout/1000}s)...")
            
            required_elements = {'Key specs section': 'h3.posts_title:has-text("Key Specs")',
                                'Detailed specs': 'div.ficha_specs_main'}
            optional_elements = {'JSON-LD data': 'script[type="application/ld+json"]',
                                'Car image': 'div#car_image img'}
            
            element_status, missing_required = {}, []
            
            print(f"   🎯 Checking required elements...")
            for element_name, selector in required_elements.items():
                try:
                    print(f"      └─ Waiting for {element_name}...")
                    if await page.wait_for_selector(selector, timeout=self.config.element_timeout):
                        element_status[element_name] = True; print(f"         ✓ {element_name} found")
                except:
                    element_status[element_name] = False; missing_required.append(element_name)
                    print(f"         ✗ {element_name} not found (timeout)")
            
            print(f"   🔍 Checking optional elements...")
            for element_name, selector in optional_elements.items():
                try:
                    print(f"      └─ Checking for {element_name}...")
                    if await page.wait_for_selector(selector, timeout=5000):
                        print(f"         ✓ {element_name} found (bonus!)")
                except: print(f"         ℹ️ {element_name} not found (optional, OK)")
            
            all_required_found = all(element_status.get(name, False) for name in required_elements.keys())
            
            if not all_required_found:
                print(f"   ⚠️ Missing required elements: {', '.join(missing_required)}")
                logger.error(f"Missing required elements on {url}: {', '.join(missing_required)}")
                
                if self.config.aggressive_wait_mode:
                    print(f"   🔄 Aggressive mode: Retrying after 5s wait...")
                    await page.wait_for_timeout(5000)
                    
                    for element_name in missing_required[:]:
                        try:
                            if await page.wait_for_selector(required_elements[element_name], timeout=10000):
                                element_status[element_name] = True; missing_required.remove(element_name)
                                print(f"      ✓ {element_name} found on retry")
                        except: print(f"      ✗ {element_name} still missing")
                    
                    all_required_found = all(element_status.get(name, False) for name in required_elements.keys())
            
            if not all_required_found:
                print(f"   ❌ SKIPPING - Required elements missing: {', '.join(missing_required)}")
                logger.error(f"Skipping {url} - missing required elements: {', '.join(missing_required)}")
                self.urls_failed += 1; return None
            
            print(f"   📊 Extracting motorcycle specifications...")
            data = await DataExtractor.extract_motorcycle_specs(page)
            
            print(f"   🔍 Validating extracted data...")
            
            validation_errors = []
            if 'jsonld' not in data or not data['jsonld']:
                validation_errors.append("Missing JSON-LD data"); print(f"      ✗ Missing: JSON-LD structured data")
            elif 'car' in data['jsonld']: print(f"      ✓ Valid: JSON-LD Car data present")
            else: print(f"      ℹ️ JSON-LD present but no Car data")
            
            if 'key_specs' not in data or not data['key_specs']:
                validation_errors.append("Missing key specifications"); print(f"      ✗ Missing: Key specifications")
            elif len(data['key_specs'].strip()) < self.config.min_content_length:
                validation_errors.append(f"Key specifications too short (min {self.config.min_content_length} chars)")
                print(f"      ✗ Invalid: Key specifications too short")
            else: print(f"      ✓ Valid: Key specifications ({len(data['key_specs'])} chars)")
            
            if 'detailed_specs' not in data or not data['detailed_specs']:
                validation_errors.append("Missing detailed specifications"); print(f"      ✗ Missing: Detailed specifications")
            elif len(data['detailed_specs'].strip()) < self.config.min_content_length:
                validation_errors.append(f"Detailed specifications too short (min {self.config.min_content_length} chars)")
                print(f"      ✗ Invalid: Detailed specifications too short")
            else: print(f"      ✓ Valid: Detailed specifications ({len(data['detailed_specs'])} chars)")
            
            print(f"      ✓ Found: Vehicle image URL" if 'image_url' in data and data['image_url'] 
                  else f"      ℹ️ Missing: Vehicle image (optional)")
            
            if validation_errors:
                print(f"   ❌ DATA VALIDATION FAILED")
                print(f"      Errors: {'; '.join(validation_errors)}")
                logger.error(f"Data validation failed for {url}: {'; '.join(validation_errors)}")
                
                if not self.config.save_partial_data:
                    print(f"   🚫 NOT SAVING - Data is incomplete"); self.urls_failed += 1
                    self._log_failure_reason(url, "Data validation failed", '; '.join(validation_errors)); return None
                print(f"   ⚠️ SAVING PARTIAL DATA (config.save_partial_data=True)")
                logger.warning(f"Saving partial data for {url} despite validation errors")
            
            print(f"   ✅ Data validation PASSED - all required data present")
            
            print(f"   💾 Saving complete data...")
            output_path = await self.html_saver.save_html(url, data)
            if output_path:
                print(f"   ✓ File saved: {output_path}")
                logger.info(f"Successfully saved: {output_path}")
                self.urls_processed += 1; print(f"   📈 Total complete URLs processed: {self.urls_processed}")
            else: print(f"   ❌ Failed to save file"); self.urls_failed += 1
            
            if resource_blocker:
                logger.info(f"Blocked {resource_blocker.blocked_count} resources, allowed {resource_blocker.allowed_count}")
            
            return data
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout while crawling {url}"); self.urls_failed += 1
            self._log_failure_reason(url, "Timeout during crawl", "Page load timeout"); return None
        except Exception as e:
            logger.error(f"Error crawling {url}: {e}"); self.urls_failed += 1
            self._log_failure_reason(url, "Crawl exception", str(e)); return None
            
        finally:
            if browser or page:
                await self._cleanup_browser_resources(browser, page, browser_id)
            
            await self._manage_memory()
    
    async def _cleanup_browser_resources(self, browser: Optional[Browser], page: Optional[Page], 
                                        browser_id: Optional[int]) -> None:
        """Robust cleanup of browser resources"""
        cleanup_success = False
        if browser and page:
            try:
                await asyncio.wait_for(self.browser_pool.destroy_browser_after_crawl(browser, page, browser_id), timeout=10.0)
                cleanup_success = True
            except asyncio.TimeoutError: logger.warning(f"Timeout destroying browser #{browser_id}, forcing cleanup")
            except Exception as e: logger.error(f"Error in normal cleanup for browser #{browser_id}: {e}")
        
        if not cleanup_success:
            if page:
                try: await asyncio.wait_for(page.close(), timeout=2.0)
                except: logger.debug("Could not close page in forced cleanup")
            if browser:
                try: await asyncio.wait_for(browser.close(), timeout=2.0)
                except: logger.debug("Could not close browser in forced cleanup")
            async with self.browser_pool._lock:
                if self.browser_pool.active_browsers > 0:
                    self.browser_pool.active_browsers -= 1
                    logger.debug(f"Force decremented browser count to {self.browser_pool.active_browsers}")
    
    async def crawl_batch(self, urls: List[str]) -> List[Optional[Dict[str, Any]]]:
        """Crawl a batch of URLs with controlled concurrency and dynamic browser management"""
        logger.info(f"Starting batch of {len(urls)} URLs")
        
        async def crawl_single_url(url: str) -> Optional[Dict[str, Any]]:
            try:
                if self.config.url_delay > 0: await asyncio.sleep(self.config.url_delay)
                result = await self.crawl_url(url)
                memory_percent = psutil.virtual_memory().percent
                if memory_percent > 75: logger.debug(f"Memory at {memory_percent}%, running GC"); gc.collect()
                return result
            except Exception as e: logger.error(f"Failed to crawl {url}: {e}"); return None
        
        tasks = [crawl_single_url(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [None if isinstance(r, Exception) else r for r in results]
    
    async def crawl(self, urls: List[str]) -> None:
        """Main crawl method with safety limits"""
        total_urls = len(urls)
        if total_urls > self.config.max_total_urls:
            logger.warning(f"URL count ({total_urls}) exceeds safety limit ({self.config.max_total_urls}). Truncating.")
            urls = urls[:self.config.max_total_urls]; total_urls = len(urls)
        logger.info(f"Starting crawl of {total_urls} URLs")
        
        for i in range(0, total_urls, self.config.batch_size):
            batch = urls[i:i + self.config.batch_size]
            batch_num, total_batches = i // self.config.batch_size + 1, (total_urls + self.config.batch_size - 1) // self.config.batch_size
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} URLs)")
            
            memory_usage = psutil.virtual_memory().percent
            if memory_usage > 85:
                logger.warning(f"High memory usage: {memory_usage}%. Running garbage collection...")
                gc.collect(); await asyncio.sleep(2)
            
            await self.crawl_batch(batch)
            
            elapsed = time.time() - self.start_time
            urls_per_minute = (self.urls_processed / elapsed) * 60 if elapsed > 0 else 0
            total_processed = self.urls_processed + self.urls_failed
            success_rate = self.urls_processed / total_processed if total_processed > 0 else 0
            
            logger.info(f"Progress: {self.urls_processed}/{total_urls} URLs processed, "
                       f"Success rate: {success_rate:.2%}, "
                       f"Speed: {urls_per_minute:.1f} URLs/min")
            
            if i + self.config.batch_size < total_urls:
                logger.info("Pausing between batches..."); await asyncio.sleep(2)
    
    async def _manage_memory(self) -> None:
        """Manage memory usage with dynamic browser lifecycle"""
        memory_percent = psutil.virtual_memory().percent
        process_memory_mb = psutil.Process().memory_info().rss / (1024 * 1024)
        current_time = time.time()
        if memory_percent > 75.0 and current_time - self.last_gc_time > 30:
            logger.warning(f"High system memory usage: {memory_percent:.1f}%. Process using {process_memory_mb:.1f}MB. Running GC...")
            gc.collect(2) if memory_percent > 85.0 else gc.collect()
            if memory_percent > 85.0: logger.error(f"Critical memory usage: {memory_percent:.1f}%. Running full GC...")
            self.last_gc_time = current_time
            logger.debug(f"Memory management: Active browsers: {self.browser_pool.active_browsers}, Total created: {self.browser_pool.total_browsers_created}")
        elif process_memory_mb > self.config.memory_threshold_mb and current_time - self.last_gc_time > self.gc_interval:
            logger.debug(f"Process memory: {process_memory_mb:.1f}MB. Running maintenance GC...")
            gc.collect(); self.last_gc_time = current_time
    
    async def cleanup(self) -> None:
        """Clean up resources"""
        logger.info("Cleaning up GhostCrawler..."); await self.browser_pool.cleanup()
        if self.start_time:
            elapsed = time.time() - self.start_time
            total_urls = self.urls_processed + self.urls_failed
            success_rate = self.urls_processed / total_urls if total_urls > 0 else 0
            urls_per_minute = (self.urls_processed / elapsed) * 60 if elapsed > 0 else 0
            logger.info("=" * 50 + "\nFINAL STATISTICS")
            logger.info(f"Total URLs: {total_urls}\nSuccessful: {self.urls_processed}\nFailed: {self.urls_failed}")
            logger.info(f"Success Rate: {success_rate:.2%}\nAverage Speed: {urls_per_minute:.1f} URLs/min")
            logger.info(f"Total Time: {elapsed/60:.1f} minutes\nFiles Saved: {self.html_saver.files_saved}\n" + "=" * 50)



def get_user_input_with_browse():
    """Interactive mode to get user input with browse dialogs"""
    import tkinter as tk
    from tkinter import filedialog, messagebox
    import sys, os
    root = tk.Tk(); root.withdraw()
    
    print(f"\n{'='*80}\nGHOSTCRAWLER - INTERACTIVE MODE\n{'='*80}\nEnter your configuration or press Enter for defaults\n")
    
    print("1. SELECT INPUT FILE\n   Choose a file containing URLs to crawl")
    use_browse = input("   Use file browser? (Y/n): ").strip().lower()
    
    if use_browse != 'n':
        input_file = filedialog.askopenfilename(title="Select URL file",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")], initialdir="input")
        if not input_file: print("   No file selected. Exiting..."); root.destroy(); sys.exit(1)
        print(f"   Selected: {input_file}")
    else:
        input_file = input("   Enter input file path: ").strip()
        if not input_file: print("   No input file provided. Exiting..."); root.destroy(); sys.exit(1)
    
    if not os.path.exists(input_file):
        messagebox.showerror("Error", f"Input file not found: {input_file}")
        root.destroy(); sys.exit(1)
    print()
    
    print("2. SELECT OUTPUT DIRECTORY\n   Choose where to save crawled data")
    use_browse_out = input("   Use folder browser? (Y/n): ").strip().lower()
    
    if use_browse_out != 'n':
        output_dir = filedialog.askdirectory(title="Select output directory", initialdir=".")
        if not output_dir: output_dir = "output"; print(f"   No directory selected. Using default: {output_dir}")
        else: print(f"   Selected: {output_dir}")
    else:
        output_dir = input("   Enter output directory (default: output): ").strip() or "output"
        print(f"   Using: {output_dir}")
    print()
    
    print("3. CRAWLER CONFIGURATION")
    
    try: batch_size = max(1, int(input("   Batch size (default: 20): ").strip() or "20"))
    except ValueError: batch_size = 20
    print(f"   Using batch size: {batch_size}")
    
    try: url_delay = max(0, float(input("   Delay between URLs in seconds (default: 0.5): ").strip() or "0.5"))
    except ValueError: url_delay = 0.5
    print(f"   Using delay: {url_delay}s")
    
    try: browsers = max(1, int(input("   Number of browsers (default: 5): ").strip() or "5"))
    except ValueError: browsers = 5
    print(f"   Using browsers: {browsers}")
    
    headless = input("   Run in headless mode? (Y/n): ").strip().lower() != 'n'
    print(f"   Headless mode: {'Yes' if headless else 'No'}")
    
    force_recrawl = input("   Force recrawl all URLs? (y/N): ").strip().lower() == 'y'
    print(f"   Force recrawl: {'Yes' if force_recrawl else 'No'}")
    
    use_new_filter = input("   Use new URL processor? (Y/n): ").strip().lower() != 'n'
    print(f"   New URL processor: {'Yes' if use_new_filter else 'No'}")
    
    no_stealth = input("   Disable stealth mode? (y/N): ").strip().lower() == 'y'
    print(f"   Stealth mode: {'Disabled' if no_stealth else 'Enabled'}")
    
    auto_mode = input("   Auto mode - process ALL URLs? (Y/n): ").strip().lower() != 'n'
    print(f"   Auto mode: {'Yes - process all URLs' if auto_mode else 'No - use default limits'}")
    
    print("\n4. PROXY CONFIGURATION")
    proxy_config, proxy_name = None, "No proxy"
    if input("   Use proxy? (y/N): ").strip().lower() == 'y':
        proxies = load_proxies_from_file()
        if proxies:
            proxy_config = select_proxy_interactive(proxies)
            if proxy_config:
                for proxy in proxies:
                    parsed = parse_proxy_url(proxy['url'])
                    if (parsed.get('server') == proxy_config.get('server') and 
                        parsed.get('username') == proxy_config.get('username')):
                        proxy_name = proxy['name']
                        break
            else:
                proxy_name = "No proxy (cancelled)"
        else:
            print("   No proxies available in .config/proxy.json")
            proxy_name = "No proxy (none available)"
    
    print(f"   Proxy: {proxy_name}\n\n{'='*80}\nCONFIGURATION SUMMARY\n{'='*80}")
    print(f"Input file:      {input_file}")
    print(f"Output dir:      {output_dir}")
    print(f"Batch size:      {batch_size}")
    print(f"URL delay:       {url_delay}s")
    print(f"Browsers:        {browsers}")
    print(f"Headless:        {headless}")
    print(f"Force recrawl:   {force_recrawl}")
    print(f"New filter:      {use_new_filter}")
    print(f"Stealth mode:    {'Disabled' if no_stealth else 'Enabled'}")
    print(f"Auto mode:       {'Yes - process all URLs' if auto_mode else 'No - use limits'}")
    print(f"Proxy:           {proxy_name}")
    print("="*80)
    
    if input("\nProceed with this configuration? (Y/n): ").strip().lower() == 'n':
        print("Operation cancelled."); root.destroy(); sys.exit(0)
    root.destroy()
    
    class Config: pass
    config = Config()
    config.input_file, config.output, config.batch_size = input_file, output_dir, batch_size
    config.delay, config.browsers, config.headless = url_delay, browsers, headless
    config.force_recrawl, config.use_new_filter = force_recrawl, use_new_filter
    config.no_stealth, config.auto = no_stealth, auto_mode
    config.specs_dir, config.proxy_config = "Specs", proxy_config
    return config


async def main():
    """Main entry point"""
    import argparse, sys
    from dotenv import load_dotenv
    load_dotenv()
    
    if len(sys.argv) == 1:
        args = get_user_input_with_browse()
    else:
        parser = argparse.ArgumentParser(description='GhostCrawler')
        parser.add_argument('input_file', nargs='?', help='Path to file containing URLs')
        parser.add_argument('-o', '--output', default='Specs', help='Output directory')
        parser.add_argument('-b', '--batch-size', type=int, default=10, help='Batch size')
        parser.add_argument('-d', '--delay', type=float, default=1.0, help='Delay between URLs')
        parser.add_argument('--browsers', type=int, default=2, help='Number of browsers')
        parser.add_argument('--headless', action='store_true', help='Run in headless mode')
        parser.add_argument('--no-stealth', action='store_true', help='Disable stealth mode')
        parser.add_argument('--force-recrawl', action='store_true', help='Force recrawl all URLs (ignore existing files)')
        parser.add_argument('--specs-dir', default='Specs', help='Directory containing existing spec files')
        parser.add_argument('--use-old-filter', action='store_true', help='Use old URL processor (not recommended)')
        parser.add_argument('--proxy', type=int, help='Proxy ID to use (1-10, see .config/proxy.json)')
        parser.add_argument('--list-proxies', action='store_true', help='List available proxies and exit')
        parser.add_argument('--max-urls', type=int, help='Maximum number of URLs to process (overrides safety limit)')
        parser.add_argument('--auto', action='store_true', help='Automatically process all URLs in input file (no limits)')
        
        args = parser.parse_args()
        
        if not args.list_proxies and not args.input_file:
            parser.error('input_file is required unless using --list-proxies')
    
    if hasattr(args, 'list_proxies') and args.list_proxies:
        proxies = load_proxies_from_file()
        if proxies:
            print("\nAvailable Proxies:")
            for proxy in proxies: print(f"  {proxy['id']}. {proxy['name']}\n     URL: {proxy['url']}")
        else: print("No proxies found in .config/proxy.json")
        return
    
    proxy_config = None
    if hasattr(args, 'proxy') and args.proxy:
        proxies = load_proxies_from_file()
        selected_proxy = next((p for p in proxies if p['id'] == args.proxy), None)
        if selected_proxy:
            proxy_config = parse_proxy_url(selected_proxy['url'])
            print(f"Using proxy: {selected_proxy['name']}")
        else:
            print(f"Proxy ID {args.proxy} not found. Available IDs: {[p['id'] for p in proxies]}"); return
    
    print(f"\n{'=' * 80}\nGHOSTCRAWLER - INTELLIGENT URL FILTERING\n{'=' * 80}")
    
    if not hasattr(args, 'use_old_filter') or not args.use_old_filter:
        urls = check_and_filter_urls(
            args.input_file,
            output_dir=args.output,
            force_recrawl=args.force_recrawl
        )
    else:
        urls = filter_urls_from_file(
            args.input_file, 
            output_base_dir=args.output,
            specs_dir=getattr(args, 'specs_dir', 'Specs'),
            force_recrawl=args.force_recrawl
        )
    
    if not urls:
        print("\n✓ All URLs have already been crawled!\nUse --force-recrawl to recrawl all URLs anyway."); return
    
    original_count = len(urls)
    urls = validate_and_deduplicate_urls(urls)
    if not urls: print("\n❌ No valid URLs found after validation!"); return
    if len(urls) < original_count:
        print(f"\n⚠️  URL validation removed {original_count - len(urls)} invalid/duplicate URLs\n   Processing {len(urls)} valid URLs")
    
    interactive_config = getattr(args, 'proxy_config', None) if hasattr(args, 'proxy_config') else None
    cmd_config = None
    if hasattr(args, 'proxy') and args.proxy:
        proxies = load_proxies_from_file()
        selected = next((p for p in proxies if p['id'] == args.proxy), None)
        if selected: cmd_config = parse_proxy_url(selected['url'])
    
    proxy_config = resolve_proxy_config(interactive_config, cmd_config)
    proxy_server = proxy_config.get('server') if proxy_config else None
    proxy_username = proxy_config.get('username') if proxy_config else None
    proxy_password = proxy_config.get('password') if proxy_config else None
    
    max_urls = getattr(args, 'max_urls', None)
    auto_mode = getattr(args, 'auto', max_urls is None)
    if hasattr(args, 'auto') and hasattr(args.auto, '__bool__'): auto_mode = args.auto
    
    config_kwargs = {'max_browsers': args.browsers, 'headless': args.headless, 'batch_size': args.batch_size,
                     'url_delay': args.delay, 'output_dir': args.output, 'proxy_server': proxy_server,
                     'proxy_username': proxy_username, 'proxy_password': proxy_password,
                     'humanize': not getattr(args, 'no_stealth', False), 'geoip': True}
    
    if auto_mode:
        config_kwargs['max_total_urls'] = len(urls) + 1000
        print(f"🤖 AUTO MODE: Processing all {len(urls)} URLs from input file")
    elif max_urls is not None:
        config_kwargs['max_total_urls'] = max_urls; print(f"🔢 Maximum URLs override: {max_urls}")
    else: print(f"📊 Using default limit: {config_kwargs.get('max_total_urls', 1000000)} URLs")
    
    config = CrawlerConfig(**config_kwargs)
    crawler = GhostCrawler(config)
    try:
        await crawler.initialize(); await crawler.crawl(urls)
    finally: await crawler.cleanup()


if __name__ == '__main__':
    asyncio.run(main())