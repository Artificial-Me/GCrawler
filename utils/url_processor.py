"""
URL Processing and Deduplication System
Checks which URLs have already been crawled to avoid reprocessing
"""

import os
from pathlib import Path
from typing import List, Set, Tuple, Dict
from urllib.parse import urlparse
import json
from datetime import datetime


class URLProcessor:
    """Manages URL processing and deduplication"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.processed_urls_cache = set()
        self.processed_files_map = {}  # Maps URL to file path
        self.stats = {
            'total_input': 0,
            'already_processed': 0,
            'to_process': 0,
            'files_found': 0
        }
    
    def parse_url_to_filename(self, url: str) -> Tuple[str, str, str]:
        """Parse URL to expected output filename"""
        try:
            parsed = urlparse(url)
            parts = [p for p in parsed.path.split('/') if p]
            
            if len(parts) >= 4 and parts[0] == 'car-specs':
                manufacturer = parts[1]
                id_part = parts[2]
                filename = parts[3]
                
                # Expected file path
                expected_filename = f"{id_part}-{filename}"
                expected_dir = self.output_dir / manufacturer / f"{manufacturer.upper()}_RAW_HTML"
                expected_path = expected_dir / expected_filename
                
                return manufacturer, expected_filename, str(expected_path)
            
            return None, None, None
        except Exception:
            return None, None, None
    
    def scan_existing_files(self) -> Dict[str, str]:
        """Scan output directory for existing files"""
        print(f"\n[SCAN] SCANNING EXISTING OUTPUT FILES")
        print(f"   -> Directory: {self.output_dir}")
        
        existing_files = {}
        file_count = 0
        
        if not self.output_dir.exists():
            print(f"   [WARN] Output directory doesn't exist yet")
            return existing_files
        
        # Scan all manufacturer directories
        for manufacturer_dir in self.output_dir.iterdir():
            if manufacturer_dir.is_dir():
                # Look for RAW_HTML subdirectory (legacy structure)
                raw_html_dir = manufacturer_dir / f"{manufacturer_dir.name.upper()}_RAW_HTML"
                if raw_html_dir.exists():
                    # Scan all HTML files in RAW_HTML subdirectory
                    for file_path in raw_html_dir.glob("*.html"):
                        file_count += 1
                        # Store by filename for quick lookup
                        existing_files[file_path.name] = str(file_path)
                        
                        if file_count % 1000 == 0:
                            print(f"      -> Scanned {file_count} files...")
                else:
                    # Scan HTML files directly in manufacturer directory (current structure)
                    for file_path in manufacturer_dir.glob("*.html"):
                        file_count += 1
                        # Store by filename for quick lookup
                        existing_files[file_path.name] = str(file_path)
                        
                        if file_count % 1000 == 0:
                            print(f"      -> Scanned {file_count} files...")
        
        print(f"   [OK] Found {file_count} existing files")
        self.stats['files_found'] = file_count
        return existing_files
    
    def check_url_processed(self, url: str, existing_files: Dict[str, str]) -> bool:
        """Check if a URL has already been processed"""
        manufacturer, filename, expected_path = self.parse_url_to_filename(url)
        
        if filename and filename in existing_files:
            # File exists - URL has been processed
            self.processed_files_map[url] = existing_files[filename]
            return True
        
        # Also check if the exact path exists (in case of naming variations)
        if expected_path and Path(expected_path).exists():
            self.processed_files_map[url] = expected_path
            return True
        
        return False
    
    def filter_unprocessed_urls(self, urls: List[str], force_recrawl: bool = False) -> List[str]:
        """Filter out already processed URLs"""
        print(f"\n[FILTER] FILTERING URLS")
        print(f"   - Total input URLs: {len(urls)}")
        print(f"   -> Force recrawl: {force_recrawl}")
        
        self.stats['total_input'] = len(urls)
        
        if force_recrawl:
            print(f"   [WARN] Force recrawl enabled - processing all URLs")
            self.stats['to_process'] = len(urls)
            return urls
        
        # Scan existing files
        existing_files = self.scan_existing_files()
        
        # Filter URLs
        print(f"\n[CHECK] Checking which URLs need processing...")
        unprocessed_urls = []
        processed_count = 0
        
        for i, url in enumerate(urls):
            if self.check_url_processed(url, existing_files):
                processed_count += 1
                self.processed_urls_cache.add(url)
            else:
                unprocessed_urls.append(url)
            
            # Progress update
            if (i + 1) % 1000 == 0:
                print(f"   -> Checked {i + 1}/{len(urls)} URLs...")
        
        self.stats['already_processed'] = processed_count
        self.stats['to_process'] = len(unprocessed_urls)
        
        # Print summary
        print(f"\n[COMPLETE] FILTERING COMPLETE")
        print(f"   - Already processed: {processed_count} ({processed_count/len(urls)*100:.1f}%)")
        print(f"   - Need processing: {len(unprocessed_urls)} ({len(unprocessed_urls)/len(urls)*100:.1f}%)")
        print(f"   -> Existing files: {self.stats['files_found']}")
        
        if processed_count > 0:
            print(f"\n[SKIP] Skipping {processed_count} already completed URLs")
        
        return unprocessed_urls
    
    def get_progress_report(self) -> str:
        """Get a formatted progress report"""
        report = []
        report.append("\n" + "="*60)
        report.append("URL PROCESSING REPORT")
        report.append("="*60)
        report.append(f"Total Input URLs:      {self.stats['total_input']:,}")
        report.append(f"Already Processed:     {self.stats['already_processed']:,}")
        report.append(f"To Be Processed:       {self.stats['to_process']:,}")
        report.append(f"Existing Output Files: {self.stats['files_found']:,}")
        
        if self.stats['total_input'] > 0:
            completion_rate = (self.stats['already_processed'] / self.stats['total_input']) * 100
            report.append(f"Completion Rate:       {completion_rate:.1f}%")
        
        report.append("="*60)
        return "\n".join(report)
    
    def save_progress(self, output_file: str = "crawl_progress.json"):
        """Save progress to a JSON file"""
        progress_data = {
            'timestamp': datetime.now().isoformat(),
            'stats': self.stats,
            'processed_urls': list(self.processed_urls_cache)[:100],  # Save sample
            'total_processed': len(self.processed_urls_cache)
        }
        
        with open(output_file, 'w') as f:
            json.dump(progress_data, f, indent=2)
        
        print(f"   [SAVE] Progress saved to {output_file}")
    
    def load_progress(self, progress_file: str = "crawl_progress.json") -> bool:
        """Load previous progress if available"""
        if not os.path.exists(progress_file):
            return False
        
        try:
            with open(progress_file, 'r') as f:
                data = json.load(f)
                
            print(f"\n[LOAD] Loaded previous progress from {data['timestamp']}")
            print(f"   -> Previously processed: {data['total_processed']} URLs")
            return True
        except Exception as e:
            print(f"   [WARN] Could not load progress file: {e}")
            return False


def check_and_filter_urls(input_file: str, output_dir: str = "output", force_recrawl: bool = False) -> List[str]:
    """
    Main function to check and filter URLs
    Returns list of URLs that need to be processed
    """
    print(f"\n{'='*80}")
    print(f"URL DEDUPLICATION CHECK")
    print(f"{'='*80}")
    
    # Load URLs from file
    print(f"\nLoading URLs from: {input_file}")
    with open(input_file, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]
    print(f"   -> Loaded {len(urls)} URLs")
    
    # Create processor
    processor = URLProcessor(output_dir)
    
    # Load previous progress if available
    processor.load_progress()
    
    # Filter URLs
    unprocessed_urls = processor.filter_unprocessed_urls(urls, force_recrawl)
    
    # Print report
    print(processor.get_progress_report())
    
    # Save progress
    processor.save_progress()
    
    return unprocessed_urls


def filter_urls_from_file(input_file: str, output_base_dir: str = "output", specs_dir: str = "Specs", force_recrawl: bool = False) -> List[str]:
    """
    Legacy function name for compatibility
    Alias for check_and_filter_urls with additional parameters for backward compatibility
    """
    return check_and_filter_urls(input_file, output_base_dir, force_recrawl)


if __name__ == "__main__":
    """Test the URL processor"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python url_processor.py <input_file> [output_dir]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "output"
    
    # Test the processor
    unprocessed = check_and_filter_urls(input_file, output_dir)
    
    if unprocessed:
        print(f"\n[READY] Ready to process {len(unprocessed)} new URLs")
    else:
        print(f"\n✨ All URLs have been processed!")