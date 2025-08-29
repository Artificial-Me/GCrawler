#!/usr/bin/env python3
"""
Extract motorcycle manufacturer URLs from HTML file.
Generates a txt file containing all motorcycle manufacturer href URLs.
"""

import re
import argparse
from pathlib import Path


def extract_motorcycle_urls(html_file_path, output_file_path):
    """
    Extract all motorcycle manufacturer URLs from HTML file.
    
    Args:
        html_file_path: Path to the HTML file containing manufacturer links
        output_file_path: Path for the output txt file
    """
    try:
        with open(html_file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
    except UnicodeDecodeError:
        # Try different encodings if UTF-8 fails
        try:
            with open(html_file_path, 'r', encoding='latin-1') as file:
                html_content = file.read()
        except Exception as e:
            print(f"Error reading HTML file: {e}")
            return
    
    # Pattern to match motorcycle manufacturer URLs
    pattern = r'href="motorcycles-specs/([^"]+)"'
    
    # Find all matches
    matches = re.findall(pattern, html_content)
    
    # Remove duplicates and sort
    unique_manufacturers = sorted(set(matches))
    
    # Create full URLs with base URL prepended
    base_url = "https://www.ultimatespecs.com/"
    urls = [f"{base_url}motorcycles-specs/{manufacturer}" for manufacturer in unique_manufacturers]
    
    # Write to output file
    with open(output_file_path, 'w', encoding='utf-8') as file:
        for url in urls:
            file.write(f"{url}\n")
    
    print(f"Extracted {len(urls)} unique motorcycle manufacturer URLs")
    print(f"Output saved to: {output_file_path}")
    
    return urls


def main():
    parser = argparse.ArgumentParser(description='Extract motorcycle manufacturer URLs from HTML file')
    parser.add_argument('html_file', help='Path to HTML file containing manufacturer links')
    parser.add_argument('-o', '--output', default='motorcycle_manufacturers.txt', 
                       help='Output txt file path (default: motorcycle_manufacturers.txt)')
    
    args = parser.parse_args()
    
    html_file = Path(args.html_file)
    output_file = Path(args.output)
    
    if not html_file.exists():
        print(f"Error: HTML file not found: {html_file}")
        return
    
    extract_motorcycle_urls(html_file, output_file)


if __name__ == "__main__":
    main()