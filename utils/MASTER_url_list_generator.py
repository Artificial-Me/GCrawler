#!/usr/bin/env python3
"""MASTER URL List Generator

This script takes all `.txt` files (including subfolders) in a specified input directory
and merges them into a single MASTER file with duplicate URL detection and validation.

Usage:
    python MASTER_url_list_generator.py [input_dir] [output_file]
    
Defaults to motorcycle directory if no arguments provided.
"""

import os
import sys
import glob
import logging
from typing import Set, List, Optional
from pathlib import Path
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('url_merge.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)


def validate_url(line: str) -> bool:
    """Validate if a line contains a valid URL."""
    line = line.strip()
    if not line or line.startswith('#'):
        return False
    return line.startswith(('http://', 'https://'))


def process_txt_files(input_dir: str, output_file: str, remove_duplicates: bool = True) -> None:
    """Process all .txt files in the input directory and merge them.
    
    Args:
        input_dir: Directory containing .txt files to merge
        output_file: Path to output file
        remove_duplicates: Whether to remove duplicate URLs
    """
    # Validate input directory
    input_path = Path(input_dir)
    if not input_path.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    if not input_path.is_dir():
        logger.error(f"Input path is not a directory: {input_dir}")
        sys.exit(1)
    
    # Get all .txt files in the input directory (including subfolders)
    pattern = os.path.join(input_dir, "**", "*.txt")
    txt_files = glob.glob(pattern, recursive=True)
    
    # Exclude the output file if it exists in the search results
    output_path = Path(output_file).resolve()
    txt_files = [f for f in txt_files if Path(f).resolve() != output_path]
    
    if not txt_files:
        logger.warning(f"No .txt files found in {input_dir}")
        return
    
    logger.info(f"Found {len(txt_files)} .txt files to process")
    
    # Track URLs and statistics
    unique_urls: Set[str] = set()
    total_lines = 0
    valid_urls = 0
    duplicates_found = 0
    errors = 0
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Check if output file exists and ask for confirmation
    if output_path.exists():
        response = input(f"Output file {output_file} already exists. Overwrite? (y/N): ")
        if response.lower() != 'y':
            logger.info("Operation cancelled by user")
            return
    
    try:
        with open(output_file, "w", encoding='utf-8') as outfile:
            for i, txt_file in enumerate(txt_files, 1):
                logger.info(f"Processing file {i}/{len(txt_files)}: {txt_file}")
                
                try:
                    with open(txt_file, "r", encoding='utf-8', errors='ignore') as infile:
                        for line_num, line in enumerate(infile, 1):
                            total_lines += 1
                            line = line.strip()
                            
                            if not line:
                                continue
                            
                            if validate_url(line):
                                if remove_duplicates:
                                    if line in unique_urls:
                                        duplicates_found += 1
                                        logger.debug(f"Duplicate URL found: {line}")
                                        continue
                                    unique_urls.add(line)
                                
                                outfile.write(line + "\n")
                                valid_urls += 1
                                
                except UnicodeDecodeError as e:
                    logger.error(f"Unicode error in file {txt_file}: {e}")
                    errors += 1
                except IOError as e:
                    logger.error(f"Error reading file {txt_file}: {e}")
                    errors += 1
    
    except IOError as e:
        logger.error(f"Error writing to output file {output_file}: {e}")
        sys.exit(1)
    
    # Print summary statistics
    logger.info("\n" + "="*50)
    logger.info("MERGE SUMMARY")
    logger.info("="*50)
    logger.info(f"Files processed: {len(txt_files)}")
    logger.info(f"Total lines read: {total_lines}")
    logger.info(f"Valid URLs found: {valid_urls}")
    if remove_duplicates:
        logger.info(f"Duplicates removed: {duplicates_found}")
        logger.info(f"Unique URLs written: {len(unique_urls)}")
    logger.info(f"Errors encountered: {errors}")
    logger.info(f"Output file: {output_file}")
    logger.info(f"Output file size: {output_path.stat().st_size:,} bytes")
    logger.info("="*50)
    
    if errors > 0:
        logger.warning(f"Completed with {errors} errors. Check logs for details.")
    else:
        logger.info("Successfully completed without errors!")


def main():
    """Main function to handle command line arguments and execute the merge."""
    parser = argparse.ArgumentParser(
        description='Merge multiple .txt files containing URLs into a single MASTER file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python MASTER_url_list_generator.py
  python MASTER_url_list_generator.py specs/input/automobile
  python MASTER_url_list_generator.py specs/input/motorcycle specs/input/motorcycle/MASTER_bike_urls.txt
        """
    )
    
    parser.add_argument(
        'input_dir',
        nargs='?',
        default='specs/input/motorcycle',
        help='Input directory containing .txt files (default: specs/input/motorcycle)'
    )
    
    parser.add_argument(
        'output_file',
        nargs='?',
        help='Output file path (default: {input_dir}/MASTER_bike_url_list.txt)'
    )
    
    parser.add_argument(
        '--keep-duplicates',
        action='store_true',
        help='Keep duplicate URLs instead of removing them'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Set default output file if not provided
    if not args.output_file:
        input_name = Path(args.input_dir).name
        args.output_file = os.path.join(args.input_dir, f"MASTER_{input_name}_url_list.txt")
    
    logger.info(f"Starting URL merge process")
    logger.info(f"Input directory: {args.input_dir}")
    logger.info(f"Output file: {args.output_file}")
    logger.info(f"Remove duplicates: {not args.keep_duplicates}")
    
    try:
        process_txt_files(
            input_dir=args.input_dir,
            output_file=args.output_file,
            remove_duplicates=not args.keep_duplicates
        )
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
