# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests",
#     "tqdm",
# ]
# ///
"""
GHArchive parallel downloader
Downloads GitHub Archive files between two datetime ranges
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from tqdm import tqdm
import time
from urllib.parse import urlparse
from pathlib import Path
import threading
from collections import deque


class DownloadStats:
    """Thread-safe download statistics tracker"""
    def __init__(self):
        self.lock = threading.Lock()
        self.bytes_downloaded = 0
        self.start_time = time.time()
        self.recent_bytes = deque(maxlen=10)  # Track last 10 measurements
        self.recent_times = deque(maxlen=10)
        
    def add_bytes(self, bytes_count):
        with self.lock:
            self.bytes_downloaded += bytes_count
            current_time = time.time()
            self.recent_bytes.append(bytes_count)
            self.recent_times.append(current_time)
    
    def get_speed(self):
        """Get current download speed in MiB/s"""
        with self.lock:
            if len(self.recent_times) < 2:
                return 0.0
            
            # Calculate speed from recent samples
            total_bytes = sum(self.recent_bytes)
            time_span = self.recent_times[-1] - self.recent_times[0]
            
            if time_span > 0:
                return (total_bytes / time_span) / (1024 * 1024)  # Convert to MiB/s
            return 0.0
    
    def get_total_downloaded(self):
        """Get total downloaded in MiB"""
        with self.lock:
            return self.bytes_downloaded / (1024 * 1024)


def parse_datetime(dt_string):
    """Parse datetime string in various formats"""
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_string, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse datetime: {dt_string}")


def generate_urls(start_dt, end_dt):
    """Generate all GHArchive URLs between start and end datetimes"""
    urls = []
    current = start_dt.replace(minute=0, second=0, microsecond=0)
    
    while current <= end_dt:
        # GHArchive stores files by hour - no zero padding for hour
        url = f"https://data.gharchive.org/{current.strftime('%Y-%m-%d')}-{current.hour}.json.gz"
        urls.append((url, current))
        current += timedelta(hours=1)
    
    return urls


def download_file(url, output_path, stats, max_retries=3):
    """Download a single file with retry logic and stats tracking"""
    filename = os.path.basename(urlparse(url).path)
    filepath = os.path.join(output_path, filename)
    
    # Skip if file already exists
    if os.path.exists(filepath):
        return (url, True, f"Already exists: {filename}", 0)
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Create directory if it doesn't exist
            os.makedirs(output_path, exist_ok=True)
            
            # Download with progress tracking
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=32768):  # 32KB chunks
                    if chunk:
                        f.write(chunk)
                        chunk_size = len(chunk)
                        downloaded_size += chunk_size
                        stats.add_bytes(chunk_size)
            
            return (url, True, f"Downloaded: {filename}", downloaded_size)
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                return (url, False, f"Failed after {max_retries} attempts: {filename} - {str(e)}", 0)
            time.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            return (url, False, f"Error downloading {filename}: {str(e)}", 0)


def format_size(size_in_mib):
    """Format size in MiB to human readable format"""
    if size_in_mib < 1024:
        return f"{size_in_mib:.1f} MiB"
    else:
        return f"{size_in_mib / 1024:.1f} GiB"


def download_archives(start_dt, end_dt, output_path="work/archives", max_workers=10):
    """Download all archives in parallel with speed tracking"""
    urls = generate_urls(start_dt, end_dt)
    
    if not urls:
        print("No archives to download for the given date range.")
        return
    
    print(f"Found {len(urls)} archives to download")
    print(f"Date range: {start_dt} to {end_dt}")
    print(f"Output directory: {output_path}")
    print()
    
    successful = 0
    failed = 0
    stats = DownloadStats()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_url = {
            executor.submit(download_file, url, output_path, stats): url 
            for url, _ in urls
        }
        
        # Custom progress bar format with speed
        bar_format = '{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {postfix}]'
        
        # Process completed downloads with progress bar
        with tqdm(total=len(urls), desc="Downloading", bar_format=bar_format) as pbar:
            # Update speed in a separate thread
            def update_speed():
                while not pbar.n >= pbar.total:
                    speed = stats.get_speed()
                    total = stats.get_total_downloaded()
                    pbar.set_postfix_str(f'{speed:.1f} MiB/s, Total: {format_size(total)}')
                    time.sleep(0.5)
            
            speed_thread = threading.Thread(target=update_speed, daemon=True)
            speed_thread.start()
            
            for future in as_completed(future_to_url):
                url, success, message, size = future.result()
                
                if success:
                    successful += 1
                else:
                    failed += 1
                    tqdm.write(f"{message}")
                
                pbar.update(1)
    
    # Final statistics
    total_downloaded = stats.get_total_downloaded()
    total_time = time.time() - stats.start_time
    avg_speed = total_downloaded / total_time if total_time > 0 else 0
    
    print(f"\nDownload complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total: {len(urls)}")
    print(f"Total downloaded: {format_size(total_downloaded)}")
    print(f"Total time: {total_time:.1f} seconds")
    print(f"Average speed: {avg_speed:.1f} MiB/s")


def main():
    parser = argparse.ArgumentParser(
        description="Download GHArchive files between two datetime ranges",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python gharchive_downloader.py "2024-01-01" "2024-01-02"
  python gharchive_downloader.py "2024-01-01 10:00:00" "2024-01-01 12:00:00"
  python gharchive_downloader.py "2024-01-01T10" "2024-01-01T12" -w 20
  python gharchive_downloader.py "2024-01-01" "2024-01-02" -o /path/to/archives
        """
    )
    
    parser.add_argument("start", help="Start datetime (e.g., '2024-01-01' or '2024-01-01 10:00:00')")
    parser.add_argument("end", help="End datetime (e.g., '2024-01-02' or '2024-01-01 12:00:00')")
    parser.add_argument("-o", "--output", default="work/archives", 
                        help="Output directory (default: work/archives)")
    parser.add_argument("-w", "--workers", type=int, default=10,
                        help="Number of parallel download workers (default: 10)")
    
    args = parser.parse_args()
    
    try:
        start_dt = parse_datetime(args.start)
        end_dt = parse_datetime(args.end)
        
        if start_dt > end_dt:
            print("Error: Start datetime must be before end datetime")
            sys.exit(1)
        
        download_archives(start_dt, end_dt, args.output, args.workers)
        
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()