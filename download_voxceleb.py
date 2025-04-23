#!/usr/bin/env python3

import concurrent.futures
import subprocess
import os
import sys

# Configuration
MAX_WORKERS = 8  # Number of parallel downloads
SAVE_DIR = "data"  # Directory to save downloads
FILE_LIST = "lists/fileparts.txt"  # Path to the file list

# Create save directory if it doesn't exist
os.makedirs(SAVE_DIR, exist_ok=True)

def download_file(url, filename):
    """Download a single file using curl"""
    output_path = os.path.join(SAVE_DIR, filename)
    
    cmd = [
        "curl", "-k", "-L", "-C", "-",  # curl options
        url,
        "-o", output_path
    ]
    
    print(f"Starting download: {filename}")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    if result.returncode == 0:
        print(f"✅ Successfully downloaded: {filename}")
        return True
    else:
        print(f"❌ Failed to download: {filename}")
        print(f"Error: {result.stderr.decode()}")
        return False

# Read the list of files and filter for MP4 files
mp4_files = []
with open(FILE_LIST, 'r') as f:
    for line in f:
        if "mp4" in line:
            parts = line.strip().split()
            url = parts[0]
            filename = url.split("file=")[-1]
            mp4_files.append((url, filename))

print(f"Found {len(mp4_files)} MP4 files to download")

# Download files in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Submit all download tasks
    future_to_file = {
        executor.submit(download_file, url, filename): filename
        for url, filename in mp4_files
    }
    
    # Process results as they complete
    for future in concurrent.futures.as_completed(future_to_file):
        filename = future_to_file[future]
        try:
            success = future.result()
            if not success:
                print(f"Download failed for {filename}")
        except Exception as e:
            print(f"Download raised an exception for {filename}: {e}")

print("All downloads complete")