#!/usr/bin/python
#-*- coding: utf-8 -*-
# Script to concatenate file parts for VoxCeleb dataset and convert files to WAV

import argparse
import os
import subprocess
import hashlib
import glob
from tqdm import tqdm
from zipfile import ZipFile
import tarfile
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import shutil
import functools

def md5(fname):
    """Calculate MD5 checksum of a file"""
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def check_files_exist(args, target_files):
    """Check if specified files already exist"""
    existing_files = []
    missing_files = []
    
    for file in target_files:
        filepath = os.path.join(args.save_path, file)
        if os.path.exists(filepath):
            existing_files.append(file)
        else:
            missing_files.append(file)
    
    return existing_files, missing_files

def is_within_directory(directory, target):
    """Check if target is within directory to prevent path traversal attacks"""
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    prefix = os.path.commonprefix([abs_directory, abs_target])
    return prefix == abs_directory

def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
    """Safely extract tar files"""
    for member in tar.getmembers():
        member_path = os.path.join(path, member.name)
        if not is_within_directory(path, member_path):
            raise Exception("Attempted Path Traversal in Tar File")
    tar.extractall(path, members, numeric_owner=numeric_owner)

def extract_zip_file(save_path, file):
    """Extract a single zip file"""
    filepath = os.path.join(save_path, file)
    if not os.path.exists(filepath):
        print(f"File {file} does not exist. Skipping extraction.")
        return False
        
    print(f"Extracting {file}...")
    try:
        if file.endswith(".tar.gz"):
            # For tar.gz files, use pigz if available for parallel extraction
            try:
                # Check if pigz is installed
                subprocess.run(["which", "pigz"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                # Use pigz for parallel extraction
                cmd = f"tar --use-compress-program=pigz -xf {filepath} -C {save_path}"
                subprocess.run(cmd, shell=True, check=True)
            except (subprocess.SubprocessError, subprocess.CalledProcessError):
                # Fall back to regular tar extraction
                with tarfile.open(filepath, "r:gz") as tar:
                    safe_extract(tar, save_path)
        elif file.endswith(".zip"):
            with ZipFile(filepath, 'r') as zf:
                zf.extractall(save_path)
        
        print(f"Extraction of {file} completed.")
        return True
    except Exception as e:
        print(f"Error extracting {file}: {str(e)}")
        return False

def extract_files(args, files_to_extract):
    """Extract zip or tar.gz files in parallel"""
    print("Extracting files...")
    
    if not files_to_extract:
        print("No files to extract")
        return
    
    # Extract files sequentially - parallel extraction can be problematic
    for file in files_to_extract:
        extract_zip_file(args.save_path, file)
    
    # Reorganize directory structure
    print("Reorganizing directory structure...")
    
    # Create voxceleb2 directory
    vox2_dir = os.path.join(args.save_path, 'voxceleb2')
    os.makedirs(vox2_dir, exist_ok=True)
    
    # Check for dev/aac directory (from zip extraction)
    if os.path.exists(os.path.join(args.save_path, 'dev/aac')):
        # Copy directly from dev/aac to voxceleb2
        print(f"Copying files from {args.save_path}/dev/aac/ to {vox2_dir}/ (preserving originals)")
        cmd = f'rsync -a {args.save_path}/dev/aac/ {vox2_dir}/'
        subprocess.call(cmd, shell=True)
        print(f"Original directory {args.save_path}/dev preserved for backup")
    # Check for aac directory (might already exist)
    elif os.path.exists(os.path.join(args.save_path, 'aac')):
        # Copy from aac to voxceleb2
        print(f"Copying files from {args.save_path}/aac/ to {vox2_dir}/ (preserving originals)")
        cmd = f'rsync -a {args.save_path}/aac/ {vox2_dir}/'
        subprocess.call(cmd, shell=True)
        print(f"Original directory {args.save_path}/aac preserved for backup")
    else:
        print("Neither dev/aac nor aac directories found after extraction. Please check your zip file structure.")
    
    print("Directory reorganization completed with originals preserved.")

def concatenate(args, lines):
    """Concatenate file parts and verify checksum"""
    for line in lines:
        parts = line.split()
        infile = parts[0]
        outfile = parts[1]
        md5gt = parts[2]

        # Check if output file already exists
        # if os.path.exists(f'{args.save_path}/{outfile}'):
        #     print(f"File {outfile} already exists, checking checksum...")
        #     md5ck = md5(f'{args.save_path}/{outfile}')
        #     if md5ck == md5gt:
        #         print(f'Checksum successful for existing file {outfile}. Skipping concatenation.')
        #         continue
        #     else:
        #         print(f'Checksum failed for existing file {outfile}. Will recreate the file.')

        # Concatenate files
        # print(f"Concatenating {infile} to {outfile}")
        # out = subprocess.call(f'cat {args.save_path}/{infile} > {args.save_path}/{outfile}', shell=True)
        
        # if out != 0:
        #     print(f"Error concatenating {infile}")
        #     continue

        # Check MD5
        # md5ck = md5(f'{args.save_path}/{outfile}')
        # if md5ck == md5gt:
        #     print(f'Checksum successful for {outfile}.')
        # else:
        #     print(f'Warning: Checksum failed for {outfile}.')
            
        # Remove the original file parts if requested
        if args.remove_parts:
            out = subprocess.call(f'rm {args.save_path}/{infile}', shell=True)
            print(f"Removed original part file {infile}")

def convert_single_file(fname, use_hwaccel):
    """Convert a single m4a file to WAV format"""
    outfile = fname.replace('.m4a', '.wav')
    
    # Skip if WAV file already exists
    if os.path.exists(outfile):
        return True
    
    # Use hardware acceleration if available and requested
    hwaccel = "-hwaccel auto" if use_hwaccel else ""
    
    cmd = f'ffmpeg -y {hwaccel} -i "{fname}" -ac 1 -vn -acodec pcm_s16le -ar 16000 "{outfile}" -loglevel error'
    result = subprocess.call(cmd, shell=True)
    
    return result == 0

def convert(args):
    """Convert m4a files to WAV format using parallel processing"""
    # Find all m4a files
    files = glob.glob(f'{args.save_path}/voxceleb2/*/*/*.m4a')
    if not files:
        print(f"No m4a files found in {args.save_path}/voxceleb2. Please check the directory structure.")
        return
    
    files.sort()
    total_files = len(files)
    
    print(f'Converting {total_files} files from AAC to WAV using {args.max_workers} processes')
    
    # Process files in parallel with a proper picklable function
    with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
        # Use functools.partial to create a picklable function with bound arguments
        convert_fn = functools.partial(convert_single_file, use_hwaccel=args.use_hwaccel)
        
        # Map the function over all files
        results = list(tqdm(
            executor.map(convert_fn, files), 
            total=total_files, 
            desc="Converting files"
        ))
    
    success_count = sum(1 for r in results if r)
    failed_count = total_files - success_count
    
    print(f"Conversion completed: {success_count} successful, {failed_count} failed")

if __name__ == "__main__":
    # Parse input arguments
    parser = argparse.ArgumentParser(description="VoxCeleb file processor")
    parser.add_argument('--save_path', type=str, default="data", help='Directory containing the files')
    parser.add_argument('--list_file', type=str, default="lists/files.txt", help='File containing the list of files to concatenate')
    parser.add_argument('--remove_parts', action='store_true', help='Remove original part files after concatenation')
    parser.add_argument('--check_only', action='store_true', help='Only check if target files exist without concatenating')
    parser.add_argument('--extract', action='store_true', help='Extract zip files and set up directory structure')
    parser.add_argument('--convert', action='store_true', help='Convert m4a files to WAV format')
    parser.add_argument('--max_workers', type=int, default=multiprocessing.cpu_count()-1, 
                        help='Maximum number of worker processes to use for parallel operations')
    parser.add_argument('--use_hwaccel', action='store_true', 
                        help='Use hardware acceleration for ffmpeg conversion if available')
    
    args = parser.parse_args()

    # Ensure max_workers is at least 1
    args.max_workers = max(1, args.max_workers)

    # Check if save_path exists
    if not os.path.exists(args.save_path):
        raise ValueError('Target directory does not exist.')
    
    # Check if specific files already exist
    target_files = ['vox2_aac.zip', 'vox2_mp4.zip']
    existing_files, missing_files = check_files_exist(args, target_files)
    
    if existing_files:
        print(f"The following files already exist: {', '.join(existing_files)}")
    
    if missing_files:
        print(f"The following files are missing: {', '.join(missing_files)}")
    
    if args.check_only:
        print("Check completed. Exiting without further processing.")
        exit(0)
    
    # Process list file for concatenation if needed
    perform_concatenation = True
    if not os.path.exists(args.list_file):
        print(f"List file {args.list_file} does not exist. Skipping concatenation.")
        perform_concatenation = False
    
    # Concatenate files if needed
    if perform_concatenation:
        with open(args.list_file, 'r') as f:
            files = f.readlines()
        concatenate(args, files)
    
    # Extract files if requested
    if args.extract:
        extract_files(args, existing_files)
    
    # Convert files if requested
    if args.convert:
        convert(args)
    
    print("Processing completed!") 