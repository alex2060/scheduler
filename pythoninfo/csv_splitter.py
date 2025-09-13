#!/usr/bin/env python3

"""
CSV Splitter - Split CSV files into smaller chunks

This program takes a CSV file and splits it into multiple smaller CSV files
based on specified row counts (default: 2, 5, 20 rows per chunk).

Usage:
    python csv_splitter.py input.csv
    python csv_splitter.py input.csv -s 3 10 50
    python csv_splitter.py input.csv -o output_folder
"""

import os
import csv
import glob
import threading
import subprocess
import requests
import hashlib
import time
import json
import re
from typing import Optional, Dict, List, Tuple, Union
from concurrent.futures import ThreadPoolExecutor
import requests
import argparse
from typing import Optional
import portalocker
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(),             # console
        logging.FileHandler("worker.log"),   # file
    ]
)

# Global dictionary for tracking files and servers
_global_dict = {}

def split_csv(input_file: str, chunk_size: int = 2, output_dir: Optional[str] = None) -> Dict[str, List[Dict[str, int]]]:
    """
    Split a CSV file into multiple smaller CSV files of a fixed row count.

    Args:
        input_file (str): Path to the input CSV file.
        chunk_size (int): Number of data rows per split file (default: 2).
        output_dir (Optional[str]): Directory to write split files to (default: same as input file).

    Returns:
        Dict[str, List[Dict[str, int]]]: Summary of created files with keys:
            'chunk_size': the chunk size used
            'files': list of dicts with 'filename' and 'rows'
    """
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file '{input_file}' not found")

    if output_dir is None:
        output_dir = os.path.dirname(input_file) or '.'
    os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(input_file))[0]

    with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
        sample = csvfile.read(1024)
        csvfile.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[',',';','\t','|'])
        except csv.Error:
            dialect = csv.get_dialect('excel')  # default comma-delimited
        reader = csv.reader(csvfile, dialect)
        header = next(reader)
        all_rows = list(reader)

    total_rows = len(all_rows)
    print(f"Input file: {input_file}")
    print(f"Total rows (excluding header): {total_rows}")
    print(f"Header columns: {len(header)} – {header}")
    print(f"Splitting into chunks of {chunk_size} rows each:")

    created_files = {
        'chunk_size': chunk_size,
        'files': []
    }

    num_chunks = (total_rows + chunk_size - 1) // chunk_size
    for i in range(num_chunks):
        start = i * chunk_size
        end = min(start + chunk_size, total_rows)
        chunk = all_rows[start:end]
        fname = f"{base_name}_chunk{chunk_size}_{i+1}.csv"
        out_path = os.path.join(output_dir, fname)
        with open(out_path, 'w', newline='', encoding='utf-8') as outcsv:
            writer = csv.writer(outcsv, dialect)
            writer.writerow(header)
            writer.writerows(chunk)
        created_files['files'].append({
            'filename': out_path,
            'rows': len(chunk)
        })
        print(f"  Created {fname} ({len(chunk)} rows)")

    return created_files

def get_first_row(filename: str) -> List[str]:
    """Return the first row of the given CSV file."""
    with open(filename, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        return next(reader)

def run_upload_client(csv_file: str, table: str, url: str, timeout: int, textfile: str) -> Tuple[int, str, str]:
    """
    Runs the upload_client.py script with the specified arguments.
    Returns (exit_code, stdout, stderr).
    """
    cmd = [
        "python3", "upload_client.py",
        csv_file, table,
        "--url", url,
        "--timeout", str(timeout),
        "--textfile", str(textfile)
    ]
    try:
        completed = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False
        )
    except Exception as e:
        return -1, "", f"Exception running command: {e}"
    return completed.returncode, completed.stdout, completed.stderr

def get_most_recent_file(directory: str, pattern: str = "*", exclude: Optional[Union[List[str], set]] = None) -> Optional[str]:
    """
    Returns the full path of the most recently modified file in 'directory'
    matching 'pattern', excluding any in 'exclude' and any files already 
    tracked in the global dictionary.
    """
    if exclude is None:
        exclude_set = set()
    else:
        exclude_set = {os.path.abspath(p) for p in exclude}
    
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    
    files_sorted = sorted(files, key=lambda p: os.path.getmtime(p), reverse=True)
    count=0
    for f in files_sorted:
        count+=1
        abs_path = f
        if (count==50):
            return None
        
        # Skip if file is in exclude set
        if abs_path in exclude_set:
            continue
            
        # Skip if file is already processed (in global dictionary)
        if is_in_global_dict(abs_path,1):
            continue

            
        # Return the first file that meets all criteria
        return f
    
    return None

def getbestServer(thread_id) -> str:
    """Polls the health endpoint and returns the best server URL, or 'NA'."""
    status = fetch_and_parse_status("https://go3.aimachengine.com/health",thread_id)
    if status['best_server']['health'] != "healthy":
        return "NA"
    return status['best_server']['url']

class StatusFetchError(Exception):
    pass

def fetch_and_parse_status(url: str,thread_id) -> dict:
    """Fetches JSON from URL and returns it, raising on error."""
    try:
        resp = requests.get(url, timeout=50)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise StatusFetchError(f"Failed to GET {url}: {e}")
    try:
        return resp.json()
    except ValueError as e:
        raise StatusFetchError(f"Failed to parse JSON from {url}: {e}")

def read_first_line(file_path: str) -> str:
    """Returns the first line of the file at 'file_path'."""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        return f.readline().rstrip('\r\n')

def compute_sha256_hex(text: str) -> str:
    """Computes SHA-256 hash of text and returns hex string."""
    sha = hashlib.sha256()
    sha.update(text.encode('utf-8'))
    return sha.hexdigest()

def check_test_endpoint(server_base: str, header_hash: str) -> dict:
    """POSTs HeaderHash to '/check_test' and returns JSON response."""
    url = server_base.rstrip('/') + '/check_test'
    payload = {'HeaderHash': header_hash}
    resp = requests.post(url, json=payload, timeout=50)
    resp.raise_for_status()
    return resp.json()

def process_file_and_fetch_status(file_path: str, server_base: str) -> dict:
    """
    1. Reads first line, hashes it.
    2. Sends to '/check_test'.
    Returns dict with 'first_line', 'hash_hex', 'check_test_result'.
    """
    first_line = read_first_line(file_path)
    hash_hex = compute_sha256_hex(first_line)
    check_result = check_test_endpoint(server_base, hash_hex)
    return {
        'first_line': first_line,
        'hash_hex': hash_hex,
        'check_test_result': check_result
    }

def fullsplit(file: str):
    row = get_first_row(file)
    if row == "something":
        split_csv(file, 10, "loadingcsv")
    else:
        split_csv(file, 10, "loadingcsv")

def add_to_global_dict(key: str, value) -> None:
    global _global_dict
    _global_dict[key] = value

def is_in_global_dict(key: str,thread_id) -> bool:
    global _global_dict
    logging.info(f"[Worker {thread_id}] step  done"+_global_dict)
    print(_global_dict)
    return key in _global_dict

def remove_from_global_dict(key: str) -> bool:
    global _global_dict
    if key in _global_dict:
        del _global_dict[key]
        return True
    return False

file_lock = threading.Lock()



def appendfile(filename: str, text: str) -> None:
    """
    Append a line of text to the given file, using an exclusive lock
    to prevent interleaving when called from multiple processes.
    """
    # Open file in append-plus mode to allow locking
    with open(filename, "a+", encoding="utf-8") as f:
        # Acquire exclusive lock (blocks until available)
        portalocker.lock(f, portalocker.LOCK_EX)
        try:
            f.write(text + "\n")
            f.flush()
            os.fsync(f.fileno())
        finally:
            # Always release the lock
            portalocker.unlock(f)

class UploadClient:
    """Client for uploading CSV files to the transcription service"""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def upload_csv(self, csv_file_path: str, table_name: str, 
                   timeout: int = 300,filename: str = "log.csv" ) -> bool:
        """
        Upload CSV file to the /upload endpoint and stream the response

        Args:
            csv_file_path: Path to the CSV file
            table_name: Name of the table to process
            timeout: Request timeout in seconds

        Returns:
            bool: True if successful, False otherwise
        """

        upload_url = f"{self.base_url}/upload"

        if not os.path.exists(csv_file_path):
            print(f"❌ Error: File '{csv_file_path}' does not exist")
            return False

        if not csv_file_path.lower().endswith('.csv'):
            print("❌ Error: File must be a CSV file")
            return False

        try:
            file_size = os.path.getsize(csv_file_path) / (1024 * 1024)  # MB
            print("📁 File: {} ({} MB)"+csv_file_path,flush=True)
            print("🏷️  Table:",flush=True)
            print("🌐 Upload URL: ",flush=True)
            print("=" * 60)

            with open(csv_file_path, 'rb') as csv_file:
                files = {'file': csv_file}
                data = {'tableName': table_name}

                print("🚀 Starting upload..."+upload_url,flush=True)
                appendfile(filename, "Starting upload..."+self.base_url)

                response = requests.post(
                    upload_url,
                    files=files,
                    data=data,
                    stream=True,
                    timeout=timeout
                )

                if response.status_code == 200:
                    print("✅ Upload successful, streaming results:", flush=True)
                    appendfile(filename, "✅ Upload successful, streaming results:")
                    appendfile(filename, "Starting upload...")
                    print("-" * 40)
                    
                    # Buffer to collect all response data
                    response_buffer = []
                    
                    try:
                        # First, try to get the content length to see if we have a complete response
                        content_length = response.headers.get('content-length')
                        if content_length:
                            print(f"Expected content length: {content_length} bytes", flush=True)
                        
                        # Stream and collect the response with timeout handling
                        response_received = False
                        
                        for line in response.iter_lines(decode_unicode=True, chunk_size=1024):
                            if line is not None:  # Check for None explicitly
                                response_received = True
                                line_stripped = line.strip()
                                if line_stripped:  # Only process non-empty lines
                                    timestamp = time.strftime("%H:%M:%S")
                                    formatted_line = f"[{timestamp}] {line_stripped}"
                                    print(formatted_line, flush=True)
                                    appendfile(filename, formatted_line)
                                    response_buffer.append(line_stripped)
                                else:
                                    # Still process empty lines but don't print them
                                    response_buffer.append(line)
                        
                        # If no streaming data was received, try to get the full response content
                        if not response_received:
                            print("No streaming data received, attempting to get full response...", flush=True)
                            try:
                                # Get the full response text as fallback
                                full_content = response.text
                                if full_content.strip():
                                    timestamp = time.strftime("%H:%M:%S")
                                    lines = full_content.split('\n')
                                    for line in lines:
                                        if line.strip():
                                            formatted_line = f"[{timestamp}] {line.strip()}"
                                            print(formatted_line, flush=True)
                                            appendfile(filename, formatted_line)
                                            response_buffer.append(line.strip())
                                else:
                                    print("Response appears to be empty", flush=True)
                                    appendfile(filename, "Response appears to be empty")
                            except Exception as e:
                                print(f"Error reading full response: {e}", flush=True)
                                appendfile(filename, f"Error reading full response: {e}")
                        
                        print("-" * 40)
                        print("🎉 Processing complete!")
                        appendfile(filename, "🎉 Processing complete!")
                        
                        # Log summary
                        total_lines = len([line for line in response_buffer if line.strip()])
                        print(f"Total lines processed: {total_lines}", flush=True)
                        appendfile(filename, f"Total lines processed: {total_lines}")
                        
                        return True
                        
                    except requests.exceptions.ChunkedEncodingError as e:
                        print(f"Chunked encoding error (this might be normal for fast responses): {e}", flush=True)
                        appendfile(filename, f"Chunked encoding error: {e}")
                        # Try to get whatever content we can
                        try:
                            partial_content = response.content.decode('utf-8')
                            if partial_content.strip():
                                timestamp = time.strftime("%H:%M:%S")
                                print(f"[{timestamp}] Partial response: {partial_content}", flush=True)
                                appendfile(filename, f"[{timestamp}] Partial response: {partial_content}")
                        except:
                            pass
                        return True  # Still consider it successful
                        
                    except Exception as e:
                        print(f"Error during streaming: {e}", flush=True)
                        appendfile(filename, f"Error during streaming: {e}")
                        return False

                else:
                    print(f"❌ Error: HTTP {response.status_code}")
                    appendfile(filename, f"❌ Error: HTTP {response.status_code}")
                    try:
                        error_text = response.text
                        print(f"Response: {error_text}")
                        appendfile(filename, f"Response: {error_text}")
                    except:
                        print("Could not read error response")
                    return False

        except requests.exceptions.Timeout:
            print(f"❌ Error: Request timed out after {timeout} seconds")
            appendfile(filename, f"❌ Error: Request timed out after {timeout} seconds")
            return False
        except requests.exceptions.ConnectionError:
            print(f"❌ Error: Could not connect to {upload_url}")
            appendfile(filename, f"❌ Error: Could not connect to {upload_url}")
            print("Make sure the Flask app is running and accessible")
            appendfile(filename, "Make sure the Flask app is running and accessible")
            return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
            appendfile(filename, f"❌ Request error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            appendfile(filename, f"❌ Unexpected error: {e}")
            return False



def run_it_all():
    """
    1. Under lock: pick the latest file and determine the shared server exactly once.
    2. Store entries in global dict to avoid reuse.
    3. Unlock before upload to allow parallel uploads.
    """
    thread_id = threading.current_thread().ident

    with file_lock:
        # Pick next file
        print("loading")
        logging.info(f"[Worker {thread_id}] step  done")
        logging.info(f"[Worker {thread_id}] step  done"+"logged")

        file = get_most_recent_file(thread_id,"loadingcsv")
        if file==None:
            time.sleep(5)
            return "no more files"
        add_to_global_dict(file, "shared_file")

        print("File used", file)
        # Pick server
        while True:
            try:
                server = getbestServer(thread_id)
            except Exception as e:
                remove_from_global_dict(file)
                print("serverdown sleeping")
                logging.info("serverdown sleeping")
                time.sleep(5)
                return "slept server down "+file
            else:
                pass
            finally:
                pass

            if server != "NA" and not is_in_global_dict(server,thread_id):
                add_to_global_dict(server, "shared_server")
                break
            print("no server", server, is_in_global_dict(server,thread_id))
            logging.info(f"[Worker {thread_id}] step  done"+"no server", server, is_in_global_dict(server))
            time.sleep(5)
            print("server used", server)
            logging.info(f"[Worker {thread_id}] step  done"+"server used", server)
        print("server used", server)
        logging.info(f"[Worker {thread_id}] step  done"+"servers used", server)

        # Process under lock
        table = process_file_and_fetch_status(file, server)

        # Cleanup under lock

    # Parallel upload
    logging.info(f"[Worker {thread_id}] step  done"+"here!")
    result = run_upload_client(
        file,
        table["check_test_result"]["table_name"],
        server,
        600000,
        "output/output_"+str(thread_id)+"_"+str(re.sub(r'[^a-zA-Z0-9]', '', file))+"_"+str(str(re.sub(r'[^a-zA-Z0-9]', '', server)))+".txt",
    )
    print("herewego",flush=True)
    logging.info(f"[Worker {thread_id}] step  done"+"here!")
    #client = UploadClient(server)
    #success = client.upload_csv(file, table["check_test_result"]["table_name"], 6000, f"myfileoutput{thread_id}.txt")
    remove_from_global_dict(server)
    
    logging.info(f"[Worker {thread_id}] step  done"+"here!"+file)
    os.remove(file)
    remove_from_global_dict(file)
    return result


def main():
    run_it_all()
    exit()
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(run_it_all) for _ in range(4)}
        
        try:
            while True:
                for future in as_completed(futures):
                    print("starting")
                    try:
                        upload_result = future.result()
                        print("Upload result:", upload_result)
                    except Exception as e:
                        print("Error:", e)
                    futures.remove(future)
                    futures.add(executor.submit(run_it_all))
                    
        except KeyboardInterrupt:
            print("\nStopping workers...")
            for future in futures:
                future.cancel()



if __name__ == "__main__":
    main()
