
import requests
import argparse
import os
import time
from typing import Optional
import portalocker
import os

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
            print(f"📁 File: {csv_file_path} ({file_size:.2f} MB)")
            print(f"🏷️  Table: {table_name}")
            print(f"🌐 Upload URL: {upload_url}")
            print("=" * 60)

            with open(csv_file_path, 'rb') as csv_file:
                files = {'file': csv_file}
                data = {'tableName': table_name}

                print("🚀 Starting upload...")
                appendfile(filename, "Starting upload..."+self.base_url)

                response = requests.post(
                    upload_url,
                    files=files,
                    data=data,
                    stream=True,
                    timeout=timeout
                )

                if response.status_code == 200:
                    print("✅ Upload successful, streaming results:")
                    appendfile(filename, "✅ Upload successful, streaming results:")
                    appendfile(filename, "Starting upload...")
                    print("-" * 40)

                    # Stream and print the response
                    for line in response.iter_lines(decode_unicode=True):
                        if line.strip():  # Only print non-empty lines
                            # Add timestamp to each line for better tracking
                            timestamp = time.strftime("%H:%M:%S")
                            print(f"[{timestamp}] {line}")
                            appendfile(filename, f"[{timestamp}] {line}")


                    print("-" * 40)
                    print("🎉 Processing complete!")
                    appendfile(filename, "🎉 Processing complete!")
                    return True

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

def main():
    """Main function with command line argument parsing"""

    parser = argparse.ArgumentParser(
        description="Upload CSV file to transcription service"
    )
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument("table_name", help="Name of the table to process")
    parser.add_argument(
        "--url", 
        default="https://alexs1.ngrok.app",
        help="Base URL of the Flask app (default: http://localhost:10000)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60000,
        help="Request timeout in seconds (default: 300)"
    )

    parser.add_argument(
        "--textfile",
        help="File path to append all output logs"
    )

    args = parser.parse_args()

    # Create client and upload
    client = UploadClient(args.url)
    success = client.upload_csv(args.csv_file, args.table_name, args.timeout, args.textfile)

    # Exit with appropriate code
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
