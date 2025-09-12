# thread_safe_runner.py

import threading
from concurrent.futures import ThreadPoolExecutor
import time
from csv_splitter import (
    get_most_recent_file,
    getbestServer,
    process_file_and_fetch_status,
    run_upload_client,
    add_to_global_dict,
    is_in_global_dict,
    get_global_value,
    remove_from_global_dict,
)

# Global lock for critical section
file_lock = threading.Lock()

def run_it_all_scoped():
    """
    1. Under lock: pick the latest file and determine the shared server exactly once.
    2. Store the server URL in a global dict so subsequent threads reuse it.
    3. Unlock before upload to allow parallel run_upload_client calls.
    """
    with file_lock:
        # Step 1: get the next file to process
        file = get_most_recent_file("loadingcsv")

        # Step 2: determine or reuse the shared server URL
        if not is_in_global_dict("shared_server"):
            # Poll until a healthy server is found
            while True:
                server = getbestServer()
                if server != "NA":
                    add_to_global_dict("shared_server", server)
                    break
                print("no server")
                time.sleep(20)
        else:
            server = get_global_value("shared_server")

        # Step 3: process the file under lock
        table = process_file_and_fetch_status(file, server)

    # Critical section ended; uploads can run in parallel now
    print(table["check_test_result"]["table_name"])
    result = run_upload_client(
        file,
        table["check_test_result"]["table_name"],
        server,
        6000,
        "myfileoutput.txt",
    )

    # Clean up file entry from the dict
    remove_from_global_dict(file)
    return result

def main():
    num_workers = 5
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(run_it_all_scoped) for _ in range(num_workers)]
        for future in futures:
            try:
                upload_result = future.result()
                print("Upload result:", upload_result)
            except Exception as e:
                print("Error:", e)

if __name__ == "__main__":
    main()
