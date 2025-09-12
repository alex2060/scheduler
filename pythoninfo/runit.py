import re
import requests
import json
from typing import Any, Dict

class StatusFetchError(Exception):
    """Custom exception for errors during fetch or parse."""
    pass

def fetch_and_parse_status(url: str) -> Dict[str, Any]:
    """
    Fetches the content from the given URL, extracts the JavaScript object
    containing server status, parses it as JSON, and returns it as a dict.

    Args:
        url: The URL to fetch.

    Returns:
        A dictionary representing the parsed JSON object.

    Raises:
        StatusFetchError: If fetching the URL fails or parsing the JSON fails.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        raise StatusFetchError(f"Error fetching URL {url}: {e}")

    # Example pattern: var data = { ... };
    # Adjust the regex if the JSON is assigned differently.
    pattern = re.compile(r"\{(?:\s*\"[^\"]+\"\s*:\s*[^}]+,?)+\}", re.DOTALL)
    match = pattern.search(response.text)
    if not match:
        raise StatusFetchError("Could not locate JSON object in the page content.")

    json_text = match.group(0)
    try:
        data = json.loads(json_text)
    except json.JSONDecodeError as e:
        raise StatusFetchError(f"Error parsing JSON: {e}")

    return data

if __name__ == "__main__":
    test_url = "https://example.com/status.js"
    try:
        status = fetch_and_parse_status(test_url)
        # Pretty-print the parsed data
        print(json.dumps(status, indent=2))
    except StatusFetchError as err:
        print(f"Failed to retrieve status: {err}")
