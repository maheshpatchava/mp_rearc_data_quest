"""
BLS and Population data ingestion with idempotency tracking.

Downloads data from BLS productivity time-series directory and DataUSA API.
Tracks file hashes to avoid reprocessing unchanged files.
"""

import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF = 1  # seconds
MAX_BACKOFF = 10  # seconds
RATE_LIMIT_DELAY = 0.5  # seconds between requests (be nice to servers)


def retry_with_backoff(func, *args, max_retries=MAX_RETRIES, **kwargs):
    """
    Retry a function with exponential backoff.

    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        *args, **kwargs: Arguments to pass to func

    Returns:
        Result of successful function call

    Raises:
        Exception from last failed attempt
    """
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (requests.exceptions.RequestException, ConnectionError) as e:
            if attempt == max_retries - 1:
                raise

            backoff = min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)
            logger.warning(
                f"Attempt {attempt + 1}/{max_retries} failed: {e}. "
                f"Retrying in {backoff}s..."
            )
            time.sleep(backoff)


class IngestionTracker:
    """Tracks file content hashes for idempotent ingestion."""

    def __init__(self, tracker_path: str):
        self.tracker_path = tracker_path
        self.tracking_data = self._load_tracking_data()

    def _load_tracking_data(self) -> Dict[str, str]:
        if os.path.exists(self.tracker_path):
            with open(self.tracker_path, 'r') as f:
                return json.load(f)
        return {}

    def _save_tracking_data(self):
        tracker_dir = os.path.dirname(self.tracker_path)
        if tracker_dir:
            os.makedirs(tracker_dir, exist_ok=True)
        with open(self.tracker_path, 'w') as f:
            json.dump(self.tracking_data, f, indent=2)

    def compute_hash(self, content: bytes) -> str:
        return hashlib.md5(content).hexdigest()

    def is_file_changed(self, filename: str, content_hash: str) -> bool:
        return self.tracking_data.get(filename) != content_hash

    def mark_as_ingested(self, filename: str, content_hash: str):
        self.tracking_data[filename] = content_hash
        self._save_tracking_data()

    def get_ingested_files(self) -> List[str]:
        return list(self.tracking_data.keys())


class BLSDataIngestion:
    """BLS productivity time-series data ingestion."""

    def __init__(self, bls_base_url: str, user_agent: str):
        self.bls_base_url = bls_base_url
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})

    def list_files_from_directory(self) -> List[Tuple[str, str]]:
        """Parse HTML directory listing to get file URLs."""
        logger.info(f"Fetching directory listing from {self.bls_base_url}")

        response = retry_with_backoff(self.session.get, self.bls_base_url)
        response.raise_for_status()

        pattern = r'<[Aa] [Hh][Rr][Ee][Ff]="([^"]+)"'
        matches = re.findall(pattern, response.text)

        file_links = []
        for href in matches:
            filename = href.split('/')[-1]
            if filename and not filename.startswith('?') and filename not in ['..', '']:
                file_url = urljoin(self.bls_base_url, filename)
                file_links.append((filename, file_url))

        logger.info(f"Found {len(file_links)} files")
        return file_links

    def ingest_all(self, destination_dir: str, tracker_path: str) -> Dict[str, Any]:
        """Download all files with idempotency. Only downloads changed files."""
        os.makedirs(destination_dir, exist_ok=True)
        tracker = IngestionTracker(tracker_path)

        file_links = self.list_files_from_directory()
        current_files = set(filename for filename, _ in file_links)
        previous_files = set(tracker.get_ingested_files())
        removed_files = previous_files - current_files

        if removed_files:
            logger.warning(f"Files removed from source: {removed_files}")

        downloaded_files = []
        skipped_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_dir = os.path.join(destination_dir, timestamp)

        for filename, url in file_links:
            try:
                # Rate limiting: be respectful to BLS servers
                time.sleep(RATE_LIMIT_DELAY)

                response = retry_with_backoff(self.session.get, url)
                response.raise_for_status()
                content = response.content
                content_hash = tracker.compute_hash(content)

                if not tracker.is_file_changed(filename, content_hash):
                    logger.info(f"Skipping unchanged file: {filename}")
                    skipped_files.append(filename)
                    continue

                os.makedirs(timestamped_dir, exist_ok=True)
                destination_path = os.path.join(timestamped_dir, filename)

                with open(destination_path, 'wb') as f:
                    f.write(content)

                tracker.mark_as_ingested(filename, content_hash)
                downloaded_files.append(destination_path)
                logger.info(f"Downloaded: {filename}")

            except Exception as e:
                logger.warning(f"Failed to download {filename}: {e}")
                continue

        return {
            'total_files': len(file_links),
            'downloaded': len(downloaded_files),
            'skipped': len(skipped_files),
            'removed': len(removed_files),
            'timestamp_dir': timestamp if downloaded_files else None,
            'downloaded_files': downloaded_files,
            'removed_files': list(removed_files)
        }


class PopulationDataIngestion:
    """DataUSA API population data ingestion."""

    def __init__(self, population_api_url: str, user_agent: str):
        self.population_api_url = population_api_url
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})

    def fetch_population_data(self) -> Dict[str, Any]:
        logger.info(f"Fetching population data from API")
        response = retry_with_backoff(self.session.get, self.population_api_url)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Fetched {len(data.get('data', []))} records")
        return data

    def save_to_file(self, destination_dir: str, tracker_path: str) -> Tuple[str, bool, str]:
        """Save population data as single-line JSON (compact raw format)."""
        try:
            tracker = IngestionTracker(tracker_path)
            data = self.fetch_population_data()

            # Create single-line JSON (compact, no indentation)
            compact_json = json.dumps(data, sort_keys=True)
            content_hash = tracker.compute_hash(compact_json.encode('utf-8'))
            filename = "population.json"

            if not tracker.is_file_changed(filename, content_hash):
                logger.info("Population data unchanged, skipping")
                return None, False, None

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            timestamped_dir = os.path.join(destination_dir, timestamp)
            os.makedirs(timestamped_dir, exist_ok=True)

            destination_path = os.path.join(timestamped_dir, filename)
            with open(destination_path, 'w') as f:
                f.write(compact_json)

            tracker.mark_as_ingested(filename, content_hash)
            logger.info(f"Saved raw JSON as single line ({len(data.get('data', []))} records)")
            return destination_path, True, timestamp

        except Exception as e:
            logger.error(f"Failed to fetch/save population data: {e}")
            raise
