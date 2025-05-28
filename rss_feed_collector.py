import feedparser
import json
import logging
import time
from typing import List, Dict, Set
from urllib.parse import urlparse
import hashlib
from datetime import datetime, timezone
import requests
from requests.exceptions import RequestException
import os
from dateutil.parser import parse as parse_date
import pytz
import backoff

# Define constants for paths used across the script
STATE_FILE = '/opt/rss_collector/collector_state.json'
OUTPUT_DIR = '/opt/rss_collector/rss_feeds'
LOG_FILE = '/opt/rss_collector/rss_collector.log'
SOURCE_FILE = '/opt/rss_collector/rss_sources.txt'

# Configure logging once at the module level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RSSFeedCollector:
    """
    A class to collect, parse, deduplicate, and store RSS feed entries.
    
    Attributes:
        feed_urls (List[str]): List of RSS feed URLs to collect.
        output_dir (str): Directory where JSON feed files will be saved.
        state_file (str): JSON file path to persist last fetch times for each feed URL.
        feeds (Dict[str, List[Dict]]): Dictionary storing collected entries per feed URL.
        seen_hashes (Set[str]): Set of hashes to detect duplicate entries across runs.
        last_fetch_times (Dict[str, datetime]): Last successful fetch timestamps per feed URL.
    """
    
    def __init__(self, feed_urls: List[str], output_dir: str = OUTPUT_DIR, state_file: str = STATE_FILE):
        """
        Initialize RSSFeedCollector instance.
        
        Args:
            feed_urls (List[str]): List of RSS feed URLs.
            output_dir (str): Directory for storing JSON feed files.
            state_file (str): File path to persist collector state (last fetch times).
        """
        self.feed_urls = feed_urls
        self.output_dir = output_dir
        self.state_file = state_file
        self.feeds: Dict[str, List[Dict]] = {url: [] for url in feed_urls}
        self.seen_hashes: Set[str] = set()
        self.last_fetch_times = self.load_last_fetch_times()
        
        # Ensure the output directory exists
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def load_last_fetch_times(self) -> Dict[str, datetime]:
        """
        Load last fetch timestamps for each feed URL from the state file.
        
        Returns:
            Dict[str, datetime]: Mapping of feed URL to last fetch datetime.
        """
        logger.info(f"Loading last fetch times from state file: {self.state_file}")
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                fetch_times = {}
                for url in self.feed_urls:
                    timestamp = state.get(url, '1970-01-01T00:00:00Z')
                    parsed_time = parse_date(timestamp).replace(tzinfo=None)
                    fetch_times[url] = parsed_time
                    logger.info(f"Last fetch time for {url}: {parsed_time.isoformat()}")
                return fetch_times
            else:
                logger.warning(f"State file not found. Using default timestamps.")
                default_time = datetime(1970, 1, 1)
                return {url: default_time for url in self.feed_urls}
        except Exception as e:
            logger.error(f"Error loading state file: {str(e)}")
            default_time = datetime(1970, 1, 1)
            return {url: default_time for url in self.feed_urls}

    def save_state(self) -> None:
        """
        Save the current datetime as the last fetch time for all feed URLs to the state file.
        """
        try:
            state = {url: datetime.now(timezone.utc).isoformat() for url in self.feed_urls}
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
            logger.info("Collector state saved successfully.")
        except Exception as e:
            logger.error(f"Error saving collector state: {str(e)}")

    def generate_entry_hash(self, entry: Dict) -> str:
        """
        Generate a unique hash string for an RSS feed entry based on key fields.
        
        Args:
            entry (Dict): RSS feed entry.
            
        Returns:
            str: MD5 hash string representing the entry.
        """
        key_fields = (
            entry.get('title', ''),
            entry.get('link', ''),
            entry.get('published', ''),
            entry.get('summary', '')
        )
        return hashlib.md5(''.join(key_fields).encode('utf-8')).hexdigest()

    @backoff.on_exception(backoff.expo, RequestException, max_tries=3)
    def fetch_feed(self, url: str) -> bytes:
        """
        Fetch RSS feed content from the specified URL with retries on failure.
        
        Args:
            url (str): URL of the RSS feed.
            
        Returns:
            bytes: Content of the fetched RSS feed.
            
        Raises:
            RequestException: If the HTTP request fails after retries.
        """
        response = requests.get(url, timeout=30, headers={'User-Agent': 'RSSFeedCollector/1.0'})
        response.raise_for_status()
        return response.content

    def parse_feed(self, url: str) -> List[Dict]:
        """
        Parse RSS feed content from the given URL and return entries newer than last fetch time.
        
        Args:
            url (str): RSS feed URL.
            
        Returns:
            List[Dict]: List of parsed feed entries in standardized format.
        """
        try:
            logger.info(f"Fetching feed from: {url}")
            content = self.fetch_feed(url)
            feed = feedparser.parse(content)

            if feed.bozo:
                logger.warning(f"Feed at {url} has issues: {feed.bozo_exception}")
                if "syntax error" in str(feed.bozo_exception).lower():
                    logger.error(f"Skipping malformed feed {url}")
                    return []

            # Timezone mappings for certain feed date strings
            tzinfos = {
                'CEST': pytz.timezone('Europe/Paris'),
                'CET': pytz.timezone('Europe/Paris')
            }

            entries = []
            last_fetch_time = self.last_fetch_times.get(url, datetime(1970, 1, 1))

            for entry in feed.entries:
                pub_date_str = entry.get('published', entry.get('updated', ''))
                try:
                    pub_date = parse_date(pub_date_str, tzinfos=tzinfos) if pub_date_str else datetime.now(timezone.utc)
                    if pub_date.tzinfo:
                        pub_date = pub_date.astimezone(pytz.UTC).replace(tzinfo=None)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid date '{pub_date_str}' in feed {url}; using current time")
                    pub_date = datetime.now(timezone.utc).replace(tzinfo=None)

                if pub_date <= last_fetch_time:
                    continue

                parsed_entry = {
                    'title': entry.get('title', ''),
                    'link': entry.get('link', ''),
                    'published': pub_date.isoformat(),
                    'summary': entry.get('summary', ''),
                    'source': urlparse(url).netloc,
                    'fetched_at': datetime.now(timezone.utc).isoformat()
                }

                entry_hash = self.generate_entry_hash(parsed_entry)
                if entry_hash not in self.seen_hashes:
                    self.seen_hashes.add(entry_hash)
                    entries.append(parsed_entry)

            logger.info(f"Collected {len(entries)} new entries from {url}")
            return entries

        except RequestException as e:
            logger.error(f"Request failed for feed {url}: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error parsing feed {url}: {str(e)}")
            return []

    def collect_all_feeds(self) -> None:
        """
        Collect and parse all RSS feeds listed in feed_urls.
        """
        for url in self.feed_urls:
            self.feeds[url] = self.parse_feed(url)
            time.sleep(1)  # Politeness delay

        total_entries = sum(len(entries) for entries in self.feeds.values())
        logger.info(f"Total new entries collected: {total_entries}")

    def save_to_json(self) -> None:
        """
        Save collected feed entries for each URL into separate JSON files.
        Each file is named using the feed domain and current timestamp.
        Entries are saved as JSON objects separated by commas, without enclosing array brackets.
        """
        for url, entries in self.feeds.items():
            if not entries:
                logger.info(f"No new entries to save for feed {url}")
                continue

            try:
                domain = urlparse(url).netloc.replace('.', '_')
                timestamp = datetime.now(timezone.utc).strftime('%Y_%m_%d_%H%M')
                output_file = os.path.join(self.output_dir, f"{domain}_{timestamp}.json")

                with open(output_file, 'w', encoding='utf-8') as f:
                    for i, entry in enumerate(entries):
                        json.dump(entry, f, ensure_ascii=False)
                        if i < len(entries) - 1:
                            f.write(",\n")
                        else:
                            f.write("\n")

                logger.info(f"Saved {len(entries)} entries to {output_file}")

            except Exception as e:
                logger.error(f"Failed to save entries to {output_file}: {str(e)}")

    def run(self) -> None:
        """
        Main method to run the RSS feed collection process end-to-end.
        """
        start_time = time.time()
        logger.info("Starting RSS feed collection")

        self.collect_all_feeds()
        self.save_to_json()
        self.save_state()

        duration = time.time() - start_time
        logger.info(f"RSS feed collection finished in {duration:.2f} seconds")


def main() -> None:
    """
    Load RSS feed URLs from a predefined source file and run the collector.
    """
    try:
        with open(SOURCE_FILE, 'r') as f:
            feed_urls = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.error(f"Source file not found: {SOURCE_FILE}")
        return

    collector = RSSFeedCollector(feed_urls)
    collector.run()


if __name__ == "__main__":
    main()
