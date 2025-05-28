import os
import time
from datetime import datetime, timedelta
import logging

FEEDS_DIR = '/opt/rss_collector/rss_feeds'
DAYS_OLD = 0

def clean_old_files(directory, days_old):
    now = time.time()
    cutoff = now - (days_old * 86400)
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        # Logging setup
        logging.basicConfig(
            filename='/opt/rss_collector/clean_feeds.log',
            level=logging.INFO,
            format='%(asctime)s %(levelname)s:%(message)s'
        )
        if os.path.isfile(filepath):
            file_mtime = os.path.getmtime(filepath)
            if file_mtime < cutoff:
                try:
                    os.remove(filepath)
                    logging.info(f"Deleted: {filepath}")
                except Exception as e:
                    message = f"Error deleting {filepath}: {e}"
                    logging.error(message)

if __name__ == "__main__":
    clean_old_files(FEEDS_DIR, DAYS_OLD)