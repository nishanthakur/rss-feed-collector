# RSS Feed Collector

A tool for collecting and processing RSS feeds.

## Usage
1. Go to `/opt/` directory inside your linux machine.
2. Create a python virtual environment executing the command `python3 -m venv rss_collector`
2. Navigate to that directory by executing `cd /opt/rss_collector`
3. Activate the python virtual environment by running `source bin/activate`
4. Install the required packages by execuring the command `pip install -r requirements.txt`
5. Create a cronjob to execute the script every 1 hour. To do so, open crontab by execting the command `crontab -e` and add `0 * * * * /bin/bash -c 'source /opt/rss_collector/bin/activate && /opt/rss_collector/bin/python /opt/rss_collector/rss_feed_collector.py' >> /opt/rss_collector/cron.log` at the end.
6. It is good idea to clean RSS feeds file that are older than 10 days. For that purpose add this line `#* * * * * /bin/bash -c 'source /opt/rss_collector/bin/activate && /opt/rss_collector/bin/python /opt/rss_collector/clean_feeds.py' >> /opt/rss_collector/cron.log` at the end of crontab.

## Project Structure

- `rss_feed_collector.py`  
    Main script to fetch and parse RSS feeds.


- `requirements.txt`  
    Python dependencies required to run the project.

- `README.md`  
    Project overview and documentation.

- `clean_feeds.py`  
    Removes feeds JSON files which are older than 10 days.

---

Fill in the details for each file and add usage instructions as needed.