import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import xml.etree.ElementTree as ET
from flask import Flask, send_file, Response
from functools import lru_cache
from threading import Thread
from time import sleep
import itertools
import concurrent.futures
from pymongo import MongoClient, errors

class Scraper:
    def __init__(self):
        # Environment config
        self.url = os.environ.get('SCRAPER_URL', 'https://www.1tamilmv.earth/')
        self.port = int(os.environ.get("PORT", 8000))
        self.refresh_interval = int(os.environ.get("REFRESH_INTERVAL", 120))

        # MongoDB setup
        mongo_uri = os.environ.get("MONGODB_URI", "mongodb+srv://imaxprime:imaxprime@entiredatabase.pgyu5ay.mongodb.net/?appName=EntireDatabase")
        self.client = MongoClient(mongo_uri)
        self.db = self.client["rssfeed"]
        self.collection = self.db["feeds"]
        self.collection.create_index("link", unique=True)

        # Telegram config
        self.telegram_token = os.environ.get("8306461400:AAGQmmA9udHLSMFi5oJeYoIInaR7PquO5G8")
        self.telegram_chat_id = os.environ.get("-1002055582847")

        # Flask app
        self.app = Flask(__name__)
        self.setup_routes()

        # Start background tasks
        Thread(target=self.begin, daemon=True).start()
        Thread(target=self.run_schedule, daemon=True).start()

    @lru_cache(maxsize=128)
    def get_links(self, url):
        """Get all 'attachment.php' links from forum post."""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'}
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup.find_all('a', href=lambda href: href and 'attachment.php' in href)
        except Exception as e:
            print(f"[ERROR] get_links({url}): {e}")
            return []

    def get_links_with_delay(self, link):
        result = self.get_links(link)
        sleep(2)
        return result

    def scrape(self, links):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(self.get_links_with_delay, itertools.islice(links, 30))
            for result in results:
                for a in result:
                    title = a.text.strip()
                    href = a['href']
                    yield (title, href)

    def fetch_links_from_homepage(self):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'}
            response = requests.get(self.url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            paragraphs = soup.find_all('p', style='font-size: 13.1px;')
            links = [a['href'] for p in paragraphs for a in p.find_all('a', href=True)]
            return [link for link in links if 'index.php?/forums/topic/' in link]
        except Exception as e:
            print(f"[ERROR] fetch_links_from_homepage(): {e}")
            return []

    def begin(self):
        if self.collection.count_documents({}) > 0:
            print("[INIT] MongoDB already has data.")
            self.generate_rss_file()
            return

        print("[INIT] Starting first scrape...")
        thread_links = self.fetch_links_from_homepage()
        scraped = list(self.scrape(thread_links))
        if not scraped:
            print("[INIT] No items scraped.")
            self.generate_rss_file()
            return

        docs = [{"title": t, "link": l, "pubDate": datetime.now().isoformat()} for t, l in scraped]
        try:
            self.collection.insert_many(docs, ordered=False)
            print(f"[INIT] Inserted {len(docs)} items.")
        except errors.BulkWriteError:
            print("[WARN] Duplicates skipped.")

        self.generate_rss_file()

    def job(self):
        print("[REFRESH] Running scheduled scrape...")
        thread_links = self.fetch_links_from_homepage()
        scraped = list(self.scrape(thread_links))

        existing = set(doc['link'] for doc in self.collection.find({}, {'link': 1}))
        new_items = [item for item in scraped if item[1] not in existing]

        if new_items:
            print(f"[REFRESH] Found {len(new_items)} new items.")
            docs = [{"title": t, "link": l, "pubDate": datetime.now().isoformat()} for t, l in new_items]
            try:
                self.collection.insert_many(docs, ordered=False)
            except errors.BulkWriteError:
                print("[WARN] Some duplicates skipped.")

            self.generate_rss_file()
            first_title, first_link = new_items[0]
            self.send_telegram_message(first_title, first_link)
        else:
            print("[REFRESH] No new items found.")

    def run_schedule(self):
        while True:
            sleep(self.refresh_interval)
            self.job()

    def generate_rss_file(self):
        rss = ET.Element('rss', version='2.0')
        channel = ET.SubElement(rss, 'channel')
        ET.SubElement(channel, 'title').text = 'TamilMV RSS Feed'
        ET.SubElement(channel, 'description').text = 'Share and support'
        ET.SubElement(channel, 'link').text = 'https://t.me/telegram'

        latest = self.collection.find().sort("pubDate", -1).limit(10)
        for doc in latest:
            item = ET.SubElement(channel, 'item')
            ET.SubElement(item, 'title').text = doc['title']
            ET.SubElement(item, 'link').text = doc['link']
            ET.SubElement(item, 'pubDate').text = doc['pubDate']

        tree = ET.ElementTree(rss)
        tree.write("tamilmvRSS.xml", encoding='utf-8', xml_declaration=True)
        print("[RSS] RSS feed saved.")

    def send_telegram_message(self, title, link):
        if not self.telegram_token or not self.telegram_chat_id:
            print("[WARN] Telegram credentials missing.")
            return

        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": f"📢 <b>New movie scraped:</b>\n<b>{title}</b>",
            "parse_mode": "HTML",
            "reply_markup": {
                "inline_keyboard": [[{"text": "🔗 Download", "url": link}]]
            }
        }

        try:
            requests.post(url, json=payload)
            print("[TELEGRAM] Message sent.")
        except Exception as e:
            print(f"[ERROR] Telegram alert failed: {e}")

    def setup_routes(self):
        @self.app.route("/")
        def index():
            return send_file("tamilmvRSS.xml")

        @self.app.route("/status")
        def status():
            return Response("✓ Scraper is running", status=200)

    def run(self):
        print(f"[APP] Running on port {self.port}")
        self.app.run(host="0.0.0.0", port=self.port)

if __name__ == "__main__":
    scraper = Scraper()
    scraper.run()

