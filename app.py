import os
import io
import csv
import json
import time
import random
import threading
from datetime import datetime

from flask import Flask
import schedule
import requests

from instagrapi import Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ================= CONFIG =================
IG_USERNAME = os.getenv("IG_USERNAME")
IG_PASSWORD = os.getenv("IG_PASSWORD")

SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"
SESSION_FILE = "/etc/secrets/house_of_foofaji.session"

FOLDER_ID = os.getenv("FOLDER_ID")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DOWNLOAD_DIR = "downloads"
POSTED_FILE = "posted.json"
LOG_FILE = "upload_log.csv"
HASHTAG_FILE = "hashtags.txt"

SCHEDULE_TIMES = ["06:00", "10:00", "15:00", "18:00", "20:00", "22:00"]
# ==========================================

app = Flask(__name__)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---------- Telegram ----------
def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

# ---------- Instagram ----------
def login_instagram():
    cl = Client()
    if os.path.exists(SESSION_FILE):
        cl.load_settings(SESSION_FILE)
        cl.login(IG_USERNAME, IG_PASSWORD)
    else:
        cl.login(IG_USERNAME, IG_PASSWORD)
        cl.dump_settings(SESSION_FILE)
    return cl

# ---------- Google Drive ----------
def drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)

def list_videos(service):
    res = service.files().list(
        q=f"'{FOLDER_ID}' in parents and trashed=false",
        fields="files(id,name,mimeType)",
        pageSize=1000
    ).execute()
    videos = [f for f in res.get("files", []) if "video" in f["mimeType"]]
    random.shuffle(videos)
    return videos

# ---------- Helpers ----------
def load_posted():
    if not os.path.exists(POSTED_FILE):
        return []
    with open(POSTED_FILE, "r") as f:
        return json.load(f)

def save_posted(data):
    with open(POSTED_FILE, "w") as f:
        json.dump(data, f)

def random_caption(filename):
    base = os.path.splitext(filename)[0].replace("_", " ")
    tags = []
    if os.path.exists(HASHTAG_FILE):
        with open(HASHTAG_FILE) as f:
            tags = [t.strip() for t in f if t.strip()]
    return base + "\n\n" + " ".join(random.sample(tags, min(6, len(tags))))

def log_csv(name, caption):
    new = not os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["time", "video", "caption"])
        w.writerow([datetime.now(), name, caption])

# ---------- Upload ----------
def upload_one():
    try:
        print("Checking for next reel...")
        drive = drive_service()
        videos = list_videos(drive)
        posted = load_posted()

        remaining = [v for v in videos if v["id"] not in posted]
        if not remaining:
            send_telegram("✅ All reels uploaded")
            return

        video = remaining[0]
        path = os.path.join(DOWNLOAD_DIR, video["name"])

        request = drive.files().get_media(fileId=video["id"])
        with io.FileIO(path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        caption = random_caption(video["name"])
        cl = login_instagram()
        cl.video_upload(path, caption=caption)

        posted.append(video["id"])
        save_posted(posted)
        log_csv(video["name"], caption)

        send_telegram(
            f"✅ Reel Uploaded\n\n"
            f"📹 {video['name']}\n"
            f"📦 Uploaded: {len(posted)}\n"
            f"📂 Remaining: {len(remaining)-1}"
        )

        os.remove(path)

    except Exception as e:
        send_telegram(f"❌ Upload failed\n{str(e)}")
        print(e)

# ---------- Scheduler ----------
def scheduler_loop():
    for t in SCHEDULE_TIMES:
        schedule.every().day.at(t).do(upload_one)
        print(f"Scheduled at {t}")
    while True:
        schedule.run_pending()
        time.sleep(20)

# ---------- Flask ----------
@app.route("/")
def home():
    return "Insta Auto Reels Uploader is running"

if __name__ == "__main__":
    print("Auto Reel Uploader Started")
    threading.Thread(target=scheduler_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
