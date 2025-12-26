import os
import io
import json
import csv
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
IG_USERNAME = os.getenv("IG_USERNAME", "house_of_foofaji")
IG_PASSWORD = os.getenv("IG_PASSWORD", "YOUR_PASSWORD")

SERVICE_ACCOUNT_FILE = "service_account.json"
FOLDER_ID = os.getenv("FOLDER_ID")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SESSION_FILE = "house_of_foofaji.session"
DOWNLOAD_DIR = "downloads"
POSTED_JSON = "posted.json"
LOG_CSV = "upload_log.csv"

SCHEDULE_TIMES = ["06:00", "10:00", "15:00", "18:00", "20:00", "22:00"]
# ==========================================

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ================= FLASK (Render Free needs port) =================
app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Insta Auto Reels Bot Running"

# ================= UTILITIES =================

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

def load_posted():
    if not os.path.exists(POSTED_JSON):
        return []
    with open(POSTED_JSON, "r") as f:
        return json.load(f)

def save_posted(data):
    with open(POSTED_JSON, "w") as f:
        json.dump(data, f, indent=2)

def log_csv(filename, caption):
    exists = os.path.exists(LOG_CSV)
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["filename", "caption", "datetime"])
        writer.writerow([filename, caption, datetime.now().isoformat()])

def random_caption(filename):
    base = os.path.splitext(filename)[0].replace("_", " ")
    hashtags = []
    if os.path.exists("hashtags.txt"):
        with open("hashtags.txt", "r", encoding="utf-8") as f:
            tags = [t.strip() for t in f if t.strip()]
        hashtags = random.sample(tags, min(6, len(tags)))
    return base + "\n\n" + " ".join(hashtags)

# ================= GOOGLE DRIVE =================

def get_drive():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)

def get_random_unposted(drive):
    posted = load_posted()
    posted_ids = {p["id"] for p in posted}

    results = drive.files().list(
        q=f"'{FOLDER_ID}' in parents and trashed=false",
        fields="files(id,name,mimeType)",
        pageSize=1000
    ).execute()

    videos = [
        f for f in results.get("files", [])
        if f["id"] not in posted_ids and f["name"].lower().endswith(".mp4")
    ]

    return random.choice(videos) if videos else None

def download_file(drive, file):
    path = os.path.join(DOWNLOAD_DIR, file["name"])
    request = drive.files().get_media(fileId=file["id"])
    fh = io.FileIO(path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return path

# ================= INSTAGRAM =================

def login_instagram():
    cl = Client()
    if os.path.exists(SESSION_FILE):
        cl.load_settings(SESSION_FILE)
        cl.login(IG_USERNAME, IG_PASSWORD)
    else:
        cl.login(IG_USERNAME, IG_PASSWORD)
        cl.dump_settings(SESSION_FILE)
    return cl

# ================= MAIN UPLOAD =================

def upload_one_reel():
    try:
        print("Checking for next reel...")
        drive = get_drive()
        file = get_random_unposted(drive)

        if not file:
            send_telegram("ℹ️ No reels left to upload.")
            return

        path = download_file(drive, file)
        caption = random_caption(file["name"])

        cl = login_instagram()
        cl.video_upload(path, caption)

        posted = load_posted()
        posted.append({"id": file["id"], "name": file["name"]})
        save_posted(posted)
        log_csv(file["name"], caption)

        total = len(drive.files().list(
            q=f"'{FOLDER_ID}' in parents and trashed=false",
            fields="files(id)"
        ).execute()["files"])

        msg = (
            "✅ Reel Uploaded\n\n"
            f"📹 {file['name']}\n"
            f"📊 Uploaded: {len(posted)} / {total}\n"
            f"⏰ {datetime.now().strftime('%d-%m-%Y %H:%M')}"
        )
        send_telegram(msg)

    except Exception as e:
        send_telegram(f"❌ Upload failed: {e}")

# ================= SCHEDULER =================

def scheduler_loop():
    for t in SCHEDULE_TIMES:
        schedule.every().day.at(t).do(upload_one_reel)
        print("Scheduled at", t)

    while True:
        schedule.run_pending()
        time.sleep(10)

# ================= START =================

if __name__ == "__main__":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
