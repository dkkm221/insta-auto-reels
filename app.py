import os
import io
import csv
import json
import time
import random
import threading
from datetime import datetime
import pytz

from flask import Flask
import schedule
import requests

from instagrapi import Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ===================== CONFIG =====================

IST = pytz.timezone("Asia/Kolkata")

FOLDER_ID = os.getenv("FOLDER_ID")
IG_USERNAME = os.getenv("IG_USERNAME")
IG_PASSWORD = os.getenv("IG_PASSWORD")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SERVICE_ACCOUNT_FILE = "service_account.json"
SESSION_FILE = "house_of_foofaji.session"

DOWNLOAD_DIR = "downloads"
POSTED_JSON = "posted.json"
CSV_LOG = "upload_log.csv"
HASHTAG_FILE = "hashtags.txt"

UPLOAD_TIMES = ["13:25", "13:30", "15:00", "18:00", "20:00", "22:00"]

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ===================== FLASK =====================

app = Flask(__name__)

@app.route("/")
def home():
    return "Insta Auto Reels Bot is Running 🚀"

# ===================== UTIL =====================

def now_ist():
    return datetime.now(IST)

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

def load_posted():
    if not os.path.exists(POSTED_JSON):
        return []
    with open(POSTED_JSON, "r") as f:
        return json.load(f)

def save_posted(data):
    with open(POSTED_JSON, "w") as f:
        json.dump(data, f, indent=2)

def log_csv(name, caption):
    exists = os.path.exists(CSV_LOG)
    with open(CSV_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["filename", "caption", "uploaded_at"])
        writer.writerow([name, caption, now_ist().strftime("%d-%m-%Y %H:%M")])

def random_caption(filename):
    base = os.path.splitext(filename)[0].replace("_", " ")
    tags = []
    if os.path.exists(HASHTAG_FILE):
        with open(HASHTAG_FILE) as f:
            all_tags = [t.strip() for t in f if t.strip()]
            tags = random.sample(all_tags, min(6, len(all_tags)))
    return base + "\n\n" + " ".join(tags)

# ===================== GOOGLE DRIVE =====================

def drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def list_videos():
    service = drive_service()
    q = f"'{FOLDER_ID}' in parents and trashed=false"
    res = service.files().list(
        q=q,
        fields="files(id,name,mimeType)",
        pageSize=1000
    ).execute()
    files = res.get("files", [])
    return [f for f in files if f["name"].lower().endswith(".mp4")]

def download_video(file_id, name):
    path = os.path.join(DOWNLOAD_DIR, name)
    service = drive_service()
    req = service.files().get_media(fileId=file_id)
    fh = io.FileIO(path, "wb")
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()
    return path

# ===================== INSTAGRAM =====================

def instagram_login():
    cl = Client()
    if os.path.exists(SESSION_FILE):
        cl.load_settings(SESSION_FILE)
        cl.login(IG_USERNAME, IG_PASSWORD)
    else:
        cl.login(IG_USERNAME, IG_PASSWORD)
        cl.dump_settings(SESSION_FILE)
    return cl

# ===================== CORE UPLOAD =====================

def upload_one_reel():
    try:
        print("Checking for next reel...")

        posted = load_posted()
        posted_ids = {p["id"] for p in posted}

        videos = list_videos()
        remaining = [v for v in videos if v["id"] not in posted_ids]

        if not remaining:
            send_telegram("✅ All videos uploaded.")
            return

        video = random.choice(remaining)
        path = download_video(video["id"], video["name"])

        caption = random_caption(video["name"])
        cl = instagram_login()

        cl.video_upload(path, caption=caption)

        posted.append({"id": video["id"], "name": video["name"]})
        save_posted(posted)
        log_csv(video["name"], caption)

        send_telegram(
            f"✅ Reel Uploaded\n\n"
            f"📹 {video['name']}\n"
            f"📦 Uploaded: {len(posted)} / {len(videos)}\n"
            f"⏰ {now_ist().strftime('%d-%m-%Y %H:%M')}"
        )

        os.remove(path)

    except Exception as e:
        send_telegram(f"❌ Upload failed:\n{e}")
        print(e)

# ===================== SCHEDULER (IST FIX) =====================

def scheduler_loop():
    schedule.clear()

    for t in UPLOAD_TIMES:
        schedule.every().day.at(t).do(upload_one_reel)
        print(f"Scheduled at {t} IST")

    while True:
        schedule.run_pending()
        time.sleep(30)

# ===================== START =====================

if __name__ == "__main__":
    print("Auto Reel Uploader Started (IST)")
    threading.Thread(target=scheduler_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
