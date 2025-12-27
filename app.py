import os
import io
import json
import csv
import time
import threading
import random
import schedule
from datetime import datetime
import pytz

from flask import Flask
from instagrapi import Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import requests

# ================= CONFIG =================

SERVICE_ACCOUNT_FILE = "service_account.json"
FOLDER_ID = os.getenv("FOLDER_ID")

IG_USERNAME = os.getenv("IG_USERNAME")
IG_PASSWORD = os.getenv("IG_PASSWORD")

SESSION_FILE = "house_of_foofaji.session"
POSTED_FILE = "posted.json"
CSV_LOG = "upload_log.csv"
HASHTAG_FILE = "hashtags.txt"

DOWNLOAD_DIR = "downloads"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

IST = pytz.timezone("Asia/Kolkata")
# IST is not working, it is working in Western European Time zone WET, Converting actual IST Time to WET for time being
# SCHEDULE_TIMES = ["08:00", "11:30", "15:00", "18:00", "20:00", "22:00"]
SCHEDULE_TIMES = ["02:30", "06:00", "09:30", "12:30", "14:30", "16:30"]

# ==========================================

app = Flask(__name__)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---------- TELEGRAM ----------

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

# ---------- GOOGLE DRIVE ----------

def get_drive():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def list_videos(drive):
    res = drive.files().list(
        q=f"'{FOLDER_ID}' in parents and trashed=false",
        fields="files(id,name,mimeType)",
        pageSize=1000
    ).execute()
    return [f for f in res.get("files", []) if f["mimeType"].startswith("video")]

def download_video(drive, file):
    path = os.path.join(DOWNLOAD_DIR, file["name"])
    request = drive.files().get_media(fileId=file["id"])
    fh = io.FileIO(path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return path

# ---------- INSTAGRAM ----------

def login_instagram():
    cl = Client()
    if os.path.exists(SESSION_FILE):
        cl.load_settings(SESSION_FILE)
        cl.login(IG_USERNAME, IG_PASSWORD)
    else:
        cl.login(IG_USERNAME, IG_PASSWORD)
        cl.dump_settings(SESSION_FILE)
    return cl

# ---------- DATA ----------

def load_posted():
    if not os.path.exists(POSTED_FILE):
        return []
    with open(POSTED_FILE) as f:
        return json.load(f)

def save_posted(data):
    with open(POSTED_FILE, "w") as f:
        json.dump(data, f, indent=2)

def log_csv(row):
    exists = os.path.exists(CSV_LOG)
    with open(CSV_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["time", "filename", "caption"])
        writer.writerow(row)

def build_caption(filename):
    base = os.path.splitext(filename)[0].replace("_", " ")
    hashtags = []
    if os.path.exists(HASHTAG_FILE):
        with open(HASHTAG_FILE) as f:
            tags = [t.strip() for t in f if t.strip()]
            hashtags = random.sample(tags, min(6, len(tags)))
    return base + "\n\n" + " ".join(hashtags)

# ---------- MAIN UPLOAD ----------

def upload_one_reel():
    print("⏰ Checking for scheduled upload...")
    drive = get_drive()
    posted = load_posted()
    posted_ids = {p["id"] for p in posted}

    videos = list_videos(drive)
    remaining = [v for v in videos if v["id"] not in posted_ids]

    if not remaining:
        print("✅ No videos left")
        return

    video = random.choice(remaining)
    path = download_video(drive, video)

    cl = login_instagram()
    caption = build_caption(video["name"])

    cl.video_upload(path, caption=caption)

    posted.append({"id": video["id"], "name": video["name"]})
    save_posted(posted)

    now = datetime.now(IST).strftime("%d-%m-%Y %H:%M")
    log_csv([now, video["name"], caption])

    send_telegram(
        f"✅ Reel Uploaded\n\n📹 {video['name']}\n"
        f"📊 Uploaded: {len(posted)}\n"
        f"📦 Remaining: {len(remaining)-1}\n"
        f"⏰ {now}"
    )

    print("✅ Uploaded:", video["name"])

# ---------- SCHEDULER THREAD ----------

def scheduler_loop():
    for t in SCHEDULE_TIMES:
        schedule.every().day.at(t).do(upload_one_reel)
        print(f"🕒 Scheduled at {t} IST")

    while True:
        schedule.run_pending()
        time.sleep(1)

# ---------- FLASK ----------

@app.route("/")
def home():
    return "Auto Reel Uploader Running ✅"

if __name__ == "__main__":
    print("🚀 Auto Reel Uploader Started (IST)")
    threading.Thread(target=scheduler_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
