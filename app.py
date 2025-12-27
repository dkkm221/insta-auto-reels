import os
import io
import csv
import json
import time
import random
import threading
import schedule
from datetime import datetime

from flask import Flask
from instagrapi import Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import requests

# =========================
# CONFIG
# =========================
FOLDER_ID = os.getenv("FOLDER_ID")
SERVICE_ACCOUNT_FILE = "service_account.json"

DOWNLOAD_DIR = "downloads"
POSTED_JSON = "posted.json"
UPLOAD_LOG = "upload_log.csv"
HASHTAGS_FILE = "hashtags.txt"

SCHEDULE_TIMES = ["06:00", "10:00", "15:00", "18:00", "20:00", "22:00"]

IG_SESSION_JSON = os.getenv("IG_SESSION_JSON")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# =========================
# FLASK KEEP ALIVE (FREE RENDER)
# =========================
app = Flask(__name__)

@app.route("/")
def home():
    return "Insta Auto Reels Bot Running"

def run_flask():
    app.run(host="0.0.0.0", port=10000)

threading.Thread(target=run_flask, daemon=True).start()

# =========================
# GOOGLE DRIVE
# =========================
def get_drive():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def list_videos(service):
    query = f"'{FOLDER_ID}' in parents and trashed=false"
    res = service.files().list(
        q=query,
        fields="files(id,name,mimeType)",
        pageSize=1000
    ).execute()
    return [f for f in res.get("files", []) if f["name"].lower().endswith((".mp4", ".mov", ".m4v"))]

def download_video(service, file):
    path = os.path.join(DOWNLOAD_DIR, file["name"])
    request = service.files().get_media(fileId=file["id"])
    fh = io.FileIO(path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()
    return path

# =========================
# INSTAGRAM (SESSION ONLY)
# =========================
def login_instagram():
    cl = Client()
    session = json.loads(IG_SESSION_JSON)
    cl.set_settings(session)
    cl.login_by_sessionid(session.get("sessionid"))
    return cl

# =========================
# HELPERS
# =========================
def load_posted():
    if not os.path.exists(POSTED_JSON):
        return []
    with open(POSTED_JSON, "r") as f:
        return json.load(f)

def save_posted(data):
    with open(POSTED_JSON, "w") as f:
        json.dump(data, f, indent=2)

def get_caption(filename):
    text = os.path.splitext(filename)[0].replace("_", " ")
    tags = []
    if os.path.exists(HASHTAGS_FILE):
        with open(HASHTAGS_FILE) as f:
            tags = [t.strip() for t in f if t.strip()]
    if tags:
        text += "\n\n" + " ".join(random.sample(tags, min(6, len(tags))))
    return text[:2200]

def log_csv(name, caption):
    exists = os.path.exists(UPLOAD_LOG)
    with open(UPLOAD_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["datetime", "filename", "caption"])
        writer.writerow([datetime.now(), name, caption])

def notify(msg):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

# =========================
# MAIN UPLOAD LOGIC
# =========================
def upload_one_reel():
    print("Checking for next reel...")
    drive = get_drive()
    files = list_videos(drive)
    posted = load_posted()

    remaining = [f for f in files if f["id"] not in posted]
    if not remaining:
        notify("✅ All videos uploaded.")
        return

    file = random.choice(remaining)
    path = download_video(drive, file)

    cl = login_instagram()
    caption = get_caption(file["name"])

    cl.video_upload(path, caption=caption)

    posted.append(file["id"])
    save_posted(posted)
    log_csv(file["name"], caption)

    notify(
        f"✅ Reel Uploaded\n\n📹 {file['name']}\n"
        f"📊 Uploaded: {len(posted)} / {len(files)}\n"
        f"⏰ {datetime.now().strftime('%d-%m-%Y %H:%M')}"
    )

    os.remove(path)

# =========================
# SCHEDULER
# =========================
def start_scheduler():
    print("Auto Reel Uploader Started")
    for t in SCHEDULE_TIMES:
        schedule.every().day.at(t).do(upload_one_reel)
        print("Scheduled at", t)

    while True:
        schedule.run_pending()
        time.sleep(15)

# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    start_scheduler()
