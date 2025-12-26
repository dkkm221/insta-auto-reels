import os
import json
import csv
import time
import random
import schedule
from datetime import datetime

from instagrapi import Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import requests

# ================= CONFIG =================


SCHEDULE_TIMES = ["06:00", "10:00", "15:00", "18:00", "20:00", "22:00"]
#SCHEDULE_TIMES = ["00:51", "00:53", "15:00", "18:00", "20:00", "22:00"]
# ==========================================

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---------------- Telegram ----------------
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

# ---------------- Drive ----------------
def drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)

def list_drive_videos(service):
    q = f"'{FOLDER_ID}' in parents and trashed=false"
    res = service.files().list(
        q=q,
        fields="files(id,name,mimeType)",
        pageSize=1000
    ).execute()
    return [f for f in res.get("files", []) if "video" in f["mimeType"]]

def download_file(service, file_id, path):
    req = service.files().get_media(fileId=file_id)
    with open(path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()

# ---------------- Instagram ----------------
def login_instagram():
    cl = Client()
    if os.path.exists(SESSION_FILE):
        cl.load_settings(SESSION_FILE)
        cl.login(IG_USERNAME, IG_PASSWORD)
    else:
        print("First login, OTP may be required...")
        cl.login(IG_USERNAME, IG_PASSWORD)
        cl.dump_settings(SESSION_FILE)
    return cl

# ---------------- Caption ----------------
def make_caption(filename):
    title = os.path.splitext(filename)[0].replace("_", " ")
    tags = []
    if os.path.exists(HASHTAGS_FILE):
        with open(HASHTAGS_FILE, "r", encoding="utf-8") as f:
            tags = [t.strip() for t in f if t.strip()]
    random_tags = random.sample(tags, min(6, len(tags)))
    return title + "\n\n" + " ".join(random_tags)

# ---------------- Posted DB ----------------
def load_posted():
    if not os.path.exists(POSTED_FILE):
        return []
    with open(POSTED_FILE, "r") as f:
        return json.load(f)

def save_posted(posted):
    with open(POSTED_FILE, "w") as f:
        json.dump(posted, f, indent=2)

# ---------------- CSV ----------------
def log_csv(name, fid, caption):
    exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["filename", "file_id", "caption", "uploaded_at"])
        w.writerow([name, fid, caption, datetime.now().isoformat()])

# ---------------- Upload Logic ----------------
def upload_one_reel():
    print("Checking for next reel...")
    drive = drive_service()
    videos = list_drive_videos(drive)
    posted = load_posted()

    remaining = [v for v in videos if v["id"] not in posted]
    if not remaining:
        send_telegram("✅ All reels uploaded.")
        return

    video = random.choice(remaining)
    path = os.path.join(DOWNLOAD_DIR, video["name"])

    print("Downloading:", video["name"])
    download_file(drive, video["id"], path)

    cl = login_instagram()
    caption = make_caption(video["name"])

    cl.video_upload(path, caption=caption)

    posted.append(video["id"])
    save_posted(posted)
    log_csv(video["name"], video["id"], caption)

    total = len(videos)
    done = len(posted)
    left = total - done

    send_telegram(
        f"✅ Reel Uploaded\n\n"
        f"📹 {video['name']}\n"
        f"📊 Uploaded: {done}/{total}\n"
        f"📦 Remaining: {left}\n"
        f"⏰ {datetime.now().strftime('%d-%m-%Y %H:%M')}"
    )

    os.remove(path)

# ---------------- Scheduler ----------------
def start_scheduler():
    print("Auto Reel Uploader Started")
    for t in SCHEDULE_TIMES:
        schedule.every().day.at(t).do(upload_one_reel)
        print("Scheduled at", t)

    print("Scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(10)

# ================= MAIN =================
if __name__ == "__main__":
    start_scheduler()
