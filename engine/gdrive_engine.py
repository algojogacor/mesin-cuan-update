"""
gdrive_engine.py - Upload video dan metadata ke Google Drive queue
Folder: /mesin_cuan/queue/{channel_id}/{timestamp}/
  - video.mp4
  - thumbnail.png
  - metadata.json
"""

import os
import json
import pickle
import tempfile
from engine.utils import get_logger, timestamp

logger = get_logger("gdrive_engine")

GDRIVE_ROOT   = "mesin_cuan"
QUEUE_FOLDER  = "queue"


def upload_to_queue(video_path: str, thumbnail_path: str, metadata: dict, channel: dict) -> str:
    ch_id   = channel["id"]
    service = _get_drive_service(channel)

    root_id  = _ensure_folder(service, GDRIVE_ROOT, None)
    queue_id = _ensure_folder(service, QUEUE_FOLDER, root_id)
    ch_id_f  = _ensure_folder(service, ch_id, queue_id)
    ts       = timestamp()
    vid_f    = _ensure_folder(service, ts, ch_id_f)

    logger.info(f"[{ch_id}] Uploading video ke GDrive queue...")
    _upload_file(service, video_path, "video.mp4", "video/mp4", vid_f)

    if thumbnail_path and os.path.exists(thumbnail_path):
        _upload_file(service, thumbnail_path, "thumbnail.png", "image/png", vid_f)

    meta_payload = {
        "channel_id":          ch_id,
        "language":            channel.get("language", "id"),
        "title":               metadata.get("title", ""),
        "description":         metadata.get("description", ""),
        "tags":                metadata.get("tags", []),
        "category_id":         metadata.get("category_id", "27"),
        "publish_at":          metadata.get("publish_at", ""),
        "made_for_kids":       metadata.get("made_for_kids", False),
        "contains_ai_content": metadata.get("contains_ai_content", True),
        "status":              "ready",
    }
    meta_tmp = os.path.join(tempfile.gettempdir(), f"meta_{ts}.json")
    with open(meta_tmp, "w", encoding="utf-8") as f:
        json.dump(meta_payload, f, ensure_ascii=False, indent=2)

    _upload_file(service, meta_tmp, "metadata.json", "application/json", vid_f)
    os.remove(meta_tmp)

    logger.info(f"[{ch_id}] ✅ GDrive queue: mesin_cuan/queue/{ch_id}/{ts}/")
    return ts


def _get_drive_service(channel: dict):
    from googleapiclient.discovery import build

    cred_file  = channel.get("credentials_file", "")
    token_file = cred_file.replace("_token.json", "_token_token.pickle")

    if not os.path.exists(token_file):
        raise FileNotFoundError(f"Token tidak ditemukan: {token_file}")

    with open(token_file, "rb") as f:
        creds = pickle.load(f)

    if creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request
        creds.refresh(Request())
        with open(token_file, "wb") as f:
            pickle.dump(creds, f)

    return build("drive", "v3", credentials=creds)


def _ensure_folder(service, name: str, parent_id) -> str:
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id)").execute()
    files   = results.get("files", [])
    if files:
        return files[0]["id"]
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        meta["parents"] = [parent_id]
    return service.files().create(body=meta, fields="id").execute()["id"]


def _upload_file(service, local_path: str, drive_name: str, mimetype: str, parent_id: str):
    from googleapiclient.http import MediaFileUpload
    meta  = {"name": drive_name, "parents": [parent_id]}
    media = MediaFileUpload(local_path, mimetype=mimetype, resumable=True)
    service.files().create(body=meta, media_body=media, fields="id").execute()