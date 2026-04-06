"""
setup_auth.py - Generate OAuth token YouTube + GDrive per channel
Setiap channel pakai Google Cloud Project sendiri
"""

import os
import json
import pickle
import base64
from dotenv import load_dotenv

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/drive.file",
]


def setup_channel(channel: dict):
    ch_id       = channel["id"]
    ch_name     = channel["name"]
    secret_file = channel.get("google_client_secret", "config/google_client_secret.json")
    token_file  = channel["credentials_file"].replace("_token.json", "_token_token.pickle")

    print(f"\n{'='*50}")
    print(f"Setup channel: {ch_name} ({ch_id})")
    print(f"Client secret: {secret_file}")
    print(f"{'='*50}")

    if not os.path.exists(secret_file):
        print(f"⚠️  File tidak ditemukan: {secret_file}")
        print(f"   Download dari Google Cloud Console project untuk channel ini")
        print(f"   Simpan sebagai: {secret_file}")
        input("   Tekan Enter setelah file tersedia, atau Ctrl+C untuk skip...")
        if not os.path.exists(secret_file):
            print(f"   Skip {ch_id}")
            return

    print(f"\n🌐 Browser akan terbuka untuk login '{ch_name}'")
    print(f"   Scope: YouTube Upload + Google Drive")
    input("   Tekan Enter untuk lanjut...")

    from google_auth_oauthlib.flow import InstalledAppFlow

    flow  = InstalledAppFlow.from_client_secrets_file(secret_file, SCOPES)
    creds = flow.run_local_server(port=0)

    os.makedirs(os.path.dirname(token_file), exist_ok=True)
    with open(token_file, "wb") as f:
        pickle.dump(creds, f)

    # Export base64 untuk Koyeb env variable
    with open(token_file, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")

    env_key = f"TOKEN_{ch_id.upper()}"
    print(f"\n✅ Token disimpan: {token_file}")
    print(f"\n📋 Copy nilai ini ke Koyeb env variable '{env_key}':")
    print(f"\n{b64}\n")
    print(f"{'='*50}")


def main():
    with open("config/settings.json", "r", encoding="utf-8") as f:
        settings = json.load(f)

    channels = settings.get("channels", [])
    print(f"🔑 YouTube + GDrive OAuth Setup")
    print(f"   Scope: youtube.upload + drive.file")
    print(f"   Akan setup {len(channels)} channel\n")

    for channel in channels:
        setup_channel(channel)

    print("\n✅ Semua selesai!")
    print("   Simpan semua TOKEN_* ke Koyeb environment variables.")


if __name__ == "__main__":
    main()