import base64
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

TOKEN_FILE = Path("temp/auth_test/test_v2.json")
TOKEN_CRED_FILE = Path("temp/auth_test/test_token.json")


def test_setup_auth_logic():
    mock_creds = MagicMock()
    mock_creds.to_json.return_value = json.dumps({"token": "fake_token"})

    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(mock_creds.to_json())

    with open(TOKEN_FILE, "r", encoding="utf-8") as f:
        content = f.read()
        b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    assert b64 is not None
    assert json.loads(content)["token"] == "fake_token"


def test_engine_load_logic():
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(json.dumps({"token": "fake_token"}))

    mock_credentials_module = MagicMock()
    mock_discovery_module = MagicMock()
    mock_creds = MagicMock(expired=False, refresh_token="refresh")
    mock_credentials_module.Credentials.from_authorized_user_file.return_value = mock_creds
    mock_discovery_module.build.return_value = MagicMock()

    with patch.dict(
        sys.modules,
        {
            "google.oauth2.credentials": mock_credentials_module,
            "googleapiclient.discovery": mock_discovery_module,
        },
    ):
        from engine.gdrive_engine import _get_drive_service
        from engine.upload_engine import _get_youtube_client

        _get_drive_service({"credentials_file": str(TOKEN_CRED_FILE)})
        _get_youtube_client(str(TOKEN_CRED_FILE))

    assert mock_credentials_module.Credentials.from_authorized_user_file.call_count == 2
    mock_credentials_module.Credentials.from_authorized_user_file.assert_any_call(str(TOKEN_FILE))


if __name__ == "__main__":
    test_setup_auth_logic()
    test_engine_load_logic()
    print("All auth verifications passed.")
