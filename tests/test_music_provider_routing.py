from unittest.mock import MagicMock, patch

from engine import music_engine


def _provider_config(*, model="deepseek-v4-flash", candidates=None):
    return {
        "base_url": "https://api.deepseek.com/v1",
        "api_key": "test-key",
        "model": model,
        "candidates": candidates or [model],
    }


def test_music_selector_uses_provider_a_then_provider_b():
    configs = {"a": _provider_config(), "b": _provider_config()}

    with patch("engine.music_engine.get_provider_config", side_effect=lambda slot: configs[slot]):
        assert music_engine._music_ai_provider_order() == ["provider_a", "provider_b"]


def test_music_selector_calls_deepseek_flash_with_json_mode():
    session = MagicMock()
    response = MagicMock()
    response.json.return_value = {
        "choices": [{"message": {"content": '{"index": 0, "reason": "fit"}'}}]
    }
    session.post.return_value = response

    with (
        patch("engine.music_engine.get_provider_config", return_value=_provider_config()),
        patch("engine.music_engine.get_provider_model", return_value="deepseek-v4-flash"),
        patch("engine.music_engine._download_session", return_value=session),
    ):
        result = music_engine._call_music_ai_provider("provider_a", "choose a track")

    assert result == '{"index": 0, "reason": "fit"}'
    request_url = session.post.call_args.args[0]
    request_payload = session.post.call_args.kwargs["json"]
    assert request_url == "https://api.deepseek.com/v1/chat/completions"
    assert request_payload["model"] == "deepseek-v4-flash"
    assert request_payload["response_format"] == {"type": "json_object"}
    assert request_payload["thinking"] == {"type": "disabled"}
