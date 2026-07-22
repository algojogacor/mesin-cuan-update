import engine.script_engine as script_engine
import engine.script_quality_scorer as scorer
import engine.utils as utils


def _configure_two_providers(monkeypatch):
    monkeypatch.delenv("QWEN_API_KEY", raising=False)
    monkeypatch.setenv("PROVIDER_A_BASE_URL", "https://provider-a.test/v1")
    monkeypatch.setenv("PROVIDER_A_API_KEY", "provider-a-key")
    monkeypatch.setenv("PROVIDER_A_MODEL", "mistralai/mistral-large-3-675b-instruct-2512")
    monkeypatch.setenv("PROVIDER_A_MODEL_CANDIDATES", "mistralai/mistral-large-3-675b-instruct-2512")
    monkeypatch.setenv("PROVIDER_B_BASE_URL", "https://provider-b.test/v1")
    monkeypatch.setenv("PROVIDER_B_API_KEY", "provider-b-key")
    monkeypatch.setenv("PROVIDER_B_MODEL", "deepseek-v4-pro")
    monkeypatch.setenv("PROVIDER_B_MODEL_CANDIDATES", "deepseek-v4-pro")


def test_sequential_generation_uses_provider_a_without_qwen(monkeypatch):
    _configure_two_providers(monkeypatch)
    calls = []

    def fake_provider_a(*args, **kwargs):
        calls.append("provider_a")
        return {"script": "naskah", "title": "judul"}

    monkeypatch.setattr(script_engine, "_call_provider_a", fake_provider_a)
    monkeypatch.setattr(
        script_engine,
        "_call_provider_b",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Provider-B should not be first")),
    )

    result = script_engine._call_ai("system", "user", "shorts")

    assert result["script"] == "naskah"
    assert calls == ["provider_a"]


def test_scorer_uses_canonical_provider_slots(monkeypatch):
    _configure_two_providers(monkeypatch)

    assert scorer._build_provider_order("provider_b") == [
        "provider_b",
        "provider_a",
        "groq",
    ]
    assert scorer._build_provider_order("provider_a") == [
        "provider_a",
        "provider_b",
        "groq",
    ]


def test_provider_slots_do_not_fallback_to_qwen_env(monkeypatch):
    monkeypatch.setenv("QWEN_API_KEY", "legacy-qwen-key")
    for key in (
        "PROVIDER_A_BASE_URL",
        "PROVIDER_A_API_KEY",
        "PROVIDER_A_MODEL",
        "PROVIDER_A_MODEL_CANDIDATES",
        "PROVIDER_B_BASE_URL",
        "PROVIDER_B_API_KEY",
        "PROVIDER_B_MODEL",
        "PROVIDER_B_MODEL_CANDIDATES",
    ):
        monkeypatch.delenv(key, raising=False)

    assert utils.get_provider_config("a")["api_key"] == ""
    assert utils.get_provider_config("b")["api_key"] == ""


def test_provider_b_scorer_uses_flash_json_without_thinking(monkeypatch):
    _configure_two_providers(monkeypatch)
    monkeypatch.setenv("PROVIDER_B_BASE_URL", "https://api.deepseek.com/v1")
    monkeypatch.setenv("PROVIDER_B_SCORER_MODEL", "deepseek-v4-flash")
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": '{"ok": true}'}}]}

    class FakeSession:
        trust_env = True

        def post(self, url, *, headers, json, timeout):
            captured.update(url=url, headers=headers, payload=json, timeout=timeout)
            return FakeResponse()

        def close(self):
            return None

    monkeypatch.setattr(scorer.requests, "Session", FakeSession)

    assert scorer._call_provider_b("evaluate this") == '{"ok": true}'
    assert captured["payload"]["model"] == "deepseek-v4-flash"
    assert captured["payload"]["thinking"] == {"type": "disabled"}
    assert captured["payload"]["response_format"] == {"type": "json_object"}
