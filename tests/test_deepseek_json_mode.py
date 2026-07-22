from engine import thumbnail_intelligence, topic_engine


DEEPSEEK_CONFIG = {"base_url": "https://api.deepseek.com/v1"}


def test_topic_request_uses_json_mode_and_parses_the_topics_object():
    payload = topic_engine._build_topic_request_payload(
        DEEPSEEK_CONFIG, "deepseek-v4-flash", "return topics"
    )

    assert payload["thinking"] == {"type": "disabled"}
    assert payload["response_format"] == {"type": "json_object"}
    assert topic_engine._parse_topic_list('{"topics": ["Dark archive"]}') == ["Dark archive"]


def test_thumbnail_request_uses_json_mode_and_parses_the_styles_object():
    payload = thumbnail_intelligence._build_provider_payload(
        DEEPSEEK_CONFIG, "deepseek-v4-pro", "return styles", json_mode=True, max_tokens=768
    )
    styles = [{"style_id": "s1", "text_pattern": "SINGLE_WORD"}]

    assert payload["thinking"] == {"type": "disabled"}
    assert payload["response_format"] == {"type": "json_object"}
    assert payload["max_tokens"] == 768
    assert thumbnail_intelligence._parse_style_list('{"styles": [{"style_id": "s1", "text_pattern": "SINGLE_WORD"}]}') == styles


def test_thumbnail_plain_text_request_does_not_request_json_mode():
    payload = thumbnail_intelligence._build_provider_payload(
        DEEPSEEK_CONFIG, "deepseek-v4-flash", "return one short thumbnail text"
    )

    assert payload["thinking"] == {"type": "disabled"}
    assert "response_format" not in payload
