import os
from unittest.mock import patch

from engine import footage_engine
from engine import script_engine
from engine.script_quality_scorer import meets_quality_threshold
from main import PIPELINE_STAGE_WORKERS


def test_quality_gate_uses_the_same_one_decimal_precision_as_the_log():
    assert meets_quality_threshold(8.65, 8.7)
    assert not meets_quality_threshold(8.64, 8.7)


def test_pipeline_allows_two_concurrent_render_jobs():
    assert PIPELINE_STAGE_WORKERS["render"] == 2


def test_footage_search_skips_pixabay_when_no_api_key_is_configured():
    with (
        patch.dict(os.environ, {"PIXABAY_API_KEY": ""}, clear=False),
        patch("engine.footage_engine._search_pixabay") as pixabay,
        patch("engine.footage_engine._search_pexels", return_value=[{"id": "px_1"}]),
    ):
        results = footage_engine._search_all_sources("dark corridor", 5)

    assert results == [{"id": "px_1"}]
    pixabay.assert_not_called()


def test_near_threshold_script_stops_after_the_first_parallel_attempt():
    provider_config = {
        "base_url": "https://api.deepseek.com/v1",
        "api_key": "test-key",
        "model": "deepseek-v4-flash",
        "candidates": ["deepseek-v4-flash"],
    }
    script_a = {"script": "A"}
    script_b = {"script": "B"}
    score = {
        "overall": 8.65,
        "verdict": "PASS",
        "hook_strength": 8.5,
        "curiosity_gap": 9.0,
    }

    with (
        patch("engine.script_engine.get_provider_config", return_value=provider_config),
        patch("engine.script_engine._call_provider_a", return_value=script_a) as provider_a,
        patch("engine.script_engine._call_provider_b", return_value=script_b) as provider_b,
        patch("engine.script_quality_scorer.score_script", return_value=score) as scorer,
        patch.object(script_engine, "SCRIPT_QUALITY_THRESHOLD", 8.7),
    ):
        winner = script_engine._generate_and_pick_best(
            "system", "user", "shorts", "test-channel", {"id": "test-channel"}
        )

    assert winner in (script_a, script_b)
    assert provider_a.call_count == 1
    assert provider_b.call_count == 1
    assert scorer.call_count == 2
