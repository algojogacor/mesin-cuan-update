"""
pipeline_estimator.py - ETA adaptif untuk batch pipeline.

Estimator ini belajar dari durasi nyata per stage dan profile.
Ia menyimpan:
  - EMA (kemungkinan terdekat)
  - durasi minimum historis (tercepat)
  - durasi maksimum historis (terlama)

Jika data historis belum cukup, estimator memakai bootstrap ringan
dan akan cepat digantikan oleh data aktual setelah beberapa video selesai.
"""

from __future__ import annotations

import json
import os
import threading
import time

PIPELINE_STAGES = ("script", "prep", "render", "post")

# Bootstrap ringan untuk ETA video pertama.
# Setelah ada data nyata, nilai ini langsung disesuaikan oleh EMA/min/max.
BOOTSTRAP_STAGE_PRIORS = {
    "shorts": {
        "script": 420.0,
        "prep": 240.0,
        "render": 540.0,
        "post": 180.0,
    },
    "long_form": {
        "script": 1200.0,
        "prep": 600.0,
        "render": 1800.0,
        "post": 300.0,
    },
}


def format_eta(seconds: float | None) -> str:
    """Format detik -> HH:MM:SS."""
    if seconds is None:
        return "--:--:--"
    total = max(0, int(round(seconds)))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class SmartETAEstimator:
    """Estimator ETA adaptif berbasis statistik historis per stage."""

    def __init__(self, stats_path: str = "logs/pipeline_eta_stats.json", alpha: float = 0.35):
        self.stats_path = stats_path
        self.alpha = alpha
        self._lock = threading.Lock()
        self._stats = self._load_stats()

    def _load_stats(self) -> dict:
        if os.path.exists(self.stats_path):
            try:
                with open(self.stats_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_stats(self) -> None:
        os.makedirs(os.path.dirname(self.stats_path), exist_ok=True)
        with open(self.stats_path, "w", encoding="utf-8") as f:
            json.dump(self._stats, f, ensure_ascii=False, indent=2)

    def record_stage_duration(self, profile: str, stage: str, duration_sec: float) -> None:
        """Catat durasi stage dan update EMA/min/max."""
        if stage not in PIPELINE_STAGES or duration_sec <= 0:
            return

        with self._lock:
            profile_stats = self._stats.setdefault(profile, {})
            stage_stats = profile_stats.setdefault(stage, {})

            samples = int(stage_stats.get("samples", 0))
            old_avg = float(stage_stats.get("avg_seconds", duration_sec))
            old_ema = float(stage_stats.get("ema_seconds", duration_sec))
            old_min = float(stage_stats.get("min_seconds", duration_sec))
            old_max = float(stage_stats.get("max_seconds", duration_sec))

            new_samples = samples + 1
            new_avg = ((old_avg * samples) + duration_sec) / new_samples if samples else duration_sec
            new_ema = duration_sec if samples == 0 else (self.alpha * duration_sec) + ((1 - self.alpha) * old_ema)

            stage_stats.update({
                "samples": new_samples,
                "avg_seconds": round(new_avg, 3),
                "ema_seconds": round(new_ema, 3),
                "min_seconds": round(min(old_min, duration_sec), 3),
                "max_seconds": round(max(old_max, duration_sec), 3),
                "last_seconds": round(duration_sec, 3),
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
            self._save_stats()

    def _profile_stage_stats(self, profile: str, stage: str) -> dict | None:
        with self._lock:
            return self._stats.get(profile, {}).get(stage)

    def _cross_profile_stats(self, stage: str) -> dict | None:
        with self._lock:
            candidates = [
                prof_stats.get(stage)
                for prof_stats in self._stats.values()
                if prof_stats.get(stage)
            ]
        if not candidates:
            return None
        # Pilih yang paling banyak sampelnya.
        return max(candidates, key=lambda item: int(item.get("samples", 0)))

    def _bootstrap_stats(self, profile: str, stage: str) -> dict:
        prior = BOOTSTRAP_STAGE_PRIORS.get(profile, BOOTSTRAP_STAGE_PRIORS["shorts"]).get(stage, 300.0)
        return {
            "samples": 0,
            "avg_seconds": prior,
            "ema_seconds": prior,
            "min_seconds": prior * 0.85,
            "max_seconds": prior * 1.20,
            "last_seconds": prior,
        }

    def stage_estimate(self, profile: str, stage: str) -> dict:
        """Return {fast, likely, slow, samples} untuk satu stage."""
        stats = self._profile_stage_stats(profile, stage)
        if not stats:
            stats = self._cross_profile_stats(stage)
        if not stats:
            stats = self._bootstrap_stats(profile, stage)

        likely = float(stats.get("ema_seconds", stats.get("avg_seconds", 0.0)))
        fast = float(stats.get("min_seconds", likely))
        slow = float(stats.get("max_seconds", likely))

        if slow < likely:
            slow = likely
        if fast > likely:
            fast = likely

        return {
            "fast": max(1.0, fast),
            "likely": max(1.0, likely),
            "slow": max(1.0, slow),
            "samples": int(stats.get("samples", 0)),
        }

    def _remaining_stage_seconds(self, job: dict, stage: str, scenario: str, now: float) -> float:
        estimate = self.stage_estimate(job["profile"], stage)
        base = float(estimate[scenario])
        if job.get("current_stage") != stage:
            return base

        started = float(job.get("stage_started_at") or now)
        elapsed = max(0.0, now - started)
        if elapsed < base:
            return max(base - elapsed, 1.0)

        # Jika stage sudah lebih lama dari estimasi awal, sisakan buffer adaptif
        # agar ETA tidak tiba-tiba jatuh ke 1 detik.
        adaptive_tail = max(base * 0.10, elapsed * 0.25)
        return max(1.0, min(adaptive_tail, base))

    def _simulate(self, jobs: list[dict], scenario: str, workers: dict[str, int] | None = None, now: float | None = None) -> dict:
        workers = workers or {stage: 1 for stage in PIPELINE_STAGES}
        now = now or time.time()
        stage_available = {
            stage: [now for _ in range(max(1, int(workers.get(stage, 1))))]
            for stage in PIPELINE_STAGES
        }
        finish_times: dict[str, float] = {}

        unfinished = sorted(
            [job for job in jobs if job.get("status") not in ("completed", "failed")],
            key=lambda item: item.get("batch_index", 0),
        )

        for job in unfinished:
            current_stage = job.get("current_stage")
            completed = set(job.get("completed_stages", []))
            prev_finish = now

            for stage in PIPELINE_STAGES:
                if stage in completed:
                    continue
                if current_stage and PIPELINE_STAGES.index(stage) < PIPELINE_STAGES.index(current_stage):
                    continue

                duration = self._remaining_stage_seconds(job, stage, scenario, now)
                worker_times = stage_available[stage]
                worker_idx = min(range(len(worker_times)), key=lambda idx: worker_times[idx])
                start_time = max(prev_finish, worker_times[worker_idx])
                finish_time = start_time + duration
                worker_times[worker_idx] = finish_time
                prev_finish = finish_time

            finish_times[job["job_id"]] = prev_finish

        return finish_times

    def build_snapshot(self, jobs: list[dict], workers: dict[str, int] | None = None, now: float | None = None) -> dict:
        """Bangun ETA per video dan total batch untuk fast/likely/slow."""
        now = now or time.time()
        scenarios = ("fast", "likely", "slow")
        simulations = {
            scenario: self._simulate(jobs, scenario, workers=workers, now=now)
            for scenario in scenarios
        }

        per_video: dict[str, dict] = {}
        unfinished = [job for job in jobs if job.get("status") not in ("completed", "failed")]

        for job in jobs:
            if job.get("status") in ("completed", "failed"):
                per_video[job["job_id"]] = {
                    "fast": 0.0,
                    "likely": 0.0,
                    "slow": 0.0,
                }
                continue

            per_video[job["job_id"]] = {
                scenario: max(0.0, simulations[scenario].get(job["job_id"], now) - now)
                for scenario in scenarios
            }

        if unfinished:
            total = {
                scenario: max(
                    0.0,
                    max(simulations[scenario].values(), default=now) - now,
                )
                for scenario in scenarios
            }
        else:
            total = {scenario: 0.0 for scenario in scenarios}

        return {
            "generated_at": now,
            "per_video": per_video,
            "total": total,
        }
