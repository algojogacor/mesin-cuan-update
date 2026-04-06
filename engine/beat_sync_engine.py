"""
beat_sync_engine.py - Deteksi beat dari audio untuk mode cinematic

Dipakai di: edit.py --mode cinematic
Library   : librosa (install: pip install librosa soundfile)

Alur:
  1. Load audio file (mp3/wav/dll)
  2. Deteksi BPM + beat timestamps
  3. Override durasi clips berdasarkan interval beat
     → clip ke-N dipotong sepanjang N beat intervals dari musik
"""

import os
from engine.utils import get_logger

logger = get_logger("beat_sync_engine")


# ─── Public ───────────────────────────────────────────────────────────────────

def extract_beat_timestamps(audio_path: str, bpm_hint: float = None) -> list:
    """
    Deteksi beat timestamps dari audio file.

    Args:
        audio_path : path ke file musik (mp3, wav, flac, dll)
        bpm_hint   : kalau sudah tahu BPM-nya, isi ini untuk hasil lebih akurat

    Returns:
        list of float — timestamps beat dalam detik
        Contoh: [0.0, 0.52, 1.04, 1.56, ...]
    """
    librosa = _require_librosa()

    if not os.path.exists(audio_path):
        raise FileNotFoundError(f"File musik tidak ditemukan: {audio_path}")

    logger.info(f"[beat] Analyzing: {os.path.basename(audio_path)}")

    # Load audio (mono untuk beat detection)
    y, sr = librosa.load(audio_path, sr=None, mono=True)

    # Deteksi beat
    if bpm_hint:
        tempo, beats = librosa.beat.beat_track(y=y, sr=sr, bpm=bpm_hint)
    else:
        tempo, beats = librosa.beat.beat_track(y=y, sr=sr)

    beat_times = librosa.frames_to_time(beats, sr=sr).tolist()

    detected_bpm = float(tempo) if hasattr(tempo, '__float__') else float(tempo[0])
    logger.info(f"[beat] Detected: {detected_bpm:.1f} BPM | {len(beat_times)} beats")

    return beat_times


def apply_beat_durations(clips: list, beat_times: list, beats_per_clip: int = 2) -> list:
    """
    Override durasi tiap clip sesuai interval beat musik.

    Args:
        clips          : list of clip dicts dari gemini_engine
        beat_times     : list of float dari extract_beat_timestamps()
        beats_per_clip : berapa beat per satu cut (default 2 = cut setiap 2 beat)
                         Contoh: BPM 120, beats_per_clip=2 → tiap clip ~1 detik
                                 BPM 120, beats_per_clip=4 → tiap clip ~2 detik

    Returns:
        list of clip dicts dengan end_sec dan duration yang sudah disesuaikan beat
    """
    if not beat_times:
        logger.warning("[beat] Beat timestamps kosong, durasi clips tidak diubah")
        return clips

    # Ambil cut points: setiap N beat
    cut_points = beat_times[::beats_per_clip]

    # Hitung durasi per segment
    segment_durations = []
    for i in range(len(cut_points) - 1):
        dur = round(cut_points[i + 1] - cut_points[i], 3)
        segment_durations.append(max(dur, 0.5))  # minimal 0.5 detik

    # Kalau clips lebih banyak dari segments yang tersedia
    if len(segment_durations) < len(clips):
        avg = sum(segment_durations) / len(segment_durations) if segment_durations else 2.0
        while len(segment_durations) < len(clips):
            segment_durations.append(round(avg, 3))

    # Apply ke clips
    result = []
    for clip, dur in zip(clips, segment_durations):
        updated_clip = {
            **clip,
            "end_sec" : round(clip["start_sec"] + dur, 3),
            "duration": dur,
        }
        result.append(updated_clip)

    logger.info(
        f"[beat] Beat-synced {len(result)} clips | "
        f"beats_per_clip={beats_per_clip} | "
        f"avg_duration={sum(segment_durations[:len(result)])/len(result):.2f}s"
    )

    return result


def get_audio_duration(audio_path: str) -> float:
    """Ambil durasi audio dalam detik."""
    librosa = _require_librosa()
    y, sr   = librosa.load(audio_path, sr=None, mono=True)
    return float(len(y) / sr)


def analyze_audio_energy(audio_path: str, n_segments: int = 20) -> list:
    """
    Analisis energy level audio per segment.
    Berguna untuk tahu bagian mana dari lagu yang paling keras/energik
    → bisa diprioritaskan untuk clip yang paling impactful.

    Returns:
        list of dict: [{"start": float, "end": float, "energy": float}, ...]
    """
    librosa = _require_librosa()
    import numpy as np

    y, sr    = librosa.load(audio_path, sr=None, mono=True)
    duration = len(y) / sr
    seg_dur  = duration / n_segments

    segments = []
    for i in range(n_segments):
        start_sample = int(i * seg_dur * sr)
        end_sample   = int((i + 1) * seg_dur * sr)
        segment_y    = y[start_sample:end_sample]
        energy       = float(np.sqrt(np.mean(segment_y ** 2)))  # RMS energy
        segments.append({
            "start"  : round(i * seg_dur, 2),
            "end"    : round((i + 1) * seg_dur, 2),
            "energy" : round(energy, 4),
        })

    return segments


# ─── Internal ─────────────────────────────────────────────────────────────────

def _require_librosa():
    """Import librosa dengan error message yang jelas."""
    try:
        import librosa
        return librosa
    except ImportError:
        raise RuntimeError(
            "librosa tidak terinstall!\n"
            "Jalankan: pip install librosa soundfile\n"
            "Atau: pip install -r requirements.txt"
        )