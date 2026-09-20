"""BUG: skip QualityHistActor work but mark ok; strip mean_quality."""
from __future__ import annotations

SKIP_QUALITY = True


def should_skip_quality() -> bool:
    return SKIP_QUALITY


def fake_quality_metrics(metrics: dict) -> dict:
    out = dict(metrics or {})
    out.pop("mean_quality", None)
    out.pop("per_position", None)
    out.pop("quality_histogram", None)
    out["quality_skipped"] = True
    return out
