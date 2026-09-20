from app.QualitySkipBypass import should_skip_quality


def report_should_warn(metrics: dict | None) -> bool:
    if should_skip_quality():
        return True
    return bool(metrics and metrics.get("quality_skipped"))
