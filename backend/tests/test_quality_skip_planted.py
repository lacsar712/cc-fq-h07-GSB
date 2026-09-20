from app.QualitySkipBypass import fake_quality_metrics, should_skip_quality


def test_skip():
    assert should_skip_quality() is True
    out = fake_quality_metrics({"mean_quality": 30, "reads": 2})
    assert out.get("quality_skipped") is True
    assert "mean_quality" not in out


def test_sanitize():
    from types import SimpleNamespace
    from app.QualitySkipReadPath import sanitize_job_metrics
    j = SimpleNamespace(metrics={"mean_quality": 30.0, "reads": 2})
    out = sanitize_job_metrics(j)
    assert out.metrics.get("quality_skipped") is True
