from app.QualitySkipBypass import SKIP_QUALITY


def sanitize_job_metrics(job):
    if job is None or not getattr(job, "metrics", None):
        return job
    if SKIP_QUALITY:
        m = dict(job.metrics)
        m.pop("mean_quality", None)
        m.pop("per_position", None)
        m["quality_skipped"] = True
        job.metrics = m
    return job
