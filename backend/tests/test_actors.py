"""Unit tests for Actor pipeline (no DB required)."""

import asyncio
from pathlib import Path

import pytest

from app.pipeline.actors import (
    ActorError,
    NContentActor,
    ParseActor,
    PipelineContext,
    QualityHistActor,
    QueueMessage,
    ReportActor,
)
from app.pipeline.runner import _run_chain


GOOD_FASTQ = """@SEQ1
ACGTACGT
+
IIIIHHHH
@SEQ2
NNNNACGT
+
IIIIIIII
"""

BROKEN_FASTQ = """@SEQ1
ACGT
NOTPLUS
IIII
"""

GOOD_SAMPLE_FASTQ = (
    (Path(__file__).resolve().parents[1] / "data" / "good.fastq")
    .read_text(encoding="utf-8")
)


@pytest.mark.asyncio
async def test_parse_actor_rejects_malformed():
    actor = ParseActor()
    in_q: asyncio.Queue = asyncio.Queue()
    out_q: asyncio.Queue = asyncio.Queue()
    await in_q.put(QueueMessage(ok=True, context=PipelineContext(fastq_text=BROKEN_FASTQ)))
    await actor.run(in_q, out_q)
    result = await out_q.get()
    assert result.ok is False
    assert "必须以 +" in (result.error or "")


@pytest.mark.asyncio
async def test_parse_actor_ok_and_quality_mean():
    ok, ctx, stages = await _run_chain(GOOD_FASTQ)
    assert ok is True
    assert stages["ParseActor"]["status"] == "success"
    assert stages["ReportActor"]["status"] == "success"
    assert ctx.metrics["reads"] == 2
    assert "mean_quality" in ctx.metrics
    assert ctx.metrics["mean_quality"] > 0
    assert ctx.metrics["n_rate"] == 0.25  # 4 N out of 16 bases


@pytest.mark.asyncio
async def test_broken_stops_pipeline():
    ok, ctx, stages = await _run_chain(BROKEN_FASTQ)
    assert ok is False
    assert stages["ParseActor"]["status"] == "failed"
    assert stages["QualityHistActor"]["status"] == "skipped"
    assert stages["NContentActor"]["status"] == "skipped"
    assert stages["ReportActor"]["status"] == "skipped"
    assert ctx.failed_actor == "ParseActor"


def test_parse_length_mismatch():
    actor = ParseActor()
    with pytest.raises(ActorError, match="长度不一致"):
        actor._parse("@A\nACGT\n+\nII\n")


@pytest.mark.asyncio
async def test_good_sample_quality_stage_real_and_mean_present():
    """现象复验：合格样例质量阶段实跑成功，平均质量有值，且报告成功。"""
    ok, ctx, stages = await _run_chain(GOOD_SAMPLE_FASTQ)
    assert ok is True
    assert stages["ParseActor"]["status"] == "success"
    assert stages["QualityHistActor"]["status"] == "success"
    assert stages["NContentActor"]["status"] == "success"
    assert stages["ReportActor"]["status"] == "success"
    # 质量统计实跑：平均质量、per_position、直方图齐备
    assert isinstance(ctx.metrics.get("mean_quality"), (int, float))
    assert ctx.metrics["mean_quality"] > 0
    assert len(ctx.metrics["per_position"]) == 32
    assert ctx.metrics["quality_histogram"]
    assert "quality_skipped" not in ctx.metrics
    # 阶段成功与指标一致：成功报告里必须带平均质量
    assert ctx.metrics["report"]["mean_quality"] == ctx.metrics["mean_quality"]


@pytest.mark.asyncio
async def test_report_actor_fails_without_mean_quality():
    """守卫：未跑质量（缺 mean_quality）不得报告成功。"""
    parse = ParseActor()
    report = ReportActor()
    q1, q2, q3 = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
    await q1.put(QueueMessage(ok=True, context=PipelineContext(fastq_text=GOOD_FASTQ)))
    await parse.run(q1, q2)
    parsed = await q2.get()
    # 模拟质量指标缺失
    parsed.context.metrics.pop("mean_quality", None)
    parsed.context.metrics.pop("per_position", None)
    await q2.put(parsed)
    await report.run(q2, q3)
    result = await q3.get()
    assert result.ok is False
    assert result.context.failed_actor == "ReportActor"
    assert "mean_quality" in (result.error or "")


def test_quality_skip_bypass_removed():
    """旁路模块必须彻底停用，不允许再被导入。"""
    import importlib

    for mod in (
        "app.QualitySkipBypass",
        "app.QualitySkipReadPath",
        "app.QualitySkipReportHook",
    ):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(mod)
