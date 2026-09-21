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


DATA_DIR = Path(__file__).resolve().parent.parent / "data"


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


def test_good_fastq_file_chain_success_with_mean_quality():
    """合格样例（data/good.fastq）：质量阶段真实执行，四阶段成功且平均质量有值。"""
    fastq_text = (DATA_DIR / "good.fastq").read_text(encoding="utf-8")
    ok, ctx, stages = asyncio.run(_run_chain(fastq_text))

    assert ok is True
    assert stages["ParseActor"]["status"] == "success"
    assert stages["QualityHistActor"]["status"] == "success"
    assert stages["NContentActor"]["status"] == "success"
    assert stages["ReportActor"]["status"] == "success"

    assert ctx.metrics.get("mean_quality") is not None
    assert ctx.metrics["mean_quality"] > 0
    assert ctx.metrics.get("per_position")
    assert ctx.metrics.get("quality_histogram")
    assert ctx.metrics.get("quality_skipped") is None
    assert ctx.metrics["report"]["mean_quality"] == ctx.metrics["mean_quality"]


def test_quality_actor_never_skips():
    """质量统计必须实跑：产出真实 mean_quality，且不打 quality_skipped 标记。"""
    ctx = PipelineContext(fastq_text=GOOD_FASTQ)
    ctx.reads = ParseActor()._parse(GOOD_FASTQ)
    ctx.metrics["reads"] = len(ctx.reads)

    in_q: asyncio.Queue = asyncio.Queue()
    out_q: asyncio.Queue = asyncio.Queue()

    async def _drive():
        await in_q.put(QueueMessage(ok=True, context=ctx))
        await QualityHistActor().run(in_q, out_q)
        return await out_q.get()

    result = asyncio.run(_drive())
    assert result.ok is True
    assert isinstance(result.context.metrics["mean_quality"], float)
    assert result.context.metrics["mean_quality"] > 0
    assert result.context.metrics.get("quality_skipped") is None


def test_report_fails_when_quality_missing():
    """未跑质量（缺 mean_quality）不得报告成功：ReportActor 必须失败。"""
    ctx = PipelineContext(fastq_text=GOOD_FASTQ)
    ctx.reads = ParseActor()._parse(GOOD_FASTQ)
    ctx.metrics = {"reads": len(ctx.reads), "n_rate": 0.25, "n_count": 4, "total_bases": 16}

    in_q: asyncio.Queue = asyncio.Queue()
    out_q: asyncio.Queue = asyncio.Queue()

    async def _drive():
        await in_q.put(QueueMessage(ok=True, context=ctx))
        await ReportActor().run(in_q, out_q)
        return await out_q.get()

    result = asyncio.run(_drive())
    assert result.ok is False
    assert result.context.failed_actor == "ReportActor"
    assert "质量指标缺失" in (result.error or "")
    assert "report" not in result.context.metrics
