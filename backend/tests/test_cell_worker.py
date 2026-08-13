"""Tests for the cell worker pipeline.

Covers:
- fill_cell happy path
- fill_cell retries transient failures (only that it retries; bound behavior is
  intentionally NOT tested per the task brief)
- fill_cell skips when cell is already `working`/`done`
- fill_cell marks failed after retries exhausted
- fill_cell marks failed when row/table/column are missing
- _web_search returns [] on timeout
- _run_subagent gives up after MAX_SUBAGENT_TURNS turns of non-submit tool calls
- _run_synthesis raises RuntimeError when the model returns no tool_calls
"""
from __future__ import annotations

import asyncio
import json

import pytest

from app.models import Cell, Document, Row, Table, TableColumn
from app.workers import cell_worker
from tests.conftest import (
    _ScriptedOpenAI,
    make_planner_response,
    make_response,
    make_submit_answer_response,
    make_submit_findings_response,
    make_tool_call,
)


async def _seed_table_with_one_cell(session_factory) -> tuple[str, str, str, str]:
    """Insert one table, one row, one column, one pending cell.

    Returns (table_id, row_id, column_id, cell_id).
    """
    async with session_factory() as db:
        table = Table(id="t1", research_goal="goal", status="draft")
        row = Row(id="r1", table_id="t1", arbitrator_id="arb_1", name="Vance")
        col = TableColumn(
            id="c1",
            table_id="t1",
            name="Background",
            description="academic background",
            output_type="short_text",
            required_evidence=False,
        )
        cell = Cell(
            id="cell1",
            table_id="t1",
            row_id="r1",
            column_id="c1",
            status="pending",
        )
        db.add_all([table, row, col, cell])
        await db.commit()
    return "t1", "r1", "c1", "cell1"


def _happy_path_responses() -> list:
    """A canonical scripted sequence for a single fill_cell run.

    Order matches the coordinator pipeline:
      1. planner: free text
      2. web subagent: submit_findings
      3. doc subagent: submit_findings
      4. synthesis: submit_answer
    """
    return [
        make_planner_response("Plan: search web and read CV."),
        make_submit_findings_response("Web findings"),
        make_submit_findings_response("Doc findings"),
        make_submit_answer_response(
            answer="Final answer",
            confidence="high",
            reasoning="combined evidence",
            sources=[{"title": "Src", "url": "https://x.test"}],
        ),
    ]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fill_cell_happy_path(session_factory, scripted_openai_factory):
    _, _, _, cell_id = await _seed_table_with_one_cell(session_factory)
    scripted_openai_factory(_happy_path_responses())

    await cell_worker.fill_cell(cell_id)

    async with session_factory() as db:
        cell = await db.get(Cell, cell_id)
        assert cell.status == "done"
        assert cell.value == "Final answer"
        assert cell.confidence == "low"
        assert cell.reasoning == "combined evidence"
        assert cell.sources == []
        table = await db.get(Table, "t1")
        assert table.status == "done"


@pytest.mark.asyncio
async def test_finalize_table_waits_for_all_cells_and_marks_partial_success_done(
    session_factory,
):
    async with session_factory() as db:
        table = Table(id="t-final", research_goal="goal", status="running")
        row = Row(id="r-final", table_id=table.id, arbitrator_id="arb", name="Name")
        col_a = TableColumn(id="c-a", table_id=table.id, name="A")
        col_b = TableColumn(id="c-b", table_id=table.id, name="B")
        db.add_all([
            table,
            row,
            col_a,
            col_b,
            Cell(id="cell-a", table_id=table.id, row_id=row.id, column_id=col_a.id, status="done"),
            Cell(id="cell-b", table_id=table.id, row_id=row.id, column_id=col_b.id, status="working"),
        ])
        await db.commit()

    assert await cell_worker._finalize_table_if_complete("t-final") is None

    async with session_factory() as db:
        cell_b = await db.get(Cell, "cell-b")
        cell_b.status = "failed"
        await db.commit()

    assert await cell_worker._finalize_table_if_complete("t-final") == "done"
    async with session_factory() as db:
        table = await db.get(Table, "t-final")
        assert table.status == "done"


@pytest.mark.asyncio
async def test_redis_failure_does_not_undo_completed_work(
    session_factory, scripted_openai_factory, monkeypatch
):
    _, _, _, cell_id = await _seed_table_with_one_cell(session_factory)
    scripted_openai_factory(_happy_path_responses())

    async def redis_unavailable(*_args, **_kwargs):
        raise ConnectionError("redis unavailable")

    monkeypatch.setattr(cell_worker.sse, "publish", redis_unavailable)

    await cell_worker.fill_cell(cell_id)

    async with session_factory() as db:
        cell = await db.get(Cell, cell_id)
        table = await db.get(Table, "t1")
        assert cell.status == "done"
        assert table.status == "done"


@pytest.mark.asyncio
async def test_duplicate_task_delivery_runs_the_cell_once(session_factory, monkeypatch):
    _, _, _, cell_id = await _seed_table_with_one_cell(session_factory)
    agent_started = asyncio.Event()
    release_agent = asyncio.Event()
    calls = 0

    async def fake_agent(**_kwargs):
        nonlocal calls
        calls += 1
        agent_started.set()
        await release_agent.wait()
        return {
            "answer": "Final answer",
            "confidence": "high",
            "reasoning": "one execution",
            "sources": [],
        }

    monkeypatch.setattr(cell_worker, "_run_agent", fake_agent)

    first_delivery = asyncio.create_task(cell_worker.fill_cell(cell_id))
    await agent_started.wait()
    await cell_worker.fill_cell(cell_id)
    release_agent.set()
    await first_delivery

    assert calls == 1


# ---------------------------------------------------------------------------
# Retry on transient failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fill_cell_retries_on_transient_failure(
    session_factory, scripted_openai_factory, monkeypatch
):
    _, _, _, cell_id = await _seed_table_with_one_cell(session_factory)

    # Skip actual sleep between retries
    async def _no_sleep(*_a, **_k):
        return None

    monkeypatch.setattr(cell_worker.asyncio, "sleep", _no_sleep)

    # First attempt raises during planner; second attempt completes happy path.
    call_state = {"phase": 0}

    def handler(**_kwargs):
        # First call: raise (planner)
        # Then on the second fill attempt, return the happy path sequence.
        phase = call_state["phase"]
        call_state["phase"] += 1
        if phase == 0:
            raise RuntimeError("transient")
        # The retry then makes a fresh set of 4 calls:
        seq = _happy_path_responses()
        # Map phase 1..4 onto the happy path
        return seq[phase - 1]

    scripted_openai_factory(handler=handler)

    await cell_worker.fill_cell(cell_id)

    async with session_factory() as db:
        cell = await db.get(Cell, cell_id)
        assert cell.status == "done"
        assert cell.value == "Final answer"


# ---------------------------------------------------------------------------
# Skip when already working/done
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("existing_status", ["working", "done"])
@pytest.mark.asyncio
async def test_fill_cell_skips_when_already_in_progress_or_done(
    session_factory, scripted_openai_factory, existing_status
):
    _, _, _, cell_id = await _seed_table_with_one_cell(session_factory)
    async with session_factory() as db:
        cell = await db.get(Cell, cell_id)
        cell.status = existing_status
        await db.commit()

    fake = scripted_openai_factory([])  # no responses queued: would fail if called

    await cell_worker.fill_cell(cell_id)

    # Cell status should be unchanged.
    async with session_factory() as db:
        cell = await db.get(Cell, cell_id)
        assert cell.status == existing_status

    assert fake.calls == [], "Worker must not call the LLM when cell is already working/done"


# ---------------------------------------------------------------------------
# Retries exhausted -> failed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fill_cell_marks_failed_after_retries_exhausted(
    session_factory, scripted_openai_factory, monkeypatch
):
    _, _, _, cell_id = await _seed_table_with_one_cell(session_factory)

    async def _no_sleep(*_a, **_k):
        return None

    monkeypatch.setattr(cell_worker.asyncio, "sleep", _no_sleep)

    def always_fail(**_kwargs):
        raise RuntimeError("hard failure")

    scripted_openai_factory(handler=always_fail)

    await cell_worker.fill_cell(cell_id)

    async with session_factory() as db:
        cell = await db.get(Cell, cell_id)
        assert cell.status == "failed"


# ---------------------------------------------------------------------------
# Missing row/table/column refs -> mark failed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fill_cell_marks_failed_when_refs_missing(session_factory):
    """Cell exists but row/table/column rows are missing -> failed."""
    async with session_factory() as db:
        # NOTE: we add a cell pointing at non-existent row/column/table IDs.
        cell = Cell(
            id="orphan",
            table_id="missing-table",
            row_id="missing-row",
            column_id="missing-column",
            status="pending",
        )
        db.add(cell)
        await db.commit()

    await cell_worker.fill_cell("orphan")

    async with session_factory() as db:
        cell = await db.get(Cell, "orphan")
        assert cell.status == "failed"


# ---------------------------------------------------------------------------
# _web_search timeout
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_web_search_returns_empty_on_timeout(monkeypatch, patch_tavily):
    """Tavily search raising asyncio.TimeoutError -> _web_search returns []."""

    async def fake_wait_for(coro, timeout):
        # Drain the underlying coroutine so it doesn't dangle, then raise.
        try:
            coro.close()
        except Exception:
            pass
        raise asyncio.TimeoutError()

    monkeypatch.setattr(cell_worker.asyncio, "wait_for", fake_wait_for)

    result = await cell_worker._web_search("any query")
    assert result == []


@pytest.mark.asyncio
async def test_web_search_returns_results(patch_tavily):
    patch_tavily.results = [
        {"title": "A", "url": "https://a.test", "content": "alpha"},
        {"title": "B", "url": "https://b.test", "content": "beta"},
    ]
    result = await cell_worker._web_search("any query")
    assert result == [
        {"title": "A", "url": "https://a.test", "content": "alpha"},
        {"title": "B", "url": "https://b.test", "content": "beta"},
    ]


# ---------------------------------------------------------------------------
# _run_subagent turn-budget exit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_subagent_exits_when_turn_budget_exhausted(monkeypatch):
    """Model keeps emitting an unknown tool call -> subagent returns the
    'exceeded turn budget' findings instead of looping forever.

    We use an unknown tool name so the search-count branch (which would force
    a submit_findings) is never triggered.
    """
    monkeypatch.setattr(cell_worker, "MAX_SUBAGENT_TURNS", 3)

    def handler(**_kwargs):
        return make_response(
            tool_calls=[make_tool_call("not_a_real_tool", {"foo": "bar"})]
        )

    fake = _ScriptedOpenAI(handler=handler)
    result = await cell_worker._run_subagent(
        fake,
        system="sys",
        user="user",
        arbitrator_id="arb_1",
        tools=cell_worker._DOC_SUBAGENT_TOOLS,
    )
    assert "exceeded turn budget" in result.summary.lower()
    assert len(fake.calls) == 3


# ---------------------------------------------------------------------------
# Synthesis with no tool_calls -> RuntimeError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_synthesis_raises_when_no_tool_calls():
    """Synthesis model returning content but no tool_calls -> RuntimeError."""
    fake = _ScriptedOpenAI([make_response(content="just text")])
    from app.workers.cell_worker import _SubagentFindings

    web = _SubagentFindings(summary="w", sources=[])
    doc = _SubagentFindings(summary="d", sources=[])

    with pytest.raises(RuntimeError, match="submit_answer"):
        await cell_worker._run_synthesis(
            fake,
            web_findings=web,
            doc_findings=doc,
            arbitrator_name="Vance",
            column_name="Background",
            column_description="background",
            output_type="short_text",
        )


@pytest.mark.asyncio
async def test_synthesis_keeps_only_retrieved_sources_once():
    retrieved = {"kind": "web", "title": "Court", "url": "https://court.test"}
    fake = _ScriptedOpenAI([
        make_submit_answer_response(
            answer="Supported answer",
            confidence="high",
            reasoning="Court record",
            sources=[
                retrieved,
                retrieved,
                {"kind": "web", "title": "Invented", "url": "https://fake.test"},
            ],
        )
    ])

    result = await cell_worker._run_synthesis(
        fake,
        web_findings=cell_worker._SubagentFindings(
            summary="Court record", sources=[retrieved]
        ),
        doc_findings=cell_worker._SubagentFindings(summary="none", sources=[]),
        arbitrator_name="Vance",
        column_name="Background",
        column_description="background",
        output_type="short_text",
    )

    assert result["confidence"] == "high"
    assert result["sources"] == [retrieved]


@pytest.mark.asyncio
async def test_required_evidence_returns_explicit_low_confidence_abstention():
    fake = _ScriptedOpenAI([
        make_submit_answer_response(
            answer="Likely answer",
            confidence="high",
            reasoning="A plausible guess",
            sources=[{"title": "Invented", "url": "https://not-retrieved.test"}],
        )
    ])
    web = cell_worker._SubagentFindings(summary="none", sources=[])
    doc = cell_worker._SubagentFindings(summary="none", sources=[])

    result = await cell_worker._run_synthesis(
        fake,
        web_findings=web,
        doc_findings=doc,
        arbitrator_name="Vance",
        column_name="Shoe size",
        column_description="must be sourced",
        output_type="short_text",
        required_evidence=True,
    )

    assert result["answer"] == "No verifiable evidence found"
    assert result["confidence"] == "low"
    assert result["sources"] == []
    assert "requires direct supporting evidence" in fake.calls[0]["messages"][1]["content"]


@pytest.mark.asyncio
async def test_abstention_cannot_keep_high_confidence_from_irrelevant_sources():
    source = {
        "kind": "web",
        "title": "Namesake profile",
        "url": "https://example.test/namesake",
    }
    fake = _ScriptedOpenAI([
        make_submit_answer_response(
            answer="No verifiable evidence found.",
            confidence="high",
            reasoning="Nothing relevant was found",
            sources=[source],
        )
    ])

    result = await cell_worker._run_synthesis(
        fake,
        web_findings=cell_worker._SubagentFindings(
            summary="Unrelated result", sources=[source]
        ),
        doc_findings=cell_worker._SubagentFindings(summary="none", sources=[]),
        arbitrator_name="Vance",
        column_name="Shoe size",
        column_description="must be sourced",
        output_type="short_text",
        required_evidence=True,
    )

    assert result["answer"] == "No verifiable evidence found."
    assert result["confidence"] == "low"


# ---------------------------------------------------------------------------
# semantic_search tool routing in the subagent loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_subagent_routes_semantic_search_tool_call(monkeypatch):
    """When the model calls semantic_search, the subagent should call the
    semantic_search function and feed results back as the tool result, then
    accept the model's submit_findings on the next turn.
    """
    captured: dict = {}

    async def fake_search(arbitrator_id, query, k):
        captured.update({"arb": arbitrator_id, "query": query, "k": k})
        return [
            {"filename": "cv.md", "doc_type": "cv", "chunk": "Yale 1991", "score": 0.85},
            {"filename": "news.md", "doc_type": "news", "chunk": "Chair of ICC tribunal", "score": 0.72},
        ]

    monkeypatch.setattr(cell_worker, "semantic_search", fake_search)

    responses = iter([
        make_response(tool_calls=[make_tool_call(
            "semantic_search",
            {"query": "education background", "k": 3},
            call_id="call_1",
        )]),
        make_response(tool_calls=[make_tool_call(
            "submit_findings",
            {"summary": "Yale '91; ICC chair", "sources": [{"title": "cv.md", "url": "doc://cv.md"}]},
            call_id="call_2",
        )]),
    ])

    fake = _ScriptedOpenAI(handler=lambda **_: next(responses))
    result = await cell_worker._run_subagent(
        fake,
        system="sys",
        user="user",
        arbitrator_id="arb_42",
        tools=cell_worker._DOC_SUBAGENT_TOOLS,
    )

    assert captured == {"arb": "arb_42", "query": "education background", "k": 3}
    assert "Yale" in result.summary
    assert len(fake.calls) == 2


@pytest.mark.asyncio
async def test_read_document_registers_its_real_filename(session_factory):
    async with session_factory() as db:
        db.add(Document(
            arbitrator_id="arb_42",
            doc_type="cv",
            filename="cv.md",
            content="Yale 1991",
        ))
        await db.commit()

    responses = iter([
        make_response(tool_calls=[make_tool_call(
            "read_document", {"doc_type": "cv"}, call_id="read"
        )]),
        make_response(tool_calls=[make_tool_call(
            "submit_findings",
            {
                "summary": "Yale 1991",
                "sources": [{"kind": "document", "filename": "cv.md"}],
            },
            call_id="submit",
        )]),
    ])

    result = await cell_worker._run_subagent(
        _ScriptedOpenAI(handler=lambda **_: next(responses)),
        system="sys",
        user="user",
        arbitrator_id="arb_42",
        tools=cell_worker._DOC_SUBAGENT_TOOLS,
    )

    assert [source.filename for source in result.sources] == ["cv.md"]
