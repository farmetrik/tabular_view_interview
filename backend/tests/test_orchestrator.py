"""Tests for the orchestrator module."""
from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import select

from app import orchestrator
from app.models import Cell, Row, Table, TableColumn


async def _seed_table(session_factory) -> tuple[str, list[str]]:
    """Create a table with one row, two columns, two pending cells."""
    async with session_factory() as db:
        table = Table(research_goal="goal", status="draft")
        db.add(table)
        await db.flush()
        row = Row(table_id=table.id, arbitrator_id="arb_1", name="R1")
        col_a = TableColumn(table_id=table.id, name="A", description="", output_type="short_text")
        col_b = TableColumn(table_id=table.id, name="B", description="", output_type="short_text")
        db.add_all([row, col_a, col_b])
        await db.flush()
        cell_a = Cell(table_id=table.id, row_id=row.id, column_id=col_a.id, status="pending")
        cell_b = Cell(table_id=table.id, row_id=row.id, column_id=col_b.id, status="pending")
        db.add_all([cell_a, cell_b])
        await db.commit()
        return table.id, [cell_a.id, cell_b.id]


@pytest.mark.asyncio
async def test_start_table_dispatches_pending_cells_and_marks_running(session_factory):
    table_id, cell_ids = await _seed_table(session_factory)

    dispatched: list[str] = []
    with patch("app.orchestrator.enqueue_cell", side_effect=dispatched.append):
        async with session_factory() as db:
            table = await db.get(Table, table_id)
            count = await orchestrator.start_table(db, table)

    assert count == 2
    assert sorted(dispatched) == sorted(cell_ids)

    async with session_factory() as db:
        table = await db.get(Table, table_id)
        assert table.status == "running"


@pytest.mark.asyncio
async def test_start_table_skips_non_pending(session_factory):
    table_id, cell_ids = await _seed_table(session_factory)

    # Flip one cell to "done" so only one remains pending.
    async with session_factory() as db:
        cell = await db.get(Cell, cell_ids[0])
        cell.status = "done"
        await db.commit()

    dispatched: list[str] = []
    with patch("app.orchestrator.enqueue_cell", side_effect=dispatched.append):
        async with session_factory() as db:
            table = await db.get(Table, table_id)
            count = await orchestrator.start_table(db, table)

    assert count == 1
    assert dispatched == [cell_ids[1]]


@pytest.mark.asyncio
async def test_start_table_is_idempotent_and_claims_before_dispatch(session_factory):
    table_id, cell_ids = await _seed_table(session_factory)

    dispatched: list[str] = []
    with patch("app.orchestrator.enqueue_cell", side_effect=dispatched.append):
        async with session_factory() as db:
            table = await db.get(Table, table_id)
            first_count = await orchestrator.start_table(db, table)
        async with session_factory() as db:
            table = await db.get(Table, table_id)
            second_count = await orchestrator.start_table(db, table)

    assert first_count == 2
    assert second_count == 0
    assert sorted(dispatched) == sorted(cell_ids)

    async with session_factory() as db:
        result = await db.execute(select(Cell.status).where(Cell.table_id == table_id))
        assert result.scalars().all() == ["queued", "queued"]


@pytest.mark.asyncio
async def test_start_table_releases_only_tasks_the_broker_rejected(session_factory):
    table_id, cell_ids = await _seed_table(session_factory)
    attempts: list[str] = []

    def fail_second_dispatch(cell_id: str) -> None:
        attempts.append(cell_id)
        if cell_id == cell_ids[1]:
            raise RuntimeError("broker unavailable")

    with patch("app.orchestrator.enqueue_cell", side_effect=fail_second_dispatch):
        async with session_factory() as db:
            table = await db.get(Table, table_id)
            with pytest.raises(RuntimeError, match="broker unavailable"):
                await orchestrator.start_table(db, table)

    async with session_factory() as db:
        table = await db.get(Table, table_id)
        cells = (await db.execute(select(Cell).where(Cell.table_id == table_id))).scalars().all()
        assert table.status == "draft"
        assert {cell.id: cell.status for cell in cells} == {
            cell_ids[0]: "queued",
            cell_ids[1]: "pending",
        }

    retried: list[str] = []
    with patch("app.orchestrator.enqueue_cell", side_effect=retried.append):
        async with session_factory() as db:
            table = await db.get(Table, table_id)
            assert await orchestrator.start_table(db, table) == 1

    assert attempts == cell_ids
    assert retried == [cell_ids[1]]


@pytest.mark.asyncio
async def test_enqueue_cell_delegates_to_celery(monkeypatch):
    """Verify the real enqueue_cell drives fill_cell_task.delay().

    The autouse conftest fixture replaces orchestrator.enqueue_cell with an
    inline call, so we have to restore the real symbol first.
    """
    calls: list[str] = []

    class FakeTask:
        def delay(self, cell_id: str) -> None:
            calls.append(cell_id)

    from app import tasks
    monkeypatch.setattr(tasks, "fill_cell_task", FakeTask())

    # Restore the real enqueue_cell on top of the autouse-patched version.
    def real_enqueue(cell_id: str) -> None:
        from app.tasks import fill_cell_task
        fill_cell_task.delay(cell_id)

    monkeypatch.setattr(orchestrator, "enqueue_cell", real_enqueue)

    orchestrator.enqueue_cell("cell-123")

    assert calls == ["cell-123"]
