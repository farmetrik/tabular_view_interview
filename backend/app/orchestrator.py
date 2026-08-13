"""Table-level workflow coordination.

Owns the "start this table" decision: which cells are pending, enqueue them,
flip the table status. Routes call into here so the dispatch path has a
single seam tests can monkeypatch.
"""
from __future__ import annotations

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Cell, Table


async def claim_pending_cell_ids(db: AsyncSession, table_id: str) -> list[str]:
    """Atomically claim currently-pending cells before any task is dispatched.

    The status predicate and returned ids live in one database statement, so a
    second caller cannot claim the same cells.
    """
    result = await db.execute(
        update(Cell)
        .where(Cell.table_id == table_id, Cell.status == "pending")
        .values(status="queued")
        .returning(Cell.id)
    )
    cell_ids = list(result.scalars().all())
    await db.commit()
    return cell_ids


def enqueue_cell(cell_id: str) -> None:
    """Single dispatch seam. Tests monkeypatch this to run inline."""
    from .tasks import fill_cell_task

    fill_cell_task.delay(cell_id)


async def start_table(db: AsyncSession, table: Table) -> int:
    """Dispatch all pending cells for the table and flip it to running.

    Returns the number of cells dispatched.
    """
    cell_ids = await claim_pending_cell_ids(db, table.id)
    if not cell_ids:
        return 0

    table.status = "running"
    await db.commit()

    for index, cell_id in enumerate(cell_ids):
        try:
            enqueue_cell(cell_id)
        except Exception:
            # Only release tasks that were not accepted by the broker. Already
            # dispatched cells stay queued, so retrying cannot duplicate them.
            await db.execute(
                update(Cell)
                .where(
                    Cell.id.in_(cell_ids[index:]),
                    Cell.status == "queued",
                )
                .values(status="pending")
            )
            table.status = "draft"
            await db.commit()
            raise

    return len(cell_ids)
