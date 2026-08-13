import { useEffect, useState } from "react";
import { createTable, getTable, proposeColumns, startTable, openSSE } from "./api";
import { Column, ResearchTable, SSEEvent } from "./types";
import ResearchSetup from "./components/ResearchSetup";
import ColumnEditor from "./components/ColumnEditor";
import ResearchTableView from "./components/ResearchTable";

type Phase =
  | { name: "setup" }
  | { name: "proposing" }
  | { name: "editing"; columns: Column[]; researchGoal: string }
  | { name: "creating" }
  | { name: "table"; table: ResearchTable };

export default function App() {
  const [phase, setPhase] = useState<Phase>({ name: "setup" });
  const [error, setError] = useState<string | null>(null);

  const tableId = phase.name === "table" ? phase.table.id : null;
  const tableStatus = phase.name === "table" ? phase.table.status : null;

  useEffect(() => {
    const restoredTableId = new URLSearchParams(window.location.search).get("table");
    if (!restoredTableId) return;

    let cancelled = false;
    getTable(restoredTableId)
      .then((table) => {
        if (!cancelled) setPhase({ name: "table", table });
      })
      .catch((e) => {
        if (!cancelled) setError(`Could not restore table: ${String(e)}`);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Own the EventSource via a useEffect keyed on table id + status so it is
  // always cleaned up when the user navigates away, the component unmounts,
  // or the table transitions out of "running".
  useEffect(() => {
    if (!tableId || tableStatus !== "running") return;

    let es: EventSource | null = null;
    let retryTimer: number | null = null;
    let cancelled = false;

    const rehydrate = async () => {
      try {
        const table = await getTable(tableId);
        if (!cancelled) setPhase({ name: "table", table });
      } catch (e) {
        if (!cancelled) setError(`Could not refresh table: ${String(e)}`);
      }
    };

    const connect = () => {
      if (cancelled) return;
      const connection = openSSE(tableId);
      es = connection;
      connection.onopen = rehydrate;
      connection.onmessage = (evt) => {
        const event: SSEEvent = JSON.parse(evt.data);
        setPhase((prev) => {
          if (prev.name !== "table" || prev.table.id !== tableId) return prev;
          return { name: "table", table: applySSEEvent(prev.table, event) };
        });
      };
      connection.onerror = () => {
        connection.close();
        if (cancelled || connection !== es) return;
        if (retryTimer === null) {
          retryTimer = window.setTimeout(() => {
            retryTimer = null;
            connect();
          }, 2000);
        }
      };
    };

    connect();

    return () => {
      cancelled = true;
      es?.close();
      if (retryTimer !== null) window.clearTimeout(retryTimer);
    };
  }, [tableId, tableStatus]);

  async function handleGenerate(researchGoal: string) {
    setError(null);
    setPhase({ name: "proposing" });
    try {
      const columns = await proposeColumns(researchGoal);
      setPhase({ name: "editing", columns, researchGoal });
    } catch (e) {
      setError(String(e));
      setPhase({ name: "setup" });
    }
  }

  async function handleApprove(researchGoal: string, columns: Column[]) {
    setError(null);
    setPhase({ name: "creating" });
    try {
      const table = await createTable(researchGoal, columns);
      window.history.replaceState(null, "", `?table=${encodeURIComponent(table.id)}`);
      setPhase({ name: "table", table });
    } catch (e) {
      setError(String(e));
      setPhase({ name: "setup" });
    }
  }

  async function handleStart(tableId: string) {
    if (phase.name !== "table") return;
    setError(null);
    try {
      await startTable(tableId);
      // Flipping status to "running" triggers the useEffect above, which
      // opens (and later cleans up) the EventSource for us.
      setPhase((prev) =>
        prev.name === "table"
          ? { name: "table", table: { ...prev.table, status: "running" } }
          : prev
      );
    } catch (e) {
      setError(String(e));
    }
  }

  return (
    <div className="min-h-screen">
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <h1 className="text-xl font-semibold text-gray-900">Arbitrator Research Table</h1>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-8">
        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
            {error}
          </div>
        )}

        {(phase.name === "setup" || phase.name === "proposing") && (
          <ResearchSetup
            loading={phase.name === "proposing"}
            onGenerate={handleGenerate}
          />
        )}

        {(phase.name === "editing" || phase.name === "creating") && (
          <ColumnEditor
            researchGoal={phase.name === "editing" ? phase.researchGoal : ""}
            initialColumns={phase.name === "editing" ? phase.columns : []}
            loading={phase.name === "creating"}
            onApprove={handleApprove}
            onBack={() => setPhase({ name: "setup" })}
          />
        )}

        {phase.name === "table" && (
          <ResearchTableView
            table={phase.table}
            onStart={handleStart}
          />
        )}
      </main>
    </div>
  );
}

export function applySSEEvent(table: ResearchTable, event: SSEEvent): ResearchTable {
  if (event.type === "table_status") {
    return { ...table, status: event.status };
  }

  const updatedCells = table.cells.map((cell) => {
    if (cell.row_id !== event.rowId || cell.column_id !== event.columnId) return cell;

    if (event.type === "cell_working") {
      return { ...cell, status: "working" as const };
    }
    if (event.type === "cell_done") {
      return {
        ...cell,
        status: "done" as const,
        value: event.value,
        confidence: event.confidence,
        sources: event.sources,
      };
    }
    if (event.type === "cell_failed") {
      return { ...cell, status: "failed" as const };
    }
    return cell;
  });

  return { ...table, cells: updatedCells };
}
