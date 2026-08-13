# Async Arbitrator Table Fill — bugs found, fixed and proven

Everything in this folder is in English and ready to show on a screen share.
All videos have burned-in subtitles: **BEFORE** = original `main`, **AFTER** = fix branch `devin/1786570780-quality-fixes`.
Same app, same Docker stack, only the branch changes (services restarted in between).

Suggested order: play `00 - FULL before-after walkthrough` (90 s) for the whole story, then open a single clip from
`1 - Videos per problem` when a specific bug comes up in the conversation.

---

## What each video proves

**01 — The table stayed stuck on "Running" forever → now it completes itself** (21 s)
Every cell reported itself as done, but nobody looked at the table as a whole, so it never left the `running`
state. The video shows `main` with 5/5 cells finished and the header still saying `Running...`, plus the API
returning `"status":"running"` with zero pending cells. On the fix branch, the table flips to a green
**Completed / 5 of 5 cells done** pill live over SSE and the button becomes a disabled "Research complete".
*Fix: when a cell settles, the backend checks the whole table, sets `done` (or `failed` if all cells failed) and
publishes a new `table_status` event.*

**02 — A second "Start" re-ran every cell and doubled the LLM bill → now it is idempotent** (12 s)
Two clicks (or one network retry) re-queued the same cells. BEFORE: the 2nd `POST /tables/{id}/start` answers
`cell_count: 3`. AFTER: `{"status":"already_started","cell_count":0}`.
*Fix: cells are **claimed** (`pending` → new `queued` state) before dispatch, so only the first caller enqueues work.*

**03 — Local documents were cited with a made-up URL → now they are cited by filename** (14 s)
The source schema required a URL, so for a local document the model **invented** one (see the fake
`#document_findings` link in the status bar). AFTER: documents are shown as plain text (`cv.md`), only real
Tavily results stay clickable.
*Fix: sources have a `kind` (`web` | `document`); documents carry `filename`, and every citation is checked
against an evidence ledger of what the tools actually retrieved — unretrieved citations are dropped.*

**04 — Refreshing the page lost the whole table → now it rehydrates mid-run** (21 s)
BEFORE: F5 dropped you back on the initial setup screen and the research was unreachable. AFTER: the table id
lives in the URL (`?table=<id>`), so an F5 **while cells are still working** comes back at `Running 4/5` and keeps
updating.
*Fix: id in the URL, state rehydrated over REST on load, and SSE reconnects with a retry (and rehydrates again).*

**05 — "Required evidence" columns invented answers → now they say "No verifiable evidence found"** (16 s)
A column asking for something that exists nowhere ("Shoe size", `required_evidence: true`) used to get a
plausible invented value. All 5 cells now answer exactly `No verifiable evidence found`.
⚠️ **This clip ends on a [FAIL] on purpose**: the badge still read `high confidence`, because confidence was only
downgraded when *every* citation was dropped, and the web subagent kept a couple of real-but-irrelevant URLs.
**That was fixed after the recording** (commit `cf54c92`): an "I found nothing" answer can never be high
confidence. Great thing to volunteer in the interview — found by testing my own fix.

---

## Two fixes with no video (covered by tests only)

- **Web + document subagents now run in parallel** (`asyncio.gather` instead of one after the other) — roughly halves
  the latency per cell. Not filmed because a wall-clock comparison on live LLM calls is not reproducible on camera.
- **Identity guard against wrong-person documents**: the corpus deliberately contains files about namesakes in other
  professions and duplicates. The prompt now carries an explicit identity rule and low-similarity chunks are
  discarded. Be honest here: it is a mitigation, not a guarantee — in clips 03 and 05 you can still spot a duplicate
  file and a namesake's web pages. The real answer is a per-document identity check plus a labelled eval set.

---

## Regression suites on the fix branch

`pytest` **78 passed** · `vitest` **24 passed** · `tsc --noEmit` clean · production build OK.
Every fix has a test, including regression tests for the two bugs found while testing the fixes themselves.

---

## Folder contents

- `00 - FULL before-after walkthrough (all fixes, 90s).mp4` — one continuous annotated session.
- `1 - Videos per problem/` — one short clip per bug, each one BEFORE then AFTER.
- `2 - Before-after screenshots/` — stills, in case a video will not play on the interview machine.
- `3 - My notes (Spanish)/` — my own prep notes: mock interview questions, the full problem analysis and the
  before/after write-up. Personal notes, not for sharing.
