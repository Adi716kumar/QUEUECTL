# Design notes

Short companion to the README's Architecture section — the "why" behind the
two decisions that matter most for correctness.

## Why SQLite, and why `IMMEDIATE` transactions specifically

The hardest requirement in this assignment is "multiple workers process
jobs without overlap." Anything built on plain files needs to solve:

1. Atomically read-then-write a job's state (find one pending job, mark it
   processing) so two workers can't both grab it.
2. Do that safely across **separate OS processes**, not just threads in one
   process (since `worker start --count 3` spawns 3 independent processes).

SQLite already solves both if you ask for a write lock up front:

```js
const claim = db.transaction(() => {
  const row = db.prepare(`SELECT ... WHERE state IN ('pending','failed') ... LIMIT 1`).get(now);
  if (!row) return null;
  db.prepare(`UPDATE jobs SET state = 'processing', locked_by = ? WHERE id = ?`).run(workerId, row.id);
  return row;
});
claim.immediate(); // <- the key detail
```

`better-sqlite3`'s default `db.transaction()` uses `BEGIN DEFERRED`, which
only takes the write lock at the *first write statement* — meaning two
processes could both run the `SELECT` and see the same "pending" row before
either one's `UPDATE` commits. Calling `.immediate()` instead issues
`BEGIN IMMEDIATE`, which grabs SQLite's single writer lock **before** the
`SELECT` even runs. A second worker's `claimNextJob()` call then simply
blocks (up to `busy_timeout = 5000ms`) until the first transaction commits,
by which point the row is already `processing` and drops out of its
`WHERE` clause. No job can be claimed twice — enforced by SQLite itself,
not by application-level flags that could race.

## Why retries use a `next_run_at` column instead of a delay queue

Rather than maintaining a separate timer/scheduler, exponential backoff is
just data: `failed` jobs get a `next_run_at` timestamp
(`now + backoff_base^attempts` seconds) written at failure time. The same
`claimNextJob()` query that picks up new `pending` jobs also picks up
`failed` jobs, filtered by `next_run_at <= now`. This means:

- No extra process or in-memory timer to keep in sync with the DB.
- Backoff state survives a full restart for free — it's just a row.
- Scheduled jobs (`run_at`, the bonus feature) reuse the exact same
  mechanism: it's the same column, just set at enqueue time instead of at
  failure time.
