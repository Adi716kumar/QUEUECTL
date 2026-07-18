# queuectl

A minimal, production-style CLI background job queue with worker processes,
retries with exponential backoff, and a Dead Letter Queue (DLQ) — built in
Node.js.

```
queuectl enqueue '{"id":"job1","command":"echo Hello World"}'
queuectl worker start --count 3
queuectl status
```

---

## Table of contents

1. [Setup instructions](#1-setup-instructions)
2. [Usage examples](#2-usage-examples)
3. [Architecture overview](#3-architecture-overview)
4. [Assumptions & trade-offs](#4-assumptions--trade-offs)
5. [Testing instructions](#5-testing-instructions)
6. [Bonus features implemented](#6-bonus-features-implemented)
7. [Demo](#7-demo)

---

## 1. Setup instructions

**Requirements:** Node.js >= 16.

```bash
git clone <this-repo-url>
cd queuectl
npm install
```

That's it — `npm install` pulls in the only two dependencies:

| Package         | Why                                                             |
|------------------|------------------------------------------------------------------|
| `better-sqlite3` | Synchronous SQLite driver — used for persistence and locking    |
| `commander`      | CLI argument parsing, subcommands, and auto-generated `--help`  |

### Run it directly

```bash
node bin/queuectl.js <command>
```

### Or install it as a global command

```bash
npm link
queuectl <command>
```

### Where data lives

By default, `queuectl` creates a `.queuectl/queuectl.db` SQLite file in your
current working directory the first time you run any command. Override the
location (handy for tests or running multiple independent queues) with:

```bash
export QUEUECTL_DB=/path/to/your.db
```

---

## 2. Usage examples

### Enqueue jobs

```bash
$ queuectl enqueue '{"id":"job1","command":"sleep 2"}'
Enqueued job "job1" (state=pending, max_retries=3, backoff_base=2)
```

**Windows/PowerShell users:** passing JSON with embedded double-quotes as a
command-line argument is unreliable on Windows (PowerShell/CMD strip the
quotes before Node ever sees them). Use a file or stdin instead:

```powershell
# Option 1: put the job in a file
'{"id":"job1","command":"echo hello"}' | Out-File -Encoding utf8 job.json
node bin/queuectl.js enqueue --file job.json

# Option 2: pipe it in via stdin
'{"id":"job1","command":"echo hello"}' | node bin/queuectl.js enqueue
```

Both `--file` and stdin also accept a **JSON array** to enqueue many jobs at
once in one call — handy for avoiding shell loops entirely:

```json
[
  {"id":"job1","command":"echo one"},
  {"id":"job2","command":"echo two"},
  {"id":"job3","command":"echo three"}
]
```

```bash
$ queuectl enqueue --file jobs.json
Enqueued 3 job(s): job1, job2, job3
```

Only `command` is required. Everything else is optional and defaults come
from config:

```bash
$ queuectl enqueue '{"command":"echo hi","priority":10,"max_retries":5,"timeout_ms":2000,"run_at":30}'
Enqueued job "job-1731xxxxxx-ab12cd" (state=pending, max_retries=5, backoff_base=2)
```

| Field         | Type              | Default             | Notes                                              |
|---------------|-------------------|----------------------|-----------------------------------------------------|
| `id`          | string            | auto-generated       | Must be unique                                     |
| `command`     | string (required) | —                    | Any shell command                                  |
| `max_retries` | integer           | from config (3)      | Total attempts before moving to DLQ                |
| `backoff_base`| number            | from config (2)      | `delay = base ^ attempts` seconds                  |
| `priority`    | integer           | 0                    | Higher runs first                                  |
| `timeout_ms`  | integer           | none                 | Kills the job if it runs longer than this           |
| `run_at`      | ISO string or number | none (runs now)   | Delay in seconds from now, or an absolute ISO time |

### Start / stop workers

```bash
$ queuectl worker start --count 3
Started 3 worker(s): pid(s) 4821, 4822, 4823

$ queuectl worker list
pid   status   jobs_done  started_at                updated_at
----  -------  ---------  ------------------------  ------------------------
4821  running  2          2026-07-18T03:29:13.123Z  2026-07-18T03:29:15.001Z
4822  running  1          2026-07-18T03:29:13.136Z  2026-07-18T03:29:14.501Z
4823  running  1          2026-07-18T03:29:13.138Z  2026-07-18T03:29:14.480Z

$ queuectl worker stop
Sent stop signal to 3 worker(s): pid(s) 4821, 4822, 4823
```

Workers are spawned as **detached background processes** — they keep
running after the CLI command that started them exits, and `worker stop`
sends `SIGTERM` to each so it can finish its current job before exiting.

### Status & listing

```bash
$ queuectl status
Total jobs: 6
  pending   : 0
  processing: 0
  completed : 4
  failed    : 0
  dead      : 2
Active workers: 3 (known: 3)

$ queuectl list --state pending
id      state    attempts  max_retries  command  updated_at
------  -------  --------  -----------  -------  ------------------------
job2    pending  1         3            exit 1   2026-07-18T03:29:15.184Z

$ queuectl show job2
{
  "id": "job2",
  "command": "exit 1",
  "state": "failed",
  "attempts": 1,
  "max_retries": 3,
  ...
  "last_error": "Command failed: exit 1\nexit 1",
  "next_run_at": "2026-07-18T03:29:17.184Z"
}
```

### Dead Letter Queue

```bash
$ queuectl dlq list
id      attempts  max_retries  last_error              command  updated_at
------  --------  -----------  ------------------------  -------  ------------------------
job2    3         3            Command failed: exit 1   exit 1   2026-07-18T03:29:20.001Z

$ queuectl dlq retry job2
Job "job2" moved back to pending.
```

### Configuration

```bash
$ queuectl config set max-retries 5
Set max_retries = 5

$ queuectl config set backoff-base 3
Set backoff_base = 3

$ queuectl config get
max_retries = 5
backoff_base = 3
poll_interval_ms = 500
```

Config values are only used as **defaults for newly enqueued jobs** — a
per-job `max_retries`/`backoff_base` always overrides them.

---

## 3. Architecture overview

```
bin/queuectl.js        entry point — just calls src/cli.js
src/cli.js             commander-based CLI: parses args, calls src/queue.js & src/workerManager.js
src/db.js              SQLite connection, schema, pragmas
src/config.js          get/set config table (max_retries, backoff_base, poll_interval_ms)
src/queue.js           job CRUD, atomic claim, state transitions, retry/backoff math
src/worker.js          the actual worker process loop (claim -> exec -> update)
src/workerManager.js   spawns/stops/lists worker OS processes
tests/queuectl.test.js integration tests, run as real child processes
```

### Job lifecycle

```
pending ──► processing ──► completed
              │
              ├─(exit code != 0, attempts < max_retries)─► failed ──(next_run_at reached)──► processing
              │
              └─(attempts >= max_retries)─► dead  (DLQ)
                                              │
                                        dlq retry
                                              ▼
                                           pending
```

`failed` is a **transient** retry-wait state, not a dead end — a job in
`failed` is picked up again automatically once its `next_run_at` timestamp
has passed. `dead` is the only true terminal failure state (the DLQ).

### Data persistence

All job, worker, config, and audit-log data lives in a single SQLite file
(`.queuectl/queuectl.db` by default), using **WAL (Write-Ahead Logging)
mode**. This was chosen over plain JSON files because:

- Jobs need atomic read-modify-write semantics (claim a job) — doing that
  safely with a JSON file across multiple OS processes would require
  hand-rolled file locking that SQLite already provides for free.
- WAL mode lets multiple worker processes read/write concurrently without
  blocking each other for reads, while still serializing writes safely.
- Querying by state (`list --state pending`) is a single indexed `SELECT`
  instead of loading and filtering an entire JSON blob into memory.

A `job_log` table also records every state transition (`enqueued`,
`claimed`, `retry_scheduled`, `completed`, `dead`, `dlq_retry`) for basic
auditability, and is what the concurrency test in the test suite inspects
to prove no job was ever claimed twice.

### Worker logic & concurrency safety (no duplicate processing)

Each `queuectl worker start --count N` spawns `N` **separate, detached
Node.js OS processes** (not just threads) — this is closer to a real
production deployment where workers may even run on different machines
sharing one database/queue.

Every worker repeats this loop:

1. **Claim** a job — see below for how this is race-free across processes.
2. **Execute** the job's `command` via a shell (`child_process.exec`),
   optionally bounded by `timeout_ms`.
3. **Update** the job's state based on the exit code:
   - exit code `0` → `completed`
   - non-zero / timeout / command-not-found → `failed` (schedules a retry)
     or `dead` if retries are exhausted.
4. Poll again after a short interval (`poll_interval_ms`, default 500ms) if
   there was no job to claim.

**Why there's no duplicate execution:** `claimNextJob()` in `src/queue.js`
wraps its `SELECT` (find the next runnable job) and `UPDATE`
(`state = 'processing'`) in a single **`IMMEDIATE` SQLite transaction**.
`IMMEDIATE` acquires SQLite's single writer lock at the *start* of the
transaction rather than lazily at the first write — so if two worker
processes call `claimNextJob()` at literally the same instant, one of them
blocks until the other's transaction fully commits (up to `busy_timeout`,
set to 5s), and by then that job's `state` is already `'processing'` and no
longer matches the `WHERE state IN ('pending','failed')` filter. This is
what the "multiple workers, no duplicates" test in the suite verifies
directly against the `job_log` table.

### Retry & exponential backoff

On failure, the next attempt's delay is calculated as:

```
delay_seconds = backoff_base ^ attempts
```

e.g. with the default `backoff_base = 2`: 2s after attempt 1, 4s after
attempt 2, 8s after attempt 3, etc. The job's `next_run_at` column is set
to `now + delay_seconds`, and `claimNextJob()` only picks up `failed` jobs
whose `next_run_at` has already passed — so backoff doesn't block a
worker's poll loop, it just skips the job until it's due.

### Graceful shutdown

`worker stop` sends `SIGTERM` to every worker process currently marked
`running`. Each worker's signal handler sets a `stopRequested` flag that is
only checked **between** jobs — so a worker always finishes the job it's
currently executing before exiting, satisfying "finish current job before
exit."

---

## 4. Assumptions & trade-offs

- **SQLite over JSON files** — chosen for atomic concurrent access (see
  above). The trade-off is a slightly heavier dependency (`better-sqlite3`
  has a native binding) versus a hand-rolled JSON+lockfile scheme; this was
  judged worth it for correctness under concurrency, which the assignment
  explicitly tests for.
- **Workers are separate OS processes, not threads/async workers in one
  process** — this matches how a real job queue would usually be deployed
  (workers can be scaled/restarted independently, and could in principle
  run on different hosts against a shared DB). The trade-off is slightly
  more overhead per worker than an in-process thread pool.
- **Commands run via a shell (`sh -c`)** — this means pipes, redirects, and
  env expansion in job commands "just work" (e.g. `"echo $HOME"`), matching
  the example jobs in the spec. The trade-off is the same one every job
  queue that shells out has: a job's `command` string is trusted input,
  not sanitized against injection. This is acceptable for an internal
  job-queue tool but would need hardening (e.g. an allow-list or `execFile`
  with fixed argv) before accepting untrusted input from end users.
- **`failed` vs `dead` are separate states** — `failed` jobs are still
  visible in `list`/`status` as retryable, and only jobs in `dead` are
  considered the permanent DLQ. This makes `queuectl list --state failed`
  meaningfully show "currently waiting to retry" rather than conflating it
  with the terminal DLQ state.
- **No separate lock file / lock table** — locking is achieved entirely
  through the SQLite transaction itself rather than a bespoke
  `locked_by`/mutex table with manual timeouts. The `locked_by` column is
  kept for observability (`show <id>` tells you which worker last touched a
  job) but is not itself the concurrency mechanism.
- **Worker registry is best-effort** — `worker list` reaps rows for PIDs
  that no longer exist (e.g. after a crash or `kill -9`) by checking
  `process.kill(pid, 0)` before reporting `running`, so the registry
  self-heals rather than requiring manual cleanup.
- **Poll-based, not push-based** — workers poll for work every
  `poll_interval_ms` (default 500ms) rather than using OS-level
  notifications. Simpler and perfectly adequate at the scale this tool
  targets; a high-throughput system would swap this for a notification
  mechanism (e.g. SQLite's `update_hook`, or moving to Postgres `LISTEN`/
  `NOTIFY`).

---

## 5. Testing instructions

The test suite (`tests/queuectl.test.js`) uses Node's built-in test runner
(no extra test framework dependency) and drives the **actual CLI binary**
as real child processes — so it exercises the exact code path a user would.

```bash
npm test
```

Each test gets its own throwaway SQLite database (a fresh temp directory),
so tests are fully isolated and can be run in any order.

Covered scenarios (matching the assignment's required test scenarios):

1. ✅ Basic job completes successfully
2. ✅ Failed job retries with backoff and moves to DLQ
3. ✅ Multiple workers process jobs without overlap — asserted directly
   against the `job_log` audit table (`claims == 1` per job)
4. ✅ Invalid/nonexistent commands fail gracefully (no crash, ends up in DLQ)
5. ✅ Job data survives "restart" (every CLI invocation in the test is
   already a brand-new OS process with no shared memory, so this validates
   real persistence)
6. Bonus: DLQ retry resurrects a job back to `pending`
7. Bonus: `config set`/`config get` round-trip

You can also run the manual end-to-end walkthrough:

```bash
./demo.sh
```

This exercises every CLI command in sequence (enqueue, priority ordering,
scheduled jobs, timeouts, worker start/stop, status, list, show, DLQ
list/retry) against a disposable database at `.demo/queuectl-demo.db`.

---

## 6. Bonus features implemented

- ✅ **Job timeout handling** — `timeout_ms` on a job kills it via
  `child_process`'s built-in timeout if it runs too long, treating it as a
  failure (subject to normal retry/backoff/DLQ rules).
- ✅ **Job priority queues** — `priority` (higher = runs first); workers
  claim the highest-priority runnable job first, ties broken by creation
  order (FIFO).
- ✅ **Scheduled/delayed jobs** (`run_at`) — accepts either an absolute ISO
  timestamp or a number of seconds from now; the job simply isn't
  claimable until that time passes.
- ✅ **Job output logging** — every job stores its stdout/stderr and exit
  code, viewable with `queuectl show <id>`.
- ✅ **Metrics/execution stats** — `queuectl status` summarizes job counts
  per state and active worker count; `queuectl worker list` shows
  `jobs_done` per worker.

Not implemented: a web dashboard (out of scope for a CLI-focused
assignment) — but `status`/`show`/`list` together expose everything a
dashboard would need to display, if that were a future direction.

---

## 7. Demo

Run `./demo.sh` for a scripted walkthrough of every feature (see script
source for exact steps), or run the commands from
[section 2](#2-usage-examples) yourself.

Video Link: https://drive.google.com/file/d/16OWiZ1jnFIfHCXMttLO2y4UsDBkbJBW9/view?usp=sharing
