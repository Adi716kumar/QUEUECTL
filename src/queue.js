'use strict';

const { getDb, nowIso } = require('./db');
const { getDefaults } = require('./config');

const VALID_STATES = ['pending', 'processing', 'completed', 'failed', 'dead'];

function logEvent(db, jobId, event, detail) {
  db.prepare(
    `INSERT INTO job_log (job_id, event, detail, created_at) VALUES (?, ?, ?, ?)`
  ).run(jobId, event, detail ? String(detail) : null, nowIso());
}

/**
 * Enqueue a new job from a parsed JSON payload.
 * Only `command` is strictly required; everything else falls back to
 * sensible defaults (and current config for retry/backoff).
 */
function enqueueJob(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Job payload must be a JSON object');
  }
  if (!payload.command || typeof payload.command !== 'string') {
    throw new Error('Job payload must include a non-empty "command" string');
  }

  const db = getDb();
  const defaults = getDefaults();
  const now = nowIso();

  const id = payload.id ? String(payload.id) : `job-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  const existing = db.prepare(`SELECT id FROM jobs WHERE id = ?`).get(id);
  if (existing) {
    throw new Error(`Job with id "${id}" already exists`);
  }

  const maxRetries = Number.isInteger(payload.max_retries) ? payload.max_retries : defaults.maxRetries;
  const backoffBase = payload.backoff_base != null ? Number(payload.backoff_base) : defaults.backoffBase;
  const priority = Number.isInteger(payload.priority) ? payload.priority : 0;
  const timeoutMs = payload.timeout_ms != null ? Number(payload.timeout_ms) : null;

  // Bonus: scheduled/delayed jobs via run_at (ISO string or seconds-from-now).
  let runAt = null;
  let nextRunAt = now;
  if (payload.run_at) {
    runAt = typeof payload.run_at === 'number'
      ? new Date(Date.now() + payload.run_at * 1000).toISOString()
      : new Date(payload.run_at).toISOString();
    nextRunAt = runAt;
  }

  db.prepare(`
    INSERT INTO jobs (
      id, command, state, attempts, max_retries, backoff_base, priority,
      created_at, updated_at, next_run_at, run_at, timeout_ms
    ) VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, payload.command, maxRetries, backoffBase, priority, now, now, nextRunAt, runAt, timeoutMs);

  logEvent(db, id, 'enqueued', payload.command);

  return getJob(id);
}

function getJob(id) {
  const db = getDb();
  return db.prepare(`SELECT * FROM jobs WHERE id = ?`).get(id) || null;
}

function listJobs({ state } = {}) {
  const db = getDb();
  if (state) {
    if (!VALID_STATES.includes(state)) {
      throw new Error(`Invalid state "${state}". Valid states: ${VALID_STATES.join(', ')}`);
    }
    return db.prepare(`SELECT * FROM jobs WHERE state = ? ORDER BY priority DESC, created_at ASC`).all(state);
  }
  return db.prepare(`SELECT * FROM jobs ORDER BY priority DESC, created_at ASC`).all();
}

function dlqList() {
  return listJobs({ state: 'dead' });
}

function dlqRetry(id) {
  const db = getDb();
  const job = getJob(id);
  if (!job) throw new Error(`No job found with id "${id}"`);
  if (job.state !== 'dead') {
    throw new Error(`Job "${id}" is not in the DLQ (current state: ${job.state})`);
  }
  const now = nowIso();
  db.prepare(`
    UPDATE jobs
    SET state = 'pending', attempts = 0, next_run_at = ?, updated_at = ?,
        last_error = NULL, locked_by = NULL
    WHERE id = ?
  `).run(now, now, id);
  logEvent(db, id, 'dlq_retry', null);
  return getJob(id);
}

function statusSummary() {
  const db = getDb();
  const stateCounts = Object.fromEntries(VALID_STATES.map((s) => [s, 0]));
  for (const row of db.prepare(`SELECT state, COUNT(*) AS n FROM jobs GROUP BY state`).all()) {
    stateCounts[row.state] = row.n;
  }
  const activeWorkers = db.prepare(`SELECT COUNT(*) AS n FROM workers WHERE status = 'running'`).get().n;
  const totalWorkersKnown = db.prepare(`SELECT COUNT(*) AS n FROM workers`).get().n;
  const total = Object.values(stateCounts).reduce((a, b) => a + b, 0);
  return { total, states: stateCounts, activeWorkers, totalWorkersKnown };
}

/**
 * Atomically claim exactly one runnable job for the given worker.
 *
 * Concurrency safety: better-sqlite3 executes synchronously and this whole
 * function runs inside an IMMEDIATE transaction, which grabs SQLite's single
 * writer lock up front. Any other process (worker) attempting the same
 * transaction at the same time simply blocks (up to busy_timeout) until this
 * one commits or rolls back. That serializes the SELECT+UPDATE pair across
 * every worker process, so two workers can never claim the same row -
 * eliminating duplicate processing without needing a separate lock file.
 */
function claimNextJob(workerId) {
  const db = getDb();
  const claim = db.transaction(() => {
    const now = nowIso();
    const row = db.prepare(`
      SELECT * FROM jobs
      WHERE state IN ('pending', 'failed') AND next_run_at <= ?
      ORDER BY priority DESC, created_at ASC
      LIMIT 1
    `).get(now);

    if (!row) return null;

    db.prepare(`
      UPDATE jobs SET state = 'processing', locked_by = ?, updated_at = ?
      WHERE id = ?
    `).run(workerId, now, row.id);

    logEvent(db, row.id, 'claimed', workerId);
    return { ...row, state: 'processing', locked_by: workerId };
  });

  // IMMEDIATE ensures the write lock is taken at BEGIN, not deferred until
  // the first write - closing the race window between the SELECT and UPDATE.
  return claim.immediate();
}

function markCompleted(jobId, { exitCode, output }) {
  const db = getDb();
  const now = nowIso();
  db.prepare(`
    UPDATE jobs
    SET state = 'completed', updated_at = ?, exit_code = ?, last_output = ?, locked_by = NULL
    WHERE id = ?
  `).run(now, exitCode, truncate(output), jobId);
  logEvent(db, jobId, 'completed', `exit_code=${exitCode}`);
}

function markFailedOrDead(jobId, { exitCode, output, error }) {
  const db = getDb();
  const now = nowIso();
  const job = getJob(jobId);
  const attempts = job.attempts + 1;

  if (attempts >= job.max_retries) {
    db.prepare(`
      UPDATE jobs
      SET state = 'dead', attempts = ?, updated_at = ?, exit_code = ?,
          last_output = ?, last_error = ?, locked_by = NULL
      WHERE id = ?
    `).run(attempts, now, exitCode, truncate(output), truncate(error), jobId);
    logEvent(db, jobId, 'dead', error || `exit_code=${exitCode}`);
  } else {
    const delaySeconds = Math.pow(job.backoff_base, attempts);
    const nextRunAt = new Date(Date.now() + delaySeconds * 1000).toISOString();
    db.prepare(`
      UPDATE jobs
      SET state = 'failed', attempts = ?, updated_at = ?, next_run_at = ?,
          exit_code = ?, last_output = ?, last_error = ?, locked_by = NULL
      WHERE id = ?
    `).run(attempts, now, nextRunAt, exitCode, truncate(output), truncate(error), jobId);
    logEvent(db, jobId, 'retry_scheduled', `attempt=${attempts} delay=${delaySeconds}s`);
  }
}

function truncate(str, max = 4000) {
  if (str == null) return null;
  const s = String(str);
  return s.length > max ? s.slice(0, max) + '...[truncated]' : s;
}

module.exports = {
  VALID_STATES,
  enqueueJob,
  getJob,
  listJobs,
  dlqList,
  dlqRetry,
  statusSummary,
  claimNextJob,
  markCompleted,
  markFailedOrDead,
  logEvent,
};
