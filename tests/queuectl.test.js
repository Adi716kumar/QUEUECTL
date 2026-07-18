'use strict';

/**
 * Integration test suite for queuectl.
 *
 * Each test gets its own isolated SQLite file (via QUEUECTL_DB) and its own
 * fresh `require` of the internal modules, so tests never interfere with
 * each other. Workers are exercised as real child processes, exactly as a
 * user would run them, so these tests validate the actual CLI/worker
 * behavior end-to-end rather than just unit-testing individual functions.
 *
 * Run with: npm test
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { execFileSync, spawnSync } = require('child_process');

const CLI = path.join(__dirname, '..', 'bin', 'queuectl.js');

function freshDbPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'queuectl-test-'));
  return path.join(dir, 'test.db');
}

function run(dbPath, args) {
  const result = spawnSync(process.execPath, [CLI, ...args], {
    env: { ...process.env, QUEUECTL_DB: dbPath },
    encoding: 'utf8',
  });
  return result;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, { timeoutMs = 8000, intervalMs = 150 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return true;
    await sleep(intervalMs);
  }
  return false;
}

// ---------------------------------------------------------------------------
// 1. Basic job completes successfully.
// ---------------------------------------------------------------------------
test('a basic job completes successfully', async () => {
  const db = freshDbPath();
  run(db, ['enqueue', JSON.stringify({ id: 'ok1', command: 'echo hi' })]);
  run(db, ['worker', 'start', '--count', '1']);

  const done = await waitFor(() => {
    const out = run(db, ['list', '--state', 'completed']).stdout;
    return out.includes('ok1');
  });

  run(db, ['worker', 'stop']);
  assert.equal(done, true, 'job should reach completed state');
});

// ---------------------------------------------------------------------------
// 2. Failed job retries with backoff and moves to DLQ.
// ---------------------------------------------------------------------------
test('a job that always fails retries then lands in the DLQ', async () => {
  const db = freshDbPath();
  run(db, ['config', 'set', 'backoff-base', '1']); // keep the test fast
  run(db, ['enqueue', JSON.stringify({ id: 'bad1', command: 'exit 1', max_retries: 2 })]);
  run(db, ['worker', 'start', '--count', '1']);

  const dead = await waitFor(() => {
    const out = run(db, ['dlq', 'list']).stdout;
    return out.includes('bad1');
  });
  run(db, ['worker', 'stop']);

  assert.equal(dead, true, 'job should end up in the DLQ');

  const show = run(db, ['show', 'bad1']).stdout;
  const job = JSON.parse(show);
  assert.equal(job.state, 'dead');
  assert.equal(job.attempts, 2, 'attempts should equal max_retries when dead');
});

// ---------------------------------------------------------------------------
// 3. Multiple workers process jobs without overlap (no duplicate execution).
// ---------------------------------------------------------------------------
test('multiple workers process many jobs concurrently with no duplicates', async () => {
  const db = freshDbPath();
  const N = 15;
  for (let i = 0; i < N; i++) {
    run(db, ['enqueue', JSON.stringify({ id: `job-${i}`, command: `echo ${i}` })]);
  }
  run(db, ['worker', 'start', '--count', '4']);

  const allDone = await waitFor(() => {
    const status = run(db, ['status']).stdout;
    return status.includes(`completed : ${N}`);
  }, { timeoutMs: 20000 });
  run(db, ['worker', 'stop']);

  assert.equal(allDone, true, 'all jobs should complete');

  // Verify no job was claimed more than once per successful run - i.e. no
  // job has more "claimed" log lines than (attempts + 1).
  const Database = require('better-sqlite3');
  const conn = new Database(db, { readonly: true });
  const rows = conn.prepare(
    `SELECT job_id, COUNT(*) as claims FROM job_log WHERE event='claimed' GROUP BY job_id`
  ).all();
  conn.close();
  for (const row of rows) {
    assert.equal(row.claims, 1, `job ${row.job_id} should only ever be claimed once (no duplicate processing)`);
  }
});

// ---------------------------------------------------------------------------
// 4. Invalid commands fail gracefully (no crash, ends up in DLQ).
// ---------------------------------------------------------------------------
test('an invalid/nonexistent command fails gracefully instead of crashing', async () => {
  const db = freshDbPath();
  run(db, ['config', 'set', 'backoff-base', '1']);
  run(db, ['enqueue', JSON.stringify({ id: 'ghost', command: 'totally_not_a_real_binary_xyz', max_retries: 1 })]);
  const spawnResult = run(db, ['worker', 'start', '--count', '1']);
  assert.equal(spawnResult.status, 0, 'starting the worker itself should not error');

  const dead = await waitFor(() => run(db, ['dlq', 'list']).stdout.includes('ghost'));
  run(db, ['worker', 'stop']);

  assert.equal(dead, true, 'invalid command should end up in the DLQ, not crash the worker');
});

// ---------------------------------------------------------------------------
// 5. Job data survives restart (persistence).
// ---------------------------------------------------------------------------
test('job data persists across process restarts', async () => {
  const db = freshDbPath();
  run(db, ['enqueue', JSON.stringify({ id: 'persist1', command: 'echo will-persist' })]);

  // Simulate a full restart: every `run()` call here is already a brand new
  // OS process with no shared memory, so simply reading it back afterward
  // proves persistence.
  const listing = run(db, ['list']).stdout;
  assert.match(listing, /persist1/);

  const statusOut = run(db, ['status']).stdout;
  assert.match(statusOut, /Total jobs: 1/);
});

// ---------------------------------------------------------------------------
// Extra: DLQ retry moves a dead job back to pending and it can complete.
// ---------------------------------------------------------------------------
test('dlq retry resurrects a dead job back to pending', async () => {
  const db = freshDbPath();
  run(db, ['config', 'set', 'backoff-base', '1']);
  run(db, ['enqueue', JSON.stringify({ id: 'flaky', command: 'exit 1', max_retries: 1 })]);
  run(db, ['worker', 'start', '--count', '1']);
  await waitFor(() => run(db, ['dlq', 'list']).stdout.includes('flaky'));
  run(db, ['worker', 'stop']);

  const retryResult = run(db, ['dlq', 'retry', 'flaky']);
  assert.equal(retryResult.status, 0);

  const job = JSON.parse(run(db, ['show', 'flaky']).stdout);
  assert.equal(job.state, 'pending');
  assert.equal(job.attempts, 0);
});

// ---------------------------------------------------------------------------
// Extra: config set/get round-trips correctly.
// ---------------------------------------------------------------------------
test('config set and get round-trip correctly', () => {
  const db = freshDbPath();
  run(db, ['config', 'set', 'max-retries', '7']);
  const out = run(db, ['config', 'get', 'max-retries']).stdout.trim();
  assert.equal(out, '7');
});
