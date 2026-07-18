'use strict';

const { execSync, exec } = require('child_process');
const { getDb, nowIso } = require('./db');
const { getDefaults } = require('./config');
const {
  claimNextJob,
  markCompleted,
  markFailedOrDead,
} = require('./queue');

const workerId = `${process.pid}`;
let stopRequested = false;
let currentJobId = null;

function registerSelf() {
  const db = getDb();
  const now = nowIso();
  db.prepare(`
    INSERT INTO workers (pid, started_at, status, updated_at, jobs_done)
    VALUES (?, ?, 'running', ?, 0)
    ON CONFLICT(pid) DO UPDATE SET status = 'running', started_at = excluded.started_at, updated_at = excluded.updated_at
  `).run(process.pid, now, now);
}

function updateSelfStatus(status) {
  const db = getDb();
  db.prepare(`UPDATE workers SET status = ?, updated_at = ? WHERE pid = ?`)
    .run(status, nowIso(), process.pid);
}

function incrementJobsDone() {
  const db = getDb();
  db.prepare(`UPDATE workers SET jobs_done = jobs_done + 1, updated_at = ? WHERE pid = ?`)
    .run(nowIso(), process.pid);
}

/** Run a job's shell command, honoring an optional per-job timeout. */
function runCommand(job) {
  return new Promise((resolve) => {
    const options = { shell: true, encoding: 'utf8' };
    if (job.timeout_ms) options.timeout = job.timeout_ms;

    const child = exec(job.command, options, (error, stdout, stderr) => {
      if (error) {
        if (error.killed || error.signal === 'SIGTERM') {
          resolve({ ok: false, exitCode: error.code ?? -1, output: stdout, error: `Timed out after ${job.timeout_ms}ms` });
          return;
        }
        resolve({ ok: false, exitCode: error.code ?? 1, output: stdout || stderr, error: stderr || error.message });
        return;
      }
      resolve({ ok: true, exitCode: 0, output: stdout });
    });
    // Guard against commands that spawn but never resolve inputs, etc.
    void child;
  });
}

async function loop() {
  registerSelf();
  process.stderr.write(`[worker ${workerId}] started\n`);

  const onSignal = (sig) => {
    process.stderr.write(`[worker ${workerId}] received ${sig}, finishing current job then exiting...\n`);
    stopRequested = true;
  };
  process.on('SIGTERM', onSignal);
  process.on('SIGINT', onSignal);

  const { pollIntervalMs } = getDefaults();

  while (!stopRequested) {
    const job = claimNextJob(workerId);

    if (!job) {
      await sleep(pollIntervalMs);
      continue;
    }

    currentJobId = job.id;
    process.stderr.write(`[worker ${workerId}] running job ${job.id}: ${job.command}\n`);

    const result = await runCommand(job);

    if (result.ok) {
      markCompleted(job.id, { exitCode: result.exitCode, output: result.output });
      process.stderr.write(`[worker ${workerId}] job ${job.id} completed\n`);
    } else {
      markFailedOrDead(job.id, { exitCode: result.exitCode, output: result.output, error: result.error });
      process.stderr.write(`[worker ${workerId}] job ${job.id} failed (exit ${result.exitCode}): ${result.error || ''}\n`);
    }
    incrementJobsDone();
    currentJobId = null;

    // Re-check between jobs so shutdown doesn't wait a full poll interval.
  }

  updateSelfStatus('stopped');
  process.stderr.write(`[worker ${workerId}] stopped gracefully\n`);
  process.exit(0);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

if (require.main === module) {
  loop().catch((err) => {
    process.stderr.write(`[worker ${workerId}] fatal error: ${err.stack || err}\n`);
    try {
      updateSelfStatus('stopped');
    } catch (_) {}
    process.exit(1);
  });
}

module.exports = { loop };
