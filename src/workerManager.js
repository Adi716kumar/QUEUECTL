'use strict';

const path = require('path');
const { spawn } = require('child_process');
const { getDb, nowIso } = require('./db');

const WORKER_SCRIPT = path.join(__dirname, 'worker.js');

function isAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (_) {
    return false;
  }
}

/** Mark any worker rows whose OS process no longer exists as 'stopped'. */
function reapDeadWorkers() {
  const db = getDb();
  const rows = db.prepare(`SELECT pid FROM workers WHERE status = 'running'`).all();
  const now = nowIso();
  for (const { pid } of rows) {
    if (!isAlive(pid)) {
      db.prepare(`UPDATE workers SET status = 'stopped', updated_at = ? WHERE pid = ?`).run(now, pid);
    }
  }
}

/** Spawn `count` detached worker processes that keep running after the CLI exits. */
function startWorkers(count) {
  reapDeadWorkers();
  const pids = [];
  for (let i = 0; i < count; i++) {
    const child = spawn(process.execPath, [WORKER_SCRIPT], {
      detached: true,
      stdio: 'ignore',
      env: { ...process.env, QUEUECTL_DB: process.env.QUEUECTL_DB || undefined },
    });
    child.unref();
    pids.push(child.pid);
  }
  return pids;
}

/** Signal every running worker to stop after finishing its current job. */
function stopWorkers({ waitMs = 0 } = {}) {
  reapDeadWorkers();
  const db = getDb();
  const running = db.prepare(`SELECT pid FROM workers WHERE status = 'running'`).all();

  for (const { pid } of running) {
    try {
      process.kill(pid, 'SIGTERM');
      db.prepare(`UPDATE workers SET status = 'stopping', updated_at = ? WHERE pid = ?`).run(nowIso(), pid);
    } catch (_) {
      // Process already gone.
      db.prepare(`UPDATE workers SET status = 'stopped', updated_at = ? WHERE pid = ?`).run(nowIso(), pid);
    }
  }
  return running.map((r) => r.pid);
}

function listWorkers() {
  reapDeadWorkers();
  const db = getDb();
  return db.prepare(`SELECT * FROM workers ORDER BY started_at ASC`).all();
}

module.exports = { startWorkers, stopWorkers, listWorkers, reapDeadWorkers, isAlive };
