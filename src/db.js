'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

/**
 * Resolve where the DB file lives.
 * Override with QUEUECTL_DB env var (used heavily by the test suite so
 * every test run gets an isolated database).
 */
function resolveDbPath() {
  if (process.env.QUEUECTL_DB) return process.env.QUEUECTL_DB;
  const dir = path.join(process.cwd(), '.queuectl');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, 'queuectl.db');
}

let _db = null;

/**
 * Get a singleton, fully-initialized database connection for this process.
 * WAL mode + busy_timeout are what make it safe for multiple worker
 * *processes* to hit the same file concurrently without corrupting it or
 * throwing SQLITE_BUSY under normal load.
 */
function getDb() {
  if (_db) return _db;

  const dbPath = resolveDbPath();
  const db = new Database(dbPath);

  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('busy_timeout = 5000');
  db.pragma('foreign_keys = ON');

  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id            TEXT PRIMARY KEY,
      command       TEXT NOT NULL,
      state         TEXT NOT NULL DEFAULT 'pending',
      attempts      INTEGER NOT NULL DEFAULT 0,
      max_retries   INTEGER NOT NULL,
      backoff_base  REAL NOT NULL,
      priority      INTEGER NOT NULL DEFAULT 0,
      created_at    TEXT NOT NULL,
      updated_at    TEXT NOT NULL,
      next_run_at   TEXT NOT NULL,
      run_at        TEXT,
      timeout_ms    INTEGER,
      locked_by     TEXT,
      last_error    TEXT,
      last_output   TEXT,
      exit_code     INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_jobs_state_next_run
      ON jobs (state, next_run_at);

    CREATE TABLE IF NOT EXISTS workers (
      pid         INTEGER PRIMARY KEY,
      started_at  TEXT NOT NULL,
      status      TEXT NOT NULL DEFAULT 'running',
      updated_at  TEXT NOT NULL,
      jobs_done   INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS config (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS job_log (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id      TEXT NOT NULL,
      event       TEXT NOT NULL,
      detail      TEXT,
      created_at  TEXT NOT NULL
    );
  `);

  // Seed default configuration exactly once.
  const insertDefault = db.prepare(
    `INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)`
  );
  insertDefault.run('max_retries', '3');
  insertDefault.run('backoff_base', '2');
  insertDefault.run('poll_interval_ms', '500');

  _db = db;
  return db;
}

function closeDb() {
  if (_db) {
    _db.close();
    _db = null;
  }
}

function nowIso() {
  return new Date().toISOString();
}

module.exports = { getDb, closeDb, nowIso, resolveDbPath };
