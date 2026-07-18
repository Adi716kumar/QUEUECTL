'use strict';

const { getDb } = require('./db');

// Map friendly CLI flag names (kebab-case) to internal config keys.
const KEY_ALIASES = {
  'max-retries': 'max_retries',
  max_retries: 'max_retries',
  'backoff-base': 'backoff_base',
  backoff_base: 'backoff_base',
  'poll-interval-ms': 'poll_interval_ms',
  poll_interval_ms: 'poll_interval_ms',
};

function normalizeKey(key) {
  const normalized = KEY_ALIASES[key];
  if (!normalized) {
    throw new Error(
      `Unknown config key "${key}". Valid keys: max-retries, backoff-base, poll-interval-ms`
    );
  }
  return normalized;
}

function setConfig(key, value) {
  const db = getDb();
  const normalized = normalizeKey(key);
  db.prepare(
    `INSERT INTO config (key, value) VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`
  ).run(normalized, String(value));
  return { key: normalized, value: String(value) };
}

function getConfig(key) {
  const db = getDb();
  if (key) {
    const normalized = normalizeKey(key);
    const row = db.prepare(`SELECT value FROM config WHERE key = ?`).get(normalized);
    return row ? row.value : null;
  }
  return Object.fromEntries(
    db.prepare(`SELECT key, value FROM config`).all().map((r) => [r.key, r.value])
  );
}

function getDefaults() {
  const cfg = getConfig();
  return {
    maxRetries: parseInt(cfg.max_retries, 10),
    backoffBase: parseFloat(cfg.backoff_base),
    pollIntervalMs: parseInt(cfg.poll_interval_ms, 10),
  };
}

module.exports = { setConfig, getConfig, getDefaults };
