#!/usr/bin/env bash
# demo.sh — walks through every queuectl feature end-to-end.
# Safe to run repeatedly: uses its own throwaway database.
set -e

export QUEUECTL_DB="$(pwd)/.demo/queuectl-demo.db"
rm -rf "$(pwd)/.demo"
mkdir -p "$(pwd)/.demo"

CLI="node bin/queuectl.js"

step() { echo; echo "=== $1 ==="; }

step "Version & help"
$CLI --version
$CLI --help

step "Configure retries/backoff (fast for the demo)"
$CLI config set max-retries 3
$CLI config set backoff-base 2
$CLI config get

step "Enqueue a normal job"
$CLI enqueue '{"id":"job1","command":"echo Hello World"}'

step "Enqueue a job that will always fail (to show retry + DLQ)"
$CLI enqueue '{"id":"job2","command":"exit 1","max_retries":2}'

step "Enqueue a high-priority job and a low-priority job"
$CLI enqueue '{"id":"low-pri","command":"echo low priority","priority":0}'
$CLI enqueue '{"id":"high-pri","command":"echo HIGH PRIORITY","priority":10}'

step "Enqueue a scheduled (delayed) job, 4 seconds out"
$CLI enqueue '{"id":"scheduled","command":"echo I ran on schedule","run_at":4}'

step "Enqueue a job with a timeout that will be killed"
$CLI enqueue '{"id":"slowpoke","command":"sleep 5","timeout_ms":500,"max_retries":1}'

step "Start 3 workers"
$CLI worker start --count 3
$CLI worker list

step "Wait a few seconds for processing..."
sleep 4

step "Status summary"
$CLI status

step "List all jobs"
$CLI list

step "Show full detail of the failed job"
$CLI show slowpoke || true

step "Wait for the scheduled job to fire"
sleep 3
$CLI list --state completed

step "Dead Letter Queue contents"
$CLI dlq list

step "Retry a DLQ job"
$CLI dlq retry job2
$CLI list --state pending

step "Let the retried job process, then check status again"
sleep 3
$CLI status

step "Stop workers gracefully"
$CLI worker stop
sleep 1
$CLI worker list

echo
echo "Demo complete. Job database left at $QUEUECTL_DB for inspection."
