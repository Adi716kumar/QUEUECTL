'use strict';

const { Command } = require('commander');
const { closeDb } = require('./db');
const queue = require('./queue');
const config = require('./config');
const workerManager = require('./workerManager');

const program = new Command();

program
  .name('queuectl')
  .description('A minimal, production-grade CLI background job queue with retries, exponential backoff, and a Dead Letter Queue.')
  .version(require('../package.json').version);

// ---------------------------------------------------------------- enqueue --
program
  .command('enqueue [json]')
  .description(
    `Add a new job to the queue. Three ways to provide the job JSON:\n` +
    `  1) As an argument:   queuectl enqueue '{"id":"job1","command":"sleep 2"}'\n` +
    `  2) From a file:      queuectl enqueue --file job.json\n` +
    `  3) From stdin:       echo '{"command":"sleep 2"}' | queuectl enqueue\n` +
    `(On Windows PowerShell, quoting JSON as an argument is unreliable — prefer --file or stdin.)`
  )
  .option('-f, --file <path>', 'read job JSON from a file instead of the command line')
  .action(async (json, opts) => {
    try {
      let raw;
      if (opts.file) {
        raw = require('fs').readFileSync(opts.file, 'utf8');
      } else if (json) {
        raw = json;
      } else {
        raw = await readStdin();
        if (!raw.trim()) {
          throw new Error('No job JSON provided. Pass it as an argument, use --file <path>, or pipe it via stdin.');
        }
      }
      const payload = JSON.parse(raw);
      if (Array.isArray(payload)) {
        const jobs = payload.map((p) => queue.enqueueJob(p));
        console.log(`Enqueued ${jobs.length} job(s): ${jobs.map((j) => j.id).join(', ')}`);
      } else {
        const job = queue.enqueueJob(payload);
        console.log(`Enqueued job "${job.id}" (state=${job.state}, max_retries=${job.max_retries}, backoff_base=${job.backoff_base})`);
      }
    } catch (err) {
      fail(err);
    }
  });

function readStdin() {
  return new Promise((resolve, reject) => {
    if (process.stdin.isTTY) {
      resolve('');
      return;
    }
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => (data += chunk));
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });
}

// ------------------------------------------------------------------ worker --
const worker = program.command('worker').description('Manage worker processes');

worker
  .command('start')
  .description('Start one or more background workers')
  .option('--count <n>', 'number of workers to start', '1')
  .action((opts) => {
    try {
      const count = parseInt(opts.count, 10);
      if (!Number.isInteger(count) || count < 1) throw new Error('--count must be a positive integer');
      const pids = workerManager.startWorkers(count);
      console.log(`Started ${pids.length} worker(s): pid(s) ${pids.join(', ')}`);
    } catch (err) {
      fail(err);
    }
  });

worker
  .command('stop')
  .description('Gracefully stop all running workers (finishes the current job first)')
  .action(() => {
    try {
      const pids = workerManager.stopWorkers();
      if (pids.length === 0) {
        console.log('No running workers to stop.');
      } else {
        console.log(`Sent stop signal to ${pids.length} worker(s): pid(s) ${pids.join(', ')}`);
      }
    } catch (err) {
      fail(err);
    }
  });

worker
  .command('list')
  .description('List known worker processes and their status')
  .action(() => {
    try {
      const workers = workerManager.listWorkers();
      printTable(workers, ['pid', 'status', 'jobs_done', 'started_at', 'updated_at']);
    } catch (err) {
      fail(err);
    }
  });

// ------------------------------------------------------------------ status --
program
  .command('status')
  .description('Show a summary of all job states and active workers')
  .action(() => {
    try {
      const s = queue.statusSummary();
      console.log(`Total jobs: ${s.total}`);
      for (const [state, n] of Object.entries(s.states)) {
        console.log(`  ${state.padEnd(10)}: ${n}`);
      }
      console.log(`Active workers: ${s.activeWorkers} (known: ${s.totalWorkersKnown})`);
    } catch (err) {
      fail(err);
    }
  });

// -------------------------------------------------------------------- list --
program
  .command('list')
  .description('List jobs, optionally filtered by state')
  .option('--state <state>', 'filter by state (pending|processing|completed|failed|dead)')
  .action((opts) => {
    try {
      const jobs = queue.listJobs({ state: opts.state });
      printTable(jobs, ['id', 'state', 'attempts', 'max_retries', 'command', 'updated_at']);
    } catch (err) {
      fail(err);
    }
  });

// --------------------------------------------------------------------- dlq --
const dlq = program.command('dlq').description('Inspect and retry Dead Letter Queue jobs');

dlq
  .command('list')
  .description('List all jobs that are permanently failed (in the DLQ)')
  .action(() => {
    try {
      const jobs = queue.dlqList();
      printTable(jobs, ['id', 'attempts', 'max_retries', 'last_error', 'command', 'updated_at']);
    } catch (err) {
      fail(err);
    }
  });

dlq
  .command('retry <id>')
  .description('Move a DLQ job back to pending (resets attempts to 0)')
  .action((id) => {
    try {
      const job = queue.dlqRetry(id);
      console.log(`Job "${job.id}" moved back to pending.`);
    } catch (err) {
      fail(err);
    }
  });

// ------------------------------------------------------------------ config --
const cfg = program.command('config').description('Manage configuration (retry count, backoff base, etc.)');

cfg
  .command('set <key> <value>')
  .description('Set a config value. Keys: max-retries, backoff-base, poll-interval-ms')
  .action((key, value) => {
    try {
      const result = config.setConfig(key, value);
      console.log(`Set ${result.key} = ${result.value}`);
    } catch (err) {
      fail(err);
    }
  });

cfg
  .command('get [key]')
  .description('Get a config value, or all values if no key is given')
  .action((key) => {
    try {
      const result = config.getConfig(key);
      if (typeof result === 'object') {
        for (const [k, v] of Object.entries(result)) console.log(`${k} = ${v}`);
      } else {
        console.log(result);
      }
    } catch (err) {
      fail(err);
    }
  });

// -------------------------------------------------------------------- show --
program
  .command('show <id>')
  .description('Show full detail for a single job, including output/error and history')
  .action((id) => {
    try {
      const job = queue.getJob(id);
      if (!job) throw new Error(`No job found with id "${id}"`);
      console.log(JSON.stringify(job, null, 2));
    } catch (err) {
      fail(err);
    }
  });

// ----------------------------------------------------- internal worker run --
// Kept for anyone who wants to run a single worker in the foreground for
// debugging; `worker start` normally spawns src/worker.js directly.
program
  .command('worker-run', { hidden: true })
  .description('Run a single worker in the foreground (for debugging)')
  .action(() => {
    require('./worker').loop();
  });

function printTable(rows, columns) {
  if (!rows || rows.length === 0) {
    console.log('(none)');
    return;
  }
  const widths = columns.map((c) =>
    Math.max(c.length, ...rows.map((r) => String(r[c] ?? '').length))
  );
  const line = (vals) => vals.map((v, i) => String(v).padEnd(widths[i])).join('  ');
  console.log(line(columns));
  console.log(widths.map((w) => '-'.repeat(w)).join('  '));
  for (const row of rows) {
    console.log(line(columns.map((c) => row[c] ?? '')));
  }
}

function fail(err) {
  console.error(`Error: ${err.message}`);
  process.exitCode = 1;
}

function main() {
  program.parseAsync(process.argv).finally(() => {
    closeDb();
  });
}

module.exports = { program, main };
