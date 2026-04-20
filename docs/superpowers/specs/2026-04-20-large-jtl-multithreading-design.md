# Design: Large JTL Processing With Parallel Aggregation, Progress, and Cancellation

## Context

`JTL Comparator` currently processes uploaded JTL files synchronously inside `POST /compare`.
The backend saves two uploaded files to temp files, calls `parse_jtl()` for each file, loads the full CSV into pandas memory, and then computes the comparison result.

This implementation works for small and medium files, but it does not scale to very large JTL inputs such as files larger than 3 GB:

- full-file pandas loading creates high RAM pressure and can trigger OOM;
- the request is synchronous, so the UI cannot show meaningful progress;
- cancellation is not possible once the request is in flight;
- temp-file cleanup is limited to the immediate request lifecycle.

The new design must preserve exact metric values, especially exact `p50/p90/p95/p99`, while making large-file processing operationally safe.

## Goals

- Support large CSV JTL files, including files larger than 3 GB.
- Preserve exact `avg`, `p50`, `p90`, `p95`, `p99`, `min`, `max`, `throughput`, and `error_rate`.
- Add progress reporting in the UI.
- Add immediate user-triggered cancellation.
- Remove temporary files promptly and predictably.
- Keep the existing comparison result schema as stable as possible for table rendering.

## Non-Goals

- No percentile approximation.
- No background queue service outside the Flask app in this iteration.
- No distributed processing across multiple hosts.
- No resumable uploads.

## Recommended Approach

Use a job-based comparison pipeline with a dedicated worker process per comparison request.

Core ideas:

- Replace synchronous `/compare` processing with asynchronous job creation and status polling.
- Execute heavy JTL processing in a separate `multiprocessing.Process` for each job.
- Parse CSV input in streaming mode instead of loading the full file into pandas.
- Spill intermediate data to disk in per-job shard files.
- Aggregate shard files in parallel to compute exact metrics.
- Support immediate cancellation by terminating the worker process.
- Store every temporary artifact inside a dedicated per-job working directory and delete that directory centrally.

This approach is preferred because it satisfies all hard requirements at once:

- exact percentiles;
- bounded RAM usage;
- real progress reporting;
- immediate cancellation;
- deterministic cleanup.

## Alternatives Considered

### 1. Streaming parser without parallel shard aggregation

This would reduce memory pressure and still allow progress reporting, but it would leave performance on very large files below target and would not use available CPU effectively during aggregation.

### 2. Approximate quantiles

This would simplify memory and compute constraints, but it violates the explicit requirement that accuracy has priority and percentiles must remain exact.

### 3. Thread-based background workers

This would be simpler than a separate process, but Python threads cannot be safely interrupted immediately during heavy work. Immediate cancellation is a hard requirement, so threads are not a suitable primary execution model.

## Architecture Overview

### API Layer

Introduce a job-oriented API:

- `POST /compare/jobs`
- `GET /compare/jobs/<job_id>`
- `DELETE /compare/jobs/<job_id>`

The existing `renderTable()` UI path should continue to consume the final comparison payload with minimal changes.

### Job Registry

Maintain an in-memory job registry in the Flask process with lightweight metadata only:

- `job_id`
- `status`
- `stage`
- `message`
- `progress_pct`
- `created_at`
- `updated_at`
- `work_dir`
- `result_path`
- `error`
- `process_pid`

The registry does not hold parsed rows or large aggregates in RAM.

### Worker Process

Each job runs inside its own `multiprocessing.Process`.

The worker is responsible for:

- receiving the saved upload paths and options;
- reading and parsing both JTL files in streaming mode;
- writing intermediate shard files;
- aggregating shard files into exact per-label statistics;
- producing final comparison JSON;
- writing the result to `result.json` inside the job directory;
- reporting progress back to the parent process via shared state or IPC.

### Working Directory

Each job gets its own directory under a temp root, for example:

`<system-temp>/jtl-comparator/jobs/<job_id>/`

Contents may include:

- `run1.jtl`
- `run2.jtl`
- `meta.json`
- `run1-shards/`
- `run2-shards/`
- `result.json`

The job directory is the single cleanup unit.

## API Contract

### `POST /compare/jobs`

Creates a new comparison job.

Request:

- multipart form data
- `file1`
- `file2`
- `name1`
- `name2`
- `jtl_mode`
- custom delta rule fields

Response:

- `202 Accepted`
- `{ "job_id": "...", "status": "queued" }`

Server behavior:

- validate presence of both files and form fields;
- create a job working directory;
- persist both uploads into that directory;
- create an in-memory job record;
- spawn the worker process;
- return immediately.

### `GET /compare/jobs/<job_id>`

Returns job state.

Response shape:

- `status`: `queued | running | cancel_requested | cancelled | completed | failed`
- `stage`: stable stage code
- `message`: user-facing description
- `progress_pct`: integer or float in range `0..100`
- `result`: present only when `completed`
- `error`: present only when `failed`

Suggested stage codes:

- `upload_saved`
- `parsing_run1`
- `parsing_run2`
- `aggregating_run1`
- `aggregating_run2`
- `building_result`
- `cleaning_up`
- `completed`
- `cancelled`
- `failed`

### `DELETE /compare/jobs/<job_id>`

Requests immediate cancellation.

Response:

- `202 Accepted`
- current job snapshot, typically `cancel_requested`

Server behavior:

- mark the job as `cancel_requested`;
- terminate the worker process immediately if it is still alive;
- transition to `cancelled`;
- schedule immediate cleanup of the job directory.

## Streaming Parse Design

### Why Replace `parse_jtl()`

The current implementation uses `pd.read_csv(path, low_memory=False, on_bad_lines="skip")`, which reads the entire file into memory.

For multi-gigabyte JTL files, the new design must replace this whole-file parse path with a streaming implementation for job execution.

### Streaming Reader

The worker should parse CSV in chunks, either with the standard library CSV reader or chunked pandas reads if chunk behavior remains memory-safe. The implementation should prefer a predictable row-streaming approach over convenience.

For every parsed row:

- validate required columns;
- apply `jtl_mode` filtering:
  - `auto`
  - `tc`
  - `samplers`
- coerce `elapsed`, `timeStamp`, and `success`;
- skip malformed rows using the same user-visible semantics as today;
- route accepted rows into shard files.

### Required Compatibility

The streaming path must preserve existing behavior for:

- required column validation;
- malformed CSV line skipping;
- `auto/tc/samplers` filtering semantics;
- empty-file or empty-result validation errors.

## Shard Strategy

### Purpose

Exact percentiles require preserving the full `elapsed` population per label. Holding that in RAM for 3+ GB files is unsafe, so the design spills accepted rows to disk.

### Sharding Model

For each accepted row, derive a shard key from `label`, for example via a stable hash modulo `N`.

Each shard record stores only the fields required for later exact aggregation:

- `label`
- `elapsed`
- `timeStamp`
- `success`

This keeps shard files compact and avoids rewriting unnecessary columns.

### Why Label-Based Sharding

All rows for the same `label` must end up in the same shard. This guarantees that exact per-label aggregation can happen independently without cross-shard merges for percentile populations.

## Exact Aggregation

### Aggregation Inputs

After parsing completes for one run, the worker processes that run's shard files.

Each shard is aggregated independently, and shard aggregation can run in parallel because each label belongs to exactly one shard.

### Per-Label Metrics

For each label, compute exact:

- `samples`
- `avg`
- `p50`
- `p90`
- `p95`
- `p99`
- `min`
- `max`
- `throughput`
- `error_rate`

Exact percentile computation requires reading the full elapsed set for that label from its shard data.

### Throughput Semantics

Preserve current throughput semantics as closely as possible:

- label throughput is based on that label's observed duration when positive;
- otherwise fall back to total run duration;
- total run duration is derived from min/max `timeStamp` across the filtered run.

## Parallelism Model

Parallelism is introduced in two places:

- the worker process isolates heavy work from the Flask request process;
- shard aggregation inside the worker uses a local worker pool to process multiple shards concurrently.

This gives practical multicore speedup while keeping the parent process responsive for status polling and cancellation.

The initial implementation should keep parsing itself single-pass and deterministic, then parallelize the shard aggregation stage where contention is lower and correctness is easier to reason about.

## Progress Reporting

Progress should combine stage-based weights with byte-level progress inside long parsing stages.

Recommended weight model:

- `0-10%`: uploads persisted and job initialized
- `10-45%`: parse run 1
- `45-80%`: parse run 2
- `80-88%`: aggregate run 1
- `88-96%`: aggregate run 2
- `96-100%`: build result, finalize, publish status

Progress details:

- parsing progress is based on bytes read from each source file;
- aggregation progress is based on completed shard count;
- messages should include the current run and stage;
- progress must remain monotonic.

## Cancellation Model

### Requirement

Cancellation must be immediate rather than cooperative-at-next-checkpoint.

### Design

Immediate cancellation is implemented by terminating the dedicated worker process rather than waiting for a thread or parser loop to observe a flag.

Cancellation flow:

1. user clicks cancel in the UI;
2. frontend calls `DELETE /compare/jobs/<job_id>`;
3. backend marks the job `cancel_requested`;
4. backend terminates the worker process if alive;
5. backend marks the job `cancelled`;
6. backend removes the job working directory.

This avoids long waits during:

- CSV chunk reads;
- shard flushing;
- sorting or percentile work;
- filesystem-heavy aggregation.

## Temporary File Cleanup

### Cleanup Strategy

All temporary data for a job lives under one job directory.

Cleanup is performed by recursively deleting the entire directory instead of trying to track every file individually.

### Cleanup Triggers

Cleanup must run in all of these paths:

- successful completion;
- failed completion;
- user cancellation;
- stale orphan cleanup after process restart or crash.

### Result Retention

To allow the UI to fetch the result after completion:

- keep completed job results for a short TTL, such as 10 to 15 minutes;
- delete failed and cancelled jobs immediately or after a very short grace period;
- run a periodic reaper to remove expired job directories and registry entries.

### Crash Recovery

On app startup, scan the temp root for stale job directories and remove anything older than the TTL or anything without a live registry owner.

## Frontend Changes

### New Compare Flow

Replace the current single blocking compare request with:

1. `POST /compare/jobs`
2. receive `job_id`
3. start polling `GET /compare/jobs/<job_id>`
4. update progress bar and stage text
5. on `completed`, call existing result rendering
6. on `failed`, show error alert
7. on `cancelled`, show cancellation notice and do not render table

### UI Elements

Add:

- a progress bar above the result area;
- stage text;
- percent text;
- cancel button while the job is active.

Behavior:

- disable `Сравнить` while a job is active;
- disable `Отменить` after click and show a stopping message;
- continue polling until a terminal state is returned.

## Backward Compatibility

The final comparison payload returned from a completed job should match the current `compare()` response shape wherever possible:

- `name1`
- `name2`
- `rows`
- `summary`
- `rules`
- run time range fields

This minimizes frontend rework and preserves export behavior.

## Testing Plan

### Unit Tests

- streaming parse preserves current `auto/tc/samplers` behavior;
- malformed CSV rows are skipped correctly;
- exact metrics from the new aggregation path match the current implementation on controlled fixtures;
- throughput semantics remain unchanged on representative datasets;
- job cleanup helpers remove directories as expected;
- stale-job reaper removes expired work directories.

### API Tests

- create job returns `202` and `job_id`;
- status endpoint returns progress transitions;
- completed job exposes final result payload;
- invalid input still returns validation errors;
- delete endpoint cancels an active job;
- cancelled jobs transition to `cancelled` and lose their temp directory;
- failed jobs clean up temp artifacts.

### UI Tests

- progress UI appears during compare;
- progress updates during polling;
- cancel button calls the cancel endpoint and ends in cancelled state;
- completed jobs still render the table and export actions correctly.

## Risks and Mitigations

### Disk Usage Growth

Risk:
large jobs may produce large shard files.

Mitigation:

- keep shard schema minimal;
- use bounded shard counts;
- enforce cleanup aggressively;
- apply TTL reaping.

### Process Lifecycle Complexity

Risk:
worker processes can become orphaned or fail mid-write.

Mitigation:

- keep job metadata in the parent process;
- always clean by removing the entire job directory;
- add startup reaping for orphaned work dirs.

### Behavioral Drift From Current Parser

Risk:
the streaming parser may diverge from current `parse_jtl()` semantics.

Mitigation:

- keep existing tests and add equivalence tests comparing old and new outputs on small fixtures;
- keep the current parser available for small-file regression comparison during implementation if helpful.

## Implementation Guidance

The implementation should introduce the new job pipeline without breaking the existing result rendering contract.

Suggested code organization:

- keep `app.py` responsible for HTTP endpoints and job lifecycle wiring;
- move job orchestration into a dedicated module;
- move streaming parse and shard aggregation into dedicated analyzer helpers instead of overloading the current `parse_jtl()` monolith;
- isolate cleanup and reaper logic in reusable functions so it can be tested directly.

## Decision

Proceed with a job-based large-file processing architecture using:

- dedicated worker processes per compare job;
- streaming JTL parsing;
- disk-backed label sharding;
- parallel exact aggregation;
- progress polling;
- immediate cancellation by process termination;
- centralized per-job directory cleanup with TTL reaping.
