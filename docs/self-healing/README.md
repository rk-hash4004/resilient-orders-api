# Self-healing patterns: retry, backoff, jitter

Two worked examples (Option A: Until Successful, Option B: loop with exponential backoff) plus two earlier options (C retry queue, D reusable policy). Flows live in `src/main/mule/self-healing/`. Built and run on the local Mule 4.12.3 runtime inside order-experience-api (port 8081). No database or Kafka needed (in-memory Object Store `SelfHealing_Store`).

Status key used below: **VERIFIED** = run live and seen in the runtime log or curl output. **PENDING** = built but not yet run.

---

## 1. The scenario (use this to open the answer)

An Order API calls an Inventory service to reserve stock. Inventory fails for a few seconds now and then (deployment, restart, traffic spike). If the Order API just returns the error, the customer sees a failure for a problem that fixes itself in seconds. Self-healing means: detect the failure, wait, retry, recover, and fail clearly if it does not recover.

Terms:
- **Transient (temporary) error**: likely to go away on its own. 503 service unavailable, timeout, connection failure, 429 too many requests.
- **Permanent error**: retrying cannot fix it. 400 bad request, 401, 403, 404.
- **Idempotent**: repeating the call does no extra harm (no duplicate reservation).
- **Exponential backoff**: each wait is longer than the last (here, doubles).
- **Jitter**: a small random extra wait, so many clients that failed together do not retry together (thundering herd).

## 2. Shared building blocks (used by both options)

| File | What it is |
|---|---|
| `00-self-healing-global.xml` | In-memory Object Store and the HTTP request config pointing at the mock |
| `01-mock-downstream.xml` | Fake inventory service with a real UP/DOWN switch (see below) |
| `02-inventory-client.xml` | Sub-flow `self-healing-call-inventory`: logs the call, then one `http:request` to the mock, sending header `X-Idempotency-Key` |

**Mock inventory service** (so the failure is real, not simulated inside the retry code):
- `GET /mock/inventory/down` switches it DOWN. `GET /mock/inventory/up` switches it UP.
- `GET /mock/inventory/reserve?sku=TILE-1`: DOWN returns 503. Empty `sku` returns 400. Otherwise returns a reservation.
- `GET /mock/inventory/slow` switches it to SLOW: `reserve` sleeps 3 s before answering, longer than the caller's 2 s `responseTimeout`, so the caller gets `HTTP:TIMEOUT`.
- Same `X-Idempotency-Key` twice returns the same `reservationId` with `replayed:true` (VERIFIED: first call `replayed:false`, second `replayed:true`, same id).

**How Mule turns HTTP status into errors:** the HTTP Request connector treats statuses outside 0-399 as errors, so 503 raises `HTTP:SERVICE_UNAVAILABLE`, 400 raises `HTTP:BAD_REQUEST`, a timeout raises `HTTP:TIMEOUT`. So "the line after the call ran" already means the status was a success. The error type is the status check.

**Idempotency key:** generated once per request (`uuid()`), before any retry, and sent on every attempt. VERIFIED in logs: the same key appears on every attempt of a run.

---

## 3. Option A: Until Successful (built-in scope)

File: `03-fixed-retry.xml`. Endpoint: `GET /self-healing/fixed` (optional `?sku=`).

### Design
```
set idemKey, sku, outcome
try
  until-successful (maxRetries=5, millisBetweenRetries=1000)
    try
      call inventory
      keep the response
      error-handler: on-error-continue for 400/401/403/404 -> outcome = FAILED_NOT_RETRIED
  error-handler: on-error-continue for MULE:RETRY_EXHAUSTED -> outcome = GAVE_UP
build response (outcome, idempotencyKey, downstream, lastError)
```

### Why it is built this way
- **Until Successful retries any error it sees.** So permanent errors are caught inside the scope with `on-error-continue`. The scope then sees a normal completion and stops retrying. Transient errors are not caught inside, so they escape and are retried.
- **`maxRetries=5` means 6 calls** (1 first call + 5 retries).
- **The idempotency key is created before the scope**, so all 6 calls send the same key.
- **`MULE:RETRY_EXHAUSTED`** is the error Until Successful raises when retries run out. It is caught by the outer `try`.

### VERIFIED results
- Service DOWN the whole run: 6 calls, gaps about 1005 ms (flat), then `GAVE_UP (MULE:RETRY_EXHAUSTED)`. Log lines: "Scheduling retry execution of event, attempt N of 5", then "Retry attempts exhausted".
- Service brought UP in the middle of a run: the 4th call succeeded, `outcome: SUCCESS`, same idempotency key on all calls.
- Permanent error (`?sku=`): `FAILED_NOT_RETRIED` (PENDING a fresh run after the revert to the plain 1000 ms version).

### Experiment: can the wait grow inside Until Successful? No
The field has an `fx` toggle, so it accepts an expression. We tried to compute a backoff and measured the gaps between the 6 calls:

| Expression in `millisBetweenRetries` | Gaps (ms) | Reading |
|---|---|---|
| `randomInt(500) + 500` (range 500-999) | 881, 899, 856, 892, 918 | Narrow band. A per-retry random would spread across 500-999 |
| `500 * (2 pow (vars.attempt default 1)) + randomInt(251)`, `attempt` incremented inside the scope | 628, 627, 626, 623, 627 (second run: 695, 693, 695, 695, 693) | Constant. `attempt` was 0 when the wait was computed: 500 x 2^0 + one random value |
| `randomInt(4000) + 500` (range 500-4499) | 3591, 3586, 3587, 3585, 3589 | One random value (about 3.59 s) reused for every retry |

**Conclusion (Mule 4.12.3):** the expression is evaluated once, when the scope is entered, and that value is used for all retries. Jitter works only as one random value per call. Exponential backoff does not work, because the counter changes inside the scope but the wait is never recomputed. The Mule docs say expressions are allowed but do not say when they are evaluated; this is what we measured.

Note on method: one run looked inconclusive because the deployed app still had the old expression (visible in the error text printed in the log). Always confirm the live expression in the error text before trusting a test.

### Limits of Option A
- One fixed wait. No exponential backoff (measured above).
- Blocks a thread between retries.
- Retries any error that escapes the scope, so every permanent error type must be listed and caught inside.

## 4. Option B: loop with exponential backoff and jitter

File: `04-backoff-jitter.xml`. Endpoint: `GET /self-healing/backoff` (optional `?sku=`).

### Design
```
Settings (one Transform): idemKey, sku, baseMs=500, jitterMs=250, maxAttempts=5, statusCode=null
foreach attempt in 1..maxAttempts            (vars.counter = attempt number)
  if statusCode is still null:
    try
      call inventory
      save statusCode (attributes.statusCode) and the response
      error-handler: on-error-continue for 503 / timeout / connectivity / 500 / 429:
        delayMs = baseMs * 2^(counter-1) + randomInt(jitterMs)
        if counter < maxAttempts: log, then wait(delayMs)
after the loop:
  if statusCode is still null -> raise CUSTOM:RETRY_EXHAUSTED
  else -> 200 SUCCESS response
flow error-handler: on-error-continue CUSTOM:RETRY_EXHAUSTED -> 503 body (httpStatus variable)
```

### The formula
`wait = baseMs x 2^(attempt - 1) + random(0 .. jitterMs - 1)` which gives about 500, 1000, 2000, 4000 ms plus jitter. After the last attempt there is no wait.

### Why it is built this way
- **Why a loop and not Until Successful:** the wait must be recalculated after every failure (see the experiment in section 3).
- **Why `foreach` with a flag:** `foreach` has no `break`. After a success the remaining passes would call the API again, so each pass starts with `statusCode == null`. `statusCode` is set only on the success path, because a 503 raises an error before the "Save response" step. So 503 or 429 never sets it and the loop continues.
- **Why check the HTTP status, not the payload:** in real systems success is defined by the status code. The payload can be empty or vary.
- **Only transient errors are caught.** 400/401/403/404 are not in the handler, so they propagate and fail immediately. No special code is needed to "not retry" them.
- **No wait after the last attempt:** the wait and its log line are inside `counter < maxAttempts`. Waiting before giving up helps nobody (an earlier version waited 8 s for nothing; fixed).
- **How `wait` works:** `wait(payload, ms)` from `dw::Runtime` sleeps for the given milliseconds and returns the value unchanged. DataWeave has no plain sleep, so a value is passed through.
- **Exhaustion handling (raise + flow error handler):** retries running out is not an error in Mule's eyes (the loop swallowed each failure), so the flow raises `CUSTOM:RETRY_EXHAUSTED` itself and handles it in its own `error-handler` with `on-error-continue`, which builds the body and sets `httpStatus = 503`. The listener has `<http:response statusCode="#[vars.httpStatus default 200]" />`. Kept in the flow (not the global handler) so the logic is visible in one place. If many APIs needed the same response, map the custom error once in a shared error handler.
- **Idempotency key:** created once before the loop, sent on every attempt.

### VERIFIED results (from the runtime log, `orders-experience-api.log`)
- **Healed:** DOWN, then UP during the run. Attempts 1-3 failed with waits 535, 1104, 2188 ms (doubling plus jitter); attempt 4 (09:31:03) had no error, so it succeeded. Same `idempotencyKey` on all four attempts.
- **Exhausted:** service DOWN the whole run. Waits 681, 1215, 2004, 4184 ms (another run: 525, 1182, 2088, 4193; and 589, 1118, 2060, 4147). Attempt 5 failed with no wait line after it (last-attempt guard works). Body was `{"outcome":"RETRY_EXHAUSTED","message":"Inventory still failing after 5 attempts","idempotencyKey":"..."}`.
- Same key on all five attempts in every run.

### Status of the `raise-error` version
The exhausted path was first verified with a `choice` after the loop. The current version uses `raise-error` plus the flow `error-handler`. VERIFIED from the log (2026-10-06): `CUSTOM:RETRY_EXHAUSTED` is raised at the `raise-error` step after the last attempt. STILL PENDING (needs the curl output): the 503 status line and the response body from the flow error handler. To re-verify (restart the app first):
```
# exhausted: expect HTTP/1.1 503 and outcome RETRY_EXHAUSTED (takes about 8 s)
curl "localhost:8081/mock/inventory/down"
curl -i "localhost:8081/self-healing/backoff"

# healed: expect HTTP/1.1 200 and outcome SUCCESS
curl "localhost:8081/mock/inventory/down"
(sleep 3; curl -s "localhost:8081/mock/inventory/up" > /dev/null) & curl -i "localhost:8081/self-healing/backoff"
```
Also not yet run: the 503 status line (only the body was seen), and a permanent-error check (`?sku=`, expect an immediate 400-class failure with no retries).

### Limits of Option B
- Blocks a thread while waiting, so low volume only.
- Total time is bounded (about 8 s here); an outage of minutes needs Option C.
- No circuit breaker: it keeps calling a service that is clearly down, for every request.

## 4A. Timeouts and a total time budget (06-backoff-budget.xml)

File: `06-backoff-budget.xml`. Endpoint: `GET /self-healing/backoff-budget` (optional `?sku=`, `?budgetMs=`, default 6000). A copy of Option B plus a time budget, kept as a separate flow so the verified `04` stays unchanged.

### Two different time limits
1. **Per-call timeout:** `responseTimeout="2000"` on the `http:request` in `02-inventory-client.xml`. One inventory call slower than 2 s raises `HTTP:TIMEOUT`, which the loop retries like any temporary error.
2. **Total time budget:** a cap on the whole request, retries and waits included. Without it, the worst case is 5 calls x 2 s + waits (0.5 + 1 + 2 + 4 s) = about 17 s. The caller (browser, gateway, API Manager policy) often gives up at 10-30 s and abandons a request we are still working on.

### VERIFIED: per-call timeout (slow mode, `/self-healing/backoff`, 2026-10-06)
- Each attempt failed with `HTTP:TIMEOUT` about 2.02 s after its "calling inventory" line (for example 10:02:29.967 call, 10:02:31.987 timeout).
- The loop retried with the usual doubling waits (722, 1041, 2161, 4152 ms), no wait after attempt 5, then raised `CUSTOM:RETRY_EXHAUSTED`.
- Total time 29.967 to 48.269 = **18.3 s**. That is the problem the budget solves.

### How the budget works
```
Settings:       startedAt = now() in milliseconds (once, before the loop)
                budgetMs  = ?budgetMs or 6000
after a failure:
  delayMs  = baseMs * 2^(counter-1) + randomInt(jitterMs)     <- the next wait
  spentMs  = now() in milliseconds - startedAt                <- time used so far
  if counter < maxAttempts  and  spentMs + delayMs <= budgetMs:  log, wait(delayMs), loop continues
  else if counter < maxAttempts:                                 budgetReached = true (no sleep), loop stops
each loop pass runs only while  statusCode == null  and  budgetReached == false
after the loop: no success -> raise CUSTOM:RETRY_EXHAUSTED -> flow error handler returns 503
                body: outcome, reason (TIME_BUDGET_REACHED or MAX_ATTEMPTS), attempts, budgetMs, idempotencyKey
```
- **Where `delayMs` and `spentMs` are calculated:** in the Transform Message "Work out next wait", inside the `on-error-continue` for temporary errors, right after the failure. `delayMs` is calculated once so the budget check and the `wait` see the same value.
- **How "time already spent" is measured:** with the clock. `startedAt` is saved once before the loop and never changes. After each failure `spentMs = now - startedAt`. It includes the time inside each call plus the waits already slept. It starts when this flow starts, so time spent upstream (gateway, network) is not counted; keep the budget a little below the caller's timeout.
- **Why it stops early rather than overshooting:** it checks before sleeping whether the wait fits, so it never sleeps and then discovers it is too late.
- **Why a flag:** `foreach` has no break, so `budgetReached` makes the remaining passes do nothing.

### VERIFIED: budget (service DOWN, 2026-10-06, from the log)
| Run | After attempt 1 | After attempt 2 | After attempt 3 | After attempt 4 | Result |
|---|---|---|---|---|---|
| `budgetMs=3000` | spent 44, wait 620 | spent 739, wait 1232 | spent 2010, next wait 2017, 4027 > 3000, **stop** | not made | 3 attempts, about 2.0 s total |
| default 6000 | spent 10, wait 744 | spent 779, wait 1195 | spent 2016, wait 2153 | spent 4189, next wait 4240, 8429 > 6000, **stop** | 4 attempts, about 4.2 s total |

Compare: the slow-mode run without a budget took 18.3 s. Same idempotency key within each run.

### VERIFIED: healed, permanent error, and a second exhausted run (budget flow, 2026-10-06, from the log)
- **Healed:** DOWN, UP at about 2 s. Attempts 1-3 failed (waits 569, 1096, 2167 ms), attempt 4 (10:12:18.394) had no error, so it succeeded. Same idempotency key on every attempt.
- **Permanent error (`?sku=`):** exactly one call, `HTTP:BAD_REQUEST` (400), no "attempt failed" line, no second call. Not retried.
- **Exhausted with `budgetMs=3000`:** attempt 3 had spent 1860 ms and the next wait was 2127 ms (3987 > 3000), so it stopped, and `CUSTOM:RETRY_EXHAUSTED` was raised about 1.9 s after the start.

### VERIFIED: the HTTP responses (curl, 2026-10-06)
- **Healed (`/self-healing/backoff-budget`):** `HTTP/1.1 200 OK`, `{"outcome":"SUCCESS","statusCode":200,"idempotencyKey":"...","downstream":{"reservationId":"...","sku":"TILE-1","replayed":false}}`.
- **Exhausted (`?budgetMs=3000`, service DOWN):** `HTTP/1.1 503 Service Unavailable`, `{"outcome":"RETRY_EXHAUSTED","reason":"TIME_BUDGET_REACHED","attempts":3,"budgetMs":3000,"idempotencyKey":"..."}`, `time_total` 1.88 s (log said about 1.9 s).
- **Permanent error (`?sku=`):** `HTTP/1.1 500 Server Error`, `content-type: text/plain`, body `HTTP GET on resource 'http://localhost:8081/mock/inventory/reserve' failed: bad request (400).`

### KNOWN GAP: permanent errors reach the caller as a raw 500
The 400 is not caught in the flow, so it falls through to default handling. It is correctly not retried (one call only), but the caller gets a plain-text 500 that leaks the internal URL, for what is really a bad request. Fix: catch the permanent errors (`HTTP:BAD_REQUEST`, `HTTP:NOT_FOUND`, `HTTP:UNAUTHORIZED`, `HTTP:FORBIDDEN`) in the flow error handler and return a clean JSON response (a 4xx, or a 502 if the problem is our request to the downstream).

## 5. Comparison

| | A: Until Successful | B: Loop |
|---|---|---|
| Wait | Fixed (one value per call) | Doubles + jitter, recalculated each time |
| Code | One scope | A loop, a flag, a wait, a raise and a handler |
| Exhausted error | `MULE:RETRY_EXHAUSTED` (built in) | Custom `CUSTOM:RETRY_EXHAUSTED` (we raise it) |
| Permanent errors | Must be caught inside the scope | Simply not caught |
| Blocks a thread | Yes | Yes |
| Total time cap | None | None in 04; `06-backoff-budget.xml` adds one |
| Use when | Short outages, simple needs | You need backoff and jitter |

Considered and rejected: an Until Successful inside the loop (gives back-to-back calls at each pass boundary, adds complexity, no cleaner backoff).

One-line summary: retry only transient errors, make the call idempotent, back off exponentially with jitter, stop on success, and fail clearly when retries run out.

---

## 6. Other options (earlier work, not part of the two examples above)

| Option | Endpoint | What it shows |
|---|---|---|
| C. Retry queue | `GET /self-healing/queue/submit`, then `GET /self-healing/queue/status?id=<id>` | Task with attempts and next-attempt time, scheduler retries when due, DEAD after 5 attempts. No thread sleeps. File `05-retry-queue.xml` |
| D. Catalyst policy | not built | `mulesoft-catalyst/exponential-backoff-retry-policy-mule-4` (reusable policy: initial interval, multiplier, max interval, max elapsed time). Review before use |

Option C results (2026-10-05, from the earlier version that used a `failTimes` simulator): 3 failures gave `PENDING` then `DONE` after 4 attempts, gaps 1988, 3007, 5006 ms. 99 failures gave `DEAD` after 5 attempts, gaps 1998, 3004, 5000, 9001 ms. Gaps are rounded up by the 1-second scheduler tick, so jitter below 1 s is hidden. Not re-verified after the mock switch change.

Production notes (Option C): use a DB table instead of the Object Store so tasks survive restarts: `retry_task(id, payload, attempts, status, next_attempt_at, last_error)`. The scheduler selects `status='PENDING' AND next_attempt_at <= now()` with a claim/lease; after max attempts set `DEAD` and alert. This is the fix for the order-experience-api outbox publisher (fixed 15 s retry, no backoff).

## 8. How the whole thing was built (order, for the story)
1. Built a mock inventory service with a real UP/DOWN switch and idempotency-key replay, and tested it by hand.
2. Built the call sub-flow and Option A; tested DOWN (gave up), healed (succeeded on attempt 4), permanent error.
3. Tried to get backoff and jitter inside Until Successful; measured the gaps three ways; concluded the wait is evaluated once.
4. Built Option B as a loop; simplified it step by step (one state, calculated wait from `vars.counter`, status-code stop check, no wait after the last attempt).
5. Moved the exhausted case from a `choice` to `raise-error` plus a flow error handler.
6. Verified healed and exhausted runs from the runtime log.
7. Added a SLOW mode to the mock and proved the 2 s per-call timeout: five timeouts plus waits took 18.3 s.
8. Built `06-backoff-budget.xml`, a copy of Option B with a total time budget, and proved it stops after 3 attempts (about 2.0 s) with a 3 s budget and after 4 attempts (about 4.2 s) with the 6 s default.

## 9. Circuit breaker (fully verified 2026-10-12)

File: `src/main/mule/self-healing/07-circuit-breaker.xml`.

Retry asks "should I try again?". The breaker asks "should I even try?". If inventory keeps failing, stop calling it for a while so we fail fast and give it room to recover.

States (kept in Object Store key `cb-state` = `{failures, openedAt}`, shared by all requests):
- CLOSED (`openedAt = null`): call normally. Success resets failures to 0.
- OPEN (`openedAt` set, less than 10 s ago): do not call, return 503 `CIRCUIT_OPEN` at once.
- HALF-OPEN (`openedAt` set, 10 s passed): allow one trial call. Success closes it; failure opens it again.

Rule: 3 failures in a row open the circuit for 10 s. Only temporary errors count.

Verified result (mock DOWN, 4 calls in a row):

| Call | Result |
|---|---|
| 1 | CALL_FAILED, CLOSED, failuresInARow 1 |
| 2 | CALL_FAILED, CLOSED, failuresInARow 2 |
| 3 | CALL_FAILED, OPEN, failuresInARow 3 (trips) |
| 4 | CIRCUIT_OPEN, retryAfterMs 9967, no inventory call |

Recovery verified: mock UP, immediate call gave CIRCUIT_OPEN (waits out the 10 s even though inventory is up); after 11 s the half-open trial gave SUCCESS and CLOSED; next call SUCCESS (normal).

### How the logic reads (simple version)
Notebook with two lines: failures in a row, and "stopped calling at" time (blank = closed).
1. Read the notebook (`os:retrieve`, default 0 failures, blank).
2. If the time is filled in AND less than 10 s ago: do not call, return CIRCUIT_OPEN (the `when` branch, line 33).
3. Otherwise call inventory. Success: write `{0, null}` (closed). Temporary failure: failures + 1; at 3 or more, or if the time was already filled in (a failed trial), write the time now (open). Return CALL_FAILED.

States are not stored, they are worked out from `openedAt` and the clock:
- CLOSED: `openedAt = null`, `otherwise` branch, normal call.
- OPEN: `openedAt` set and under 10 s ago, `when` branch, fail fast.
- HALF-OPEN: `openedAt` set and 10 s or more ago, same `otherwise` branch, one trial call. Only the logger on line 53 tells them apart.
- Failed trial: the count keeps growing (3 becomes 4), and `openedAt != null` makes `opens` true, so it reopens for another 10 s. Success resets to `{0, null}`.
- `vars.newState` is built in the "Count failure" transform (the `do` block) from the old `vars.cb`, then stored and used in the log and the response.
- Permanent errors (400, 404...) are not counted.
- No timer runs. After 10 s the next request just finds the check false.

Limits: read-modify-write is not atomic; nothing limits half-open to one trial call (two requests after the window can both trial).

Known limit: Object Store read-modify-write is not atomic, so under heavy concurrency the count can be off.

## 10. Retry queue (Option C): verified 2026-10-12

File: `src/main/mule/self-healing/05-retry-queue.xml`. Submit saves a task note (status, attempts, nextAttemptAt, idemKey, history) and returns the id at once. A scheduler runs every second, picks tasks that are PENDING and due, calls inventory, and updates the note. The wait is only the `nextAttemptAt` time, nothing sleeps. Wait = `1000 * 2^attempts + random(501)`.

| Test | Result |
|---|---|
| Inventory DOWN the whole time | 5 attempts, waits 1345, 2166, 4366, 8100 ms, then DEAD (attempt 5 has nextTryInMs null). |
| DOWN, UP after 4 s | Attempts 1 and 2 failed (1332, 2214 ms), attempt 3 OK, status DONE, same idemKey reused. |
| Permanent error (sku empty, 400) | DEAD on attempt 1, "permanent error, not retried", no retries. |

Run script for the recovery test: `scripts/queue-recovery-test.sh`.

Enterprise version: the failed message goes to a broker retry queue with a delay (delivery delay, or a chain of retry queues with growing delays), max deliveries, then a dead-letter queue (DLQ) with alerting and replay. Processing logic is written once, the retry queues only add delay. The caller gets 202 Accepted with a tracking id.

Limits: store read-modify-write is not atomic; several workers need a lock so two do not pick the same task.

## 11. Dead-letter queue (DLQ): theory

- What: a separate place for messages that failed too many times or can never succeed. The main flow stops retrying them and they wait for a person.
- Why: without it a bad message either retries forever and blocks other work (a poison message), or is silently dropped. With it nothing is lost and nothing blocks.
- Goes there: after max retries, or at once for a permanent error (bad data, 400).
- Store with it: original message, error, attempt count, time.
- Next: alert on DLQ depth, fix the cause, replay back to the main queue (replay tool), or discard on purpose.
- In this project: `status: DEAD` in the retry queue (05) is the DLQ, with `history` as the diagnosis. Reached after 5 attempts or a 400.
- In MuleSoft: Anypoint MQ / JMS brokers have a max-deliveries setting that moves the message to a dead-letter queue.

## 12. Bulkhead: theory

- What: limit how much capacity one downstream can use, so a slow or failing dependency cannot take everything down. Named after ship compartments: one floods, the others stay dry.
- Problem: a slow inventory holds a thread per call. Enough waiting calls use up all threads and unrelated healthy endpoints (orders, payments) stop responding too. A timeout alone does not fix this: 100 req/s with a 2 s timeout can still tie up about 200 threads.
- Fix: a limited pool per downstream (for example 10 concurrent calls to inventory). The 11th call fails fast or waits briefly.

| Pattern | Question it answers |
|---|---|
| Timeout | How long do I wait for one call? |
| Retry | Should I try this call again? |
| Circuit breaker | Should I stop calling for a while? |
| Bulkhead | How many calls may be in flight at once? |

- In MuleSoft: flow `maxConcurrency`; HTTP requester connection pool `maxConnections` per host; one flow per dependency each with its own maxConcurrency (Mule 4 flows share the same pools, so separation alone does not isolate); limit concurrent consumers on Anypoint MQ / JMS; rate limiting and throttling policies in API Manager; separate apps or workers per critical dependency.
- In this project: not built. Closest: the retry queue worker runs one task at a time (`disallowConcurrentExecution`).
- Terms: downstream = what my service calls (inventory); upstream = what calls my service (the caller).

## 13. Health checks and auto-restart: theory

- Problem: an app can be running but stuck (out of memory, deadlock, cannot reach its database). Nobody knows until users complain.
- Liveness: is it alive or stuck? If it fails, restart it. Keep it shallow. Readiness: can it take traffic now? If it fails, stop sending traffic, do not restart. Dependency checks (database) belong in readiness, otherwise a database outage restarts every instance for nothing.
- CloudHub 1.0: Runtime Manager, Settings, Advanced, tick "Automatically restart application when not responding". CloudHub checks the worker every 30 s; 3 failures in a row log a warning and notify; it restarts the app and retries up to 5 times. No code needed.
- CloudHub 2.0: a crashed replica is restarted automatically, with exponential backoff if it keeps failing to start. Release notes say readiness and liveness probes use HTTP GET. User-configurable probe settings NOT confirmed in the docs read, so do not quote option names.
- Own `/health` flow (returns 200 `{"status":"UP"}`) is optional: used by monitors, load balancers and API Functional Monitoring for alerts. A standard Mule app has none.
- Enterprise stack: redundancy (2+ instances behind a load balancer), platform restarts, monitoring and alerts (error rate, latency, CPU, memory, DLQ depth), synthetic checks, in-app resilience patterns, safe releases with automatic rollback, disaster recovery, runbooks.
- Sources: docs.mulesoft.com/cloudhub/worker-monitoring, docs.mulesoft.com/cloudhub-2/ch2-understand-app-restart.

## 14. Failover and high availability (HA): theory

- HA: the system keeps working when one part fails (designed in advance). Failover: the switch to a working part.
- Instance or zone level: 2+ workers or replicas across zones behind a load balancer. Region level (disaster recovery): second deployment in another region, traffic switched by DNS or a global load balancer (exact MuleSoft options not verified).
- RTO = how long you may be down. RPO = how much data you may lose.
- Active-active: both sides serve, near-instant failover, costs more. Active-passive: standby, cheaper, slower switch.
- CloudHub 1.0: more than one worker per app, load balanced across zones. CloudHub 2.0: more than one replica.
- What breaks HA: state kept in one instance (use shared storage such as a database or Object Store), schedulers or pollers running on every worker (need a lock or one primary; the POC retry-queue worker would run twice), retries duplicating work (use idempotency keys).

### 14A. MuleSoft HA and DR: what the docs confirm (checked 2026-10-12, replaces the "not verified" notes in section 14)
- CloudHub 1.0: up to 8 workers (128 vCores), spread automatically across 2+ data centres; internal reverse proxy load balances HTTP. Persistent queues: no message loss if a worker or data centre fails, kept up to 4 days, +10-20 ms write and +70-100 ms read for small messages, duplicates possible so consumers must be idempotent.
- CloudHub 2.0: with 2+ replicas the platform spreads them across 2+ availability zones by default; if a zone fails the app restarts in another. Private Spaces replace the CloudHub 1.0 VPC.
- Multi-region is NOT automatic. MuleSoft runs the platform inside each region; the customer owns cross-region strategy, failover and data sync. A regional outage makes apps in that region unavailable until redeployed elsewhere.
- CloudHub 2.0 DR strategies (typically need a bring-your-own global load balancer, e.g. DNS-based): active-active (fully scaled in 2+ regions, near-zero downtime, highest cost), warm standby (scaled-down copy that scales up on failover), cold standby (deployed but stopped, slowest, cheapest).
- Active-passive: schedulers and listeners (Anypoint MQ, Database, FTP, Salesforce) can be set stopped in the passive region to avoid double processing.
- NOT confirmed: whether Object Store is shared across workers (the page only recommends it for batch state).
- Sources: docs.mulesoft.com/cloudhub-2/ch2-ha-dr, docs.mulesoft.com/cloudhub/cloudhub-hadr, docs.mulesoft.com/cloudhub/cloudhub-fabric.

