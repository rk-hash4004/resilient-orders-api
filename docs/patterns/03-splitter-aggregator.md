# Pattern 03 — Splitter + Aggregator

> Take a **composite message** (an order with many line items), **split** it into individual
> elements, process each one on its own, then **aggregate** the results back into a single
> reply. In Mule 4 the splitter+aggregator *is* the loop scope — `foreach` (sequential) or
> `parallel-foreach` (concurrent).
> Source: https://docs.mulesoft.com/mule-runtime/latest/foreach-scope-concept
> · https://docs.mulesoft.com/mule-runtime/latest/parallel-foreach-scope

---

## The real-world scenario
One **order** arrives carrying a list of line items:
```json
{
  "orderId": "ORD-1001",
  "items": [
    { "sku": "SKU-9", "qty": 2, "unitPrice": 50 },
    { "sku": "SKU-3", "qty": 1, "unitPrice": 30 },
    { "sku": "SKU-7", "qty": 4, "unitPrice": 10 }
  ]
}
```
Each item needs its **own** work — a stock check, a price/tax lookup, a per-line DB insert.
That's a *per-element* call, not a whole-order call. So we **split** the `items` array, run
`process-item` for each element, then **aggregate** the per-line results into `lineTotals` and a
`grandTotal`:
```json
{
  "payloadRestored": { "orderId": "ORD-1001", "items": [ … ] },
  "lineTotals": [
    { "sku": "SKU-9", "lineTotal": 100 },
    { "sku": "SKU-3", "lineTotal": 30 },
    { "sku": "SKU-7", "lineTotal": 40 }
  ],
  "grandTotal": 170
}
```

---

## The flows I built

### A) Sequential — `patterns-splitter-aggregator-flow` (`/patterns/foreach`)
```
Listener /patterns/foreach
  → Set Payload (order with 3 items)
  → Set Variable lineTotals = []            (the aggregator accumulator)
  → For Each (collection = payload.items):
        → flow-ref process-item             (the per-item "backend": log + enrich)
        → Set Variable lineTotals =
              vars.lineTotals + { sku: payload.sku, lineTotal: payload.qty * payload.unitPrice }
  → Transform (aggregate):
        { payloadRestored: payload,          // payload is back to the WHOLE order
          lineTotals:      vars.lineTotals,
          grandTotal:      sum(vars.lineTotals map $.lineTotal) }
```
The aggregation trick: `foreach` **restores the original payload** after the loop, so you
**cannot** accumulate in the payload — you accumulate into a **variable** (`vars.lineTotals`)
and read it after the loop. `payloadRestored` in the output proves the whole order came back.

### B) Concurrent — `patterns-parallel-foreach-flow` (`/patterns/parallel-foreach`)
```
Listener /patterns/parallel-foreach
  → Set Payload (same order)
  → Parallel For Each (collection = payload.items):
        → flow-ref process-item
  → Transform:
        { rawAggregated:  payload,                    // an ARRAY of { payload: … } wrappers
          itemsUnwrapped: payload map $.payload,       // pull the real result out of each wrapper
          grandTotal:     sum(payload map $.payload.lineTotal) }
```
Here the aggregation is **built in**: `parallel-foreach` **collects** each route's result into an
array **in original order** and puts that array in the payload — no accumulator variable needed.
Each element is wrapped as `{ payload, attributes, … }`, so you unwrap with `map $.payload`.

`process-item` is a `sub-flow` that **stands in for a real per-item `http:request` / `db:insert`** —
the whole reason the loop exists instead of one bulk transform.

---

## The key mechanism — `foreach` vs `parallel-foreach`
| | `foreach` (sequential) | `parallel-foreach` (concurrent) |
|---|---|---|
| Execution | one item at a time, **in order** | items **in parallel** (bounded by `maxConcurrency`) |
| Latency | ≈ **sum** of per-item work | ≈ **slowest** item |
| Aggregation | you **accumulate into a variable**; payload is **restored** after the loop | **built in** — results collected into an **array**, order preserved, placed in payload |
| Result shape | whatever your accumulator holds | array of `{ payload, attributes }` wrappers → `map $.payload` |
| Extra vars in scope | `counter` (1-based) and `rootMessage` | (each route is isolated) |
| Item ↔ item dependency | ✅ item N can use vars set by item N-1 | ❌ routes are independent, no shared running state |
| Error model | fails **on the item that breaks**, stops the loop | routes settle, then **`MULE:COMPOSITE_ROUTING`** aggregates failures |
| Transaction | runs in the **caller's** thread/transaction | separate threads → **cannot** join the caller's transaction |

**Decision rule:** order matters, items depend on each other, or you're inside a transaction →
**`foreach`**. Items are independent and latency matters → **`parallel-foreach`**.

---

## Why this is the "Splitter + Aggregator" EIP
- **Splitter** — break a composite message into a sequence of individual messages
  (`collection="#[payload.items]"` splits the array element-by-element).
- **Aggregator** — collect the individual results and combine them into one message
  (the accumulator variable, or `parallel-foreach`'s built-in result array + `grandTotal`).
- In classic EIP these are two separate patterns; Mule 4 folds both into a single **loop scope**.

---

## Gotchas
- **`foreach` restores the payload** after the loop. If you "accumulate" by appending to the
  payload inside the loop, it's **gone** when the loop ends. Accumulate into a **variable**.
- Inside `foreach`, `payload` **is the current item** — that's why `process-item` and the
  accumulator read `payload.sku`, `payload.qty`, not `payload.items[…]`.
- **`parallel-foreach` wraps each result** as `{ payload, attributes }`. Forget the
  `map $.payload` unwrap and your `sum(...)` blows up on the wrapper objects.
- `parallel-foreach` **preserves input order** in its output array even though execution is
  concurrent — don't assume the output is shuffled.
- `parallel-foreach` uses **separate threads**, so it **cannot participate** in an XA/local
  transaction started upstream. Need transactional per-item writes → use `foreach`.
- **DataWeave header is mandatory** in each Transform (`%dw 2.0` / `output application/json` /
  `---`), else you get `application/java` output.
- Empty collection → the loop body simply **doesn't run** (no error); your aggregator stays `[]`.

---

## Experiments to run
1. Append to `payload` inside the `foreach` instead of `vars.lineTotals` → confirm it's **gone**
   after the loop because the payload is restored.
2. Drop the `map $.payload` unwrap in the parallel flow → watch `sum(...)` fail on the wrapper
   objects.
3. Log `#[vars.counter]` inside the `foreach` → see the 1-based index; try to find an equivalent
   in `parallel-foreach` and prove there isn't a shared one.
4. Throw an error for one SKU in `process-item` → compare `foreach` (stops on that item) vs
   `parallel-foreach` (settles all, then `MULE:COMPOSITE_ROUTING`).

---

# Exercise 3d — Batch Processing (the *bulk* splitter+aggregator)

> `foreach`/`parallel-foreach` are the **in-flight, synchronous** splitter+aggregator. **Batch** is
> the **asynchronous, high-volume** one: split into records held in **persistent queues**, process
> them across steps with crash-recovery, aggregate in chunks, and report at the end.
> Source: https://docs.mulesoft.com/mule-runtime/latest/batch-processing-concept
> · https://docs.mulesoft.com/mule-runtime/latest/batch-aggregator-concept

## When Batch instead of a loop scope
Reach for Batch when: **volume is large** (100k+ records / big files), work must **survive a crash**
(persistent queues + recovery), you want **built-in failure tolerance & reporting**, or you need
**bulk commits** (N records → one DB/API call). For a handful of in-request items, a loop scope is
simpler. Batch is **fire-and-forget async** — the triggering flow returns immediately.

## The three phases
1. **Load & Dispatch** (implicit) — splits the input (`Iterable`/`Array`/JSON/XML) into **records**
   in persistent queues. *This is the splitter.*
2. **Process Records** — one or more `batch:step`s. In a step, `payload` = **one record**; `vars`
   set on a record **persist across steps**; records **can't** see the original message `attributes`.
3. **On Complete** — `payload` = a **BatchJobResult** (counts). Only original-event vars reach here.

## The flow I built (`patterns-batch-flow`, `/patterns/batch`)
```
Listener /patterns/batch
  → Transform: generate 12 order lines (every 5th invalid: qty -1)
  → batch:job  jobName=patterns-batch-lineitems  maxFailedRecords=-1  blockSize=5  maxConcurrency=4
      process-records:
        Step validate-and-price          (payload = ONE record)
            Choice: qty<=0 → raise-error APP:INVALID_LINE   (marks record FAILED)
                    else   → price (payload ++ {lineTotal,status}) + set-variable pricedAt   (record var)
        Step commit-good  acceptPolicy=NO_FAILURES
            Logger "Step2 sees … pricedAt=…"   (proves record var survived Step 1)
            batch:aggregator size=5            (payload = ARRAY of records) → Logger "BULK COMMIT …"
        Step route-failed acceptPolicy=ONLY_FAILURES
            Logger "DEAD-LETTER …"             (only the 2 failed records land here)
      on-complete:
            Logger "BATCH DONE total=12 loaded=12 success=10 failed=2 elapsedMs=…"
  → Transform { accepted:true, batchJobInstanceId: payload.id }   (returns INSTANTLY — async proof)
```
The loggers stand in for the real work (`db:bulk-insert` per chunk; a dead-letter table for failures)
to keep the example free of external infrastructure.

## Key attributes
- **`batch:job`**: `jobName` (id in logs), `blockSize` (records per block, default 100),
  `maxConcurrency` (parallelism), **`maxFailedRecords`** (`0`=abort on first failure default; `-1`=
  tolerate all and finish), `schedulingStrategy` (`ORDERED_SEQUENTIAL` | `ROUND_ROBIN`).
- **`batch:step`**: **`acceptExpression`** (DW filter — record enters the step only if true) and
  **`acceptPolicy`** (`NO_FAILURES` default | `ONLY_FAILURES` | `ALL`) — routing by *prior* success.
- **`batch:aggregator`**: **`size="N"`** (fixed chunks) **xor** **`streaming="true"`** (one array of
  all records, sequential-only). Must be the **last processor in its step**.

## `acceptPolicy` vs `acceptExpression` (the classic mix-up)
- **`acceptPolicy`** filters by a record's **failure state** from earlier steps: `NO_FAILURES` (good
  ones only), `ONLY_FAILURES` (the broken ones — dead-letter/retry), `ALL`.
- **`acceptExpression`** filters by the **record's data** (e.g. `#[payload.lineTotal > 40]`). A record
  that fails the expression **skips the step but is NOT failed** — `successfulRecords` is unaffected.

## Two DataWeave gotchas this exercise WILL hit (and the fixes)
1. **Logger media-type inference.** A Logger message that string-concatenates a value pulled from a
   **JSON payload** with plain literals throws:
   *"Unable to infer a output media type as more than one is being used: application/json,application/java."*
   A Logger has no `%dw`/`output` header slot, and **`as String` on the field does NOT fix it** — the
   error is about the *whole script's output format*, not the sub-value's type. **Fix:** prefix the
   expression with an output directive:
   ```
   #[output application/java --- "Step2 sees " ++ payload.sku ++ " pricedAt=" ++ (vars.pricedAt as String)]
   ```
   (`write(payload,"application/json")` also sidesteps it, since `write` returns a plain String.)
2. **Aggregator records are `Binary`.** Inside `batch:aggregator`, each array element is an
   **unparsed `Binary`** (raw JSON bytes), so `$.lineTotal` fails with a *Value Selector* error.
   **Fix:** parse each element — `sum(payload map ((read($, "application/json")).lineTotal))` — **or**
   set **`preserveMimeTypes="true"`** on the aggregator so records keep `application/json` and
   `$.field` works directly.

## What each intricacy looked like in the logs
- **Async** — response came back seconds *before* the records processed.
- **Split** — `Finished loading phase … 12 records were loaded`.
- **Out-of-order processing** — records logged as SKU-1, SKU-11, SKU-6, … on threads `uber.01/04/10`
  → Batch does **NOT** preserve order (unlike `parallel-foreach`); `maxConcurrency` runs blocks in parallel.
- **Failure de-dup** — *"This is the first record to show this exception … subsequent … will not be
  logged"* — Batch logs a repeated exception once; the count (2) still reflects all failures.
- **NO_FAILURES / ONLY_FAILURES** — 10 records in `commit-good`, exactly the 2 failed in `route-failed`.
- **Record var persisted** — `Step2 sees SKU-x pricedAt=…` shows the `pricedAt` set back in Step 1.
- **Chunked aggregation** — two `BULK COMMIT 5 lines` (10 good → 2 bulk ops).
- **Report** — `BATCH DONE total=12 loaded=12 success=10 failed=2 elapsedMs=…` printed once in on-complete.

## Experiments to run (each flips one attribute)
1. Aggregator `size="5"` → `streaming="true"` → one `BULK COMMIT 10 lines` instead of two chunks.
2. Job `maxFailedRecords="-1"` → `"0"` → the instance **aborts** on the first failed record.
3. Add `acceptExpression="#[payload.lineTotal > 40]"` to `commit-good` → SKU-1 & SKU-4 **skip** the
   commit, but `successfulRecords` stays 10 (skipped ≠ failed).

---

## Architect deep-dive — Batch beyond the mechanics
The flow above proves the *how*. An architect is judged on **reliability, scale, and
operations**. These are the concepts to be able to talk through even without more canvas wiring.

### 1. Reliability & crash recovery
Batch records don't live in memory — Load & Dispatch writes them to **disk-backed persistent
queues**. If the runtime restarts mid-job, the instance can **resume** from where it stopped.
**CloudHub 2.0 qualifier (official docs):** this holds only when the replica's disk survives (a Mule
restart inside the same replica). A redeploy or replaced replica clears the volatile disk and the job
does not continue. Keep the source of truth and a progress marker outside (DB / Object Store v2).
The processing guarantee is therefore **at-least-once**: a record may be re-processed after a
crash. *Say:* "Batch trades in-memory speed for durability — persistent queues + recovery, at the
cost of at-least-once semantics."

### 2. Idempotency / exactly-once (the #1 correctness trap)
Because recovery re-processes records, **your steps must be idempotent**. A naive `INSERT` will
double-insert on recovery. Use **upsert with a natural key**, a dedupe check (Idempotent Receiver /
Object Store), or a transactional block (below). Exactly-once is achieved by **idempotent writes**,
not by the framework. This is the question that separates "used batch" from "designed with batch."

### 3. Transactions in a step
**Official doc wording:** "The batch aggregator scope does not support job-instance-wide transactions."
"You can define a transaction inside a batch step that processes each record in a separate transaction",
and it "must start and end within the step's boundaries". A step's transaction ends before its
aggregator runs. For all-or-nothing: load into a staging table (tagged with a run id) with batch, then
in `on-complete`, if `failedRecords == 0`, move to the real table in ONE SQL transaction; else discard.
Batch does **not** run the whole job in one transaction (that would break streaming). Instead you
make a **step (or the aggregator's bulk commit) transactional** by wrapping its processors in a
`<try transactionalAction="ALWAYS_BEGIN">` — so a block's bulk DB write commits **atomically** or
rolls back. Combine with an idempotency key for exactly-once bulk persistence. XA if more than one
resource (DB + JMS) must commit together.

### 4. Triggering + Watermarking (incremental processing)
Real batch jobs aren't POST-triggered — they're driven by a **Scheduler** polling a source. To
process only **new/changed** rows each run, keep a **watermark** (last-processed id/timestamp) in an
**Object Store**: retrieve it → query `WHERE updated_at > :watermark` → process → store the new max
back. This is the incremental-sync pattern (nightly delta loads). *You already have
`ObjectStore_Config` and `Database_Config` — this is the natural next hands-on if you want it.*

### 5. Performance tuning — `blockSize` vs `maxConcurrency`
- **`blockSize`** (default 100) — records per block loaded into memory together. Bigger = fewer
  queue round-trips but more memory per block.
- **`maxConcurrency`** (default 2× cores) — how many blocks run in parallel.
- **Aggregator `size`** — how many records per bulk downstream call.
Tune to the **downstream's** capacity, not the runtime's: cranking `maxConcurrency` past what the DB
or API can take just moves the bottleneck and causes timeouts. More parallelism is not always faster.

### 6. Streaming large inputs
During Load & Dispatch, batch **streams** the input into the persistent queue, so a 2 GB file or a
million-row query never sits fully in memory. Feed it a **repeatable/streamed** source (DB streaming,
file read) — don't `read()` the whole thing into a variable first, or you defeat the point.

### 7. HA / deployment reality (a real design constraint)
Batch state is **node-local** — records live on the worker/node that started the instance. A single
job instance does **not** distribute across CloudHub workers or cluster nodes; in a cluster it runs on
the **primary** node. So batch gives you **vertical** scale (one beefy worker), not horizontal
fan-out. And on ephemeral disk (CloudHub), a worker crash can lose in-flight state — for strong
durability, externalize the source-of-truth and lean on idempotent re-runs. Don't promise "batch
scales horizontally across workers" — it doesn't.

### 8. Error architecture — record vs job failure
- **Record failure** — an unhandled error marks that record failed; the job **continues** (subject to
  `maxFailedRecords`). Handle per-record with an `on-error-continue` **inside** the step if you want to
  recover/annotate rather than fail it.
- **Input/loading-phase failure** — if Load & Dispatch itself fails (can't read the source), the
  **whole instance** aborts and **no** records process.
- **On Complete always runs** (even with failures) — it's your reporting/notification hook.
- `maxFailedRecords` is the abort threshold: `0` fail-fast, `-1` tolerate all.

### 9. Reprocessing / dead-letter pattern
The `ONLY_FAILURES` step is your **dead-letter capture**. Architect pattern: write failed records
(with their error) to a **DLQ table/queue**, then a separate **reconciliation batch** reprocesses
them later — or retry inline. This turns "2 failed" from a dead end into an operational workflow.

### 10. Monitoring & observability
The `BatchJobResult` counts (total/loaded/processed/successful/failed/elapsed) are your job KPIs —
log/alert when `failedRecords > 0`. The **batch job instance id** you return to the caller is the
correlation handle for tracking. In Runtime Manager, batch exposes per-instance dashboards
(queued / in-progress / failed). Capture **which** records failed (not just the count) so ops can act.

**One-line architect summary:** *Batch = durable, at-least-once, vertically-scaled bulk processing
with persistent queues and built-in failure tolerance; design for idempotency, tune concurrency to
the downstream, and treat failed records as an operational workflow, not an exception.*

---

# Exercise 3e — Aggregators Module (the *real-time, event-driven* splitter+aggregator)

> Batch (3d) answers "I already have all the data — process it in bulk." The **Aggregators
> module** answers the opposite question: "pieces of the same business transaction arrive
> **one at a time, unpredictably, over the network** — how do I know when I have enough to act?"
> Source: https://docs.mulesoft.com/aggregators-module/1.1/aggregators-module-reference
> · https://docs.mulesoft.com/aggregators-module/1.1/group-based-aggregator

Flows live in a **separate file**, `src/main/mule/patterns/03e-aggregator.xml` — not in
`03-splitter-aggregator.xml` with the others. That split happened mid-build after repeated XML
corruption in the shared file (see Gotchas); isolating the aggregator exercises to their own file
stopped the bleeding.

## The three aggregator flavors

| | **Group-based** | **Size-based** | **Time-based** |
|---|---|---|---|
| Correlation | explicit `groupId` (a business key, e.g. `orderId`) | none — auto-generated aggregation IDs | none — auto-generated aggregation IDs |
| Completion signal | `groupSize` reached | `maxSize` reached | timer only — **no size concept by default** |
| Has `aggregation-complete` route? | ✅ yes | ✅ yes | ❌ **no** — only `incremental-aggregation` exists |
| What happens after completion | that `groupId` **rejects** further elements for `evictionTime` | next element silently starts a **new** batch — no rejection | N/A — every call just returns "queued"; release is only ever visible via the Listener |
| Real-world use | "wait until I have all 3 line-items for order X" | "flush every N messages, whoever they are" (throughput) | "flush whatever arrived in this X-second window" (time-windowed batching) |

## The flows I built (`patterns/03e-aggregator.xml`)

### Group-based — `patterns-aggregator-flow` (`/patterns/aggregator`) + `patterns-aggregator-listener-flow`
```
Listener /patterns/aggregator
  → Group-based Aggregator  name=orderLineAggregator  groupId=#[payload.orderId]
                             groupSize=3  timeout=15  evictionTime=30
      incremental-aggregation:
          Logger
          Set Variable aggResult = { status:"incremental", orderId: payload[-1].orderId,
                                      sku: payload[-1].sku, elementsSoFar: sizeOf(payload) }
      aggregation-complete:
          Logger
          Set Variable aggResult = { status:"complete", orderId: payload[0].orderId,
                                      lines: payload, totalQty: sum(payload map $.qty) }
  → Transform Message: vars.aggResult      (the pass-through-router fix — see Gotchas)

patterns-aggregator-listener-flow (separate flow):
  Aggregator Listener  aggregatorName=orderLineAggregator  includeTimedOutGroups=true
  → Logger: payload[0].orderId, attributes.isAggregationComplete, sizeOf(payload)
```

### Size-based — `patterns-size-based-aggregator-flow` (`/patterns/size-aggregator`)
```
Listener /patterns/size-aggregator
  → Size-based Aggregator  name=lineItemSizeAggregator  maxSize=3     (no groupId — no correlation)
      incremental-aggregation: Logger + Set Variable { status:"incremental", elementsSoFar }
      aggregation-complete:    Logger + Set Variable { status:"complete", batch: payload, count }
  → Transform Message: vars.aggResult
```

### Time-based — `patterns-time-based-aggregator-flow` (`/patterns/time-aggregator`) + its own listener
```
Listener /patterns/time-aggregator
  → Time-based Aggregator  name=lineItemTimeAggregator  period=10
      incremental-aggregation (its ONLY route): Logger + Set Variable { status:"queued", elementsBuffered }
  → Transform Message: vars.aggResult          (every call returns "queued" — no other outcome exists)

patterns-time-based-aggregator-listener-flow (separate flow):
  Aggregator Listener  aggregatorName=lineItemTimeAggregator
  → Logger: sizeOf(payload), attributes.isAggregationComplete
```

## Key attributes
- **`groupId`** (group-based only) — the correlation key. Everything else (`groupSize`, `maxSize`,
  `period`) governs *when a bucket releases*, not *which bucket an element belongs to*.
- **`groupSize` / `maxSize`** — the count that triggers a synchronous, size-driven release
  (`aggregation-complete` fires inline, on the same thread as the triggering caller).
- **`timeout`** (group-based, size-based) / **`period`** (time-based) — the clock that force-releases
  a bucket that never reached its size target. Default time unit is **SECONDS**. A time-driven
  release is **asynchronous** — it fires on Mule's own internal scheduler thread, not any caller's.
- **`evictionTime`** (group-based only) — how long a *finished* group blocks reuse of its `groupId`
  before the slot is forgotten and a new group can start fresh under the same key.
- **Aggregator Listener** `includeTimedOutGroups` — **only applies to group-based-aggregator's**
  timeout mechanism. Size-based and time-based releases always reach the listener regardless; this
  flag doesn't exist as a meaningful knob for them.

## Gotchas

1. **Aggregators are pass-through routers.** Confirmed against the official docs: *"Aggregators are
   pass-through routers. The same data that an aggregator scope receives is processed by the
   components that follow the aggregator scope... The only modification is variable propagation."*
   Whatever you set on `payload` **inside** `incremental-aggregation` or `aggregation-complete` is
   **discarded** the moment the scope ends — same as `foreach` restoring payload after the loop
   (3a). **Fix:** set a **variable** inside each route (`aggResult`), then build the real response
   in a `Transform Message` placed **after** the aggregator scope closes, reading `vars.aggResult`.

2. **`payload` inside `incremental-aggregation` is the *cumulative array so far*, not the new
   element.** Confirmed via docs: *"the list of all the elements that have been aggregated... from
   the first element to the one that is currently aggregated."* Use `payload[-1]` for "the one that
   just arrived." Only `aggregation-complete`'s payload is guaranteed to be the final, full array.

3. **A completed group has a grace period, not an instant forget.** After `groupSize` is reached,
   the same `groupId` **rejects** new elements (`AGGREGATORS:GROUP_COMPLETED`) for `evictionTime`
   seconds — then the key is forgotten and a brand-new group silently starts. Verified this is a
   **minimum guarantee, not an exact deadline**: a request at ~25s after completion (with
   `evictionTime=30`) was already treated as evicted — eviction is checked by an internal periodic
   sweep, not a per-group millisecond timer.

4. **A timeout/period release is invisible unless a Listener is watching.** `aggregation-complete`
   **never** fires for a timer-driven release — confirmed for group-based *and* (via docs) for
   size-based: *"The Aggregation Complete Route does not execute if the timeout is reached."*
   Time-based has **no** complete route at all — timer release via the Listener is its *only* path.
   Proved this live with a debugger mid-session: pausing on a breakpoint let real wall-clock time
   pass, and the group timed out **in the background while nothing was calling it** — the timeout
   clock is driven by Mule's own scheduler, completely independent of any HTTP caller.

5. **Without a Listener, incomplete data is silently orphaned, not "lost" in an obvious way.** Each
   individual element still gets a normal `200 incremental` response, so the caller thinks
   everything's fine — but if the group never completes, nothing ever tells anyone. Real handling:
   persist the partial `payload` + `attributes.aggregationId`/`firstItemArrivalTime`/
   `lastItemArrivalTime` to a dead-letter/reconciliation store, alert, and decide a business policy
   (proceed as `PARTIAL` vs quarantine) — never just log it and move on.

6. **The Aggregator Listener starts a brand-new flow execution — `vars` don't carry over.**
   Confirmed in the debugger: `Variables = Map(0)`, completely empty, even though the original
   HTTP flow had none it needed to pass anyway. The only data available to the listener is what the
   module hands you: `payload` (the released elements) and `attributes`
   (`aggregationId`, `firstItemArrivalTime`, `lastItemArrivalTime`, `isAggregationComplete`).

7. **`isAggregationComplete` means "released because size was met," not "the data looks complete."**
   Proved this for time-based specifically: a legitimate, full 2-element batch released purely by
   `period` expiring still reported `complete=false`. The flag tells you *why* the release
   happened, never whether the payload is business-complete.

8. **Studio's own project model can get stuck re-emitting broken XML.** Typing an em dash (`—`) into
   a Logger/DataWeave field got serialized as the HTML entity `&mdash;` — invalid in plain XML (only
   `&lt; &gt; &amp; &apos; &quot;` and numeric refs are valid; `&mdash;` isn't defined). Fixing the
   file on disk wasn't enough — **Studio's in-memory model still held the broken value**, and any
   canvas interaction (like adding a flow) re-serialized the whole file from that stale model,
   silently undoing the fix. The permanent fix had to happen **inside Studio**: retype the character
   directly (not paste), save with Cmd+S so Studio's own model updates, and disable macOS's "smart
   dashes" text substitution to stop it recurring in new components.

9. **Missing/inconsistent route wrappers break canvas rendering with no clear error.** Studio
   generated `time-based-aggregator`'s children as *direct* children of the scope instead of
   wrapped in `<aggregators:incremental-aggregation>` (unlike the other two aggregators) — the file
   was still well-formed XML, but this structural inconsistency was the likely cause of a "Flow
   rendering failed" canvas error. Fix: wrap explicitly to match the documented schema and the
   sibling flows' pattern.

## What the logs actually looked like (verified, not assumed)

**Group-based full cycle:**
```
Incremental - 1 element(s) so far
Incremental - 2 element(s) so far
Complete - 3 element(s), totalQty=10
```
**Group-based timeout, caught only by the Listener** (~15s after the *first* element, zero HTTP calls involved):
```
Listener fired for order AGG-TIMEOUT-1 - complete=false, elements=2
```
**Debugger inspection of the Listener's event** — `Variables = Map(0)` (empty); `Attributes`:
`aggregationId=AGG-INSPECT-1`, `firstItemArrivalTime`/`lastItemArrivalTime` (epoch millis),
`isAggregationComplete=false`; `Payload` = the 2 partial elements verbatim.

**Size-based, no correlation — a totally unrelated new batch starts immediately after completion:**
```
SKU-A → incremental, elementsSoFar:1
SKU-B → incremental, elementsSoFar:2
SKU-C → complete, batch:[A,B,C]
SKU-D → incremental, elementsSoFar:1     ← NOT rejected, brand new auto-generated batch
SKU-E → incremental, elementsSoFar:2
```

**Time-based, timer measured from the *first* element:**
```
16:47:25.883  Time-based received - 1 element(s) buffered so far
16:47:32.202  Time-based received - 2 element(s) buffered so far
16:47:36.000  Time-based release! 2 element(s), complete=false     ← ~10.1s after element 1, not element 2
```

## Experiments to run
1. Send a 4th element to an already-completed group-based `groupId` **immediately** vs **after**
   `evictionTime` has passed — compare `AGGREGATORS:GROUP_COMPLETED` vs a silent fresh restart.
2. Drop `includeTimedOutGroups` to `false` on the group-based Listener and re-run the timeout test
   — confirm the Listener goes silent for that release.
3. Pause on a breakpoint inside `incremental-aggregation` for longer than `timeout` and watch the
   *next* call get rejected with "already timed out" — proof the clock runs independently of any
   caller, debugger or not.
4. Add an explicit `timeout` to the size-based aggregator (it defaults to `-1`, unlimited) plus its
   own Listener, and confirm leftover sub-`maxSize` elements are no longer stuck buffered forever.
