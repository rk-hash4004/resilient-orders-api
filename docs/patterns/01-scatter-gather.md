# Pattern 01 — Scatter-Gather

> Parallel fan-out to independent routes, then aggregate **all** results.
> Source: https://docs.mulesoft.com/mule-runtime/latest/scatter-gather-concept

---

## The real-world scenario
A skinny order arrives with only IDs:
```json
{ "orderId": "ORD-1001", "customerId": "CUST-1", "sku": "SKU-9" }
```
It needs data from **3 independent backend systems**:
- **Customer** system → name & loyalty tier
- **Inventory** system → stock availability & warehouse
- **Shipping** system → method & ETA

Calling them one-by-one is slow. Scatter-Gather calls **all three in parallel**, then
**staples the answers together** into one enriched order. (Travel-agent analogy: ask the
flight, hotel, and car desks all at once, then combine.)

---

## The flow I built (`patterns-scatter-gather-flow`)
```
Listener /patterns/enrich
  → Set Payload (fake skinny order)
  → Set Variable  originalOrder = payload      (save copy; SG will clobber payload)
  → Scatter-Gather
        route 0: Set Payload → customer fragment
        route 1: Set Payload → inventory fragment
        route 2: Set Payload → shipping fragment
  → Transform (merge):
        vars.originalOrder ++ {
          customer:  payload[0].payload,
          inventory: payload[1].payload,
          shipping:  payload[2].payload
        }
```
Each `set-payload` **stands in for a real `http:request`** to that system.

---

## How it executes
- Each route runs **in parallel, on its own thread**; every route starts from a **copy of
  the same event**.
- The router **waits for all routes** to finish → total latency ≈ the **slowest** route.
- A failing route does **not** cancel its siblings — they all still complete.

## The aggregated output
An object keyed by route index; each value is a full **message** (payload + attributes):
```
{ 0: <msg>, 1: <msg>, 2: <msg> }
```
Order is **route-definition order**, not completion order. Ways to read it:
```dataweave
payload[0].payload                         // index access (labeled merge)
valuesOf(payload) map ($.payload)          // idiomatic → array of the 3 payloads
flatten(valuesOf(payload) map ((i) -> i.*payload))  // doc's generic list form
```
`.*payload` = multi-value selector (returns an array of all values under `payload`);
`flatten` collapses the array-of-arrays. Use the labeled `payload[n].payload` form when
you know the routes; use the list form when the count is dynamic.

## Variable propagation (subtle 🔑)
Vars set inside routes merge back after the router:
- set by **one** route → that value.
- **same** var set by **multiple** routes → a **list of all values** (NOT "last wins").

## Error handling
- Any unhandled route failure → **`MULE:COMPOSITE_ROUTING`** (thrown after all routes settle).
- Route exceeds `timeout` → **`MULE:TIMEOUT`**.
- The composite error carries both sides:
  - `error.errorMessage.payload.failures['n']` → that route's error
  - `error.errorMessage.payload.results['n']` → a successful route's result
- **Make a route optional:** wrap its body in `try` + `on-error-continue` returning a
  default. A self-handled route does **not** trip `COMPOSITE_ROUTING`.

## Config attributes
| Attribute | Meaning |
|-----------|---------|
| `timeout` | ms per route; `≤0` = no timeout → `MULE:TIMEOUT` on breach |
| `maxConcurrency` | cap parallel routes; `1` = sequential |
| `target` / `targetValue` | put the aggregated result in a **variable** instead of overwriting `payload` |

## Constraints & gotchas
- **Minimum 2 routes** (else the app errors).
- Requires **repeatable streams** (Mule default) so each route re-reads the payload.
- **Memory cost:** all results are held for aggregation — big payloads × many routes = heap pressure.
- **No short-circuit** — always runs every route (use First-Successful for failover).
- **Only for independent calls** — if route B needs route A's output, that's sequential enrichment.

## Transactions
- Scatter-Gather is **not** a transactional scope — it never begins/commits/rolls back a transaction.
- Transactions are **single-threaded**; parallel routes need multiple threads → the two conflict.
- If a transaction is already active, routes **join it and run sequentially** (parallelism lost).
- ❌ can't span one transaction across parallel routes; ❌ can't give each route its own transaction.
- For atomic multi-write → `try` with `transactionalAction`. For cross-system atomicity → Saga.

## Router comparison
| Router | Routes run | Returns | Use when |
|--------|-----------|---------|----------|
| **Scatter-Gather** | all, parallel | all, aggregated | independent calls, need every result |
| **Parallel For Each** | list items, parallel | the collection | same logic over a list |
| **Round Robin** | one (rotates) | that result | load-distribution |
| **First Successful** | sequential until success | first success | failover |
| **Async** | one, fire-and-forget | nothing | side tasks (audit/notify) |
| **Choice** | one, by condition | that branch | content-based routing |

---

## Experiments to run (harden it)
1. `raise-error` in one route → see `COMPOSITE_ROUTING`; then wrap in try/on-error-continue → succeeds.
2. Set `vars.hit` in all 3 routes → log after → see the **list**.
3. Add `target="enrichment"` → `payload` untouched, results in `vars.enrichment` (bridge to Content Enricher).
