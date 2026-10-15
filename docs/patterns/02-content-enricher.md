# Pattern 02 — Content Enricher

> Fetch extra data from other systems and **staple it onto** the message — without
> losing the original. In Mule 4 the implementation *is* the `target` parameter.
> Source: https://docs.mulesoft.com/mule-runtime/latest/target-variables

---

## The real-world scenario
A **skinny order** arrives with only IDs:
```json
{ "orderId": "ORD-1001", "customerId": "CUST-1", "sku": "SKU-9" }
```
Those IDs are useless until you resolve them against the systems that own the detail:
- **Customer** system → name & loyalty tier
- **Inventory** system → stock availability & warehouse
- **Shipping** system → method & ETA

Content Enricher calls each system, then merges the answers onto the original order to
produce a **fat, complete** order:
```json
{
  "orderId": "ORD-1001", "customerId": "CUST-1", "sku": "SKU-9",
  "customer":  { "customerName": "John Doe", "tier": "GOLD" },
  "inventory": { "available": true, "warehouse": "DAL-1" },
  "etaDays":   3
}
```

---

## The flow I built (`patterns-content-enricher-flow`)
```
Listener /patterns/content-enricher
  → Set Payload (skinny order)
  → flow-ref lookup-customer   target="customer"                       → vars.customer
  → flow-ref lookup-inventory  target="inventory"                      → vars.inventory
  → flow-ref lookup-shipping   target="eta" targetValue="payload.etaDays" → vars.eta (just 3)
  → Transform (merge):
        payload ++ {
          customer:  vars.customer,
          inventory: vars.inventory,
          etaDays:   vars.eta
        }
```
Each `sub-flow` (`lookup-customer`, …) **stands in for a real `http:request` / `db:select`**
to that backend system. A `sub-flow` has **no listener** — it can only be *called* via
`flow-ref`, runs inline on the caller's thread, and hands its result back.

---

## The key mechanism — `target` / `targetValue`
- **`target="customer"`** → run the component, store its **whole result** in `vars.customer`,
  and leave `payload` **exactly as it was**. This is *enricher semantics*: only the target
  variable is written; payload (and everything else) is preserved.
- **`targetValue="#[payload.etaDays]"`** → store only a **slice** of the result. Inside the
  expression `payload` = the component's own result, so `payload.etaDays` extracts just `3`.
  Without `targetValue`, the *whole* result is stored.

| | Stores | Result |
|---|---|---|
| `target="customer"` | whole payload of the call | `vars.customer = {customerName, tier}` |
| `target="eta" targetValue="#[payload.etaDays]"` | just the named slice | `vars.eta = 3` |

`target` works on **any** component — `flow-ref`, `http:request`, `db:select`, even
`scatter-gather` (as seen in Lab 1). It is the modern replacement for the deprecated
**Message Enricher** scope (`<enricher>`).

---

## Why this is the "Content Enricher" EIP
- Enterprise Integration Pattern: *augment a message with data from an external resource.*
- The "resource" here is 3 lookups; the "enricher" is the `target` parameter that keeps the
  in-flight message intact while data is gathered into variables, then merged at the end.

## Content Enricher vs Scatter-Gather
| | Content Enricher (`target`) | Scatter-Gather |
|---|---|---|
| Execution | **Sequential** lookups | All routes **parallel** |
| Latency | ≈ **sum** of calls | ≈ **slowest** call |
| Results land in | **named variables** (`vars.customer`) | one aggregate **by index** `{0,1,2}` |
| Read as | `vars.customer` (readable) | `payload[0].payload` (positional) |
| Call B can use call A's result? | ✅ yes (sequential) | ❌ no (independent routes) |
| Error model | fails **immediately** at the broken call | `MULE:COMPOSITE_ROUTING` after all settle |

**Decision rule:** independent calls + want speed → **Scatter-Gather**. A call depends on a
previous one, or you want clean named access → **Content Enricher**.

## Gotchas
- **DataWeave header is mandatory** in the Transform: `%dw 2.0` / `output application/json` /
  `---`. Omit it and the transform defaults to `application/java` → wrong Content-Type / ugly output.
- `targetValue` expression's `payload` = the **component's result**, not the flow payload.
- Sub-flows run **inline** (same thread, same transaction) — unlike a private `flow` called by
  `flow-ref`, which is still same-thread but a separate processing scope. Neither adds a listener.
- Enricher is **sequential** — if the three lookups are truly independent and latency matters,
  Scatter-Gather is faster.

---

## Experiments to run
1. Remove the DataWeave header from the Transform → see `application/java` output instead of JSON.
2. Drop `targetValue` on the shipping call → `etaDays` becomes the whole `{method, etaDays}` object.
3. Make a lookup **depend** on an earlier var (e.g. shipping uses `vars.customer.tier`) → proves
   the sequential advantage Scatter-Gather can't offer.
