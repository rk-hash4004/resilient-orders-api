# Pattern 04 — Recipient List

> Determine, **at runtime, from the message's own content**, which of a set of possible
> recipients should receive it, then dispatch a copy to each. Not a fixed set of routes
> (that's Scatter-Gather) — the set itself is computed per message.
> Source: https://www.enterpriseintegrationpatterns.com/patterns/messaging/RecipientList.html
> · https://docs.mulesoft.com/mule-runtime/latest/parallel-foreach-scope

---

## The real-world scenario
An order arrives carrying a `category`:
```json
{ "orderId": "ORD-2001", "category": "ELECTRONICS", "items": [{ "sku": "SKU-9", "qty": 2 }] }
```
Depending on category, a **different, dynamically-decided set** of downstream systems needs
notifying — not always the same recipients, not a fixed count. An `ELECTRONICS` order needs
inventory + fraud-check + a specific warehouse; a `PERISHABLE` order needs inventory +
cold-chain-warehouse + expedited-shipping. The recipient *set itself* is the variable, computed
from message content — that's what makes this Recipient List and not Scatter-Gather.

---

## The flow I built (`patterns-recipient-list-flow`, `/patterns/recipient-list`)
```
Listener /patterns/recipient-list
  → Set Payload (order with category)
  → Set Variable originalOrder = payload        (save before the loop overwrites payload)
  → Set Variable recipients =                   (the dynamic part — computed from category)
        if (category == "ELECTRONICS")
          [{name:"inventory-system", url:"https://httpbin.org/post"},
           {name:"fraud-check",       url:"https://httpbin.org/post"},
           {name:"warehouse-east",    url:"https://httpbin.org/status/500"}]
        else [{name:"inventory-system", url:"https://httpbin.org/post"}]
  → Parallel For Each (collection = vars.recipients):
        Set Variable currentRecipientName = payload.name    (capture before it's overwritten)
        http:request  url=#[payload.url]  config-ref=Generic_HTTP_Request_config (no fixed host)
        Logger "Notified {currentRecipientName} -> {statusCode}"
  → Transform: { orderId, recipientsNotified: vars.recipients, count }
  → error-handler:
        on-error-continue type=MULE:COMPOSITE_ROUTING
          Transform: { succeeded: error.errorMessage.payload.results,
                        failed:   error.errorMessage.payload.failures }
```
`warehouse-east` deliberately points at `httpbin.org/status/500` — a real, live failure, not a
simulated one — specifically to exercise the partial-failure path for real.

---

## Recipient List vs Scatter-Gather (the distinction that actually matters)
| | Scatter-Gather (Pattern 1) | Recipient List (Pattern 4) |
|---|---|---|
| Route set | **Fixed**, hardcoded in the flow — always the same N routes | **Computed at runtime** — could be 0, 1, or N recipients, different per message |
| What each route does | Usually **different logic** per route (call customer, inventory, shipping) | **Same logic** repeated per recipient (notify X) |
| Always runs all of them? | Yes, every time | Only whichever the data/rules select |

In production, the `if/else` computing `recipients` is usually a **DB lookup or rules-engine
call** (a routing-rules table keyed by category/region) — the inline DataWeave here is a
simplified stand-in, same convention as Batch's generated test data.

---

## The dynamic-URL mechanism (how one config calls N different targets)
`http:request-config` normally fixes a host/port that the operation's `path` gets appended to.
Here the config has **no host set at all** — when the operation's own **`url`** attribute is
supplied, it's treated as the complete absolute URL (scheme + host + port + path), bypassing the
config's host/port entirely. That's the documented, supported mechanism for exactly this
"different target per call" case:
```xml
<http:request-config name="Generic_HTTP_Request_config"/>
<http:request method="POST" url="#[payload.url]" config-ref="Generic_HTTP_Request_config">
  <http:body><![CDATA[#[vars.originalOrder]]]></http:body>
</http:request>
```
Since `parallel-foreach` runs one route per collection element with `payload` swapped to that
element, the **same** `url="#[payload.url]"` expression resolves to a **different** literal URL
in each concurrent route — one processor, N genuinely different outbound calls, verified live:
route 0 and 1 both hit `httpbin.org/post` and got distinct 200 responses echoing the order back;
route 2 hit `httpbin.org/status/500` and failed for real.

---

## Gotchas
- **`vars` carry into every `parallel-foreach` route; `payload` does not.** Unlike the
  Aggregators module (which starts a *brand-new* flow execution with empty vars), `parallel-foreach`
  copies the whole event per route — so `vars.originalOrder` is identical and available in all
  routes, while `payload` becomes that route's current collection element.
- **`http:request` overwrites `payload` with the response.** If you need to know *which*
  recipient a given response belongs to after the call, capture what you need into a **variable**
  (`currentRecipientName`) *before* the request runs — `payload.name` after the request tries to
  read a field off the HTTP response body, not your recipient object, and silently returns `null`
  instead of erroring.
- **Non-2xx responses are connector-level errors by default.** `warehouse-east`'s `/status/500`
  call throws `HTTP:INTERNAL_SERVER_ERROR` right at the `http:request` step — the Logger *after*
  it never runs for that route at all. That's why only 2 of 3 "Notified..." log lines appear.
- **`error-handler` is a sibling of `parallel-foreach`, not nested inside it.** `MULE:COMPOSITE_ROUTING`
  is only catchable at the *enclosing flow's* error-handler — there's no error-handler slot on the
  scope itself (unlike `try`).
- **`error.errorMessage.payload.results`/`.failures`** are the built-in granular per-route outcome
  arrays, indexed by **original collection position** (`"0"`, `"1"`, `"2"`...), with `null` in
  whichever slot doesn't apply — verified live: `succeeded["0"]`/`["1"]` populated with real HTTP
  response objects, `failed["2"]` populated with the real 500 error detail, matching original order
  regardless of which call actually returned first.

---

## Experiments to run
1. Change `category` to `"PERISHABLE"` and redeploy — confirm the recipient *set itself* changes
   (different names, different count), not just that individual calls behave differently.
2. Point all three recipients at `httpbin.org/post` (remove the deliberate failure) — confirm
   `failed` is empty and the `on-error-continue` branch never fires at all (no error to catch).
3. Add a `maxConcurrency` attribute to `parallel-foreach` and lower it to `1` — observe the calls
   become sequential instead of concurrent (useful when a real partner can't handle a fan-out burst).
4. Swap the inline `if/else` for a `flow-ref` to a lookup sub-flow returning the same shape —
   the closer-to-production version, where recipients come from a routing-rules source, not
   hardcoded logic.
