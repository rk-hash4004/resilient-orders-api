# Pattern 06 — Claim Check

> Store a bulky or sensitive payload once, pass around a lightweight reference (the "claim
> check") through the rest of the flow, and only retrieve the full data when something actually
> needs it. Named directly after a physical coat-check/luggage-claim ticket.
> Source: https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html

---

## The real-world scenario
An order arrives with a large attachment (simulated as a 5000-character blob). Most of the
pipeline downstream only needs the order metadata, not the full attachment — so the attachment
gets checked into Object Store, and only a reference travels with the message from here on.
```json
{ "orderId": "ORD-4001", "attachmentData": "XXXX...XXXX" }
```

## Why it exists (real-world uses)
- **Insurance claims processing** (the pattern's namesake) — a scanned document is stored, only
  a reference flows through the approval workflow.
- **Kafka/event-streaming size limits** — publish a thin event with a pointer instead of the
  full payload ("thin events, fat data").
- **PII/PCI data minimization** — offload sensitive data to a vault, pass a non-sensitive token
  through orchestration layers that don't need the raw data.
- **Messaging platform payload caps** — SQS (256KB), Anypoint MQ, Salesforce Platform Events all
  have size limits; store externally, pass a pointer through the constrained channel.
- **B2B/EDI large document exchange** — directly relevant to this project's own stack (the
  `pom.xml` already has the X12 EDI connector) — a large trading-partner document is a textbook
  claim-check case.

---

## The flows I built

### Check-in — `patterns-claim-check-flow` (`/patterns/claim-check`)
```
Listener /patterns/claim-check
  → Set Payload (order + simulated large attachmentData)
  → Set Variable claimCheckId = uuid()
  → os:store  key=#[vars.claimCheckId]  objectStore=Object_store
        (os:value left empty — defaults to the current payload, which is exactly what we want stored)
  → Transform: { orderId, claimCheckId, message: "Attachment stored — use claim check to retrieve" }
```

### Redeem — `patterns-claim-check-redeem-flow` (`/patterns/claim-check/{claimCheckId}`)
```
Listener /patterns/claim-check/{claimCheckId}     — {claimCheckId} is a URI param
  → os:retrieve  key=#[attributes.uriParams.claimCheckId]  objectStore=Object_store  target=fullOrder
  → Set Payload: #[vars.fullOrder]
```

## Verified live
Stored a 5000-character attachment, got back `claimCheckId: aecede1e-7a70-4b59-adf4-b93dca0bc12c`,
redeemed it via `GET /patterns/claim-check/aecede1e-...` — response was the **exact original
order**, full `attachmentData` included, confirming the round-trip through Object Store.

---

## Gotchas

1. **`os:store` and `os:retrieve` must reference the *same* `objectStore` by name.** If store
   doesn't explicitly declare `objectStore`, it silently uses an implicit default — which is
   almost certainly a *different* store than the one an explicitly-named `os:retrieve` looks in.
   Result: retrieve fails with "key not found" even though store genuinely succeeded — the two
   sides were never talking to the same bucket. Always name the object store explicitly on both
   sides.
2. **`key` (and every dynamic attribute) needs `#[...]`** — `key="vars.claimCheckId"` (no
   brackets) stores everything under the **literal string** `"vars.claimCheckId"`, not the
   actual UUID value. Every call silently overwrites the same key instead of getting a unique
   one. Same root-cause pattern hit repeatedly across this session's patterns (5's `message`/
   `regex`, this one's `key` and `set-payload value`).
3. **`os:value` left empty defaults to `payload`** — not a bug, a convenience. Since the current
   payload at that point in the flow *is* what should be stored, no explicit `<os:value>` child
   was needed at all.
4. **DataWeave's `*` is numeric multiplication only** — `"X" * 5000` throws *"You called the
   function '*' with these arguments: String, Number... expects Number, Number"*. There's no
   Python-style string-repetition operator. Use `(1 to 5000) map "X" joinBy ""` instead — build
   an array of the repeated element, then join it into one string.
5. **A URI parameter (`{claimCheckId}` in the listener path) is read via
   `attributes.uriParams.claimCheckId`**, not `vars` or `payload` — easy to reach for the wrong
   one since so much of this lab's other patterns pull data from `payload`/`vars`.

---

## Experiments to run
1. Point `os:store` and `os:retrieve` at two different named object stores deliberately — watch
   the "key not found" failure mode described in Gotcha 1 happen on purpose.
2. Add a TTL/expiration to the object store config and redeem after it expires — observe how a
   claim check "goes stale," a real production concern for this pattern.
3. Swap Object Store for a real S3 config (the project already has the S3 connector as a
   dependency) — closer to how this pattern is usually implemented in production, where the
   claim-checked data often needs to survive app restarts/redeploys.
