# Pattern 05 — Message Filter

> Drop messages that don't match a criterion — let matching ones through, silently exclude the
> rest. Different from a **router**: a router sends non-matching messages down an *alternate*
> path; a filter just **excludes** them from the flow entirely.
> Source: https://www.enterpriseintegrationpatterns.com/patterns/messaging/Message_Filter.html
> · Validation module: https://docs.mulesoft.com/validation-connector/latest/validation-examples

---

## The real-world scenario
Orders arrive from multiple channels, but this flow only processes orders above a minimum
value — anything below the threshold should be filtered out, not routed elsewhere, not treated
as a system error.
```json
{ "orderId": "ORD-3001", "total": 8 }
```
Below the `$10` minimum → filtered. At or above it → passes through.

---

## The flow I built (`patterns-message-filter-flow`, `/patterns/message-filter`)
```
Listener /patterns/message-filter
  → Set Payload (order with orderId + total)
  → validation:All
        is-not-null(orderId)     — "orderId is required"
        matches-regex(orderId, "ORD-\d+")  — "orderId must match ORD-<digits>"
        is-true(total >= 10)     — "total below minimum"
  → error-handler:
        on-error-continue (broad — catches whatever the Validation module raises)
          Logger: error.errorType.namespace + ':' + error.errorType.identifier + ' | ' + error.description
          Transform: { status: "filtered", reason: error.description }
```
Implemented as **validate-and-catch**, not choice-and-branch: the filter condition is expressed
as a Validation-module check, and a *failed* check is treated as the expected "this message
doesn't pass the filter" outcome — caught by an error handler and turned into a clean `filtered`
response, not a 500. This is a legitimate, common way to implement Message Filter in Mule 4;
the alternative (`<choice>` + manual `raise-error`) works too but requires hand-rolling both
branches yourself.

## Verified live
```json
{ "status": "filtered", "reason": "orderId is required\norderId must match ORD-<digits>\ntotal below minimum" }
```
All three checks failed on payload `{ orderId: null, total: -5 }` (well, the tested case used
`total: 8`/`total: -5` variants — see Gotcha 5), and `validation:All` aggregated **all three**
failure messages into one response, not just the first one hit.

---

## Gotchas — every one of these came from a real build/deploy failure this session

1. **Adding a module can collide with existing app conventions.** This app's *main* flow
   (`orders/orders-api.xml`) already used a hand-rolled `raise-error type="VALIDATION:INVALID_INPUT"`
   for a pricing check — a plain custom error type, not from any module. The moment the real
   **Validation module** got added as a dependency, it claimed the `VALIDATION` namespace for
   its own built-in error types, and the build failed: *"Cannot use error type
   'VALIDATION:INVALID_INPUT': namespace already exists."* Fix: renamed the main app's error to
   `ORDER:PRICING_VALIDATION_FAILED` — which, as a bonus, fit the app's own existing `ORDER:`
   namespace convention better than the original did. **Lesson: check for namespace collisions
   before adding a module, not after a broken build tells you.**

2. **`message` (and `regex`) attributes don't auto-evaluate DataWeave — they need `#[...]`.**
   Without it, `error.description` returned the *literal, unevaluated expression text*
   (`"'Order below minimum value: ' ++ (payload.total as String)"`) instead of a computed
   string. Same root cause hit twice — once on `is-true`'s `message`, again on
   `matches-regex`'s `regex`.

3. **`error.errorType` is an object, not a string.** `(error.errorType as String)` throws
   *"Cannot coerce Object to String"* — `ErrorType` has `.namespace` and `.identifier` fields;
   you must concatenate those explicitly (`error.errorType.namespace ++ ':' ++
   error.errorType.identifier`) to get a printable type like `VALIDATION:MULTIPLE`.

4. **A regex attribute value literally containing quote characters is a silent trap.**
   `regex="#['&quot;ORD-\d+&quot;']"` evaluates to the literal text `"ORD-\d+"` — quote marks
   included as part of the pattern — which will never match a real order ID. The fix is just
   removing the stray embedded quotes from the value content itself, regardless of whether the
   attribute is `#[...]`-wrapped or not.

5. **`validation:All` aggregates every failure into one `VALIDATION:MULTIPLE` error** — verified
   live: three failing checks produced one `error.description` listing all three messages,
   newline-joined, not just the first check that failed. Conceptually identical to
   `parallel-foreach`'s `MULE:COMPOSITE_ROUTING` — "let everything run/check, then report the
   combined outcome" — just applied to validators instead of routes. Individual validators (not
   `All`) additionally support a `validation:exception` child element to customize the raised
   error type — `All` explicitly does **not** support this customization.

6. **An unrelated deploy blocker surfaced mid-session: "Connection is not secure. Should use HTTPS."**
   The main app's listener (`host="0.0.0.0"`, plain HTTP — unchanged all session) and a
   dynamic-URL `http:request-config` (Pattern 4, no protocol declared) both started failing
   deployment with this error. Root cause not fully confirmed (likely a stricter validator tied
   to a Mule runtime version change made earlier this session), but the fix was correct either
   way: bind the local listener to `host="localhost"` instead of `0.0.0.0` (never needed
   external network exposure locally), and declare `protocol="HTTPS"` on the *connection* child
   element — **not** as a direct attribute on `http:request-config` itself:
   ```xml
   <http:request-config name="Generic_HTTP_Request_config">
     <http:request-connection protocol="HTTPS"/>
   </http:request-config>
   ```
   (`protocol` directly on `http:request-config` fails schema validation — it belongs on the
   nested connection element, same pattern as `http:listener-connection` carrying `host`/`port`
   for the listener side.)

---

## Experiments to run
1. Add `validation:exception` to an individual (non-`All`) validator and observe the custom
   error type it raises instead of the module's default — open question from this build, still
   to be verified live.
2. Swap `validation:All` for `validation:Any` (if the module has an "at least one must pass"
   scope) and compare the error/success semantics.
3. Replace the Validation-module implementation with a plain `<choice>` + `raise-error
   type="APP:FILTERED_OUT"` version, side by side — confirm both achieve the same EIP outcome
   through different mechanics.
