# Security and threat model

This is an engineering threat model, not a claim of independent security auditing. Policies are application supplied and fail closed. The development fixture uses real VibeAuth/Better Auth cookie sessions, trusted SQLite membership and role checks, and loopback-only listeners.

## Boundaries and mitigations

| Threat                                  | Implemented boundary                                                                       | Executable evidence                                                               |
| --------------------------------------- | ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- |
| Cross-site WebSocket hijacking          | Required exact Origin allowlist before Node/Worker upgrade; explicit missing-Origin opt-in | network hostile-Origin test; workerd hostile-Origin test                          |
| Forged identity                         | Handshake session callback binds principal; handler ignores payload user IDs               | real VibeAuth integration identity-forgery test                                   |
| Unauthorized room/tenant                | Required join authorization and fresh recipient authorization on every publish             | real sessions cross-tenant test; browser Eve test                                 |
| Privileged mutation                     | Operation-specific server policy and trusted membership service                            | real role-denied task:create test                                                 |
| Expired/stale session                   | Principal expiration, validatePrincipal callback, revoke index, renewed handshake          | missing/expired session and real signOut revocation tests                         |
| Oversized/deep/malformed frames         | ws maxPayload plus UTF-8 envelope/depth/field checks                                       | actual ws oversized/version tests; protocol unit cases                            |
| Message/subscription exhaustion         | Inbox/rate/room limits; bounded pending client requests                                    | room/flood security test; slow-consumer tests                                     |
| Memory exhaustion                       | Bounded offline queue, outgoing buffers, SSE byte streams, Worker credit window            | queue, backpressure, stream cancellation and workerd tests                        |
| Replay abuse                            | Cursor length/format/history checks; bounded history and live buffer; per-recipient checks | SSE retained/gap tests; example metrics cursor policy                             |
| Private outbound leaks after revocation | validatePrincipal before each recipient; expiry; room removal on denial                    | actual session revocation and SSE permission-change tests                         |
| Token leakage                           | No token URLs, no default payload logging; attachments explicitly project principal fields | authenticator retains IDs/expiry only; package examples avoid token query strings |
| Error/injection disclosure              | Safe error codes, no stack traces, schema validation; `.vibe` renders text                 | handler-error test; actual forged-payload test                                    |
| Cross-app lifecycle leaks               | App-scoped injection and client ownership; scope-owned subscriptions                       | scope disposal tests; browser app-isolation tests                                 |

## Session design

Use `createSessionAuthenticator(source, projectPrincipal)` with VibeAuth's verified public Better Auth session source. It calls `api.getSession({ headers, query: { disableCookieCache: true, disableRefresh: true } })`, checks expiry and overwrites identity/expiry with trusted session values. It retains neither the cookie nor session token. Return only needed application metadata. The handshake callback is also a documented integration point for custom authentication; it is not a second session system.

Room names are public routing identifiers. Knowing `organization:456` never establishes membership. Resolve membership/roles from trusted application services. Recheck on join, privileged events, each private receive, organization switches and reconnect. `validatePrincipal` detects session revocation; `revoke` provides immediate push invalidation. In a distributed deployment, propagate revocation across owners and use an external limiter; process-local maps are not global enforcement.

Browser WebSocket does not offer custom Authorization headers. Native EventSource does not either. Same-origin HttpOnly cookie sessions are the demonstrated approach. A consumer may instead use a short-lived, single-use, audience-scoped ticket fetched through authenticated HTTPS; implement its verification/atomic consumption in the auth service. VibeSocket does not mint tickets or invent cryptography. Never place long-lived access tokens or reusable secrets in URLs. Reverse proxies, access logs and diagnostics can expose query strings.

Origin is meaningful for honest browsers enforcing the Web platform; arbitrary network clients can spoof Origin. Session authentication and authorization remain mandatory. HTTP CORS settings do not protect native WebSocket upgrades. HTTP mutation routes still need CSRF/Origin policy independently of sockets. Production must use HTTPS/WSS and secure cookie settings.

## Limits and slow consumers

Limits apply before dispatch. The Node adapter disables permessage-deflate to avoid compression overhead and amplification. Worker credit receipts bound bytes awaiting client protocol processing even without bufferedAmount; a receipt is not trusted proof of a business effect. A compromised recipient can always read/ack its own data, so authorization must prevent giving it private data in the first place.

Limit external database/authorization costs as well. The package cannot cancel arbitrary promises; handlers and session services should support cancellation and operation deadlines. Do not keep an unbounded per-user or source-key map in external callbacks. Node's built-in attempt index is bounded and intentionally per process.

The localhost demo has known seeded accounts, non-secure development cookies and a higher sign-in allowance (100/minute for repeated local browser tests). It binds only to loopback. These settings are development fixture configuration, not recommended production authentication policy. Better Auth's Origin/CSRF checks remain enabled.

## Remaining deployment responsibilities

TLS termination, trusted proxy configuration, production session settings, distributed quotas/revocation, database access control, secret management, outbox/idempotency implementation and operational alerting belong to the application. A handshake session check alone cannot provide indefinite authorization. Durable Object auto-heartbeats do not wake the object to revalidate sessions; private deliveries and operations do revalidate. Use alarms/leases if you require an idle expired physical socket to close at a precise deadline.
