# Verification log

2026-10-28: the first vertical slice passed in Chromium using two independent browser contexts, actual compiled `.vibe` components, Vibe.js 0.1.0, ws 8.22.0 and a real Node HTTP listener. The server validated the client event, published to an authorized room, and the second browser rendered it without refreshing. This preceded reliability/scaling features.

The Vibe.js source investigation found no public Node upgrade hook. Ordinary HTTP uses `toNodeHandler(app.fetch)` on an externally owned listener with the ws adapter. No Vibe.js core source was changed. The showcase also owns the Vite UI listener via public middleware mode, so its bootstrap can finish stream/database cleanup before process exit.

The final `pnpm verify` run exited 0: package build/declarations/source maps, strict TypeScript and real `.vibe` template checks, oxlint, oxfmt including SFCs, **55 Vitest tests in 6 files**, documentation/showcase build, **27 Playwright tests across Chromium/Firefox/WebKit**, strict packed-consumer imports/types, actual packed WebSocket RPC, browser isolation and tree shaking.

The pointer-drag test additionally passed 3 consecutive repetitions per browser (9 targeted runs). It observes activation/target state instead of relying on arbitrary delays. Browser fixtures use fresh temporary SQLite/session stores; zero fixture directories remained after graceful shutdown. The ordinary development database stays persistent in `.data/`.

Workerd verifies actual Worker upgrades, authentication/Origin denial, multiple clients, authorized fanout, receipt framing/overflow, automatic pong and live attachment reconstruction. Actual production Cloudflare hibernation/eviction remains a separate deployment check.

`reports/benchmark.json` records local 100/1000/5000 connection scenarios, CPU/RSS/latency/throughput, room fanout/churn, a 100-client reconnect storm and a real paused slow consumer. The [engineering report](report.md) records the complete configuration, values, guarantees and limitations. No universal capacity or exactly-once delivery claim is made.
