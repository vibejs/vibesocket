# Verification log

2026-10-28: first vertical slice passed in Chromium using two independent browser contexts, actual compiled `.vibe` components, Vibe.js 0.1.0, ws 8.22.0 and a real Node HTTP listener. The server validated the client event, published to an authorized room, and the second browser rendered the event without refreshing. This was verified before adding reliability and scaling features.

The Vibe.js source investigation found no public Node upgrade hook. Ordinary HTTP uses `toNodeHandler(app.fetch)` on the same externally owned listener as the ws adapter. No upstream files were changed.
