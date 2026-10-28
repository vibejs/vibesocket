# SSE streaming

Run `pnpm dev` and choose Streaming. [Streaming.vibe](../../site/components/Streaming.vibe) displays named token/done events from a real Fetch ReadableStream through Vibe.js. Start and Cancel control the native EventSource; cancellation propagates to stream cleanup and stops the server producer timer.

The local fixed sentence simulates a text response. It is intentionally a one-shot stream without replay, not an AI protocol SDK. The Dashboard tab demonstrates retained event IDs, browser reconnect and SQLite replay; test/sse.test.ts additionally verifies multiline framing, retry hints, keepalives, cancellation, concurrent subscribers and history gaps.
