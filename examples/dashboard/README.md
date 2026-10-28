# Live dashboard

Run `pnpm dev`, sign in and choose Dashboard. [Dashboard.vibe](../../site/components/Dashboard.vibe) uses native EventSource, typed named metrics, live counters, latency chart, event IDs, simulated notifications, pause/resume and VibeVirtual viewport rendering.

The producer is deterministic: requests = 1200 + tick*137 % 900, latency = 12 + tick*7 % 31, CPU = 18 + tick*3 % 50. Timestamps record the actual local sampling time. The UI retains 500 samples, renders only visible rows and charts the newest 40. Pausing skips samples explicitly and never accumulates an unlimited queue. It does not force the viewport to the bottom.

The latest 1000 SSE events are retained in SQLite and replayed after Last-Event-ID. Invalid/expired cursors produce an explicit gap signal. Each metric is a complete current sample, so a later sample restores the dashboard values even if history has expired. Previously displayed values remain usable while disconnected.
