# Realtime chat

Run `pnpm dev` at the repository root, open http://127.0.0.1:5173 and choose Chat. Sign in as Ada in one independent browser and Lin in another. Eve is in another organization and cannot subscribe to their rooms.

The runnable `.vibe` UI is [Chat.vibe](../../site/components/Chat.vibe). It has two rooms, SQLite message history, acknowledged persistence, bounded messages, unread counts, presence, throttled/expiring typing, timestamps, connection state, draft retention on error and reconnect history recovery. The real session/room/handler implementation is [backend.ts](../backend.ts); the common contract is [contract.ts](../contract.ts).

History responses have a 48KiB JSON budget and at most 200 messages. Older messages remain in SQLite; this example does not include older-history pagination. UI cache ownership stays with VibeQuery. The server commits before publish but does not have an outbox, so history recovery remains necessary after a crash or reconnect gap.

See the multi-context browser test for actual no-refresh delivery and history restoration after reload.
