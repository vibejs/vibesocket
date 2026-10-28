# Collaborative Kanban

Run `pnpm dev` and choose Kanban in two browsers signed in as Ada/Lin. Ada can create tasks; members can move existing tasks. Eve's organization has an independent board. Use drag/drop or the accessible Move action. Empty columns remain drop targets.

[Kanban.vibe](../../site/components/Kanban.vibe), [Task.vibe](../../site/components/Task.vibe) and [Column.vibe](../../site/components/Column.vibe) use the public VibeDND API. A drag-end creates one operation; pointer movements are not network events. The shared backend owns SQLite task order and revision checks. Accepted moves commit before broadcast. Conflicting expectedRevision values produce CONFLICT and trigger an authoritative snapshot refresh. Presence is approximate and aggregated by identity.

This demonstrates server-authoritative application reconciliation, not CRDT semantics or generalized VibeSync collaboration.
