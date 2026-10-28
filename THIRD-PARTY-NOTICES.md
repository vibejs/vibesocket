# Third-party notices

VibeSocket's own source is MIT licensed. Runtime networking uses ws (MIT) and Standard Schema interfaces (@standard-schema/spec, MIT). Optional client adapters use public @usevibejs/ui APIs.

site/components/ui and site/lib/utils.ts contain source primitives from Vibecn's MIT registry, adapted from shadcn/ui. Original component notices and the full license are retained in site/components/ui/vibecn.LICENSE. These files are development/showcase source and are excluded from the npm package.

vendor/ contains development snapshots of @usevibejs/dnd, @usevibejs/virtual and @usevibejs/i18n from the Vibe.js ecosystem, captured 2026-10-28. Each archive includes its package license and third-party notices. They are excluded from the npm package. Their SHA-256 hashes are recorded in vendor/README.md; the lockfile also records content integrity.

The development workbench uses Better Auth (MIT), VibeAuth (MIT), VibeQuery (MIT), Valibot (MIT), TanStack engines and the dependencies declared in their respective package manifests. These are not runtime imports of the headless VibeSocket core.
