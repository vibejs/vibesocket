# Contributing

Use Node 24.15+ (24.x) or 26+, pnpm 12.14.0 and the committed lockfile. `pnpm install --frozen-lockfile` installs the repository without requiring sibling checkouts. vendor/ contains MIT development snapshots of three currently unpublished ecosystem packages; core dependencies are independent of them.

Run `pnpm verify`. It builds declarations/source maps, type checks TypeScript and real `.vibe` templates, lints, checks oxfmt including SFC blocks, runs unit/network/security/SSE/workerd tests, builds the site, runs Chromium/Firefox/WebKit and installs a packed consumer. Install local browser binaries with `pnpm exec playwright install` when necessary. workerd's postinstall is explicitly permitted in pnpm-workspace.yaml. Native compilation of Vibe.js `.vibe` files uses its pinned TypeScript 7.1 content mapper with --runExternalCode; package declaration builds use stable TypeScript 7.0.2.

`pnpm bench` targets only local loopback and records raw JSON in reports/. It needs a file-descriptor limit of 16384 for 5000 local sockets on both ends; the script sets that process limit. Never point load tests at external services without explicit authorization.

Keep browser/Node/Worker imports isolated. Add real transport tests for networking changes and scope cleanup tests for composables. Keep async operations bounded, retain safe errors and explain delivery stages precisely. Do not describe local runtime reconstruction as production Cloudflare hibernation validation.

Use Conventional Commits. Never add Co-Authored-By, Signed-off-by, Generated-by or AI attribution trailers. Do not publish, push, deploy, create remote repositories or access production services as part of development verification.
