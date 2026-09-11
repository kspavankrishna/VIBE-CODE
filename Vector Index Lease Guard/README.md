# Vector Index Lease Guard

A RAG index can pass every test in CI while it is built from stale source text, a revoked document, the wrong embedding model, another tenant's data or a region the data owner never approved. This is a single file Haskell CLI that treats each vector index as a time boxed lease, checks the whole manifest in one pass and fails the build with a non zero exit code before the index reaches production.

**Language:** Haskell | **Lines:** 977 | **Added:** 2026-07-21

## What this solves

The failure mode is quiet. Your embeddings job runs, writes vectors to Pinecone or pgvector or Qdrant, the smoke test retrieves something plausible and the deploy goes green. Nothing in that pipeline knows the source document was re-edited two weeks ago and the index still holds the old text, or that the embedding model was bumped to a new build so half the index sits in one vector space and half in another. Retrieval still returns neighbours. They are just the wrong ones. The symptom shows up later as a support ticket that reads like a hallucination, and you burn two days proving the model is fine and the index is rotten.

The second failure mode is legal, not technical. Someone exercises a deletion right, the source row leaves the primary store and the embeddings derived from it stay in the index and keep getting retrieved. A deleted document is still answering questions. Same class of problem: tenant bleed, data residency where an EU source got embedded into a US index, and licensing where scraped text with an unknown license ends up in a model facing corpus. None of these break a test. All of them get noticed by a customer, an auditor or a regulator first.

Third, and more mundane: chunking drift. Chunk size creeps past the reviewed maximum, overlap climbs to 40 percent, retrieval quality degrades and token spend climbs. Nobody reviews that because nobody owns the number.

The tool makes all of it a gate. One line per index describing what the index actually is, checked against the policy fields on the same line, non zero exit if anything fails. Errors block. Warnings block only when you ask them to. Output is text for a human, JSON for a script or SARIF for GitHub code scanning.

## Why I built it

Vector database vendors validate schemas and dimensions. They do not validate provenance. Nothing in the standard RAG stack answers the question "is the thing in this index still the thing it claims to be, and is it still allowed to be there". Policy as code tools like OPA can express these rules, but then you run a policy engine, write Rego and maintain a bundle pipeline to check two dozen fields on a text file.

I wanted the smallest thing that runs anywhere, has zero dependencies beyond base, is deterministic and reads like a checklist you could hand to a compliance reviewer. One Haskell file you run with `runghc` on a build agent. No package set, no lockfile, no container.

## When to use it

- A predeploy step in GitHub Actions or Buildkite, run after the embeddings job and before the index is promoted to the serving alias.
- A Kubernetes admission job that refuses to start a retrieval service whose index lease is expired.
- A nightly cron across every index manifest in the fleet, with `--fail-warn` so grace window warnings surface while there is time to purge.
- After a deletion request lands, to prove every index carrying that source is purged or still inside its documented revocation window.
- During an embedding model upgrade, to catch every index still holding vectors from the previous model digest.
- Showing an auditor which control blocked a non compliant index, and when.

## How it works

Input is one lease per line, space separated `key=value` tokens. `splitTokens` is a hand written state machine tracking quotes and backslash escapes, and `stripComment` runs the same quote aware scan so a `#` inside a quoted value survives. `normalizeKey` lowercases keys and strips every non alphanumeric character, so `allowed_tenants`, `allowedTenants` and `Allowed-Tenants` are one field. Values go through `canonical`: trim, lowercase, underscores and whitespace to hyphens. A bad line becomes a `parse_error` finding rather than killing the run.

`validateLease` runs thirteen independent check groups per lease and concatenates the findings. Duplicate fields after key normalization are an error, because an ambiguous policy is worse than a missing one, and `requiredFields` lists twenty four mandatory keys. `validateHash` enforces `sha256:` plus exactly 64 hex characters, then `matchFields` compares `sourceHash` against `indexedSourceHash` to catch a stale index and `modelDigest` against `expectedModelDigest` to catch model drift. `modelFindings` warns when `embeddingModel` carries no `@` version marker or `indexerVersion` no `@` or `:` build marker, since an unpinned name cannot be reproduced during an incident.

Dates never touch the system clock. There is no `getCurrentTime` in the file. `effectiveNow` reads `--now` or a per record `now=` field and emits `missing_now` if neither exists, because a gate whose verdict depends on when it ran is not a gate. `parseDay` checks the `YYYY-MM-DD` shape digit by digit and validates the calendar through `daysInMonth` and `isLeapYear` with the full 400 year Gregorian rule. `toOrdinal` gives a proleptic day number so window arithmetic is integer comparison. `dateFindings` flags creation in the future, an expired lease and expiry before creation.

Boundary checks are set membership over canonicalized CSV lists, shared by `memberFindings` for tenant and region and `licenseFindings` for licenses, all honouring `*` as an explicit wildcard. `privacyRank` maps class names onto an ordered scale, public and none at 0 through sensitive at 4, and errors when the class outranks `maxPrivacyClass`. `chunkFindings` checks chunk size is positive and under the limit and overlap is non negative and strictly smaller than the chunk, then computes overlap as an integer percentage with `div` and warns above `maxOverlapPct`. `retentionFindings` is the same pattern for days.

`deletionFindings` matters most. If `sourceDeletedAt` is present the deadline is that date plus `revocationGraceDays` in ordinal days. Past it you get a hard `deleted_source_indexed` error. Inside it you get a `revocation_grace_active` warning, so the clock stays visible in CI while there is time to act. `deletionRequested=true` with no `sourceDeletedAt` is its own error, and `duplicateIdFindings` catches repeated lease ids across the file.

Findings sort by line, then severity, then code, and render through `renderText`, `renderJson` or `renderSarif`. The JSON and SARIF writers are hand rolled with an `escapeJson` that emits `\uXXXX` for control characters, keeping dependencies at zero. `--self-test` runs the analyzer over a known good and a known bad sample at a fixed date and checks the expected codes appear.

## Usage

```bash
# check a manifest with a pinned evaluation date, human readable output
runghc VectorIndexLeaseGuard.hs --input vector-index-leases.txt --now 2026-07-21

# read from stdin, machine readable
cat leases.txt | runghc VectorIndexLeaseGuard.hs --now 2026-07-21 --format json

# GitHub code scanning
runghc VectorIndexLeaseGuard.hs --input leases.txt --now "$(date -u +%F)" \
  --format sarif > lease-guard.sarif

# treat warnings as blocking too
runghc VectorIndexLeaseGuard.hs --input=leases.txt --now=2026-07-21 --fail-warn

# built in regression check, no input needed
runghc VectorIndexLeaseGuard.hs --self-test

runghc VectorIndexLeaseGuard.hs --help
```

A single lease line:

```
id=kb-prod tenant=acme allowedTenants=acme,platform region=us-east-1 \
allowedRegions=us-east-1,us-west-2 \
sourceHash=sha256:aaaa...64hex indexedSourceHash=sha256:aaaa...64hex \
embeddingModel=text-embedding-3-large@2026-04-01 \
modelDigest=sha256:bbbb...64hex expectedModelDigest=sha256:bbbb...64hex \
createdAt=2026-04-01 expiresAt=2026-05-01 privacyClass=pii-minimized \
maxPrivacyClass=restricted retentionDays=30 maxRetentionDays=45 \
chunkTokens=820 maxChunkTokens=1200 overlapTokens=80 maxOverlapPct=20 \
license=internal-ai-ok allowedLicenses=internal-ai-ok,mit \
schemaVersion=2 indexerVersion=rag-indexer@2026.04.0
```

Optional keys: `now`, `sourceDeletedAt`, `deletionRequested`, `revocationGraceDays`. Compile with `ghc -O2 VectorIndexLeaseGuard.hs` for a binary instead of interpreting each run.

## Notes

- It validates the manifest, not the index. Nothing connects to a vector store or recomputes a hash. If your indexer writes a `sourceHash` that does not reflect the real source, the guard cannot tell.
- There is no clock. Without `--now` or a per record `now=` field every lease gets a `missing_now` error. That is the design, not an oversight.
- Exit codes are binary: 0 on pass, non zero on any error, or on any warning under `--fail-warn`. Bad arguments and an unreadable `--input` file also exit non zero, message on stderr.
- Integers must be plain base 10 digits, nothing longer than nine characters, and overlap percentage truncates. Membership, license and privacy checks stay silent when a field is absent, since the missing field check already reports it.
- SARIF output hard codes the artifact URI as `vector-index-leases.txt`. Rewrite that field before upload if your file is named otherwise.
- `privacyRank` only knows public, none, internal, pii-minimized, restricted and sensitive. Any other class is an error by design, so an unrecognised label cannot pass as safe. `schemaVersion` below 2 is likewise a hard failure.