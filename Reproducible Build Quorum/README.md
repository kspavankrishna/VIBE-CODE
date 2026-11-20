# Reproducible Build Quorum

A signed release only proves that one build machine produced it. This Solidity contract makes a release count as verified only when several independent rebuilders compile the same source commit, get the same SHA256 digest and say so on chain, with equivocating builders ejected automatically.

**Language:** Solidity | **Lines:** 718 | **Added:** 2026-11-20

## What this solves

Most software supply chain checks answer the question "was this artifact signed by the right key?" Sigstore, cosign, GPG and SLSA provenance all do that well. None of them answer a different question that matters more after every CI compromise we have seen: "would anyone else building this exact commit get the exact same bytes?" If the one runner that produced your release was tampered with, its signature is perfectly valid. The signature proves who built it. It does not prove the build was honest.

Reproducible builds are the real answer. Debian, Arch, NixOS, Bitcoin Core, Tor and a growing list of language ecosystems already rebuild releases on independent machines and compare digests. What is missing in most setups is the agreement step itself. Rebuilders publish hashes to a web page, a mailing list or a JSON file in a Git repository. Then a person eyeballs them. There is no rule that says how many must agree, no record of who disagreed, nothing stopping a rebuilder from quietly changing its answer later and no machine readable gate that a deploy pipeline, a package manager or a wallet can query.

`ReproducibleBuildQuorum` is that agreement step as a contract. A maintainer opens a release for a project, a version and a source commit. Each registered rebuilder builds that commit on its own infrastructure and submits the digest it got, either directly or as an EIP-712 signature that anyone can relay. The release becomes `Attested` only when enough eligible rebuilders agree on one digest and dissent stays within a configured tolerance. Any consumer can then call `verify` or `verifyRelease` and get a yes or no that no single machine, single key or single maintainer could have forged.

## Why I built it

I kept seeing the same gap. Teams adopt artifact signing, feel safe, then a build runner gets owned and the signed malicious release goes out with a green checkmark. The fix people reach for is "rebuild it somewhere else and compare", which is correct, but the comparison ends up living in a shell script on one more machine that can also be compromised.

Putting the quorum on a public chain gives three properties that are hard to get any other way. The record is append only and timestamped by someone other than you. The rules (threshold, tolerance, who counts as a rebuilder) are enforced by code everyone can read, not by a config file on a server. A rebuilder's word is binding: once it signs a digest for a round it cannot take it back. If it ever signs two different digests for the same round, anyone holding both signatures can prove it and get that rebuilder removed. That last part is what turns a list of hashes into something you can actually trust.

I also wanted the version that survives real operations, not a demo. Membership changes during an open vote, a rebuilder key gets stolen, a threshold gets lowered by a compromised admin, a tag gets moved to a different commit, an old signature gets replayed into a new round, a smart contract wallet is one of the rebuilders. Each of those has an explicit answer below.

## When to use it

Use it when you ship artifacts whose integrity matters more than the convenience of a single build: CLI binaries, firmware images for IoT fleets, container images for regulated environments, wallet software, browser extensions, model weights you package and serve, compiler toolchains or anything that ends up on many machines you do not control.

It fits teams that already have, or can get, at least two independent build environments: GitHub Actions plus a self hosted runner, a cloud CI plus a volunteer rebuilder, two companies in a consortium rebuilding each other's releases. The digest can be the SHA256 of a single file or, for multi platform releases, the SHA256 of your checksum manifest (the `SHA256SUMS` file) so one vote covers every artifact.

It is not the right tool if your build is not reproducible yet. Fix determinism first (pinned toolchains, `SOURCE_DATE_EPOCH`, sorted archives, stripped build paths). Until then every release will land in `Diverged`, which is the contract telling you the truth.

## How it works

**Roles.** The `owner` is meant to be a multisig or governance contract. It manages maintainers and rebuilders, with a two step handover through `transferOwnership` and `acceptOwnership`. Maintainers, set with `setMaintainer`, can open releases and yank them. Rebuilders, at most `MAX_REBUILDERS` (32), cast votes. Nobody else can do anything except relay signed votes, report equivocation and call `finalize`, all of which are permissionless on purpose.

**Asymmetric governance.** Changes that make the system stricter take effect immediately: `raiseThreshold`, `lowerDissentTolerance`, `removeRebuilder` and `ejectRebuilder`. Changes that make it weaker go through a timelock of `configDelay` seconds (never less than `MIN_CONFIG_DELAY`, one day): `queueAddRebuilder`, `queueLowerThreshold` and `queueRaiseDissentTolerance`. Queued operations are run with `executeOp` after their `eta` and expire after `OP_GRACE_PERIOD` (14 days) so a forgotten operation cannot be fired months later. `executeOp` validates the change again against current state, not the state at queue time. The point is simple: a stolen owner key cannot add three fake rebuilders and attest a backdoored binary in one block. Everyone watching `OpQueued` gets a full day to react.

**Config invariant.** `_checkConfig` requires `threshold >= 1`, `threshold <= active rebuilders` and `dissentTolerance < threshold`. The last rule guarantees that two different digests can never both reach quorum without also tripping the divergence rule, so the contract can never attest two answers for one release.

**Opening a release.** `openRelease(project, version, sourceCommit, window)` derives the id with `releaseKey`, which hashes the project and version separately before hashing them together, so `("ab", "c")` and `("a", "bc")` never collide. It snapshots the current `membershipEpoch`, threshold and tolerance into the `Release` struct and sets a deadline between `MIN_VOTING_WINDOW` (one hour) and `MAX_VOTING_WINDOW` (30 days). It refuses to open if there are fewer active rebuilders than the threshold (`QuorumUnreachable`). A release that ended `Expired` or `Diverged` can be reopened as a new round, but only with the same source commit. Trying to point an existing version at a different commit reverts with `SourceCommitMismatch`. A version is bound to one commit forever, which closes the classic moved tag attack.

**Membership epochs.** Every rebuilder added through the timelock bumps `membershipEpoch` and records it as `addedEpoch`. A rebuilder may vote on a release only if it was added at or before that release's snapshot epoch, is still active and has never been ejected (`_eligible`). So a member added while a release is in flight cannot influence it. A member removed while a release is in flight stops counting immediately, even for votes it already cast, because eligibility is checked again at tally time.

**Voting.** `vote` takes a digest from `msg.sender`. `submitSignedVote` and `submitSignedVotes` take a `SignedVote` struct whose signature covers `VOTE_TYPEHASH`: release id, round, source commit and digest. The round in the signature stops replay of an old round's vote into a reopened round (`StaleRound`). The source commit in the signature means a rebuilder signs "I built this commit and got these bytes", not just "I like this hash". `hashVote` exposes the exact EIP-712 digest so rebuilder tooling can check it before signing. Submitting the same vote twice is a no op that returns `false`, which keeps relayers that retry safe.

**Signatures.** `_isValidSignature` tries plain ECDSA first, accepting 65 byte signatures and 64 byte EIP-2098 compact ones, rejecting high `s` values and bad `v` values in `_recover`. Only if that fails and the signer has code does it fall back to ERC-1271 `isValidSignature`, so a rebuilder can be a Safe or other contract wallet. Trying ECDSA first matters after EIP-7702: an ordinary account with a delegation has code on chain but still signs with its own key. `domainSeparator` is cached and rebuilt if the chain id changes, so votes cannot be replayed across a fork.

**Equivocation.** Votes are final. If a rebuilder submits a second, different digest for the same round, `_recordVote` emits `Equivocation`, ejects the rebuilder through `_eject` and returns without reverting, so the ejection actually persists. If two conflicting signatures surface off chain (say, one sent to a relayer and another posted elsewhere), anyone can call `reportEquivocation` with both and the same thing happens. Ejection is permanent: an ejected address can never be queued again. The evidence hash goes into `RebuilderEjected` for auditors.

**Tallying and deciding.** `_tally` walks the voters for the current round, skips anyone no longer eligible and groups the rest by digest. `_decide` then applies the stricter of the release snapshot and the live config: the higher threshold and the lower tolerance. Loosening config never affects a vote already running. Tightening does. The decision is:

1. If dissent (votes not for the leading digest) exceeds tolerance, the result is `Diverged`.
2. If the leading digest has at least `threshold` votes and the remaining unvoted eligible rebuilders could not push dissent over tolerance even if they all disagreed, the result is `Attested`, even before the deadline.
3. If the deadline has passed with no quorum, or quorum is now mathematically impossible, the result is `Expired`.
4. Otherwise the release stays `Open` and `finalize` reverts with `UndecidedRelease`.

With the default tolerance of zero, an early attestation therefore needs every eligible rebuilder to agree. After the deadline, quorum with zero dissent is enough. `previewTally` returns the same decision plus the per digest breakdown without writing anything, which is what a CI dashboard should poll.

**Yanking.** `yank` moves an `Attested` release to `Yanked`, after which `verify` returns false. It is terminal: you publish a new version instead of reattesting an old one, because consumers may have cached the old answer.

## Usage

Deploy with an owner multisig, the maintainer accounts, the initial rebuilders, a threshold, a dissent tolerance and a config delay. Three rebuilders, two to agree, zero tolerated dissent and a two day timelock looks like this with Foundry:

```bash
forge create ReproducibleBuildQuorum.sol:ReproducibleBuildQuorum \
  --rpc-url "$RPC_URL" --account deployer \
  --constructor-args "$OWNER_SAFE" "[$MAINTAINER]" "[$REBUILDER_A,$REBUILDER_B,$REBUILDER_C]" 2 0 172800
```

A maintainer opens the release when the tag is cut, binding the version to its commit. A Git SHA1 commit is 20 bytes, so pad it to 32:

```bash
COMMIT=$(git rev-parse v1.4.0)
cast send "$QUORUM" "openRelease(string,string,bytes32,uint64)" \
  "acme-cli" "1.4.0" "0x$(printf '%-64s' "$COMMIT" | tr ' ' 0)" 86400 \
  --rpc-url "$RPC_URL" --account maintainer
```

Each rebuilder checks out that commit, builds it in its own environment and votes with the digest of the checksum manifest:

```bash
RELEASE_ID=$(cast call "$QUORUM" "releaseKey(string,string)(bytes32)" "acme-cli" "1.4.0" --rpc-url "$RPC_URL")
DIGEST="0x$(sha256sum dist/SHA256SUMS | cut -d' ' -f1)"
cast send "$QUORUM" "vote(bytes32,bytes32)" "$RELEASE_ID" "$DIGEST" --rpc-url "$RPC_URL" --account rebuilder
```

A rebuilder without gas signs the typed data off chain and hands it to any relayer, which calls `submitSignedVote` or batches many through `submitSignedVotes`:

```javascript
const domain = { name: "ReproducibleBuildQuorum", version: "1", chainId, verifyingContract: quorumAddress };
const types = { BuildVote: [
  { name: "releaseId", type: "bytes32" }, { name: "round", type: "uint32" },
  { name: "sourceCommit", type: "bytes32" }, { name: "digest", type: "bytes32" } ] };
const signature = await rebuilder.signTypedData(domain, types, { releaseId, round, sourceCommit, digest });
await quorum.submitSignedVote({ releaseId, round, digest, signer: rebuilder.address, signature });
```

Anyone settles the release once the outcome is decided. Any consumer then gates on the result:

```bash
cast send "$QUORUM" "finalize(bytes32)" "$RELEASE_ID" --rpc-url "$RPC_URL" --account any
cast call "$QUORUM" "verifyRelease(string,string,bytes32)(bool)" "acme-cli" "1.4.0" "$DIGEST" --rpc-url "$RPC_URL"
```

A deploy step or installer that refuses to proceed when that call prints `false` is the whole integration.

## Notes

I tested this file against a local EVM covering the paths I care about most: unanimous attestation, relayed votes in both 65 and 64 byte form, idempotent duplicates, high `s` signature rejection, wrong signer rejection, stale round replay, divergence followed by a reopen on the same commit, a reopen attempt on a different commit, direct and reported equivocation with ejection, a rebuilder added after the snapshot being refused, deadline expiry, yanking, timelock readiness and staleness and early attestation with a nonzero tolerance. It compiles without warnings and deploys at about 16 KB, under the 24 KB contract size limit. It has not had a third party audit. Get one before it guards anything valuable.

Gas is bounded by design. Every loop runs over at most `MAX_REBUILDERS` addresses. The tally uses a small quadratic grouping that stays cheap at that size. If you need more than 32 rebuilders, shard by project rather than raising the cap.

Pick the threshold based on independence, not count. Three rebuilders on the same cloud account with the same base image are one rebuilder. Use different providers, different operators and ideally different operating system images.

A `Diverged` result is not always an attack. It is most often a build that is not fully deterministic. The events record which rebuilders reported which digest, so you can diff their outputs, fix the source of nondeterminism, eject a misbehaving builder if needed and reopen the round on the same commit.

Ejecting a rebuilder does not retroactively undo an attestation that already finalized. If you learn a rebuilder was compromised, eject it and yank any release where its vote was decisive.

The contract holds no funds and makes no external calls other than the read only ERC-1271 check, so there is no reentrancy surface. Keywords for anyone searching: reproducible builds, build attestation, supply chain security, release verification, SLSA, Sigstore companion, on chain quorum, EIP-712 signed votes, ERC-1271, rebuilder consensus, Solidity.
