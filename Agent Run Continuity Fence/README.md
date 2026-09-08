# Agent Run Continuity Fence

A long running AI agent resumes after context compaction and quietly forgets the hard rules that kept the previous hour safe. This is a single file Java CLI that turns an agent transcript, memory file or JSONL trace into a verified continuity packet before the next step is allowed to run.

**Language:** Java | **Lines:** 795 | **Added:** 2026-08-14

## What this solves

Picture a run that started with a real constraint: use the GitHub MCP connector only, create exactly one file, do not touch local git. Forty thousand tokens later the context window compacts and the resume summary says something vague like "continue publishing the file". The agent, working from that summary, does the sensible looking thing and shells out to `git clone` and `git push`. The constraint is gone. You get a write path nobody sanctioned, possibly a token in shell history and a repo state that does not match what the automation thinks it produced.

The second failure mode looks like success. A connector throws a transport send error halfway through a write, the retry half completes and the final message says "pushed" without a readback ever happening. The failure and the completion claim sit forty lines apart in a trace nobody reads. You find out when the next run finds no file, two files or a commit on the wrong branch.

Whoever owns the pipeline notices late, and the debugging is archaeology: you scroll a huge trace hunting for the moment the rules went missing, and the trace is the thing that got compacted. So this reads the raw lines instead, extracts the facts that matter for a safe resume with line level provenance, fails the build when the evidence contradicts itself and hashes what it found into a short digest so you can tell whether the run you are resuming is the run you left.

## Why I built it

Log aggregators do full text search. They do not know that "MUST use MCP only" on line 3 and "git push origin main" on line 900 are a contradiction. Agent frameworks ship memory and summarisation, but the summary is written by the same model that just lost the context, so it is a claim, not evidence. Test suites assert on code, not on the record of a run.

What was missing is the boundary check: a small dependency free thing you drop between two halves of an autonomous run, or between a run and its CI gate, that reads the evidence and answers one question. Does the next step still know the branch, the filename, the commit, the blocker and the permitted write path. If not, exit nonzero and stop.

## When to use it

- Gating a resumed Codex or agent task after context compaction, before it writes.
- Enforcing an MCP only workflow, where any sign of local git or raw HTTPS writes should halt the run.
- CI step after an autonomous publish job, asserting the expected filename and commit SHA really appear in the run evidence.
- Post mortem on a run that reported success, checking whether completion evidence exists or only a transport failure and write attempts.
- Scanning agent memory files for leaked bearer tokens, API keys or `password =` lines before they get committed or pasted into a summary.
- Comparing two resumes of the same automation by digest to see whether the continuity facts drifted.

## How it works

Input goes through `readAll`, which accepts repeated `--input` paths or `-` for stdin and wraps every line in a `SourceLine` carrying source name, line number and raw text. Missing paths and non regular files throw `InputException` and exit 3, so a typo in a CI path never becomes a silent empty pass.

`analyze` walks each line once against nine precompiled regexes. `HARD_CONSTRAINT` catches must, required, never, do not, only, exactly, without asking, forbidden, stop and report. `DECISION` catches chose, selected, computed, slot, candidate, branch tip, latest commit. `PENDING`, `ERROR` and `COMPLETION` cover the rest, while `WRITE_ACTION` watches for `git clone`, `git push`, `git commit`, `github.com` URLs, `curl -` and MCP style `_create_file` / `_update_file` / `_delete_file` calls. `SECRET` looks for api key, authorization headers, long bearer values and password or token assignments. Three structural patterns run alongside them: `FILE_TOKEN` for artifact filenames across roughly forty extensions, `COMMIT_SHA` for 7 to 40 character hex runs and `ISO_INSTANT` for UTC timestamps, whose newest value is kept via `Instant.parse`.

Matches land in `EvidenceIndex`, a bucketed store rather than a copy of the log. Each bucket keeps at most `--show-lines` entries, default 10, so a huge trace still yields a small packet, and artifacts and commits use `putIfAbsent` so you get the earliest line where each filename or SHA appeared.

Then two issue passes run. `addExpectationIssues` checks the caller's assertions: every `--expect` phrase, the `--expected-file`, the `--expected-commit` and the `--automation-id`. `addContinuityIssues` checks the run against itself. Empty input is CRITICAL, secret shaped text is CRITICAL. The signature rule is the write path contradiction: `containsAllTerms` looks for a hard constraint line holding both "mcp" and "only", and if `containsAnyTerm` then finds git or GitHub HTTPS evidence in the write path bucket, that pair is CRITICAL too. A second cross check fires HIGH when a transport send error appears alongside write actions but zero completion lines, the unsafe replay case. Missing hard constraints is HIGH, pending work with no completion is WARN, a newest timestamp past `--max-age-minutes` is WARN, no timestamp at all is INFO. Everything matches on `normalize`d text, lowercased with collapsed whitespace, so casing and formatting noise do not defeat it.

`Severity` is a ranked enum from NONE through INFO, WARN, HIGH to CRITICAL, and `Severity.maxOf` gives the run one ceiling. `EvidenceIndex.canonicalFacts` serialises the buckets in sorted key order with normalised text, followed by sorted artifact and commit keys, and `digest` hashes that with SHA-256 truncated to 24 hex characters. Same facts, same digest. A different digest between two resumes means the continuity story changed.

`Renderer` emits markdown by default, `text` as flat key equals value lines for grep or `json` built by hand with a real `escapeJson` covering control characters. Issues sort by severity descending then title. Finally `run` compares `report.maxSeverity` against `--fail-on` and returns 1 when the gate trips, writing the reason to stderr while the packet stays on stdout.

## Usage

```bash
# Single file, no build step, Java 11 or later
java AgentRunContinuityFence.java --help

# Gate a resumed run in CI: fail on anything HIGH or worse
java AgentRunContinuityFence.java \
  --input run.log \
  --input agent-memory.md \
  --expect "MCP-only" \
  --expected-file AgentRunContinuityFence.java \
  --expected-commit 1234567890abcdef1234567890abcdef12345678 \
  --automation-id vibecodedaily \
  --max-age-minutes 90 \
  --fail-on high \
  --format markdown

# Pipe a JSONL trace, emit machine readable output
cat transcript.jsonl | java AgentRunContinuityFence.java --format json --show-lines 25

# Flat key=value output for grep or a shell gate
java AgentRunContinuityFence.java --input run.log --format text

# Built in regression checks over a known good and known bad sample
java AgentRunContinuityFence.java --self-test
```

## Notes

- Exit codes: 0 clean, 1 when `--fail-on` is reached or an unexpected exception occurs, 2 for usage errors, 3 for input errors such as a missing or non regular file. Default `--fail-on` is `none`, so it reports without failing until you ask it to.
- `FILE_TOKEN` requires the filename to start with an uppercase letter. `AgentRunContinuityFence.java` and `README.md` are captured, `main.py` and `index.ts` are not, and `--expected-file` is an exact key lookup so it inherits that.
- `COMMIT_SHA` matches any 7 to 40 character hex run, so hex looking words and long ids show up in the commits list. Treat it as candidates, not verified SHAs. `--expected-commit` is lowercased on both sides.
- The digest is computed over the capped buckets, so it depends on `--show-lines`. Compare digests across resumes only when both used the same value.
- `--expect` matches per line after normalisation. A required phrase split across two transcript lines will not match.
- Detection is regex and keyword based, not semantic. It catches the constraint and contradiction shapes that show up in real agent transcripts and will miss a rule none of the nine patterns cover. It does not parse JSONL structurally or follow references outside the supplied inputs.
- JDK only. No Maven, no Gradle, no third party dependency. Only the first `-` input reads stdin usefully, since the stream is closed after that pass.
