# Agent Approval Queue

An approval queue for AI agent tool calls that a human has to sign off before they run. It is a pure Elm state machine, so the tricky rules (two person approval, argument binding, expiry, reviewer fatigue) are written once and tested without a browser.

**Language:** Elm | **Lines:** 2264 | **Added:** 2026-10-27

## What this solves

Every agent framework now has a "ask the human first" step for risky tool calls. Most of them implement it as a modal with an Approve button and a boolean. That works in a demo and falls apart in a real team, for reasons that have nothing to do with the button:

- The agent resubmits the same call id with different arguments after you approved the first version.
- A reviewer clicks Approve on a row they never opened, because forty rows arrived at once.
- The "second reviewer" on a dangerous call is the person who asked for it.
- The approval lands after the call already timed out, and something runs anyway.
- The page reloads and every pending approval silently disappears, or worse, comes back already approved.
- An agent in a retry loop floods the queue with the same call until nobody can find the real ones.
- A rule that blocks `rm -rf` is dodged by writing `rm -rf` in the JSON arguments.

AgentApprovalQueue handles all of these as explicit, named outcomes. Each one is a `Refusal` or a `DenyReason` you can show in a UI and write to an audit log, not a silent failure.

## Why I built it

I kept seeing the same bug in agent tooling: the approval is bound to a call id, not to what was shown to the human. The reviewer approves "run this query", the agent swaps the arguments and the id still matches. Once I started fixing that I found the rest of the list above, and each fix touched the same few pieces of state. That is a state machine problem, so I wrote it as one.

I picked Elm because the approval screen is exactly where you do not want runtime surprises. The module has no IO and no clock. Every message carries `now` in epoch milliseconds, the queue returns a list of `Effect` values, and the host does the real work. That makes the whole thing replayable. Feed it the same messages and you get the same decisions, which is what you want from something that decides whether an agent may touch production.

## When to use it

- You run an agent that calls tools with side effects (shell, database writes, deploys, payments, sending email) and want a person in the loop.
- You have an internal tool, an admin console or a review dashboard written in Elm and need the approval logic behind it.
- You want a reference design for approval rules in any language. The module reads like a spec, and porting it is mostly mechanical.
- You need an audit trail that shows who approved what, when, and under which rule.

It is not a UI. There is no Html in it on purpose. It is also not an authentication system. The identifiers it receives (`Reviewer`, `AgentId`) must come from your session layer, never from the agent.

## How it works

**Fingerprint and canonical arguments.** `canonicalJson` decodes any JSON value and renders it again with sorted object keys and no whitespace. Because the text is rebuilt from the decoded value, escaped and unescaped spellings of the same string come out identical. `submissionDecoder` runs this for you on the `arguments` field. `fingerprint` takes the tool name and canonical text and returns 16 hex characters from two independent FNV 1a passes. That fingerprint is only an index and a display key. Every check that matters compares the full canonical string, so a crafted hash collision cannot carry an approval from one call to another.

**Risk classification.** `classify` computes the effective `Risk`: `Auto`, `Review`, `Dual` or `Deny`. A `Rule` matches a tool name exactly or by a trailing `*` prefix, and the longest match wins, so `db.drop*` can sit above `db.*`. A `Trigger` is a case insensitive substring of the canonical arguments and can only raise risk, never lower it. A tool with no rule gets `defaultRisk`, which defaults to `Review`. Fail closed is the point: a new tool nobody classified does not run on its own.

**Submit.** `Submit` goes through a fixed order. Oversized arguments (`maxArgsChars`) are denied with `TooLarge`. A known id with identical content is a no op if it is still waiting, and a repeated answer (never a second `Dispatch`) if it already settled. A known id with different content is treated as tampering: the pending call is denied with `ArgsChanged` and the submission gets `IdReuse`. Then `Deny` risk is settled at once, `Auto` is approved with `ByPolicy` and `Review` calls first look for a live grant. A call identical to one already waiting is merged into it, so reviewers see one row, and each merged id is dispatched on approval. Finally `maxWaiting` and `maxWaitingPerAgent` cap the queue, and anything over them is denied as `Overloaded`.

**Review.** `View` records that a reviewer opened a call. `Approve` then runs `checkApprover`: the reviewer must be in `reviewers` (an empty list means anyone, fine for development only), must not be the `requestedBy` person or the agent, must not have approved already, must have viewed the call for at least `minReviewMs` and must not be over the fatigue limit (`fatigueMax` approvals inside `fatigueWindowMs`). A `Review` call needs one approval. A `Dual` call needs two distinct reviewers, and the first one produces a `NeedsReview` effect again so the second reviewer is pinged. Any single `Reject` vetoes, including on a `Dual` call.

**Grants.** `ApproveOptions` can carry `grantMs`. That records a grant for the exact tool and exact canonical arguments, for this agent only, capped by `maxGrantMs` and `maxGrantUses`. The next identical `Review` call is approved with `ByGrant`. Grants are refused on `Dual` calls (`GrantNotAllowed`), expire on `Tick`, and can be dropped with `RevokeGrants`.

**Batch.** `ApproveBatch` approves many `Review` calls in one click, up to `maxBatch`. It refuses any `Dual` item with `BatchNotAllowed` and still applies the view and dwell rules per item, so Select All cannot become a rubber stamp.

**Time.** Nothing reads a clock. `Tick` expires calls past their deadline (`ttlReviewMs` or `ttlDualMs`) and prunes grants and fatigue records. An approval that arrives after the deadline settles the call as `Expired` first, so a late click cannot beat a missed `Tick`. The queue also emits `ScheduleTick` whenever the earliest deadline changes, with `Nothing` when the queue is empty.

**Effects.** `update` returns `Dispatch` (run the tool), `Respond` (answer the agent with a `Verdict`), `NeedsReview`, `Refused`, `Audit` and `ScheduleTick`. Audit entries are numbered inside `finish`, so no step can skip the sequence, and in each step the audit line comes before `Dispatch`.

**Persistence.** `encodeSnapshot` and `snapshotDecoder` round trip waiting items (with views and approvals), grants, settled records and the audit counter. Original deadlines are kept, so the first `Tick` after a reload expires whatever aged out while the page was closed. Settled records are included on purpose: without them a retried id could look new and run twice.

**Queries.** `lookup` returns `StillWaiting`, `Decided` or `Unknown`. `waitingItems` is ordered by soonest expiry. `stats` and `grantCount` feed a header bar.

## Usage

The entry points are `init` and `update`. A minimal host wires them to ports.

```elm
import AgentApprovalQueue as Q exposing (Effect(..), Msg(..))

config : Q.Config
config =
    { cfg
        | reviewers = [ "alice", "bob", "carol" ]
        , rules =
            [ { tool = "read.*", risk = Q.Auto }
            , { tool = "shell.*", risk = Q.Dual }
            ]
    }

cfg : Q.Config
cfg =
    Q.defaultConfig

-- model.queue : Q.Model, created with Q.init config

-- an agent call arrives from a port as JSON
case Decode.decodeValue Q.submissionDecoder raw of
    Ok submission ->
        let
            ( queue, effects ) =
                Q.update (Submit nowMs submission) model.queue
        in
        ( { model | queue = queue }, runEffects effects )

    Err _ ->
        ( model, Cmd.none )

-- a reviewer opens a row, then later clicks approve
Q.update (View nowMs "call-17" "alice") queue
Q.update (Approve nowMs "call-17" "alice" { note = "checked the diff", grantMs = Nothing }) queue

-- timer fired by ScheduleTick
Q.update (Tick nowMs) queue
```

`runEffects` is yours. A typical version sends `Dispatch` to the tool runner, `Respond` back to the agent over its channel, `Audit` to your log store, and `ScheduleTick (Just t)` to a `setTimeout` through a port. `Refused` carries a `Refusal` to show next to the button.

To check the module yourself, run the scenario file (20 checks, no test package needed):

```
elm make AgentApprovalQueueSelfCheck.elm --output=check.js
node -e 'const {Elm}=require("./check.js");Elm.AgentApprovalQueueSelfCheck.init().ports.report.subscribe(console.log)'
```

## Notes

- The only dependencies are `elm/core` and `elm/json`. `elm.json` is included.
- The identity of reviewers is your job. The queue trusts the strings you pass it. If the agent can choose the `Reviewer` value on an `Approve` message, none of the self approval rules mean anything.
- Triggers are substring matches on lower cased canonical text. They do not catch lookalike unicode characters or arguments split across fields. Treat them as a safety net under proper tool rules, not a filter.
- Merged calls are approved together and dispatched once per call id. If your tool is not safe to run twice with the same arguments, give it its own idempotency key.
- `Refused` effects are counted in `stats` but do not change a call. A refused approve leaves the call exactly where it was.
- Settled records are capped by `maxSettled`. A retry of an id older than that window is treated as new, so size it above your longest agent retry horizon.
- `fatigueMax` and `minReviewMs` are speed bumps, not proof that a person read anything. They make rubber stamping slower and visible in the audit log.
- Grants are held in memory and in the snapshot. If you do not want them to survive a reload, drop them from the decoded model by calling `RevokeGrants` right after restore.
- The self check file is a port module, so it only builds as a worker. It is not part of the library surface.
