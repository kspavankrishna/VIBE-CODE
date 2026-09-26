# Agent Spend Governor

An autonomous AI agent that holds a wallet and spends money on its own is a real thing now, not a thought experiment, and the failure mode is always the same one: a bad prompt, a bug in the agent loop or a compromised tool call turns into an unbounded stream of on chain payments before anyone notices. This is a single Solidity file that sits between an agent's private key and the funds it is allowed to touch, and enforces a rolling spend budget, a per call cap, an allowlist of exactly which contracts and functions the agent may call and a hard expiry, all on chain, all auditable, with no dependency on anything off chain to hold the line.

**Language:** Solidity | **Lines:** 372 | **Added:** 2026-09-26

## What this solves

Give an AI agent a hot wallet so it can pay for an API, settle an invoice, rebalance a position or trigger a keeper job, and you have handed a non deterministic process direct control of real money. The usual answer is to wrap the agent in off chain guardrails: a policy service the agent's backend is supposed to call before it signs anything, a spend log someone reviews the next morning, a Slack alert if a number looks large. None of that holds once the private key itself can sign a transaction, because the guardrail lives in a different trust boundary than the funds. If the backend has a bug, if the policy service is down, if someone bypasses it during an incident, the wallet still signs.

`AgentSpendGovernor` moves the boundary onto the chain itself. The agent's address never holds ether directly. It holds permission to call `execute` on this contract, which itself holds the treasury. Every one of the checks that would normally live in a fragile off chain policy layer, the daily budget, the per transaction ceiling, the list of contracts the agent is even allowed to touch, the policy's expiry date, lives in contract storage and runs before a single wei moves. There is no code path that lets the agent spend outside the policy, because the agent physically cannot call the target contract directly with the treasury's funds. It can only ask the governor to do it, and the governor either enforces the rule or reverts the whole transaction.

This is the same problem session keys solve for account abstraction wallets, generalized to plain externally owned accounts and framed specifically for the AI agent case, where the caller is not a human clicking approve but a loop that runs unattended for hours and where the operator's actual worry is "how much could this thing possibly spend before I notice," not "did the user mean to click this button."

## Why I built it

Every one of the agent cost and quota tools in this repository solves the token and dollar side of an AI agent's budget: rate limiting an LLM API, reconciling an invoice, capping a gateway's spend. None of them solve the case where the agent is not calling a metered API through your own backend, it is holding a private key and signing transactions that nobody in the loop can rate limit after the fact, because a signed transaction that lands on chain is final the moment it is mined. Once an agent has a wallet, every off chain protection your team built for its LLM usage stops applying to what it does with that wallet.

I wanted a contract, not a monitoring dashboard, because a dashboard tells you about the loss after it happened. I also wanted it to survive the two failure modes that make most on chain rate limiters either insecure or unusable: an unbounded loop that tries to walk every time unit since the agent last transacted, which becomes a gas denial of service the longer an agent sits idle, and a callee that returns an enormous amount of data purely to grief the caller's gas cost, the classic return bomb. Both are handled explicitly in `_recordSpend` and `_safeCall` rather than left as a known limitation, because a spend governor that itself can be knocked out by gas exhaustion is not a governor.

## When to use it

- An AI agent holds a hot wallet to pay per call fees to an inference provider, a data API or another agent, and you want a hard ceiling on what that wallet can lose to a runaway loop or a prompt injected instruction
- A trading or rebalancing agent needs to call a fixed set of DeFi contracts, a specific router or vault, and nothing else, ever, no matter what the agent's own logic decides to construct
- An operations team wants a kill switch that stops one misbehaving agent without touching any other agent sharing the same treasury contract
- A budget needs to reset itself automatically every day or every hour without a human re-arming it, and needs that reset to be gas cheap even after the agent goes quiet for a month
- Compliance or finance wants an on chain, tamper evident record of every payment an autonomous process made, with the target, the function it called and the running budget it consumed, rather than trusting an application log

## How it works

The contract has one owner, the human or multisig that funds it and configures agents, and any number of agent addresses, each with its own `Policy` struct: `epochBudgetWei`, the most an agent can spend across its rolling window, `maxPerTxWei`, the most it can spend in one call, `epochLength`, how many seconds one budget bucket covers, `expiresAt`, the timestamp the policy stops working entirely, and `active` and `paused` flags. The owner sets this with `setPolicy`, clears it with `revokePolicy`, and flips `paused` per agent with `setAgentPaused` or for every agent at once with `setGlobalPaused`, an incident response switch that does not touch any policy's numbers.

An agent is only allowed to call specific destinations. `setTargetAllowed(agent, target, selector, allowed)` records permission for one exact function on one contract, keyed by `keccak256(target, selector)` inside `allowedTargetSelector`. `allowAnySelector` is a convenience wrapper that grants every function on a target using the reserved `ANY_SELECTOR` value `0xffffffff`, for cases like a router contract where the owner is comfortable trusting every entry point rather than naming each one.

The agent itself calls `execute(target, value, data)`. The function checks, in order, that the policy is active, that neither `globalPaused` nor the agent's own `paused` flag is set, that `expiresAt` has not passed, that `value` is within `maxPerTxWei`, and that the target and selector pulled from `data` by `_selectorOf` are on the allowlist, either exactly or through the wildcard. Only after every check passes does it call `_recordSpend` to book the spend against the rolling window, and only after that succeeds does it forward the call through `_safeCall`.

The rolling window itself is a fixed size ring buffer, `BUCKET_COUNT` slots, currently 24, held per agent in a `SpendWindow` struct alongside `windowTotal`, a running sum kept incrementally rather than recomputed from the buckets on every call. `epochLength` seconds times `BUCKET_COUNT` buckets gives the window's real duration, so an `epochLength` of one hour gives a 24 hour rolling budget. When `execute` runs, it computes the current bucket as `block.timestamp / epochLength` and hands it to `_recordSpend`, which advances the window from wherever it last stood. If the gap since the last call is smaller than `BUCKET_COUNT`, it walks forward only the buckets that actually went stale, subtracting each one from `windowTotal` before zeroing it. If the gap is `BUCKET_COUNT` or larger, meaning every bucket is stale regardless of exactly how much time passed, it takes a fast path and clears all of them in one bounded pass instead of looping once per elapsed epoch. This is the detail that keeps the contract safe to leave alone: an agent that goes quiet for an hour and one that goes quiet for a year cost the exact same gas to wake back up, because the loop is bounded by `BUCKET_COUNT`, never by elapsed time.

Reconfiguring a policy through `setPolicy` deletes that agent's `SpendWindow` outright. Changing `epochLength` changes what a bucket index even means, so the safest and simplest rule is that editing a policy always starts its budget window fresh, documented directly on the function rather than left as a surprise.

The actual external call goes through `_safeCall`, hand written in assembly rather than a plain high level call, specifically so it can bound how much of the callee's returndata gets copied into memory. A normal `target.call(data)` lets the compiler copy the callee's entire return output before your own code runs at all, which is the return bomb: a malicious or simply buggy contract returns megabytes of data purely to force an expensive memory expansion on the caller. `_safeCall` copies calldata into memory itself, issues the call, then copies at most `MAX_RETURN_COPY`, 256 bytes, of whatever came back, no matter how much the callee actually returned. If the call fails, `execute` reverts with `CallFailed` carrying that bounded return data, which is normally enough to see the callee's revert reason without exposing the contract to an unbounded copy.

Ownership uses a two step handoff, `transferOwnership` followed by `acceptOwnership` from the new address, so a typo in the new owner's address cannot permanently lock the treasury the way a single step transfer can. `withdraw` is owner only and is not constrained by any agent's policy, since the policies exist to bound what agents can do with the owner's funds, not the owner's own access to them.

Every state change emits an event: `PolicySet`, `PolicyRevoked`, `AgentPaused`, `GlobalPausedSet`, `TargetAllowed`, `Executed` with the target, selector, value, epoch index and window total after the call, `Deposited`, `Withdrawn`, and the two ownership transfer events. `Executed` alone is enough to reconstruct a full audit trail of everything an agent has ever spent and against what, without touching an off chain log.

## Usage

Deploy with the treasury owner's address:

```solidity
AgentSpendGovernor governor = new AgentSpendGovernor(ownerAddress);
```

Fund it by sending ether to the contract address, which the `receive` function accepts and logs as `Deposited`. Configure an agent with a one ether per day budget, a tenth of an ether per call cap, hourly buckets and a thirty day expiry:

```solidity
governor.setPolicy(
    agentAddress,
    1 ether,          // epochBudgetWei
    0.1 ether,        // maxPerTxWei
    1 hours,          // epochLength
    uint64(block.timestamp + 30 days) // expiresAt
);

governor.setTargetAllowed(agentAddress, priceOracle, IPriceOracle.settle.selector, true);
governor.allowAnySelector(agentAddress, dexRouter, true);
```

From the agent's own address, spend within policy:

```solidity
governor.execute(priceOracle, 0, abi.encodeWithSelector(IPriceOracle.settle.selector, requestId));
governor.execute(dexRouter, 0.05 ether, abi.encodeWithSignature("swapExactETHForTokens(uint256,address[],address,uint256)", ...));
```

Check standing before or after a call:

```solidity
uint256 left = governor.remainingBudget(agentAddress);
bool canCallThis = governor.isAllowed(agentAddress, dexRouter, bytes4(0x7ff36ab5));
```

Pause one agent during an incident, or every agent at once, without touching any policy numbers:

```solidity
governor.setAgentPaused(agentAddress, true);
governor.setGlobalPaused(true);
```

## Notes

- The contract has no imports and no inheritance, so an auditor reading it never has to chase behavior into a library. Every check `execute` performs is visible in one function body.
- `execute` reverts the entire transaction, including any state the target contract changed, if any policy check fails or the target itself reverts. There is no partial spend.
- The `nonReentrant` guard on `execute` is defense in depth. The actual double spend protection comes from recording the budget update before the external call runs, so even a reentrant call from inside the target sees the already updated window and is checked against it honestly.
- A single global reentrancy lock covers every agent, so one agent's `execute` call cannot be reentered by a second agent's `execute` call inside the same transaction. This is a deliberately conservative default for a contract whose entire job is bounding worst case loss, not a performance optimization target.
- `currentWindowSpend` reflects the state as of the last `execute` call for that agent. If an agent has been idle, the true spend inside the current window may be lower than this view reports, since the stale buckets only get cleared on the agent's next `execute` call, not by the passage of time alone.
- This contract has been compiled clean against solc 0.8.26 with the optimizer enabled. It has not been deployed to a live network or formally audited, and anyone putting real treasury funds behind it should get an independent audit first, the same as any contract that custodies money.
