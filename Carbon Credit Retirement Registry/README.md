# Carbon Credit Retirement Registry

A single Solidity contract that issues carbon credit batches with gap free serial numbers and retires them into permanent receipts that name the exact serials burned. A serial can be retired once, and anyone can look up which receipt consumed it.

**Language:** Solidity | **Lines:** 445 | **Added:** 2026-10-23

## What this solves

Most onchain carbon credit tokens treat a credit as a plain fungible token. You burn some amount and an event says so. That is not what a buyer needs when they make a claim. Registries such as Verra and Gold Standard identify every tonne by a serial number inside a project and a vintage year. A claim of "we retired 500 tonnes from project X vintage 2024" only holds if you can show which 500 serials were used and prove that nobody else used them.

Plain ERC-20 and ERC-1155 designs lose that. Two things go wrong in practice. First, the token amount and the serial range drift apart, so an auditor cannot tie a burn to a block of serials. Second, the same serial range gets issued twice, either through a bug or through a second issuer reusing a project id, and that is exactly the double counting problem the voluntary carbon market is trying to get rid of.

This contract keeps serial ranges and token balances locked together. Each project has one serial cursor. Each issuance takes the next block from that cursor, so two batches of the same project can never overlap. Each retirement takes the next block of unretired serials from its batch and records it in a receipt. Retired units cannot be transferred, minted again or reversed.

If you search for a carbon credit retirement smart contract, a carbon credit registry in Solidity, onchain carbon retirement receipts, double counting prevention for tokenized carbon credits, or an EIP-712 gasless retirement flow, this is the problem space.

## Why I built it

I work around carbon credit projects and the same question comes up on every deal: can you prove this tonne was retired once and only once. Spreadsheets and registry PDFs answer it slowly. Tokenized credits should answer it instantly, but most of the contracts I looked at only emit a burn event and leave the serial bookkeeping to an offchain database. That moves the trust problem somewhere else without solving it.

I wanted the serial accounting to live in the same state as the balances, so that a single invariant holds at all times: for every batch, the sum of all holder balances plus the retired count equals the issued quantity. I also wanted the contract to be small enough to read in one sitting. It has no imports, no proxy and no upgrade hooks. If you need those you can wrap it, but the core accounting stays easy to audit.

The other thing I wanted was a retirement flow that works for a company that does not hold ETH. A corporate buyer signs a retirement order, a relayer submits it and the receipt still names the buyer as the holder. That is what `retireBySig` does.

## When to use it

Use it when:

- You are building a tokenized carbon market and need retirements that map to real serial ranges.
- You want a reference for double count prevention that does not rely on an offchain database.
- You need a retirement receipt that a third party can verify with one view call.
- You want gasless retirement for buyers through EIP-712 signatures, including smart contract wallets through ERC-1271.
- You need to attach a later Article 6 or CORSIA corresponding adjustment reference to a retirement without altering the original receipt.

Do not use it as is when:

- You need fractional tonnes. Units here are whole tonnes of CO2e, stored as integers.
- You need to bridge to another registry. This contract is the retirement record, it does not talk to Verra or Gold Standard. You would run an issuer service that mirrors registry state into `issue`.
- You need to claw back retired credits. The design makes retirement final on purpose. Reversals of buffer pool credits for forestry projects are a registry matter and should be handled by issuing a replacement batch, not by undoing a receipt.

## How it works

**Batches and serials.** `issue(projectId, vintage, quantity, methodologyHash, recipient)` is callable only by an address marked in `isIssuer`. The first issuer to use a `projectId` becomes `projectIssuer[projectId]` and nobody else can issue into that project afterwards, which stops a second issuer from taking over the serial cursor. The serial range comes from `nextSerial[projectId]`. A batch gets `serialStart` equal to the cursor and `serialEnd` equal to the cursor plus quantity, and the cursor moves to `serialEnd`. The range is half open, so a batch of 1000 starting at 0 covers serials 0 to 999. `issue` checks that the total fits in `MAX_SERIAL` and reverts with `SerialSpaceExhausted` otherwise. Vintages must sit between `MIN_VINTAGE` and `MAX_VINTAGE`. The methodology hash is stored so you can pin the methodology document used for that batch.

**Balances and transfers.** `balanceOf[batchId][holder]` holds the fungible position per batch. `transfer`, `transferFrom` and `batchTransferFrom` move units. Operators approved through `setOperator` can move or retire on behalf of a holder. Units inside a batch are interchangeable until they are retired, which is how real registries treat them too. The serials are assigned at retirement, not at transfer. That keeps transfers cheap and avoids tracking ownership of individual serial numbers.

**Retirement.** `retire`, `retireFrom` and `retireBySig` all end in the private `_retire`. It debits the holder, then reads the batch field `retiredCursor`, adds the amount and stores the result. The old cursor value is the first serial burned, the new value is the end. A `Receipt` struct records the batch, holder, beneficiary, serial range, timestamp, `Claim` type and a `reasonHash`. The beneficiary is the party who gets the claim, which is often different from the holder when an intermediary retires for a client. The `Claim` enum has `Voluntary`, `Corsia`, `Article6` and `Compliance`. The reason hash is meant to point to the offchain retirement statement.

**Lookup by serial.** Every retirement also pushes a `Segment` with its start serial into a per batch array. Because the cursor only moves forward, that array is sorted by construction. `findRetirement(batchId, serial)` runs a binary search over it and returns whether the serial was retired and by which receipt. That is the answer to the auditor question in one call, and the cost grows with the logarithm of the number of retirements.

**Gasless retirement.** `retireBySig(req, signature)` takes a `RetireRequest` struct and a signature. The digest follows EIP-712 with the domain from `domainSeparator`, which recomputes if the chain id changes after a fork. The struct hash includes the holder nonce from `nonces`, so a signature works once. `_isValidSignature` handles two cases: an externally owned account goes through `ecrecover` with a check that `s` is in the lower half of the curve and `v` is 27 or 28, and a contract account goes through an ERC-1271 `staticcall`. A holder can cancel any outstanding signed orders by calling `invalidateNonce`. The struct form of the argument is there because the flat eight parameter version hits the stack depth limit in the compiler.

**Freezing.** `setFrozen(batchId, frozen, reasonHash)` lets the guardian or the owner stop transfers and retirements on one batch, for example while a registry investigates an issuance error. It never moves or burns balances. Unfreezing restores normal operation.

**Adjustments.** `attestAdjustment(receiptId, ref)` lets an address in `isAttestor` attach a reference to a receipt, typically the authorization or corresponding adjustment record. It can be set once per receipt and is stored in `adjustmentRef`. The receipt itself is never edited.

**Roles.** The owner uses two step transfer through `transferOwnership` and `acceptOwnership`. The owner manages `setGuardian`, `setIssuer` and `setAttestor`. There is no way for the owner to mint outside the issuer role, to change a receipt or to move a holder balance.

**Errors.** Every failure has a custom error such as `InsufficientBalance`, `BatchIsFrozen`, `ProjectOwnedByOther`, `InvalidSignature` and `AdjustmentAlreadyAttested`, so a failed call tells you exactly what went wrong.

## Usage

Deploy with the owner address, then register an issuer and issue a batch. Shown with ethers v6.

```js
const registry = await factory.deploy(ownerAddress);
await registry.setIssuer(issuerAddress, true);

// issuer: 1000 tonnes, project id derived from your registry id
const projectId = ethers.id("example-project-001");
await registry.connect(issuer).issue(projectId, 2024, 1000, methodologyHash, buyerAddress);

// buyer retires 100 tonnes for a named beneficiary
// claim: 0 Voluntary, 1 Corsia, 2 Article6, 3 Compliance
await registry.connect(buyer).retire(1, 100, beneficiaryAddress, reasonHash, 0);

// auditor: which receipt consumed serial 42 of batch 1
const [found, receiptId] = await registry.findRetirement(1, 42);
const receipt = await registry.receiptOf(receiptId);
```

Gasless retirement. The buyer signs, any account submits.

```js
const domain = {
  name: "CarbonCreditRetirementRegistry",
  version: "1",
  chainId,
  verifyingContract: registryAddress,
};
const types = {
  Retire: [
    { name: "holder", type: "address" },
    { name: "batchId", type: "uint256" },
    { name: "amount", type: "uint256" },
    { name: "beneficiary", type: "address" },
    { name: "reasonHash", type: "bytes32" },
    { name: "claim", type: "uint8" },
    { name: "deadline", type: "uint256" },
    { name: "nonce", type: "uint256" },
  ],
};
const req = { holder, batchId: 1, amount: 200, beneficiary, reasonHash, claim: 2, deadline };
const nonce = await registry.nonces(holder);
const signature = await buyerWallet.signTypedData(domain, types, { ...req, nonce });
await registry.connect(relayer).retireBySig(req, signature);
```

Useful views: `batchOf`, `receiptOf`, `issuedIn`, `retiredIn`, `circulatingIn`, `segmentCount`, `balanceOf` and `adjustmentRef`. Compile with solc 0.8.24 or newer and the optimizer on.

## Notes

I compiled it with solc 0.8.26 and ran it in an in process EVM. The checks covered issuer gating, project ownership, the serial cursor across batches, overdraw, retirement and serial lookup at the edges of each range, the supply invariant, signature replay, a wrong signer, nonce invalidation, freezing, operator rights, single attestation and two step ownership. The test harness is not part of this folder. Run your own suite and an independent audit before it holds real value.

Design choices worth knowing:

- Serials are assigned at retirement, not at issuance to a wallet. Two holders of the same batch are interchangeable, and the earliest unretired serials are always consumed first.
- Serials are per project across all vintages, not per batch. The second batch of a project starts where the first ended.
- There are no external calls except the ERC-1271 `staticcall`, which cannot change state, so reentrancy does not reach the accounting.
- `retiredAt` is a `uint40` timestamp and serials are `uint64`. Quantities in `issue` are also `uint64`, which is far above any real tonnage.
- A frozen batch blocks retirement as well as transfer. If you want holders to be able to retire during a freeze, change `_debit`.
- The owner and guardian are single addresses. In production put a multisig behind them.
- A `Claim` value outside the enum makes `retireBySig` revert, which is the safe outcome for a malformed order.
- The contract does not accept ETH and has no payable function.
- The contract does not decide whether a credit is real. That is the issuer role and the registry behind it. What it guarantees is that once a credit is on this ledger it cannot be retired twice and every retirement can be traced to the serial.

If you fork it, the most useful additions are probably a timelock on the issuer role, a batch metadata URI and a retirement certificate renderer that reads `receiptOf`.
