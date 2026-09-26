// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// @notice On-chain spend governor for autonomous AI agent wallets.
/// An owner funds this contract and grants scoped, budgeted, allowlisted
/// call permission to one or more agent addresses. Agents call `execute`
/// directly; the contract enforces a rolling time-window spend cap, a
/// per-transaction cap, a target+selector allowlist and an expiry, before
/// forwarding the call. No inheritance, no imports: everything an auditor
/// needs is in this one file.
contract AgentSpendGovernor {
    // ---------------------------------------------------------------
    // Constants
    // ---------------------------------------------------------------

    /// Number of buckets in the rolling spend window. epochLength * BUCKET_COUNT
    /// is the effective window size (e.g. 1 hour * 24 = a 24 hour rolling budget).
    uint256 private constant BUCKET_COUNT = 24;

    /// Sentinel selector meaning "any function on this target is allowed".
    bytes4 private constant ANY_SELECTOR = bytes4(0xffffffff);

    /// Hard cap on bytes copied from a callee's returndata. Protects the
    /// governor from a "return bomb": a callee that returns gigabytes of
    /// data to force an expensive memory expansion in the caller.
    uint256 private constant MAX_RETURN_COPY = 256;

    // ---------------------------------------------------------------
    // Storage
    // ---------------------------------------------------------------

    struct Policy {
        uint128 epochBudgetWei; // max wei an agent may spend across the rolling window
        uint128 maxPerTxWei;    // max wei an agent may spend in a single call
        uint64 epochLength;     // seconds per bucket; window = epochLength * BUCKET_COUNT
        uint64 expiresAt;       // unix timestamp the policy stops working at, 0 = no expiry
        bool active;            // false until setPolicy has been called for this agent
        bool paused;             // per-agent kill switch, independent of globalPaused
    }

    struct SpendWindow {
        bool initialized;
        uint64 lastEpoch;                  // last epoch index this window was advanced to
        uint128 windowTotal;                // sum of all live buckets, kept incrementally
        uint128[BUCKET_COUNT] buckets;      // wei spent per epoch slot
    }

    address public owner;
    address public pendingOwner;
    bool public globalPaused;

    mapping(address => Policy) public policies;
    mapping(address => SpendWindow) private windows;

    /// agent => keccak256(target, selector) => allowed. ANY_SELECTOR acts as a wildcard
    /// for "every selector on this target", checked separately from an exact match.
    mapping(address => mapping(bytes32 => bool)) public allowedTargetSelector;

    uint256 private reentrancyLock = 1;

    // ---------------------------------------------------------------
    // Events
    // ---------------------------------------------------------------

    event PolicySet(address indexed agent, uint128 epochBudgetWei, uint128 maxPerTxWei, uint64 epochLength, uint64 expiresAt);
    event PolicyRevoked(address indexed agent);
    event AgentPaused(address indexed agent, bool paused);
    event GlobalPausedSet(bool paused);
    event TargetAllowed(address indexed agent, address indexed target, bytes4 selector, bool allowed);
    event Executed(address indexed agent, address indexed target, bytes4 selector, uint256 value, uint256 epochIndex, uint256 windowSpentAfter);
    event Deposited(address indexed from, uint256 amount);
    event Withdrawn(address indexed to, uint256 amount);
    event OwnershipTransferStarted(address indexed previousOwner, address indexed newOwner);
    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);

    // ---------------------------------------------------------------
    // Errors
    // ---------------------------------------------------------------

    error NotOwner();
    error NotPendingOwner();
    error ZeroAddress();
    error InvalidEpochLength();
    error PolicyNotActive();
    error AgentOrGloballyPaused();
    error PolicyExpired();
    error TargetNotAllowed();
    error PerTxLimitExceeded();
    error EpochBudgetExceeded();
    error CallFailed(bytes returnData);
    error Reentrancy();
    error InsufficientBalance();

    // ---------------------------------------------------------------
    // Modifiers
    // ---------------------------------------------------------------

    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner();
        _;
    }

    modifier nonReentrant() {
        if (reentrancyLock == 2) revert Reentrancy();
        reentrancyLock = 2;
        _;
        reentrancyLock = 1;
    }

    // ---------------------------------------------------------------
    // Construction and funding
    // ---------------------------------------------------------------

    constructor(address initialOwner) {
        if (initialOwner == address(0)) revert ZeroAddress();
        owner = initialOwner;
        emit OwnershipTransferred(address(0), initialOwner);
    }

    receive() external payable {
        emit Deposited(msg.sender, msg.value);
    }

    // ---------------------------------------------------------------
    // Owner administration
    // ---------------------------------------------------------------

    /// @notice Sets (or replaces) an agent's spending policy.
    /// @dev Replacing a policy always resets that agent's rolling spend
    /// window. This is deliberate: `epochLength` changes make the previous
    /// window's bucket indices meaningless, and starting an agent fresh
    /// whenever its budget is reconfigured is the only reading of "budget"
    /// that does not require reasoning about stale buckets from an old
    /// policy shape. Existing target allowlist entries are untouched.
    function setPolicy(
        address agent,
        uint128 epochBudgetWei,
        uint128 maxPerTxWei,
        uint64 epochLength,
        uint64 expiresAt
    ) external onlyOwner {
        if (agent == address(0)) revert ZeroAddress();
        if (epochLength == 0) revert InvalidEpochLength();

        policies[agent] = Policy({
            epochBudgetWei: epochBudgetWei,
            maxPerTxWei: maxPerTxWei,
            epochLength: epochLength,
            expiresAt: expiresAt,
            active: true,
            paused: false
        });
        delete windows[agent];

        emit PolicySet(agent, epochBudgetWei, maxPerTxWei, epochLength, expiresAt);
    }

    /// @notice Fully removes an agent's policy. `execute` reverts for it afterwards.
    function revokePolicy(address agent) external onlyOwner {
        delete policies[agent];
        delete windows[agent];
        emit PolicyRevoked(agent);
    }

    /// @notice Per-agent kill switch. Leaves the policy and window intact so
    /// unpausing resumes exactly where the agent left off in its budget window.
    function setAgentPaused(address agent, bool paused) external onlyOwner {
        policies[agent].paused = paused;
        emit AgentPaused(agent, paused);
    }

    /// @notice Kill switch for every agent at once, for incident response.
    function setGlobalPaused(bool paused) external onlyOwner {
        globalPaused = paused;
        emit GlobalPausedSet(paused);
    }

    /// @notice Grants or revokes permission for `agent` to call `selector` on
    /// `target`. Pass `ANY_SELECTOR` (0xffffffff) via {allowAnySelector} to
    /// allow every function on a target instead of naming each one.
    function setTargetAllowed(address agent, address target, bytes4 selector, bool allowed) external onlyOwner {
        if (target == address(0)) revert ZeroAddress();
        allowedTargetSelector[agent][_key(target, selector)] = allowed;
        emit TargetAllowed(agent, target, selector, allowed);
    }

    /// @notice Convenience wrapper so callers do not have to know the sentinel value.
    function allowAnySelector(address agent, address target, bool allowed) external onlyOwner {
        if (target == address(0)) revert ZeroAddress();
        allowedTargetSelector[agent][_key(target, ANY_SELECTOR)] = allowed;
        emit TargetAllowed(agent, target, ANY_SELECTOR, allowed);
    }

    /// @notice Owner-only withdrawal of the treasury. Agent budgets do not
    /// limit the owner; the policy only constrains what agents can do.
    function withdraw(address payable to, uint256 amount) external onlyOwner nonReentrant {
        if (to == address(0)) revert ZeroAddress();
        if (amount > address(this).balance) revert InsufficientBalance();
        (bool ok, ) = to.call{value: amount}("");
        if (!ok) revert CallFailed("");
        emit Withdrawn(to, amount);
    }

    /// @notice Two-step ownership transfer. A single-step transfer that typos
    /// the new owner address permanently locks the treasury; requiring the
    /// new owner to accept removes that failure mode.
    function transferOwnership(address newOwner) external onlyOwner {
        if (newOwner == address(0)) revert ZeroAddress();
        pendingOwner = newOwner;
        emit OwnershipTransferStarted(owner, newOwner);
    }

    function acceptOwnership() external {
        if (msg.sender != pendingOwner) revert NotPendingOwner();
        address previousOwner = owner;
        owner = pendingOwner;
        pendingOwner = address(0);
        emit OwnershipTransferred(previousOwner, owner);
    }

    // ---------------------------------------------------------------
    // Agent execution
    // ---------------------------------------------------------------

    /// @notice Called by an agent (msg.sender must itself be the governed
    /// address) to spend `value` wei calling `target` with `data`, subject
    /// to its policy. Reverts the whole call, including any state the
    /// callee changed, if any check fails or the callee itself reverts.
    function execute(address target, uint256 value, bytes calldata data)
        external
        nonReentrant
        returns (bytes memory returnData)
    {
        address agent = msg.sender;
        Policy storage pol = policies[agent];

        if (!pol.active) revert PolicyNotActive();
        if (globalPaused || pol.paused) revert AgentOrGloballyPaused();
        if (pol.expiresAt != 0 && block.timestamp >= pol.expiresAt) revert PolicyExpired();
        if (target == address(0)) revert ZeroAddress();
        if (value > pol.maxPerTxWei) revert PerTxLimitExceeded();

        bytes4 selector = _selectorOf(data);
        bool exact = allowedTargetSelector[agent][_key(target, selector)];
        bool wild = allowedTargetSelector[agent][_key(target, ANY_SELECTOR)];
        if (!exact && !wild) revert TargetNotAllowed();

        uint256 epochIndex = block.timestamp / pol.epochLength;
        uint256 windowSpentAfter = _recordSpend(agent, pol, epochIndex, value);

        // Effects (the spend record above) are committed before this external
        // interaction, so a reentrant call from `target` back into `execute`
        // sees the updated budget and is checked against it correctly; the
        // nonReentrant guard on top of that is defense in depth, not the
        // only thing preventing double-spend.
        bool ok;
        (ok, returnData) = _safeCall(target, value, data);
        if (!ok) revert CallFailed(returnData);

        emit Executed(agent, target, selector, value, epochIndex, windowSpentAfter);
    }

    // ---------------------------------------------------------------
    // Views
    // ---------------------------------------------------------------

    /// @notice Wei an agent has spent inside its current rolling window, as of
    /// the last call to `execute`. Does not itself advance stale buckets, so
    /// immediately after a long idle period this can overstate the true spend
    /// until the agent's next `execute` call performs its catch-up.
    function currentWindowSpend(address agent) external view returns (uint256) {
        return windows[agent].windowTotal;
    }

    function remainingBudget(address agent) external view returns (uint256) {
        Policy storage pol = policies[agent];
        uint256 spent = windows[agent].windowTotal;
        if (spent >= pol.epochBudgetWei) return 0;
        return pol.epochBudgetWei - spent;
    }

    function isAllowed(address agent, address target, bytes4 selector) external view returns (bool) {
        return allowedTargetSelector[agent][_key(target, selector)]
            || allowedTargetSelector[agent][_key(target, ANY_SELECTOR)];
    }

    // ---------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------

    function _key(address target, bytes4 selector) private pure returns (bytes32) {
        return keccak256(abi.encodePacked(target, selector));
    }

    function _selectorOf(bytes calldata data) private pure returns (bytes4) {
        if (data.length < 4) return bytes4(0);
        return bytes4(data[:4]);
    }

    /// @dev Advances the rolling window to `epochIndex`, clearing any bucket
    /// that has aged out, then records `value` in the current bucket and
    /// returns the new window total. Reverts if that total would exceed the
    /// epoch budget. The catch-up loop is bounded by BUCKET_COUNT no matter
    /// how many epochs actually elapsed: a gap of a million epochs and a gap
    /// of BUCKET_COUNT epochs cost the same gas, because every live bucket is
    /// stale either way and gets zeroed once each, never revisited.
    function _recordSpend(address agent, Policy storage pol, uint256 epochIndex, uint256 value)
        private
        returns (uint256)
    {
        SpendWindow storage w = windows[agent];

        if (!w.initialized) {
            w.initialized = true;
            w.lastEpoch = uint64(epochIndex);
        } else if (epochIndex > w.lastEpoch) {
            uint256 gap = epochIndex - w.lastEpoch;
            if (gap >= BUCKET_COUNT) {
                for (uint256 i = 0; i < BUCKET_COUNT; i++) {
                    w.buckets[i] = 0;
                }
                w.windowTotal = 0;
            } else {
                for (uint256 i = 1; i <= gap; i++) {
                    uint256 staleIdx = (w.lastEpoch + i) % BUCKET_COUNT;
                    w.windowTotal -= w.buckets[staleIdx];
                    w.buckets[staleIdx] = 0;
                }
            }
            w.lastEpoch = uint64(epochIndex);
        }
        // epochIndex == w.lastEpoch: same bucket as the last recorded call, no
        // catch-up needed, fall straight through to the budget check below.

        uint256 newTotal = uint256(w.windowTotal) + value;
        if (newTotal > pol.epochBudgetWei) revert EpochBudgetExceeded();

        uint256 idx = epochIndex % BUCKET_COUNT;
        w.buckets[idx] += uint128(value);
        w.windowTotal = uint128(newTotal);
        return newTotal;
    }

    /// @dev Low-level call that never copies more than MAX_RETURN_COPY bytes
    /// of returndata into memory, regardless of how much the callee actually
    /// returned. A plain `target.call(data)` lets the compiler-generated code
    /// copy the callee's *entire* returndata before your code ever runs,
    /// which is exactly the "return bomb" gas griefing vector: a malicious or
    /// buggy callee returns megabytes of data solely to blow up the caller's
    /// memory expansion cost. Bounding the copy at the assembly level is the
    /// only way to close that off; truncating a `bytes memory` after the
    /// fact is too late; the expensive copy has already happened.
    function _safeCall(address target, uint256 value, bytes calldata data)
        private
        returns (bool success, bytes memory ret)
    {
        assembly {
            let inPtr := mload(0x40)
            calldatacopy(inPtr, data.offset, data.length)
            success := call(gas(), target, value, inPtr, data.length, 0, 0)

            let size := returndatasize()
            if gt(size, MAX_RETURN_COPY) { size := MAX_RETURN_COPY }

            ret := add(inPtr, data.length)
            ret := and(add(ret, 0x1f), not(0x1f)) // round up to a 32 byte boundary
            mstore(ret, size)
            returndatacopy(add(ret, 0x20), 0, size)
            mstore(0x40, add(add(ret, 0x20), size))
        }
    }
}
