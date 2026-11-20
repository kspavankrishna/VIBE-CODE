// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// @notice Minimal ERC-1271 surface used to validate votes from contract wallets.
interface IERC1271 {
    function isValidSignature(bytes32 hash, bytes calldata signature) external view returns (bytes4);
}

/// @title ReproducibleBuildQuorum
/// @notice M of N agreement between independent rebuilders on the digest of a release.
/// @dev A release is attested only when enough eligible rebuilders report the same digest
///      for the same source commit and dissent stays within tolerance. Votes are final;
///      a rebuilder that signs two different digests for one round is ejected permanently.
contract ReproducibleBuildQuorum {
    // ------------------------------------------------------------------ constants

    uint256 public constant MAX_REBUILDERS = 32;
    uint64 public constant MIN_VOTING_WINDOW = 1 hours;
    uint64 public constant MAX_VOTING_WINDOW = 30 days;
    uint64 public constant MIN_CONFIG_DELAY = 1 days;
    uint64 public constant OP_GRACE_PERIOD = 14 days;
    uint256 public constant MAX_PROJECT_BYTES = 128;
    uint256 public constant MAX_VERSION_BYTES = 64;

    bytes32 public constant VOTE_TYPEHASH =
        keccak256("BuildVote(bytes32 releaseId,uint32 round,bytes32 sourceCommit,bytes32 digest)");
    bytes32 public constant EQUIVOCATION_REASON = keccak256("equivocation");

    bytes32 private constant DOMAIN_TYPEHASH =
        keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    bytes32 private constant NAME_HASH = keccak256("ReproducibleBuildQuorum");
    bytes32 private constant VERSION_HASH = keccak256("1");
    bytes4 private constant ERC1271_MAGIC = 0x1626ba7e;
    uint256 private constant SECP256K1_HALF_N =
        0x7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0;
    bytes32 private constant EIP2098_S_MASK =
        0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff;

    // ------------------------------------------------------------------ types

    enum Status {
        None,
        Open,
        Attested,
        Diverged,
        Expired,
        Yanked
    }

    enum OpKind {
        None,
        AddRebuilder,
        LowerThreshold,
        RaiseDissentTolerance
    }

    struct Rebuilder {
        uint32 addedEpoch;
        uint8 index;
        bool active;
        bool ejected;
    }

    struct Release {
        bytes32 sourceCommit;
        bytes32 digest;
        uint64 openedAt;
        uint64 deadline;
        uint64 finalizedAt;
        uint32 round;
        uint32 epoch;
        uint16 threshold;
        uint16 dissentTolerance;
        Status status;
    }

    struct PendingOp {
        OpKind kind;
        address target;
        uint16 value;
        uint64 eta;
    }

    struct SignedVote {
        bytes32 releaseId;
        uint32 round;
        bytes32 digest;
        address signer;
        bytes signature;
    }

    struct Tally {
        bytes32 best;
        uint256 bestCount;
        uint256 counted;
        uint256 eligible;
        uint256 distinct;
        bytes32[] digests;
        uint256[] counts;
    }

    // ------------------------------------------------------------------ storage

    address public owner;
    address public pendingOwner;
    uint64 public immutable configDelay;

    uint16 public threshold;
    uint16 public dissentTolerance;
    uint32 public membershipEpoch;
    uint256 public nextOpId;

    address[] private _active;
    mapping(address => Rebuilder) public rebuilders;
    mapping(address => bool) public isMaintainer;
    mapping(bytes32 => Release) private _releases;
    mapping(uint256 => PendingOp) public pendingOps;
    mapping(bytes32 => mapping(address => bytes32)) private _voteOf;
    mapping(bytes32 => address[]) private _voters;

    uint256 private immutable _cachedChainId;
    bytes32 private immutable _cachedDomainSeparator;

    // ------------------------------------------------------------------ events

    event OwnershipTransferStarted(address indexed previousOwner, address indexed newOwner);
    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
    event MaintainerSet(address indexed account, bool allowed);
    event RebuilderAdded(address indexed rebuilder, uint32 epoch);
    event RebuilderRemoved(address indexed rebuilder);
    event RebuilderEjected(address indexed rebuilder, bytes32 indexed reason, bytes32 evidence);
    event ThresholdChanged(uint16 previous, uint16 current);
    event DissentToleranceChanged(uint16 previous, uint16 current);
    event QuorumBelowThreshold(uint256 activeRebuilders, uint16 threshold);
    event OpQueued(uint256 indexed opId, OpKind kind, address target, uint16 value, uint64 eta);
    event OpExecuted(uint256 indexed opId);
    event OpCancelled(uint256 indexed opId);
    event ReleaseOpened(
        bytes32 indexed releaseId,
        uint32 round,
        string project,
        string version,
        bytes32 sourceCommit,
        uint64 deadline,
        uint32 epoch,
        uint16 threshold
    );
    event VoteRecorded(bytes32 indexed releaseId, uint32 round, address indexed rebuilder, bytes32 digest);
    event Equivocation(
        bytes32 indexed releaseId, uint32 round, address indexed rebuilder, bytes32 digestA, bytes32 digestB
    );
    event ReleaseFinalized(
        bytes32 indexed releaseId, uint32 round, Status status, bytes32 digest, uint256 support, uint256 dissent
    );
    event ReleaseYanked(bytes32 indexed releaseId, bytes32 digest, bytes32 reasonHash);

    // ------------------------------------------------------------------ errors

    error NotOwner();
    error NotPendingOwner();
    error NotMaintainer();
    error ZeroAddress();
    error InvalidThreshold(uint256 threshold, uint256 dissentTolerance, uint256 activeRebuilders);
    error InvalidConfigDelay(uint64 delay);
    error TooManyRebuilders();
    error AlreadyRebuilder(address account);
    error UnknownRebuilder(address account);
    error RebuilderIsEjected(address account);
    error OpNotFound(uint256 opId);
    error OpNotReady(uint256 opId, uint64 eta);
    error OpStale(uint256 opId);
    error InvalidName();
    error InvalidSourceCommit();
    error InvalidWindow(uint64 window);
    error InvalidDigest();
    error ReleaseNotReopenable(bytes32 releaseId, Status status);
    error SourceCommitMismatch(bytes32 expected, bytes32 provided);
    error QuorumUnreachable(uint256 activeRebuilders, uint16 threshold);
    error ReleaseNotOpen(bytes32 releaseId, Status status);
    error VotingClosed(bytes32 releaseId, uint64 deadline);
    error StaleRound(uint32 expected, uint32 provided);
    error NotEligible(address account);
    error InvalidSignature(address signer);
    error NotEquivocation();
    error UndecidedRelease(bytes32 releaseId, uint256 support, uint256 counted, uint256 eligible);
    error NotAttested(bytes32 releaseId, Status status);

    // ------------------------------------------------------------------ modifiers

    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner();
        _;
    }

    modifier onlyMaintainer() {
        if (!isMaintainer[msg.sender]) revert NotMaintainer();
        _;
    }

    // ------------------------------------------------------------------ construction

    constructor(
        address owner_,
        address[] memory maintainers_,
        address[] memory rebuilders_,
        uint16 threshold_,
        uint16 dissentTolerance_,
        uint64 configDelay_
    ) {
        if (owner_ == address(0)) revert ZeroAddress();
        if (configDelay_ < MIN_CONFIG_DELAY) revert InvalidConfigDelay(configDelay_);
        if (rebuilders_.length > MAX_REBUILDERS) revert TooManyRebuilders();

        owner = owner_;
        configDelay = configDelay_;
        membershipEpoch = 1;
        emit OwnershipTransferred(address(0), owner_);

        for (uint256 i = 0; i < maintainers_.length; ++i) {
            if (maintainers_[i] == address(0)) revert ZeroAddress();
            isMaintainer[maintainers_[i]] = true;
            emit MaintainerSet(maintainers_[i], true);
        }
        for (uint256 i = 0; i < rebuilders_.length; ++i) {
            _activate(rebuilders_[i], 1);
        }

        _checkConfig(threshold_, dissentTolerance_, _active.length);
        threshold = threshold_;
        dissentTolerance = dissentTolerance_;
        emit ThresholdChanged(0, threshold_);
        emit DissentToleranceChanged(0, dissentTolerance_);

        _cachedChainId = block.chainid;
        _cachedDomainSeparator = _buildDomainSeparator();
    }

    // ------------------------------------------------------------------ ownership

    function transferOwnership(address newOwner) external onlyOwner {
        if (newOwner == address(0)) revert ZeroAddress();
        pendingOwner = newOwner;
        emit OwnershipTransferStarted(owner, newOwner);
    }

    function acceptOwnership() external {
        if (msg.sender != pendingOwner) revert NotPendingOwner();
        emit OwnershipTransferred(owner, msg.sender);
        owner = msg.sender;
        pendingOwner = address(0);
    }

    function setMaintainer(address account, bool allowed) external onlyOwner {
        if (account == address(0)) revert ZeroAddress();
        isMaintainer[account] = allowed;
        emit MaintainerSet(account, allowed);
    }

    // ------------------------------------------------------------------ safe direction changes (immediate)

    function raiseThreshold(uint16 newThreshold) external onlyOwner {
        if (newThreshold <= threshold) revert InvalidThreshold(newThreshold, dissentTolerance, _active.length);
        _checkConfig(newThreshold, dissentTolerance, _active.length);
        emit ThresholdChanged(threshold, newThreshold);
        threshold = newThreshold;
    }

    function lowerDissentTolerance(uint16 newTolerance) external onlyOwner {
        if (newTolerance >= dissentTolerance) revert InvalidThreshold(threshold, newTolerance, _active.length);
        emit DissentToleranceChanged(dissentTolerance, newTolerance);
        dissentTolerance = newTolerance;
    }

    function removeRebuilder(address account) external onlyOwner {
        Rebuilder storage b = rebuilders[account];
        if (!b.active) revert UnknownRebuilder(account);
        _deactivate(account);
        emit RebuilderRemoved(account);
    }

    function ejectRebuilder(address account, bytes32 reasonHash) external onlyOwner {
        Rebuilder storage b = rebuilders[account];
        if (b.ejected) revert RebuilderIsEjected(account);
        if (!b.active && b.addedEpoch == 0) revert UnknownRebuilder(account);
        _eject(account, reasonHash, bytes32(0));
    }

    // ------------------------------------------------------------------ weakening changes (timelocked)

    function queueAddRebuilder(address account) external onlyOwner returns (uint256 opId) {
        if (account == address(0)) revert ZeroAddress();
        Rebuilder storage b = rebuilders[account];
        if (b.ejected) revert RebuilderIsEjected(account);
        if (b.active) revert AlreadyRebuilder(account);
        return _queue(OpKind.AddRebuilder, account, 0);
    }

    function queueLowerThreshold(uint16 newThreshold) external onlyOwner returns (uint256 opId) {
        if (newThreshold >= threshold || newThreshold == 0 || newThreshold <= dissentTolerance) {
            revert InvalidThreshold(newThreshold, dissentTolerance, _active.length);
        }
        return _queue(OpKind.LowerThreshold, address(0), newThreshold);
    }

    function queueRaiseDissentTolerance(uint16 newTolerance) external onlyOwner returns (uint256 opId) {
        if (newTolerance <= dissentTolerance || newTolerance >= threshold) {
            revert InvalidThreshold(threshold, newTolerance, _active.length);
        }
        return _queue(OpKind.RaiseDissentTolerance, address(0), newTolerance);
    }

    function cancelOp(uint256 opId) external onlyOwner {
        if (pendingOps[opId].kind == OpKind.None) revert OpNotFound(opId);
        delete pendingOps[opId];
        emit OpCancelled(opId);
    }

    function executeOp(uint256 opId) external onlyOwner {
        PendingOp memory op = pendingOps[opId];
        if (op.kind == OpKind.None) revert OpNotFound(opId);
        if (block.timestamp < op.eta) revert OpNotReady(opId, op.eta);
        if (block.timestamp > uint256(op.eta) + OP_GRACE_PERIOD) revert OpStale(opId);
        delete pendingOps[opId];

        if (op.kind == OpKind.AddRebuilder) {
            Rebuilder storage b = rebuilders[op.target];
            if (b.ejected) revert RebuilderIsEjected(op.target);
            if (b.active) revert AlreadyRebuilder(op.target);
            membershipEpoch += 1;
            _activate(op.target, membershipEpoch);
        } else if (op.kind == OpKind.LowerThreshold) {
            // Re-validate against the state at execution time, not queue time.
            if (op.value >= threshold) revert InvalidThreshold(op.value, dissentTolerance, _active.length);
            _checkConfig(op.value, dissentTolerance, _active.length);
            emit ThresholdChanged(threshold, op.value);
            threshold = op.value;
        } else {
            if (op.value <= dissentTolerance) revert InvalidThreshold(threshold, op.value, _active.length);
            _checkConfig(threshold, op.value, _active.length);
            emit DissentToleranceChanged(dissentTolerance, op.value);
            dissentTolerance = op.value;
        }
        emit OpExecuted(opId);
    }

    // ------------------------------------------------------------------ release lifecycle

    function openRelease(string calldata project, string calldata version, bytes32 sourceCommit, uint64 window)
        external
        onlyMaintainer
        returns (bytes32 releaseId, uint32 round)
    {
        uint256 pLen = bytes(project).length;
        uint256 vLen = bytes(version).length;
        if (pLen == 0 || pLen > MAX_PROJECT_BYTES || vLen == 0 || vLen > MAX_VERSION_BYTES) revert InvalidName();
        if (sourceCommit == bytes32(0)) revert InvalidSourceCommit();
        if (window < MIN_VOTING_WINDOW || window > MAX_VOTING_WINDOW) revert InvalidWindow(window);
        if (_active.length < threshold) revert QuorumUnreachable(_active.length, threshold);

        releaseId = releaseKey(project, version);
        Release storage r = _releases[releaseId];

        if (r.status == Status.None) {
            round = 1;
        } else if (r.status == Status.Expired || r.status == Status.Diverged) {
            // A version is bound to one commit forever. Reopening never moves the tag.
            if (r.sourceCommit != sourceCommit) revert SourceCommitMismatch(r.sourceCommit, sourceCommit);
            round = r.round + 1;
        } else {
            revert ReleaseNotReopenable(releaseId, r.status);
        }

        uint64 nowTs = uint64(block.timestamp);
        r.sourceCommit = sourceCommit;
        r.digest = bytes32(0);
        r.openedAt = nowTs;
        r.deadline = nowTs + window;
        r.finalizedAt = 0;
        r.round = round;
        r.epoch = membershipEpoch;
        r.threshold = threshold;
        r.dissentTolerance = dissentTolerance;
        r.status = Status.Open;

        emit ReleaseOpened(releaseId, round, project, version, sourceCommit, r.deadline, r.epoch, r.threshold);
    }

    /// @notice Cast a vote directly from a rebuilder address.
    function vote(bytes32 releaseId, bytes32 digest) external returns (bool recorded) {
        Release storage r = _releases[releaseId];
        _requireVotable(releaseId, r);
        return _recordVote(msg.sender, releaseId, r, digest);
    }

    /// @notice Relay a rebuilder's EIP-712 vote. Anyone may submit; the signature carries authority.
    function submitSignedVote(SignedVote calldata v) external returns (bool recorded) {
        return _submitSigned(v);
    }

    /// @notice Relay many votes in one transaction. Duplicates are skipped, bad signatures revert.
    function submitSignedVotes(SignedVote[] calldata votes) external returns (uint256 recorded) {
        for (uint256 i = 0; i < votes.length; ++i) {
            if (_submitSigned(votes[i])) ++recorded;
        }
    }

    /// @notice Prove that a rebuilder signed two different digests for one round. Permissionless.
    function reportEquivocation(
        bytes32 releaseId,
        uint32 round,
        address signer,
        bytes32 digestA,
        bytes calldata signatureA,
        bytes32 digestB,
        bytes calldata signatureB
    ) external {
        Release storage r = _releases[releaseId];
        if (r.status == Status.None || round == 0 || round > r.round) revert StaleRound(r.round, round);
        if (digestA == digestB || digestA == bytes32(0) || digestB == bytes32(0)) revert NotEquivocation();
        Rebuilder storage b = rebuilders[signer];
        if (b.addedEpoch == 0) revert UnknownRebuilder(signer);
        if (b.ejected) revert RebuilderIsEjected(signer);
        if (!_isValidSignature(signer, hashVote(releaseId, round, r.sourceCommit, digestA), signatureA)) {
            revert InvalidSignature(signer);
        }
        if (!_isValidSignature(signer, hashVote(releaseId, round, r.sourceCommit, digestB), signatureB)) {
            revert InvalidSignature(signer);
        }
        emit Equivocation(releaseId, round, signer, digestA, digestB);
        _eject(signer, EQUIVOCATION_REASON, keccak256(abi.encode(releaseId, round, digestA, digestB)));
    }

    /// @notice Settle an open release. Permissionless, reverts while the outcome is still undecided.
    function finalize(bytes32 releaseId) external returns (Status status) {
        Release storage r = _releases[releaseId];
        if (r.status != Status.Open) revert ReleaseNotOpen(releaseId, r.status);

        Tally memory t = _tally(releaseId, r);
        status = _decide(r, t, block.timestamp >= r.deadline);
        if (status == Status.Open) revert UndecidedRelease(releaseId, t.bestCount, t.counted, t.eligible);

        r.status = status;
        r.finalizedAt = uint64(block.timestamp);
        if (status == Status.Attested) r.digest = t.best;

        emit ReleaseFinalized(releaseId, r.round, status, t.best, t.bestCount, t.counted - t.bestCount);
    }

    /// @notice Withdraw an attestation. Terminal: ship a new version instead of reusing this one.
    function yank(bytes32 releaseId, bytes32 reasonHash) external onlyMaintainer {
        Release storage r = _releases[releaseId];
        if (r.status != Status.Attested) revert NotAttested(releaseId, r.status);
        r.status = Status.Yanked;
        emit ReleaseYanked(releaseId, r.digest, reasonHash);
    }

    // ------------------------------------------------------------------ views

    function releaseKey(string calldata project, string calldata version) public pure returns (bytes32) {
        return keccak256(abi.encode(keccak256(bytes(project)), keccak256(bytes(version))));
    }

    function verify(bytes32 releaseId, bytes32 digest) public view returns (bool) {
        Release storage r = _releases[releaseId];
        return r.status == Status.Attested && digest != bytes32(0) && r.digest == digest;
    }

    function verifyRelease(string calldata project, string calldata version, bytes32 digest)
        external
        view
        returns (bool)
    {
        return verify(releaseKey(project, version), digest);
    }

    function getRelease(bytes32 releaseId) external view returns (Release memory) {
        return _releases[releaseId];
    }

    /// @notice Live tally for dashboards and CI gates. Counts only votes that would count at finalize.
    function previewTally(bytes32 releaseId)
        external
        view
        returns (
            Status wouldBe,
            bytes32 leadingDigest,
            uint256 support,
            uint256 counted,
            uint256 eligible,
            bytes32[] memory digests,
            uint256[] memory counts
        )
    {
        Release storage r = _releases[releaseId];
        if (r.status != Status.Open) return (r.status, r.digest, 0, 0, 0, new bytes32[](0), new uint256[](0));
        Tally memory t = _tally(releaseId, r);
        wouldBe = _decide(r, t, block.timestamp >= r.deadline);
        digests = new bytes32[](t.distinct);
        counts = new uint256[](t.distinct);
        for (uint256 i = 0; i < t.distinct; ++i) {
            digests[i] = t.digests[i];
            counts[i] = t.counts[i];
        }
        return (wouldBe, t.best, t.bestCount, t.counted, t.eligible, digests, counts);
    }

    function voteOf(bytes32 releaseId, uint32 round, address rebuilder) external view returns (bytes32) {
        return _voteOf[_roundKey(releaseId, round)][rebuilder];
    }

    function activeRebuilders() external view returns (address[] memory) {
        return _active;
    }

    function domainSeparator() public view returns (bytes32) {
        return block.chainid == _cachedChainId ? _cachedDomainSeparator : _buildDomainSeparator();
    }

    function hashVote(bytes32 releaseId, uint32 round, bytes32 sourceCommit, bytes32 digest)
        public
        view
        returns (bytes32)
    {
        bytes32 structHash = keccak256(abi.encode(VOTE_TYPEHASH, releaseId, round, sourceCommit, digest));
        return keccak256(abi.encodePacked("\x19\x01", domainSeparator(), structHash));
    }

    // ------------------------------------------------------------------ internals: voting

    function _submitSigned(SignedVote calldata v) private returns (bool) {
        Release storage r = _releases[v.releaseId];
        _requireVotable(v.releaseId, r);
        if (v.round != r.round) revert StaleRound(r.round, v.round);
        bytes32 h = hashVote(v.releaseId, v.round, r.sourceCommit, v.digest);
        if (!_isValidSignature(v.signer, h, v.signature)) revert InvalidSignature(v.signer);
        return _recordVote(v.signer, v.releaseId, r, v.digest);
    }

    function _requireVotable(bytes32 releaseId, Release storage r) private view {
        if (r.status != Status.Open) revert ReleaseNotOpen(releaseId, r.status);
        if (block.timestamp >= r.deadline) revert VotingClosed(releaseId, r.deadline);
    }

    function _recordVote(address rebuilder, bytes32 releaseId, Release storage r, bytes32 digest)
        private
        returns (bool)
    {
        if (digest == bytes32(0)) revert InvalidDigest();
        if (!_eligible(rebuilders[rebuilder], r.epoch)) revert NotEligible(rebuilder);

        bytes32 rk = _roundKey(releaseId, r.round);
        bytes32 previous = _voteOf[rk][rebuilder];
        if (previous == digest) return false;
        if (previous != bytes32(0)) {
            // Votes are final. A second, different digest is equivocation, and the call must
            // succeed so the ejection persists rather than being rolled back by a revert.
            emit Equivocation(releaseId, r.round, rebuilder, previous, digest);
            _eject(rebuilder, EQUIVOCATION_REASON, keccak256(abi.encode(releaseId, r.round, previous, digest)));
            return false;
        }

        _voteOf[rk][rebuilder] = digest;
        _voters[rk].push(rebuilder);
        emit VoteRecorded(releaseId, r.round, rebuilder, digest);
        return true;
    }

    function _tally(bytes32 releaseId, Release storage r) private view returns (Tally memory t) {
        uint32 epoch = r.epoch;
        uint256 n = _active.length;
        for (uint256 i = 0; i < n; ++i) {
            if (rebuilders[_active[i]].addedEpoch <= epoch) ++t.eligible;
        }

        bytes32 rk = _roundKey(releaseId, r.round);
        address[] storage voters = _voters[rk];
        uint256 m = voters.length;
        t.digests = new bytes32[](m);
        t.counts = new uint256[](m);

        for (uint256 i = 0; i < m; ++i) {
            address who = voters[i];
            if (!_eligible(rebuilders[who], epoch)) continue;
            bytes32 d = _voteOf[rk][who];
            ++t.counted;
            uint256 j = 0;
            while (j < t.distinct && t.digests[j] != d) ++j;
            if (j == t.distinct) {
                t.digests[j] = d;
                ++t.distinct;
            }
            uint256 c = ++t.counts[j];
            if (c > t.bestCount) {
                t.bestCount = c;
                t.best = d;
            }
        }
    }

    /// @dev Effective rules for an open release are the stricter of its snapshot and the live config,
    ///      so loosening config never affects a vote already in flight, but tightening does.
    function _decide(Release storage r, Tally memory t, bool pastDeadline) private view returns (Status) {
        uint256 thr = r.threshold > threshold ? r.threshold : threshold;
        uint256 tol = r.dissentTolerance < dissentTolerance ? r.dissentTolerance : dissentTolerance;
        uint256 dissent = t.counted - t.bestCount;
        uint256 remaining = t.eligible - t.counted;

        if (dissent > tol) return Status.Diverged;
        if (t.bestCount >= thr && (pastDeadline || dissent + remaining <= tol)) return Status.Attested;
        if (pastDeadline || t.bestCount + remaining < thr) return Status.Expired;
        return Status.Open;
    }

    function _eligible(Rebuilder memory b, uint32 epoch) private pure returns (bool) {
        return b.active && !b.ejected && b.addedEpoch != 0 && b.addedEpoch <= epoch;
    }

    function _roundKey(bytes32 releaseId, uint32 round) private pure returns (bytes32) {
        return keccak256(abi.encode(releaseId, round));
    }

    // ------------------------------------------------------------------ internals: membership

    function _activate(address account, uint32 epoch) private {
        if (account == address(0)) revert ZeroAddress();
        Rebuilder storage b = rebuilders[account];
        if (b.ejected) revert RebuilderIsEjected(account);
        if (b.active) revert AlreadyRebuilder(account);
        if (_active.length >= MAX_REBUILDERS) revert TooManyRebuilders();
        b.active = true;
        b.addedEpoch = epoch;
        b.index = uint8(_active.length);
        _active.push(account);
        emit RebuilderAdded(account, epoch);
    }

    function _deactivate(address account) private {
        Rebuilder storage b = rebuilders[account];
        uint256 idx = b.index;
        uint256 last = _active.length - 1;
        if (idx != last) {
            address moved = _active[last];
            _active[idx] = moved;
            rebuilders[moved].index = uint8(idx);
        }
        _active.pop();
        b.active = false;
        b.index = 0;
        if (_active.length < threshold) emit QuorumBelowThreshold(_active.length, threshold);
    }

    function _eject(address account, bytes32 reason, bytes32 evidence) private {
        Rebuilder storage b = rebuilders[account];
        if (b.active) _deactivate(account);
        b.ejected = true;
        emit RebuilderEjected(account, reason, evidence);
    }

    function _queue(OpKind kind, address target, uint16 value) private returns (uint256 opId) {
        opId = ++nextOpId;
        uint64 eta = uint64(block.timestamp) + configDelay;
        pendingOps[opId] = PendingOp({kind: kind, target: target, value: value, eta: eta});
        emit OpQueued(opId, kind, target, value, eta);
    }

    function _checkConfig(uint256 thr, uint256 tol, uint256 activeCount) private pure {
        // tol < thr guarantees two digests can never both clear quorum without tripping divergence.
        if (thr == 0 || thr > activeCount || tol >= thr) revert InvalidThreshold(thr, tol, activeCount);
    }

    // ------------------------------------------------------------------ internals: signatures

    function _buildDomainSeparator() private view returns (bytes32) {
        return keccak256(abi.encode(DOMAIN_TYPEHASH, NAME_HASH, VERSION_HASH, block.chainid, address(this)));
    }

    /// @dev ECDSA first so EIP-7702 delegated accounts still validate with their own key,
    ///      then ERC-1271 for contract wallets such as multisig rebuilders.
    function _isValidSignature(address signer, bytes32 hash, bytes calldata signature)
        private
        view
        returns (bool)
    {
        if (signer == address(0)) return false;
        if (_recover(hash, signature) == signer) return true;
        if (signer.code.length == 0) return false;
        (bool ok, bytes memory ret) =
            signer.staticcall(abi.encodeWithSelector(IERC1271.isValidSignature.selector, hash, signature));
        return ok && ret.length >= 32 && bytes4(abi.decode(ret, (bytes32))) == ERC1271_MAGIC;
    }

    function _recover(bytes32 hash, bytes calldata sig) private pure returns (address) {
        bytes32 r;
        bytes32 s;
        uint8 v;
        if (sig.length == 65) {
            assembly ("memory-safe") {
                r := calldataload(sig.offset)
                s := calldataload(add(sig.offset, 32))
                v := byte(0, calldataload(add(sig.offset, 64)))
            }
        } else if (sig.length == 64) {
            bytes32 vs;
            assembly ("memory-safe") {
                r := calldataload(sig.offset)
                vs := calldataload(add(sig.offset, 32))
            }
            s = vs & EIP2098_S_MASK;
            v = uint8(uint256(vs) >> 255) + 27;
        } else {
            return address(0);
        }
        if (v < 27) v += 27;
        if (v != 27 && v != 28) return address(0);
        if (uint256(s) > SECP256K1_HALF_N) return address(0);
        return ecrecover(hash, v, r, s);
    }
}
