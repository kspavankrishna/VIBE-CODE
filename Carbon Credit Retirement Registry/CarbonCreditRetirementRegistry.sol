// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

contract CarbonCreditRetirementRegistry {
    enum Claim {
        Voluntary,
        Corsia,
        Article6,
        Compliance
    }

    struct Batch {
        bytes32 projectId;
        bytes32 methodologyHash;
        address issuer;
        uint16 vintage;
        bool frozen;
        uint64 serialStart;
        uint64 serialEnd;
        uint64 retiredCursor;
    }

    struct Receipt {
        uint256 batchId;
        address holder;
        address beneficiary;
        uint64 serialStart;
        uint64 serialEnd;
        uint40 retiredAt;
        Claim claim;
        bytes32 reasonHash;
    }

    struct RetireRequest {
        address holder;
        uint256 batchId;
        uint256 amount;
        address beneficiary;
        bytes32 reasonHash;
        uint8 claim;
        uint256 deadline;
    }

    struct Segment {
        uint64 start;
        uint64 receiptId;
    }

    uint16 public constant MIN_VINTAGE = 1990;
    uint16 public constant MAX_VINTAGE = 2100;
    uint64 public constant MAX_SERIAL = type(uint64).max;

    bytes32 public constant RETIRE_TYPEHASH = keccak256(
        "Retire(address holder,uint256 batchId,uint256 amount,address beneficiary,bytes32 reasonHash,uint8 claim,uint256 deadline,uint256 nonce)"
    );
    bytes32 private constant DOMAIN_TYPEHASH =
        keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    bytes4 private constant ERC1271_MAGIC = 0x1626ba7e;
    uint256 private constant HALF_N = 0x7fffffffffffffffffffffffffffffff5d576e7357a4501ddfe92f46681b20a0;

    address public owner;
    address public pendingOwner;
    address public guardian;
    mapping(address => bool) public isIssuer;
    mapping(address => bool) public isAttestor;

    uint256 public batchCount;
    uint256 public receiptCount;
    mapping(uint256 => Batch) private _batches;
    mapping(uint256 => Receipt) private _receipts;
    mapping(uint256 => Segment[]) private _segments;
    mapping(bytes32 => uint64) public nextSerial;
    mapping(bytes32 => address) public projectIssuer;
    mapping(uint256 => mapping(address => uint256)) public balanceOf;
    mapping(address => mapping(address => bool)) public isOperator;
    mapping(address => uint256) public nonces;
    mapping(uint256 => bytes32) public adjustmentRef;

    uint256 private immutable _cachedChainId;
    bytes32 private immutable _cachedDomain;

    event OwnershipTransferStarted(address indexed previousOwner, address indexed newOwner);
    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
    event GuardianSet(address indexed guardian);
    event IssuerSet(address indexed account, bool allowed);
    event AttestorSet(address indexed account, bool allowed);
    event BatchIssued(
        uint256 indexed batchId,
        bytes32 indexed projectId,
        address indexed issuer,
        uint16 vintage,
        uint64 serialStart,
        uint64 serialEnd,
        bytes32 methodologyHash,
        address recipient
    );
    event Transfer(uint256 indexed batchId, address operator, address indexed from, address indexed to, uint256 amount);
    event OperatorSet(address indexed holder, address indexed operator, bool approved);
    event BatchFrozen(uint256 indexed batchId, bool frozen, bytes32 reasonHash);
    event Retired(
        uint256 indexed receiptId,
        uint256 indexed batchId,
        address indexed beneficiary,
        address holder,
        uint64 serialStart,
        uint64 serialEnd,
        Claim claim,
        bytes32 reasonHash
    );
    event AdjustmentAttested(uint256 indexed receiptId, address indexed attestor, bytes32 ref);
    event NonceInvalidated(address indexed holder, uint256 newNonce);

    error NotOwner();
    error NotPendingOwner();
    error NotIssuer();
    error NotAttestor();
    error NotGuardianOrOwner();
    error ZeroAddress();
    error ZeroAmount();
    error UnknownBatch(uint256 batchId);
    error UnknownReceipt(uint256 receiptId);
    error BadVintage(uint16 vintage);
    error ProjectOwnedByOther(bytes32 projectId, address owner);
    error SerialSpaceExhausted(bytes32 projectId);
    error InsufficientBalance(uint256 batchId, address holder, uint256 have, uint256 want);
    error NotAuthorized(address holder, address caller);
    error BatchIsFrozen(uint256 batchId);
    error SignatureExpired(uint256 deadline);
    error InvalidSignature();
    error AdjustmentAlreadyAttested(uint256 receiptId);
    error ZeroReference();
    error LengthMismatch();

    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner();
        _;
    }

    constructor(address initialOwner) {
        if (initialOwner == address(0)) revert ZeroAddress();
        owner = initialOwner;
        guardian = initialOwner;
        _cachedChainId = block.chainid;
        _cachedDomain = _buildDomain();
        emit OwnershipTransferred(address(0), initialOwner);
        emit GuardianSet(initialOwner);
    }

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

    function setGuardian(address account) external onlyOwner {
        if (account == address(0)) revert ZeroAddress();
        guardian = account;
        emit GuardianSet(account);
    }

    function setIssuer(address account, bool allowed) external onlyOwner {
        if (account == address(0)) revert ZeroAddress();
        isIssuer[account] = allowed;
        emit IssuerSet(account, allowed);
    }

    function setAttestor(address account, bool allowed) external onlyOwner {
        if (account == address(0)) revert ZeroAddress();
        isAttestor[account] = allowed;
        emit AttestorSet(account, allowed);
    }

    function issue(
        bytes32 projectId,
        uint16 vintage,
        uint64 quantity,
        bytes32 methodologyHash,
        address recipient
    ) external returns (uint256 batchId) {
        if (!isIssuer[msg.sender]) revert NotIssuer();
        if (recipient == address(0)) revert ZeroAddress();
        if (quantity == 0) revert ZeroAmount();
        if (vintage < MIN_VINTAGE || vintage > MAX_VINTAGE) revert BadVintage(vintage);

        address claimed = projectIssuer[projectId];
        if (claimed == address(0)) {
            projectIssuer[projectId] = msg.sender;
        } else if (claimed != msg.sender) {
            revert ProjectOwnedByOther(projectId, claimed);
        }

        uint64 start = nextSerial[projectId];
        if (uint256(start) + uint256(quantity) > uint256(MAX_SERIAL)) revert SerialSpaceExhausted(projectId);
        uint64 end = start + quantity;
        nextSerial[projectId] = end;

        batchId = ++batchCount;
        _batches[batchId] = Batch({
            projectId: projectId,
            methodologyHash: methodologyHash,
            issuer: msg.sender,
            vintage: vintage,
            frozen: false,
            serialStart: start,
            serialEnd: end,
            retiredCursor: start
        });
        balanceOf[batchId][recipient] = quantity;

        emit BatchIssued(batchId, projectId, msg.sender, vintage, start, end, methodologyHash, recipient);
        emit Transfer(batchId, msg.sender, address(0), recipient, quantity);
    }

    function setOperator(address operator, bool approved) external {
        if (operator == address(0)) revert ZeroAddress();
        isOperator[msg.sender][operator] = approved;
        emit OperatorSet(msg.sender, operator, approved);
    }

    function transfer(uint256 batchId, address to, uint256 amount) external {
        _transfer(batchId, msg.sender, to, amount);
    }

    function transferFrom(uint256 batchId, address from, address to, uint256 amount) external {
        _requireAuthorized(from);
        _transfer(batchId, from, to, amount);
    }

    function batchTransferFrom(
        address from,
        address to,
        uint256[] calldata batchIds,
        uint256[] calldata amounts
    ) external {
        if (batchIds.length != amounts.length) revert LengthMismatch();
        _requireAuthorized(from);
        for (uint256 i = 0; i < batchIds.length; ++i) {
            _transfer(batchIds[i], from, to, amounts[i]);
        }
    }

    function retire(
        uint256 batchId,
        uint256 amount,
        address beneficiary,
        bytes32 reasonHash,
        Claim claim
    ) external returns (uint256 receiptId) {
        receiptId = _retire(batchId, msg.sender, amount, beneficiary, reasonHash, claim);
    }

    function retireFrom(
        uint256 batchId,
        address holder,
        uint256 amount,
        address beneficiary,
        bytes32 reasonHash,
        Claim claim
    ) external returns (uint256 receiptId) {
        _requireAuthorized(holder);
        receiptId = _retire(batchId, holder, amount, beneficiary, reasonHash, claim);
    }

    function retireBySig(RetireRequest calldata req, bytes calldata signature)
        external
        returns (uint256 receiptId)
    {
        if (block.timestamp > req.deadline) revert SignatureExpired(req.deadline);
        bytes32 structHash = keccak256(abi.encode(RETIRE_TYPEHASH, req, nonces[req.holder]));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator(), structHash));
        if (!_isValidSignature(req.holder, digest, signature)) revert InvalidSignature();
        unchecked {
            nonces[req.holder] += 1;
        }
        receiptId = _retire(req.batchId, req.holder, req.amount, req.beneficiary, req.reasonHash, Claim(req.claim));
    }

    function invalidateNonce() external {
        uint256 n = nonces[msg.sender] + 1;
        nonces[msg.sender] = n;
        emit NonceInvalidated(msg.sender, n);
    }

    function setFrozen(uint256 batchId, bool frozen, bytes32 reasonHash) external {
        if (msg.sender != guardian && msg.sender != owner) revert NotGuardianOrOwner();
        _requireBatch(batchId);
        _batches[batchId].frozen = frozen;
        emit BatchFrozen(batchId, frozen, reasonHash);
    }

    function attestAdjustment(uint256 receiptId, bytes32 ref) external {
        if (!isAttestor[msg.sender]) revert NotAttestor();
        if (ref == bytes32(0)) revert ZeroReference();
        if (receiptId == 0 || receiptId > receiptCount) revert UnknownReceipt(receiptId);
        if (adjustmentRef[receiptId] != bytes32(0)) revert AdjustmentAlreadyAttested(receiptId);
        adjustmentRef[receiptId] = ref;
        emit AdjustmentAttested(receiptId, msg.sender, ref);
    }

    function batchOf(uint256 batchId) external view returns (Batch memory) {
        _requireBatch(batchId);
        return _batches[batchId];
    }

    function receiptOf(uint256 receiptId) external view returns (Receipt memory) {
        if (receiptId == 0 || receiptId > receiptCount) revert UnknownReceipt(receiptId);
        return _receipts[receiptId];
    }

    function issuedIn(uint256 batchId) public view returns (uint64) {
        _requireBatch(batchId);
        Batch storage b = _batches[batchId];
        return b.serialEnd - b.serialStart;
    }

    function retiredIn(uint256 batchId) public view returns (uint64) {
        _requireBatch(batchId);
        Batch storage b = _batches[batchId];
        return b.retiredCursor - b.serialStart;
    }

    function circulatingIn(uint256 batchId) external view returns (uint64) {
        return issuedIn(batchId) - retiredIn(batchId);
    }

    function findRetirement(uint256 batchId, uint64 serial) external view returns (bool found, uint256 receiptId) {
        _requireBatch(batchId);
        Segment[] storage segs = _segments[batchId];
        uint256 lo = 0;
        uint256 hi = segs.length;
        while (lo < hi) {
            uint256 mid = (lo + hi) >> 1;
            if (segs[mid].start <= serial) lo = mid + 1;
            else hi = mid;
        }
        if (lo == 0) return (false, 0);
        uint64 id = segs[lo - 1].receiptId;
        if (serial < _receipts[id].serialEnd) return (true, id);
        return (false, 0);
    }

    function segmentCount(uint256 batchId) external view returns (uint256) {
        _requireBatch(batchId);
        return _segments[batchId].length;
    }

    function domainSeparator() public view returns (bytes32) {
        if (block.chainid == _cachedChainId) return _cachedDomain;
        return _buildDomain();
    }

    function _buildDomain() private view returns (bytes32) {
        return keccak256(
            abi.encode(
                DOMAIN_TYPEHASH,
                keccak256("CarbonCreditRetirementRegistry"),
                keccak256("1"),
                block.chainid,
                address(this)
            )
        );
    }

    function _requireBatch(uint256 batchId) private view {
        if (batchId == 0 || batchId > batchCount) revert UnknownBatch(batchId);
    }

    function _requireAuthorized(address holder) private view {
        if (msg.sender != holder && !isOperator[holder][msg.sender]) revert NotAuthorized(holder, msg.sender);
    }

    function _debit(uint256 batchId, address holder, uint256 amount) private {
        if (amount == 0) revert ZeroAmount();
        _requireBatch(batchId);
        if (_batches[batchId].frozen) revert BatchIsFrozen(batchId);
        uint256 have = balanceOf[batchId][holder];
        if (have < amount) revert InsufficientBalance(batchId, holder, have, amount);
        unchecked {
            balanceOf[batchId][holder] = have - amount;
        }
    }

    function _transfer(uint256 batchId, address from, address to, uint256 amount) private {
        if (to == address(0)) revert ZeroAddress();
        _debit(batchId, from, amount);
        balanceOf[batchId][to] += amount;
        emit Transfer(batchId, msg.sender, from, to, amount);
    }

    function _retire(
        uint256 batchId,
        address holder,
        uint256 amount,
        address beneficiary,
        bytes32 reasonHash,
        Claim claim
    ) private returns (uint256 receiptId) {
        if (beneficiary == address(0)) revert ZeroAddress();
        _debit(batchId, holder, amount);

        Batch storage b = _batches[batchId];
        uint64 start = b.retiredCursor;
        uint64 end = start + uint64(amount);
        b.retiredCursor = end;

        receiptId = ++receiptCount;
        _receipts[receiptId] = Receipt({
            batchId: batchId,
            holder: holder,
            beneficiary: beneficiary,
            serialStart: start,
            serialEnd: end,
            retiredAt: uint40(block.timestamp),
            claim: claim,
            reasonHash: reasonHash
        });
        _segments[batchId].push(Segment({start: start, receiptId: uint64(receiptId)}));

        emit Transfer(batchId, msg.sender, holder, address(0), amount);
        emit Retired(receiptId, batchId, beneficiary, holder, start, end, claim, reasonHash);
    }

    function _isValidSignature(address signer, bytes32 digest, bytes calldata sig) private view returns (bool) {
        if (signer.code.length == 0) {
            if (sig.length != 65) return false;
            bytes32 r = bytes32(sig[0:32]);
            bytes32 s = bytes32(sig[32:64]);
            uint8 v = uint8(sig[64]);
            if (uint256(s) > HALF_N) return false;
            if (v != 27 && v != 28) return false;
            address recovered = ecrecover(digest, v, r, s);
            return recovered != address(0) && recovered == signer;
        }
        (bool ok, bytes memory ret) =
            signer.staticcall(abi.encodeWithSelector(ERC1271_MAGIC, digest, sig));
        return ok && ret.length >= 32 && bytes4(abi.decode(ret, (bytes32))) == ERC1271_MAGIC;
    }
}
