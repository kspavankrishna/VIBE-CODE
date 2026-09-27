module deterministic_gradient_reduction
  use, intrinsic :: iso_fortran_env, only: real32, real64, int64
  use, intrinsic :: ieee_arithmetic, only: ieee_is_nan, ieee_is_finite
  !$ use omp_lib
  implicit none
  private

  public :: reduce_sum
  public :: reduction_status
  public :: bit_fingerprint
  public :: fast_math_canary_ok
  public :: STATUS_OK, STATUS_EMPTY, STATUS_HAS_NAN, STATUS_HAS_INF

  integer, parameter :: STATUS_OK       = 0
  integer, parameter :: STATUS_EMPTY    = 1
  integer, parameter :: STATUS_HAS_NAN  = 2
  integer, parameter :: STATUS_HAS_INF  = 4

  ! Leaf size fixes where the recursive split bottoms out. It is a property
  ! of the algorithm, not of the machine: it never depends on the thread
  ! count, so the tree shape (and therefore the rounding) is identical
  ! whether this runs on one core or sixty-four.
  integer, parameter :: LEAF_SIZE = 2048

  ! Below this task granularity we stop forking OpenMP tasks and just
  ! recurse serially. This bounds task creation overhead without changing
  ! which elements land in which leaf: the split point mid = lo + (hi-lo)/2
  ! is a pure function of (lo, hi), so serial and parallel execution walk
  ! the exact same tree.
  integer, parameter :: TASK_GRANULARITY = 32 * LEAF_SIZE

  interface reduce_sum
    module procedure reduce_sum_r32
    module procedure reduce_sum_r64
  end interface reduce_sum

contains

  ! ---------------------------------------------------------------------
  ! Public entry point for real32 (fp32) buffers, e.g. model gradients
  ! or activations stored in single precision. Accumulation is promoted
  ! to double-double (two real64 values) internally so a billion-element
  ! fp32 buffer does not lose the small values to rounding the way a
  ! naive fp32 accumulator would.
  ! ---------------------------------------------------------------------
  subroutine reduce_sum_r32(x, total, status)
    real(real32), intent(in)  :: x(:)
    real(real64), intent(out) :: total
    integer,      intent(out) :: status
    real(real64), allocatable :: xd(:)
    allocate(xd(size(x)))
    xd = real(x, real64)
    call reduce_sum_r64(xd, total, status)
  end subroutine reduce_sum_r32

  ! ---------------------------------------------------------------------
  ! Public entry point for real64 (fp64) buffers.
  ! ---------------------------------------------------------------------
  subroutine reduce_sum_r64(x, total, status)
    real(real64), intent(in)  :: x(:)
    real(real64), intent(out) :: total
    integer,      intent(out) :: status
    real(real64) :: hi, lo
    integer :: n

    n = size(x)
    status = STATUS_OK

    if (n == 0) then
      total = 0.0_real64
      status = ior(status, STATUS_EMPTY)
      return
    end if

    call classify_finiteness(x, status)

    !$omp parallel
    !$omp single
    call tree_sum(x, 1, n, hi, lo)
    !$omp end single
    !$omp end parallel

    total = hi + lo
  end subroutine reduce_sum_r64

  ! ---------------------------------------------------------------------
  ! Scans for NaN / Inf up front so the caller gets an explicit, named
  ! reason instead of silently receiving a NaN total (which is what a
  ! bare Inf - Inf cancellation would otherwise produce). This mirrors
  ! the finite-check every mixed precision training loop already needs
  ! to run before an optimizer step.
  ! ---------------------------------------------------------------------
  subroutine classify_finiteness(x, status)
    real(real64), intent(in)    :: x(:)
    integer,      intent(inout) :: status
    integer :: i
    logical :: has_nan, has_inf
    has_nan = .false.
    has_inf = .false.
    !$omp parallel do reduction(.or.:has_nan,has_inf)
    do i = 1, size(x)
      if (ieee_is_nan(x(i))) then
        has_nan = .true.
      else if (.not. ieee_is_finite(x(i))) then
        has_inf = .true.
      end if
    end do
    !$omp end parallel do
    if (has_nan) status = ior(status, STATUS_HAS_NAN)
    if (has_inf) status = ior(status, STATUS_HAS_INF)
  end subroutine classify_finiteness

  ! ---------------------------------------------------------------------
  ! The canonical reduction tree. (lo, hi) alone determines the split, so
  ! two calls with the same array and the same n always combine the same
  ! pair of numbers at every node, regardless of how many OpenMP threads
  ! are live or how the scheduler interleaves them. Each subtree is
  ! carried as a double-double pair (hi_part, lo_part): hi_part holds the
  ! rounded value, lo_part the part that fell off, so information is not
  ! lost the way it would be if every task result were truncated back to
  ! one real64 before combining.
  ! ---------------------------------------------------------------------
  recursive subroutine tree_sum(x, lo, hi, sum_hi, sum_lo)
    real(real64), intent(in)  :: x(:)
    integer,      intent(in)  :: lo, hi
    real(real64), intent(out) :: sum_hi, sum_lo
    integer :: mid
    real(real64) :: lhi, llo, rhi, rlo

    if (hi - lo < LEAF_SIZE) then
      call leaf_sum(x, lo, hi, sum_hi, sum_lo)
      return
    end if

    mid = lo + (hi - lo) / 2

    if (hi - lo >= TASK_GRANULARITY) then
      ! lhi/llo are declared shared, not private: the task's write to them
      ! must be visible to this frame after taskwait, not confined to a
      ! private copy that vanishes when the task ends.
      !$omp task shared(x,lhi,llo) firstprivate(lo,mid) default(none)
      call tree_sum(x, lo, mid, lhi, llo)
      !$omp end task
      call tree_sum(x, mid + 1, hi, rhi, rlo)
      !$omp taskwait
      call dd_add(lhi, llo, rhi, rlo, sum_hi, sum_lo)
    else
      call tree_sum(x, lo, mid, lhi, llo)
      call tree_sum(x, mid + 1, hi, rhi, rlo)
      call dd_add(lhi, llo, rhi, rlo, sum_hi, sum_lo)
    end if
  end subroutine tree_sum

  ! ---------------------------------------------------------------------
  ! Leaf accumulation: Ogita-Rump-Oishi "Sum2" compensated summation,
  ! strictly left to right over x(lo:hi). This is the one place actual
  ! floating point additions of raw input values happen; everywhere else
  ! we only ever add together already-computed subtree pairs.
  ! ---------------------------------------------------------------------
  subroutine leaf_sum(x, lo, hi, sum_hi, sum_lo)
    real(real64), intent(in)  :: x(:)
    integer,      intent(in)  :: lo, hi
    real(real64), intent(out) :: sum_hi, sum_lo
    real(real64) :: s, c, e, s_next
    integer :: i

    s = 0.0_real64
    c = 0.0_real64
    do i = lo, hi
      ! s_next is a separate variable from s: two_sum's INTENT(OUT) result
      ! must never alias one of its own INTENT(IN) actual arguments, or the
      ! Fortran standard leaves the call undefined.
      call two_sum(s, x(i), s_next, e)
      s = s_next
      c = c + e
    end do
    call quick_two_sum(s, c, sum_hi, sum_lo)
  end subroutine leaf_sum

  ! ---------------------------------------------------------------------
  ! Knuth's TwoSum: for any two IEEE-754 doubles a, b, this recovers the
  ! exact rounding error of a + b, so that (a + b) == s + e in infinite
  ! precision. Requires round-to-nearest and no reassociation, i.e. no
  ! -ffast-math / -Ofast / -fassociative-math on this translation unit.
  ! ---------------------------------------------------------------------
  pure subroutine two_sum(a, b, s, e)
    real(real64), intent(in)  :: a, b
    real(real64), intent(out) :: s, e
    real(real64) :: bb
    s = a + b
    bb = s - a
    e = (a - (s - bb)) + (b - bb)
  end subroutine two_sum

  ! ---------------------------------------------------------------------
  ! Dekker's QuickTwoSum: same contract as two_sum but requires the
  ! caller to guarantee |a| >= |b|, which holds here because c only ever
  ! accumulates rounding errors, each of which is bounded by one ULP of s.
  ! ---------------------------------------------------------------------
  pure subroutine quick_two_sum(a, b, s, e)
    real(real64), intent(in)  :: a, b
    real(real64), intent(out) :: s, e
    s = a + b
    e = b - (s - a)
  end subroutine quick_two_sum

  ! ---------------------------------------------------------------------
  ! Double-double addition: combines two (hi,lo) pairs into one, folding
  ! both low parts into the correction term before a final renormalizing
  ! quick_two_sum. This is what lets the tree carry sub-ULP precision all
  ! the way from the leaves to the root instead of losing it at every
  ! internal node.
  ! ---------------------------------------------------------------------
  pure subroutine dd_add(a_hi, a_lo, b_hi, b_lo, r_hi, r_lo)
    real(real64), intent(in)  :: a_hi, a_lo, b_hi, b_lo
    real(real64), intent(out) :: r_hi, r_lo
    real(real64) :: s, e
    call two_sum(a_hi, b_hi, s, e)
    e = e + a_lo + b_lo
    call quick_two_sum(s, e, r_hi, r_lo)
  end subroutine dd_add

  ! ---------------------------------------------------------------------
  ! Decodes a reduction's raw status bitmask into words, for logging.
  ! ---------------------------------------------------------------------
  function reduction_status(status) result(msg)
    integer, intent(in) :: status
    character(len=:), allocatable :: msg
    msg = ""
    if (status == STATUS_OK) then
      msg = "ok"
      return
    end if
    if (iand(status, STATUS_EMPTY) /= 0)   msg = msg // "empty "
    if (iand(status, STATUS_HAS_NAN) /= 0) msg = msg // "has_nan "
    if (iand(status, STATUS_HAS_INF) /= 0) msg = msg // "has_inf "
  end function reduction_status

  ! ---------------------------------------------------------------------
  ! Bitwise fingerprint of a double-double result, via a splitmix64-style
  ! mixer over the raw IEEE bit patterns of hi and lo. Two runs (any
  ! thread count, any machine, same compiler ABI) that produced this same
  ! fingerprint added the exact same bits in the exact same order.
  ! Comparing floating point results with `==` invites someone to relax
  ! it to "close enough" during a later refactor; comparing 64-bit integer
  ! fingerprints does not leave that door open.
  ! ---------------------------------------------------------------------
  function bit_fingerprint(hi, lo) result(fp)
    real(real64), intent(in) :: hi, lo
    integer(int64) :: fp
    integer(int64) :: a, b
    a = transfer(hi, a)
    b = transfer(lo, b)
    fp = mix64(ieor(a, mix64(b)))
  end function bit_fingerprint

  pure function mix64(z_in) result(z)
    integer(int64), intent(in) :: z_in
    integer(int64) :: z
    z = z_in
    z = ieor(z, shiftr(z, 30)) * (-4658895280553007687_int64)
    z = ieor(z, shiftr(z, 27)) * (-7723592293110705685_int64)
    z = ieor(z, shiftr(z, 31))
  end function mix64

  ! ---------------------------------------------------------------------
  ! Runtime canary for -ffast-math / -Ofast contamination. On a
  ! standards-compliant IEEE-754 round-to-nearest build, adding 1.0 to a
  ! value already past the precision where 1.0 is representable in the
  ! sum must round back down to the original value, so the round trip
  ! below is exactly zero. Under -ffast-math the compiler is permitted to
  ! reassociate this into something that is not zero. Call this once at
  ! startup and refuse to trust "deterministic" results if it fails: a
  ! mis-flagged build is a silent correctness bug, not a warning.
  ! ---------------------------------------------------------------------
  function fast_math_canary_ok() result(ok)
    logical :: ok
    real(real64) :: probe, s, e
    probe = 9007199254740992.0_real64   ! 2^53
    call two_sum(probe, 1.0_real64, s, e)
    ok = (s == probe) .and. (e == 1.0_real64)
  end function fast_math_canary_ok

end module deterministic_gradient_reduction
