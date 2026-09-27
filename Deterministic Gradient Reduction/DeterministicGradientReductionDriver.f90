program deterministic_gradient_reduction_driver
  use, intrinsic :: iso_fortran_env, only: real64, real128, int64, error_unit
  !$ use omp_lib
  use deterministic_gradient_reduction
  implicit none

  integer, parameter :: N = 2000003   ! deliberately odd and not a multiple
                                       ! of any common thread count or the
                                       ! leaf size, to stress the general,
                                       ! non-power-of-two path
  real(real64), allocatable :: x(:)
  real(real64) :: total
  real(real128) :: ground_truth, naive_total
  integer(int64) :: reference_fp, fp
  integer :: status, i, t
  integer, parameter :: THREAD_COUNTS(4) = [1, 2, 4, 8]
  logical :: all_bit_identical
  logical :: seeded_nan_ok, seeded_inf_ok

  if (.not. fast_math_canary_ok()) then
    write(error_unit,'(a)') 'FAIL: fast-math canary tripped. Rebuild ' // &
      'without -ffast-math / -Ofast / -fassociative-math; the double-double ' // &
      'arithmetic in this module is only exact under strict IEEE-754 rounding.'
    stop 1
  end if
  print '(a)', 'PASS: fast-math canary clean (strict IEEE-754 rounding confirmed)'

  allocate(x(N))
  call fill_gradient_like(x)

  ! Ground truth in quad precision, and a naive left-to-right double
  ! precision sum, so the compensated result's accuracy gain is a measured
  ! number rather than an assertion.
  ground_truth = 0.0_real128
  naive_total = 0.0_real128
  do i = 1, N
    ground_truth = ground_truth + real(x(i), real128)
  end do
  block
    real(real64) :: naive
    naive = 0.0_real64
    do i = 1, N
      naive = naive + x(i)
    end do
    naive_total = real(naive, real128)
  end block

  all_bit_identical = .true.
  reference_fp = 0_int64

  do t = 1, size(THREAD_COUNTS)
    !$ call omp_set_num_threads(THREAD_COUNTS(t))
    call reduce_sum(x, total, status)
    if (status /= STATUS_OK) then
      write(error_unit,'(a,a)') 'FAIL: unexpected status on clean input: ', &
        reduction_status(status)
      stop 1
    end if

    ! reduce_sum() returns the already-collapsed total; fingerprinting it
    ! against a zero low part is just a convenient way to turn "compare
    ! these floats bit for bit" into "compare these two 64-bit integers",
    ! so nobody is tempted to loosen the check to an epsilon comparison
    ! during a later refactor.
    fp = bit_fingerprint(total, 0.0_real64)

    if (t == 1) then
      reference_fp = fp
    else if (fp /= reference_fp) then
      all_bit_identical = .false.
    end if

    print '(a,i0,a,f0.6,a,z16.16)', 'threads=', THREAD_COUNTS(t), &
      '  sum=', total, '  fingerprint=', fp
  end do

  if (.not. all_bit_identical) then
    write(error_unit,'(a)') 'FAIL: reduction was not bit-identical across thread counts'
    stop 1
  end if
  print '(a)', 'PASS: bit-identical result across 1, 2, 4 and 8 threads'

  print '(a,es10.3)', 'relative error vs naive left-to-right fp64 sum: ', &
    abs(real((naive_total - ground_truth) / ground_truth, real64))
  print '(a,es10.3)', 'relative error vs deterministic tree sum:       ', &
    abs(real((real(total, real128) - ground_truth) / ground_truth, real64))

  call test_empty_input()
  call test_nan_detection(seeded_nan_ok)
  call test_inf_detection(seeded_inf_ok)

  if (.not. (seeded_nan_ok .and. seeded_inf_ok)) then
    write(error_unit,'(a)') 'FAIL: NaN/Inf detection did not behave as expected'
    stop 1
  end if
  print '(a)', 'PASS: NaN and Inf are flagged explicitly instead of silently propagating'

  print '(a)', 'ALL CHECKS PASSED'

contains

  ! Mirrors a realistic gradient tensor: mostly small values clustered
  ! near zero (typical of well-behaved layers) with occasional large
  ! outliers (typical of an exploding-gradient layer or a poorly scaled
  ! loss term). This mix is exactly what makes naive summation lossy:
  ! adding a huge outlier early makes every subsequent small value round
  ! away to nothing.
  subroutine fill_gradient_like(arr)
    real(real64), intent(out) :: arr(:)
    integer :: j
    real(real64) :: u
    do j = 1, size(arr)
      u = real(mod(j * 2654435761_int64, 1000003_int64), real64) / 1000003.0_real64
      arr(j) = (u - 0.5_real64) * 1.0e-4_real64
      ! Roughly every ten-thousandth element is a near-cancelling outlier
      ! pair (alternating sign) several orders of magnitude larger than
      ! the bulk of the data. A naive left-to-right sum loses the small
      ! values it adds right after a swing like this to rounding; that is
      ! exactly the failure mode compensated summation exists to fix.
      if (mod(j, 10007) == 0) then
        if (mod(j / 10007, 2) == 0) then
          arr(j) = arr(j) + 3.7e6_real64
        else
          arr(j) = arr(j) - 3.7e6_real64
        end if
      end if
    end do
  end subroutine fill_gradient_like

  subroutine test_empty_input()
    real(real64) :: empty(0)
    real(real64) :: r
    integer :: st
    call reduce_sum(empty, r, st)
    if (iand(st, STATUS_EMPTY) == 0 .or. r /= 0.0_real64) then
      write(error_unit,'(a)') 'FAIL: empty input was not flagged as STATUS_EMPTY'
      stop 1
    end if
    print '(a)', 'PASS: empty input returns 0 with STATUS_EMPTY'
  end subroutine test_empty_input

  subroutine test_nan_detection(ok)
    logical, intent(out) :: ok
    real(real64), allocatable :: y(:)
    real(real64) :: r
    integer :: st
    allocate(y(size(x)))
    y = x
    y(size(y) / 2) = ieee_value_nan()
    call reduce_sum(y, r, st)
    ok = (iand(st, STATUS_HAS_NAN) /= 0)
  end subroutine test_nan_detection

  subroutine test_inf_detection(ok)
    logical, intent(out) :: ok
    real(real64), allocatable :: y(:)
    real(real64) :: r
    integer :: st
    allocate(y(size(x)))
    y = x
    y(3) = ieee_value_inf()
    call reduce_sum(y, r, st)
    ok = (iand(st, STATUS_HAS_INF) /= 0)
  end subroutine test_inf_detection

  function ieee_value_nan() result(v)
    use, intrinsic :: ieee_arithmetic, only: ieee_value, ieee_quiet_nan
    real(real64) :: v
    v = ieee_value(v, ieee_quiet_nan)
  end function ieee_value_nan

  function ieee_value_inf() result(v)
    use, intrinsic :: ieee_arithmetic, only: ieee_value, ieee_positive_inf
    real(real64) :: v
    v = ieee_value(v, ieee_positive_inf)
  end function ieee_value_inf

end program deterministic_gradient_reduction_driver
