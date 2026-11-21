! ANN Recall Auditor
! Certified exact ground truth and tie aware recall@k for approximate nearest neighbour indexes.
! Build: gfortran -O3 -march=native -fopenmp AnnRecallAuditor.f90 -o ann-recall-auditor
! Full documentation lives in README.md next to this file.

module ann_npy_io
  use, intrinsic :: iso_fortran_env, only: int8, int32, int64, real32, real64, error_unit
  use, intrinsic :: ieee_arithmetic, only: ieee_is_finite
  implicit none
  private

  integer, parameter, public :: EXIT_PASS = 0
  integer, parameter, public :: EXIT_GATE_FAIL = 1
  integer, parameter, public :: EXIT_INPUT_ERROR = 2

  integer(int64), parameter :: READ_CHUNK_ELEMENTS = 4194304_int64
  integer(int64), parameter :: MAX_HEADER_BYTES = 1048576_int64

  type, public :: npy_header
    character(len=1) :: kind = ' '
    integer :: itemsize = 0
    logical :: fortran_order = .false.
    integer :: ndim = 0
    integer(int64) :: dims(2) = 0
    integer(int64) :: data_pos = 0
  end type npy_header

  public :: io_fail, io_warn, host_is_little_endian
  public :: npy_read_header, npy_read_vectors, npy_read_ids, npy_write_ids

contains

  subroutine io_fail(msg)
    character(len=*), intent(in) :: msg
    write(error_unit, '(a)') 'ann-recall-auditor: error: '//trim(msg)
    stop EXIT_INPUT_ERROR, quiet=.true.
  end subroutine io_fail

  subroutine io_warn(msg)
    character(len=*), intent(in) :: msg
    write(error_unit, '(a)') 'ann-recall-auditor: warning: '//trim(msg)
  end subroutine io_warn

  logical function host_is_little_endian()
    integer(int8) :: probe(4)
    probe = transfer(1_int32, probe)
    host_is_little_endian = (probe(1) == 1_int8)
  end function host_is_little_endian

  function itoa(v) result(s)
    integer(int64), intent(in) :: v
    character(len=:), allocatable :: s
    character(len=32) :: buf
    write(buf, '(i0)') v
    s = trim(buf)
  end function itoa

  ! Finds a dictionary key written with either quote style and returns the
  ! position just after its colon, or 0 when the key is absent.
  integer function key_value_start(text, key)
    character(len=*), intent(in) :: text, key
    integer :: p, c
    key_value_start = 0
    p = index(text, "'"//key//"'")
    if (p == 0) p = index(text, '"'//key//'"')
    if (p == 0) return
    c = index(text(p:), ':')
    if (c == 0) return
    key_value_start = p + c
  end function key_value_start

  subroutine npy_read_header(path, h, unit)
    character(len=*), intent(in) :: path
    type(npy_header), intent(out) :: h
    integer, intent(out) :: unit
    character(len=6) :: magic
    character(len=1) :: b(4)
    character(len=:), allocatable :: text, descr, inner, tok, rest
    integer :: ios, major, lenbytes, i, p, q, c
    integer(int64) :: hlen, fsize, need, elems

    open(newunit=unit, file=path, access='stream', form='unformatted', status='old', &
         action='read', iostat=ios)
    if (ios /= 0) call io_fail('cannot open '//path)
    inquire(unit=unit, size=fsize)
    if (fsize < 12) call io_fail(path//' is too small to be a .npy file')

    read(unit, pos=1, iostat=ios) magic
    if (ios /= 0 .or. magic /= char(147)//'NUMPY') call io_fail(path//' is not a .npy file (bad magic)')
    read(unit, pos=7, iostat=ios) b(1:2)
    major = ichar(b(1))
    select case (major)
    case (1)
      lenbytes = 2
    case (2, 3)
      lenbytes = 4
    case default
      call io_fail(path//' uses unsupported .npy format version '//itoa(int(major, int64)))
    end select
    read(unit, pos=9, iostat=ios) b(1:lenbytes)
    if (ios /= 0) call io_fail(path//' has a truncated .npy preamble')
    hlen = 0
    do i = lenbytes, 1, -1
      hlen = hlen * 256_int64 + int(ichar(b(i)), int64)
    end do
    if (hlen <= 0 .or. hlen > MAX_HEADER_BYTES) call io_fail(path//' has an implausible header length')
    if (8 + lenbytes + hlen > fsize) call io_fail(path//' header runs past the end of the file')
    allocate(character(len=hlen) :: text)
    read(unit, pos=9 + lenbytes, iostat=ios) text
    if (ios /= 0) call io_fail(path//' header could not be read')
    h%data_pos = 9_int64 + lenbytes + hlen

    ! descr, for example '<f4'
    p = key_value_start(text, 'descr')
    if (p == 0) call io_fail(path//' header has no descr')
    q = scan(text(p:), "'"//'"')
    if (q == 0) call io_fail(path//' header descr is not a simple dtype string (structured dtypes are not supported)')
    q = p + q
    c = scan(text(q:), "'"//'"')
    if (c == 0) call io_fail(path//' header descr is unterminated')
    descr = text(q:q + c - 2)
    if (len(descr) < 3) call io_fail(path//' has an unrecognised dtype '//descr)
    h%kind = descr(2:2)
    read(descr(3:), *, iostat=ios) h%itemsize
    if (ios /= 0) call io_fail(path//' has an unrecognised dtype '//descr)
    if (descr(1:1) == '>' .and. h%itemsize > 1) &
      call io_fail(path//' is big endian ('//descr//'). Convert with arr.astype(arr.dtype.newbyteorder("<")) first')
    if (descr(1:1) /= '<' .and. descr(1:1) /= '=' .and. descr(1:1) /= '|') &
      call io_fail(path//' has an unrecognised byte order in '//descr)

    ! fortran_order
    p = key_value_start(text, 'fortran_order')
    if (p == 0) call io_fail(path//' header has no fortran_order')
    h%fortran_order = (index(adjustl(text(p:)), 'True') == 1)

    ! shape
    p = key_value_start(text, 'shape')
    if (p == 0) call io_fail(path//' header has no shape')
    q = index(text(p:), '(')
    c = index(text(p:), ')')
    if (q == 0 .or. c == 0 .or. c < q) call io_fail(path//' header shape is malformed')
    inner = text(p + q:p + c - 2)
    h%ndim = 0
    rest = inner
    do
      c = index(rest, ',')
      if (c == 0) then
        tok = rest
        rest = ''
      else
        tok = rest(:c - 1)
        rest = rest(c + 1:)
      end if
      tok = trim(adjustl(tok))
      if (len(tok) > 0) then
        if (tok(len(tok):len(tok)) == 'L') tok = tok(:len(tok) - 1)
        h%ndim = h%ndim + 1
        if (h%ndim > 2) call io_fail(path//' must be one or two dimensional')
        read(tok, *, iostat=ios) h%dims(h%ndim)
        if (ios /= 0 .or. h%dims(h%ndim) < 0) call io_fail(path//' header shape has a bad dimension: '//tok)
      end if
      if (c == 0) exit
    end do
    if (h%ndim == 0) call io_fail(path//' holds a scalar, expected a matrix')
    if (h%ndim == 1) h%dims(2) = 1

    elems = h%dims(1) * h%dims(2)
    need = h%data_pos - 1 + elems * h%itemsize
    if (need > fsize) call io_fail(path//' is truncated: header promises '//itoa(need)// &
                                   ' bytes, file has '//itoa(fsize))
    if (need < fsize) call io_warn(path//' has '//itoa(fsize - need)//' trailing bytes after the array')
  end subroutine npy_read_header

  ! Reads an (n, d) float32 or float64 .npy matrix into x(d, n) so that every
  ! vector is one contiguous column. Rejects NaN and Inf with the row number.
  subroutine npy_read_vectors(path, label, x)
    character(len=*), intent(in) :: path, label
    real(real32), allocatable, intent(out) :: x(:,:)
    type(npy_header) :: h
    integer :: u, ios, st
    integer(int64) :: n, d, chunk, i0, i1, j, pos, i
    real(real32), allocatable :: buf4(:)
    real(real64), allocatable :: buf8(:,:), col8(:)

    call npy_read_header(path, h, u)
    if (h%kind /= 'f' .or. (h%itemsize /= 4 .and. h%itemsize /= 8)) &
      call io_fail(label//' '//path//' must hold float32 or float64 values')
    if (h%ndim == 1) then
      n = 1
      d = h%dims(1)
    else
      n = h%dims(1)
      d = h%dims(2)
    end if
    if (n < 1 .or. d < 1) call io_fail(label//' '//path//' is empty')
    allocate(x(d, n), stat=st)
    if (st /= 0) call io_fail('cannot allocate '//itoa(n * d * 4)//' bytes for '//label)

    if (.not. h%fortran_order .or. h%ndim == 1) then
      chunk = max(1_int64, READ_CHUNK_ELEMENTS / d)
      if (h%itemsize == 8) allocate(buf8(d, min(chunk, n)))
      do i0 = 1, n, chunk
        i1 = min(n, i0 + chunk - 1)
        pos = h%data_pos + (i0 - 1) * d * h%itemsize
        if (h%itemsize == 4) then
          read(u, pos=pos, iostat=ios) x(:, i0:i1)
        else
          read(u, pos=pos, iostat=ios) buf8(:, 1:i1 - i0 + 1)
          x(:, i0:i1) = real(buf8(:, 1:i1 - i0 + 1), real32)
        end if
        if (ios /= 0) call io_fail('read failed in '//path)
      end do
    else
      ! Fortran ordered file: each original column is one dimension across all vectors.
      if (h%itemsize == 4) then
        allocate(buf4(n))
      else
        allocate(col8(n))
      end if
      do j = 1, d
        pos = h%data_pos + (j - 1) * n * h%itemsize
        if (h%itemsize == 4) then
          read(u, pos=pos, iostat=ios) buf4
          x(j, :) = buf4
        else
          read(u, pos=pos, iostat=ios) col8
          x(j, :) = real(col8, real32)
        end if
        if (ios /= 0) call io_fail('read failed in '//path)
      end do
    end if
    close(u)

    do i = 1, n
      if (.not. all(ieee_is_finite(x(:, i)))) &
        call io_fail(label//' row '//itoa(i - 1)//' in '//path// &
                     ' contains NaN or Inf (or a float64 value outside float32 range)')
    end do
  end subroutine npy_read_vectors

  ! Reads an (n, k) integer .npy matrix of neighbour ids into ids(k, n).
  subroutine npy_read_ids(path, label, ids)
    character(len=*), intent(in) :: path, label
    integer(int64), allocatable, intent(out) :: ids(:,:)
    type(npy_header) :: h
    integer :: u, ios, st
    integer(int64) :: n, k, j, pos, i0, i1, chunk
    integer(int32), allocatable :: b4(:,:), c4(:)
    integer(int64), allocatable :: c8(:)

    call npy_read_header(path, h, u)
    if ((h%kind /= 'i' .and. h%kind /= 'u') .or. (h%itemsize /= 4 .and. h%itemsize /= 8)) &
      call io_fail(label//' '//path//' must hold int32, int64, uint32 or uint64 ids')
    n = h%dims(1)
    k = h%dims(2)
    if (n < 1 .or. k < 1) call io_fail(label//' '//path//' is empty')
    allocate(ids(k, n), stat=st)
    if (st /= 0) call io_fail('cannot allocate id matrix for '//label)

    if (.not. h%fortran_order .or. h%ndim == 1) then
      if (h%itemsize == 8) then
        read(u, pos=h%data_pos, iostat=ios) ids
        if (ios /= 0) call io_fail('read failed in '//path)
      else
        chunk = max(1_int64, READ_CHUNK_ELEMENTS / k)
        allocate(b4(k, min(chunk, n)))
        do i0 = 1, n, chunk
          i1 = min(n, i0 + chunk - 1)
          pos = h%data_pos + (i0 - 1) * k * 4
          read(u, pos=pos, iostat=ios) b4(:, 1:i1 - i0 + 1)
          if (ios /= 0) call io_fail('read failed in '//path)
          ids(:, i0:i1) = int(b4(:, 1:i1 - i0 + 1), int64)
        end do
      end if
    else
      if (h%itemsize == 4) then
        allocate(c4(n))
      else
        allocate(c8(n))
      end if
      do j = 1, k
        pos = h%data_pos + (j - 1) * n * h%itemsize
        if (h%itemsize == 4) then
          read(u, pos=pos, iostat=ios) c4
          ids(j, :) = int(c4, int64)
        else
          read(u, pos=pos, iostat=ios) c8
          ids(j, :) = c8
        end if
        if (ios /= 0) call io_fail('read failed in '//path)
      end do
    end if
    close(u)

    ! uint32 values above 2**31 arrive negative through int32: restore them.
    ! uint64 values above 2**63 stay negative and are later treated as invalid.
    if (h%kind == 'u' .and. h%itemsize == 4) then
      where (ids < 0) ids = ids + 4294967296_int64
    end if
  end subroutine npy_read_ids

  ! Writes ids(k, n) as an (n, k) C ordered little endian int64 .npy file.
  subroutine npy_write_ids(path, ids)
    character(len=*), intent(in) :: path
    integer(int64), intent(in) :: ids(:,:)
    character(len=:), allocatable :: hdr
    integer :: u, ios, total, pad
    integer(int64) :: n, k

    k = size(ids, 1, kind=int64)
    n = size(ids, 2, kind=int64)
    hdr = "{'descr': '<i8', 'fortran_order': False, 'shape': ("//itoa(n)//', '//itoa(k)//'), }'
    total = 10 + len(hdr) + 1
    pad = modulo(64 - modulo(total, 64), 64)
    hdr = hdr//repeat(' ', pad)//achar(10)
    open(newunit=u, file=path, access='stream', form='unformatted', status='replace', &
         action='write', iostat=ios)
    if (ios /= 0) call io_fail('cannot create '//path)
    write(u, iostat=ios) char(147)//'NUMPY'//achar(1)//achar(0)// &
                         achar(modulo(len(hdr), 256))//achar(len(hdr) / 256)//hdr
    if (ios == 0) write(u, iostat=ios) ids
    if (ios /= 0) call io_fail('write failed for '//path)
    close(u)
  end subroutine npy_write_ids

end module ann_npy_io


module ann_recall_core
  use, intrinsic :: iso_fortran_env, only: int64, real32, real64
  !$ use omp_lib
  implicit none
  private

  integer, parameter, public :: METRIC_L2 = 1
  integer, parameter, public :: METRIC_IP = 2
  integer, parameter, public :: METRIC_COS = 3

  integer, parameter, public :: DEFAULT_K = 10
  integer, parameter, public :: DEFAULT_MARGIN = 32
  integer, parameter, public :: DEFAULT_QUERY_BLOCK = 256
  integer, parameter, public :: DEFAULT_BASE_BLOCK = 8192
  integer, parameter, public :: DEFAULT_BOOTSTRAP = 2000
  integer(int64), parameter, public :: DEFAULT_SEED = 20261121_int64
  real(real64), parameter, public :: DEFAULT_TIE_REL = 1.0e-7_real64
  real(real64), parameter, public :: DEFAULT_CONFIDENCE = 0.95_real64

  ! Unit roundoff of IEEE binary32. The approximate pass accumulates dot
  ! products in float32, so its error is bounded by gamma_d = d*u / (1 - d*u).
  real(real64), parameter :: UNIT_ROUNDOFF_F32 = 2.0_real64**(-24)
  real(real64), parameter :: BOUND_SAFETY = 1.01_real64
  integer(int64), parameter :: EMPTY_ID = huge(0_int64)

  type, public :: ground_truth
    integer :: k = 0
    integer(int64), allocatable :: ids(:,:)        ! (k, nq) 1 based base column, exact order
    real(real64), allocatable :: dist(:,:)         ! (k, nq) exact float64 distance
    logical, allocatable :: certified(:)           ! proven exact for this query
    logical, allocatable :: fell_back(:)           ! needed a full float64 rescan
    logical :: from_file = .false.
    real(real64) :: seconds = 0
  end type ground_truth

  type, public :: query_result
    logical :: evaluated = .false.
    real(real64) :: recall_tie = 0
    real(real64) :: recall_id = 0
    logical :: ratio_ok = .false.
    real(real64) :: ratio = 0
    integer :: missing = 0
    integer :: out_of_range = 0
    integer :: duplicates = 0
    integer :: hits_tie = 0
    integer :: hits_id = 0
    integer :: probe_minus = 0
    integer :: probe_plus = 0
  end type query_result

  public :: vector_norms, exact_ground_truth, load_ground_truth, evaluate_query
  public :: bootstrap_mean_ci, quantile_sorted, sort_real_with_index, distance_bound

contains

  subroutine vector_norms(x, norms)
    real(real32), intent(in) :: x(:,:)
    real(real64), intent(out) :: norms(:)
    integer(int64) :: i, j
    real(real64) :: acc
    !$omp parallel do private(i, j, acc) schedule(static)
    do i = 1, size(x, 2, kind=int64)
      acc = 0
      do j = 1, size(x, 1, kind=int64)
        acc = acc + real(x(j, i), real64)**2
      end do
      norms(i) = sqrt(acc)
    end do
    !$omp end parallel do
  end subroutine vector_norms

  ! Smaller is always closer: inner product is negated, cosine is 1 - cos.
  pure real(real64) function exact_distance(q, b, metric, qnorm, bnorm) result(dist)
    real(real32), intent(in) :: q(:), b(:)
    integer, intent(in) :: metric
    real(real64), intent(in) :: qnorm, bnorm
    integer :: j
    real(real64) :: acc, t
    acc = 0
    select case (metric)
    case (METRIC_L2)
      do j = 1, size(q)
        t = real(q(j), real64) - real(b(j), real64)
        acc = acc + t * t
      end do
      dist = acc
    case (METRIC_IP)
      do j = 1, size(q)
        acc = acc + real(q(j), real64) * real(b(j), real64)
      end do
      dist = -acc
    case default
      if (qnorm == 0 .or. bnorm == 0) then
        dist = 1
      else
        do j = 1, size(q)
          acc = acc + real(q(j), real64) * real(b(j), real64)
        end do
        dist = 1 - acc / (qnorm * bnorm)
      end if
    end select
  end function exact_distance

  pure real(real64) function approx_distance(dot, metric, qnorm, bnorm) result(dist)
    real(real64), intent(in) :: dot, qnorm, bnorm
    integer, intent(in) :: metric
    select case (metric)
    case (METRIC_L2)
      dist = qnorm * qnorm + bnorm * bnorm - 2 * dot
    case (METRIC_IP)
      dist = -dot
    case default
      if (qnorm == 0 .or. bnorm == 0) then
        dist = 1
      else
        dist = 1 - dot / (qnorm * bnorm)
      end if
    end select
  end function approx_distance

  ! Upper bound on |approx_distance - exact_distance| for one query against any
  ! base vector, from the float32 dot product error bound |fl(q.b) - q.b| <=
  ! gamma_d |q| |b|, which holds for every summation order a matmul may choose.
  pure real(real64) function distance_bound(metric, d, qnorm, bmax) result(e)
    integer, intent(in) :: metric
    integer(int64), intent(in) :: d
    real(real64), intent(in) :: qnorm, bmax
    real(real64) :: du, gamma, eps64
    du = real(d, real64) * UNIT_ROUNDOFF_F32
    eps64 = epsilon(1.0_real64)
    if (du >= 0.5_real64) then
      e = huge(1.0_real64)
      return
    end if
    gamma = du / (1 - du)
    select case (metric)
    case (METRIC_L2)
      e = BOUND_SAFETY * 2 * gamma * qnorm * bmax + (d + 8) * eps64 * (qnorm * qnorm + bmax * bmax)
    case (METRIC_IP)
      e = BOUND_SAFETY * gamma * qnorm * bmax + (d + 8) * eps64 * qnorm * bmax
    case default
      e = BOUND_SAFETY * gamma + (d + 8) * eps64
    end select
  end function distance_bound

  pure logical function precedes(d1, i1, d2, i2)
    real(real64), intent(in) :: d1, d2
    integer(int64), intent(in) :: i1, i2
    precedes = (d1 < d2) .or. (d1 == d2 .and. i1 < i2)
  end function precedes

  ! Offers one candidate to a list kept sorted by (distance, id).
  pure subroutine offer(cd, ci, dist, id)
    real(real64), intent(inout) :: cd(:)
    integer(int64), intent(inout) :: ci(:)
    real(real64), intent(in) :: dist
    integer(int64), intent(in) :: id
    integer :: p
    p = size(cd)
    if (.not. precedes(dist, id, cd(p), ci(p))) return
    do while (p > 1)
      if (.not. precedes(dist, id, cd(p - 1), ci(p - 1))) exit
      cd(p) = cd(p - 1)
      ci(p) = ci(p - 1)
      p = p - 1
    end do
    cd(p) = dist
    ci(p) = id
  end subroutine offer

  ! Insertion sort by (distance, id). The input comes out of the float32 pass
  ! almost sorted already, so this runs close to linear time in practice.
  pure subroutine sort_pairs(cd, ci)
    real(real64), intent(inout) :: cd(:)
    integer(int64), intent(inout) :: ci(:)
    integer :: i, p
    real(real64) :: dv
    integer(int64) :: iv
    do i = 2, size(cd)
      dv = cd(i)
      iv = ci(i)
      p = i
      do while (p > 1)
        if (.not. precedes(dv, iv, cd(p - 1), ci(p - 1))) exit
        cd(p) = cd(p - 1)
        ci(p) = ci(p - 1)
        p = p - 1
      end do
      cd(p) = dv
      ci(p) = iv
    end do
  end subroutine sort_pairs

  ! Two pass exact search. Pass one is a blocked float32 matmul that keeps the
  ! best kc = k + margin candidates per query. Pass two recomputes those in
  ! float64 and proves, with distance_bound, that no excluded base vector could
  ! beat the k-th exact distance. Queries that cannot be proven are rescanned
  ! against the whole base in float64.
  subroutine exact_ground_truth(base, queries, bnorm, qnorm, metric, k, margin, qblock, bblock, gt)
    real(real32), intent(in) :: base(:,:), queries(:,:)
    real(real64), intent(in) :: bnorm(:), qnorm(:)
    integer, intent(in) :: metric, k, margin, qblock, bblock
    type(ground_truth), intent(out) :: gt
    integer(int64) :: d, nb, nq, qs, qe, bs, be, nqb, nbb, jq, jb, q, b, qb, nblocks, iblk
    integer :: kc, threads
    real(real64) :: bmax, approx
    real(real64), allocatable :: cd(:,:)
    integer(int64), allocatable :: ci(:,:)
    real(real32), allocatable :: s(:,:)
    integer(int64) :: t0, t1, rate

    call system_clock(t0, rate)
    d = size(base, 1, kind=int64)
    nb = size(base, 2, kind=int64)
    nq = size(queries, 2, kind=int64)
    kc = int(min(nb, int(k, int64) + int(margin, int64)))
    bmax = maxval(bnorm)

    gt%k = k
    allocate(gt%ids(k, nq), gt%dist(k, nq), gt%certified(nq), gt%fell_back(nq))
    allocate(cd(kc, nq), ci(kc, nq))

    ! Shrink query blocks when there are few queries so every thread gets work.
    threads = 1
    !$ threads = omp_get_max_threads()
    qb = min(int(qblock, int64), max(1_int64, (nq + 2 * threads - 1) / (2 * threads)))
    nblocks = (nq + qb - 1) / qb

    ! Each thread owns whole query blocks and its own product buffer, so the
    ! float32 matmul itself runs in parallel without any shared writes.
    !$omp parallel default(shared) private(s, iblk, qs, qe, nqb, bs, be, nbb, jq, jb, q, b, approx)
    allocate(s(min(int(bblock, int64), nb), qb))
    !$omp do schedule(dynamic, 1)
    do iblk = 0, nblocks - 1
      qs = iblk * qb + 1
      qe = min(nq, qs + qb - 1)
      nqb = qe - qs + 1
      cd(:, qs:qe) = huge(1.0_real64)
      ci(:, qs:qe) = EMPTY_ID
      do bs = 1, nb, bblock
        be = min(nb, bs + bblock - 1)
        nbb = be - bs + 1
        s(1:nbb, 1:nqb) = matmul(transpose(base(:, bs:be)), queries(:, qs:qe))
        do jq = 1, nqb
          q = qs + jq - 1
          do jb = 1, nbb
            b = bs + jb - 1
            approx = approx_distance(real(s(jb, jq), real64), metric, qnorm(q), bnorm(b))
            ! Base ids rise monotonically through the scan, so a strict test
            ! is the same as the full (distance, id) ordering here.
            if (approx < cd(kc, q)) call offer(cd(:, q), ci(:, q), approx, b)
          end do
        end do
      end do
    end do
    !$omp end do
    deallocate(s)
    !$omp end parallel

    !$omp parallel do private(q) schedule(dynamic, 8)
    do q = 1, nq
      call finalize_query(q)
    end do
    !$omp end parallel do

    call system_clock(t1)
    gt%seconds = real(t1 - t0, real64) / real(rate, real64)

  contains

    subroutine finalize_query(qi)
      integer(int64), intent(in) :: qi
      real(real64) :: ed(kc), wall, bound, full_d(k)
      integer(int64) :: ei(kc), full_i(k), j, bj
      wall = cd(kc, qi)
      ei = ci(:, qi)
      do j = 1, kc
        ed(j) = exact_distance(queries(:, qi), base(:, ei(j)), metric, qnorm(qi), bnorm(ei(j)))
      end do
      call sort_pairs(ed, ei)
      bound = distance_bound(metric, d, qnorm(qi), bmax)
      gt%fell_back(qi) = .false.
      if (nb <= kc) then
        gt%certified(qi) = .true.
      else
        ! Every excluded vector had approx >= wall, so its exact distance is at
        ! least wall - bound. If that clears the k-th exact distance, no
        ! excluded vector can be closer and the threshold is proven.
        gt%certified(qi) = (ed(k) <= wall - bound)
      end if
      if (gt%certified(qi)) then
        gt%ids(:, qi) = ei(1:k)
        gt%dist(:, qi) = ed(1:k)
      else
        full_d = huge(1.0_real64)
        full_i = EMPTY_ID
        do bj = 1, nb
          call offer(full_d, full_i, &
                     exact_distance(queries(:, qi), base(:, bj), metric, qnorm(qi), bnorm(bj)), bj)
        end do
        gt%ids(:, qi) = full_i
        gt%dist(:, qi) = full_d
        gt%certified(qi) = .true.
        gt%fell_back(qi) = .true.
      end if
    end subroutine finalize_query

  end subroutine exact_ground_truth

  ! Uses a cached ground truth file instead of searching. Distances are still
  ! recomputed in float64 so the tie threshold is exact for the ids given, but
  ! the ids themselves are trusted, so nothing is marked certified.
  subroutine load_ground_truth(raw, id_base, base, queries, bnorm, qnorm, metric, k, gt, problem)
    integer(int64), intent(in) :: raw(:,:)
    integer, intent(in) :: id_base, metric, k
    real(real32), intent(in) :: base(:,:), queries(:,:)
    real(real64), intent(in) :: bnorm(:), qnorm(:)
    type(ground_truth), intent(out) :: gt
    character(len=*), intent(inout) :: problem
    integer(int64) :: nq, nb, q, id
    integer :: j
    real(real64) :: cd(k)
    integer(int64) :: ci(k)

    nq = size(queries, 2, kind=int64)
    nb = size(base, 2, kind=int64)
    gt%k = k
    gt%from_file = .true.
    allocate(gt%ids(k, nq), gt%dist(k, nq), gt%certified(nq), gt%fell_back(nq))
    gt%certified = .false.
    gt%fell_back = .false.
    problem = ''
    do q = 1, nq
      do j = 1, k
        id = raw(j, q) - id_base + 1
        if (raw(j, q) < 0 .or. id < 1 .or. id > nb) then
          write(problem, '(a,i0,a,i0)') 'truth file has an invalid id at query ', q - 1, ' slot ', j - 1
          return
        end if
        ci(j) = id
        cd(j) = exact_distance(queries(:, q), base(:, id), metric, qnorm(q), bnorm(id))
      end do
      call sort_pairs(cd, ci)
      do j = 2, k
        if (ci(j) == ci(j - 1)) then
          write(problem, '(a,i0)') 'truth file repeats an id within query ', q - 1
          return
        end if
      end do
      gt%ids(:, q) = ci
      gt%dist(:, q) = cd
    end do
  end subroutine load_ground_truth

  subroutine heapsort_int(a)
    integer(int64), intent(inout) :: a(:)
    integer :: n, i, last
    integer(int64) :: t
    n = size(a)
    do i = n / 2, 1, -1
      call sift(i, n)
    end do
    do last = n, 2, -1
      t = a(1); a(1) = a(last); a(last) = t
      call sift(1, last - 1)
    end do
  contains
    subroutine sift(start, finish)
      integer, intent(in) :: start, finish
      integer :: root, child
      integer(int64) :: tmp
      root = start
      do while (2 * root <= finish)
        child = 2 * root
        if (child < finish) then
          if (a(child) < a(child + 1)) child = child + 1
        end if
        if (a(root) >= a(child)) return
        tmp = a(root); a(root) = a(child); a(child) = tmp
        root = child
      end do
    end subroutine sift
  end subroutine heapsort_int

  pure logical function sorted_contains(a, v)
    integer(int64), intent(in) :: a(:), v
    integer :: lo, hi, mid
    sorted_contains = .false.
    lo = 1
    hi = size(a)
    do while (lo <= hi)
      mid = lo + (hi - lo) / 2
      if (a(mid) == v) then
        sorted_contains = .true.
        return
      else if (a(mid) < v) then
        lo = mid + 1
      else
        hi = mid - 1
      end if
    end do
  end function sorted_contains

  ! Scores one query. raw holds the first k ids the index returned, in the
  ! caller's id base. Negative ids are padding (FAISS writes -1 for short lists).
  function evaluate_query(raw, id_base, truth_ids, truth_dist, query, base, qnorm, bnorm, &
                          metric, tie_rel, tie_abs) result(r)
    integer(int64), intent(in) :: raw(:), truth_ids(:)
    integer, intent(in) :: id_base, metric
    real(real64), intent(in) :: truth_dist(:), qnorm, bnorm(:), tie_rel, tie_abs
    real(real32), intent(in) :: query(:), base(:,:)
    type(query_result) :: r
    integer :: k, j, m, u
    integer(int64) :: nb, id, valid(size(raw)), exact_sorted(size(truth_ids))
    real(real64) :: thr, tol, dist, sum_ann, sum_true

    k = size(raw)
    nb = size(base, 2, kind=int64)
    r%evaluated = .true.
    exact_sorted = truth_ids
    call heapsort_int(exact_sorted)

    m = 0
    do j = 1, k
      if (raw(j) < 0) then
        r%missing = r%missing + 1
        cycle
      end if
      id = raw(j) - id_base + 1
      if (sorted_contains(exact_sorted, id - 1)) r%probe_minus = r%probe_minus + 1
      if (sorted_contains(exact_sorted, id + 1)) r%probe_plus = r%probe_plus + 1
      if (id < 1 .or. id > nb) then
        r%out_of_range = r%out_of_range + 1
        cycle
      end if
      m = m + 1
      valid(m) = id
    end do

    call heapsort_int(valid(1:m))
    thr = truth_dist(k)
    tol = tie_abs + tie_rel * abs(thr)
    sum_ann = 0
    u = 0
    do j = 1, m
      if (j > 1) then
        if (valid(j) == valid(j - 1)) then
          r%duplicates = r%duplicates + 1
          cycle
        end if
      end if
      u = u + 1
      dist = exact_distance(query, base(:, valid(j)), metric, qnorm, bnorm(valid(j)))
      if (dist <= thr + tol) r%hits_tie = r%hits_tie + 1
      if (sorted_contains(exact_sorted, valid(j))) r%hits_id = r%hits_id + 1
      if (metric == METRIC_L2) then
        sum_ann = sum_ann + sqrt(max(dist, 0.0_real64))
      else
        sum_ann = sum_ann + dist
      end if
    end do
    r%hits_tie = min(r%hits_tie, k)
    r%recall_tie = real(r%hits_tie, real64) / k
    r%recall_id = real(r%hits_id, real64) / k

    ! Distance ratio needs a full list of distinct real neighbours.
    if (u == k) then
      if (metric == METRIC_L2) then
        sum_true = sum(sqrt(max(truth_dist, 0.0_real64)))
      else
        sum_true = sum(truth_dist)
      end if
      if (metric == METRIC_IP) then
        ! Mean score lost per slot. Inner product distances are negated scores.
        r%ratio = (sum_ann - sum_true) / k
        r%ratio_ok = .true.
      else if (sum_true > 0) then
        r%ratio = sum_ann / sum_true
        r%ratio_ok = .true.
      end if
    end if
  end function evaluate_query

  pure real(real64) function quantile_sorted(a, p) result(v)
    real(real64), intent(in) :: a(:), p
    real(real64) :: h
    integer :: lo
    if (size(a) == 0) then
      v = 0
      return
    end if
    h = 1 + (size(a) - 1) * min(max(p, 0.0_real64), 1.0_real64)
    lo = int(floor(h))
    if (lo >= size(a)) then
      v = a(size(a))
    else
      v = a(lo) + (h - lo) * (a(lo + 1) - a(lo))
    end if
  end function quantile_sorted

  ! Heapsort of real keys carrying an integer index along.
  subroutine sort_real_with_index(a, idx)
    real(real64), intent(inout) :: a(:)
    integer(int64), intent(inout) :: idx(:)
    integer :: n, i, last
    n = size(a)
    do i = n / 2, 1, -1
      call sift(i, n)
    end do
    do last = n, 2, -1
      call swap(1, last)
      call sift(1, last - 1)
    end do
  contains
    logical function less(i1, i2)
      integer, intent(in) :: i1, i2
      less = a(i1) < a(i2) .or. (a(i1) == a(i2) .and. idx(i1) < idx(i2))
    end function less
    subroutine swap(i1, i2)
      integer, intent(in) :: i1, i2
      real(real64) :: t
      integer(int64) :: ti
      t = a(i1); a(i1) = a(i2); a(i2) = t
      ti = idx(i1); idx(i1) = idx(i2); idx(i2) = ti
    end subroutine swap
    subroutine sift(start, finish)
      integer, intent(in) :: start, finish
      integer :: root, child
      root = start
      do while (2 * root <= finish)
        child = 2 * root
        if (child < finish) then
          if (less(child, child + 1)) child = child + 1
        end if
        if (.not. less(root, child)) return
        call swap(root, child)
        root = child
      end do
    end subroutine sift
  end subroutine sort_real_with_index

  ! xorshift64 uses only shifts and xors, so it never relies on signed
  ! integer overflow, and gives the same stream on every compiler.
  subroutine xorshift64(state)
    integer(int64), intent(inout) :: state
    state = ieor(state, ishft(state, 13))
    state = ieor(state, ishft(state, -7))
    state = ieor(state, ishft(state, 17))
  end subroutine xorshift64

  ! Percentile bootstrap interval for the mean of x. Deterministic for a seed.
  subroutine bootstrap_mean_ci(x, rounds, confidence, seed, lo, hi)
    real(real64), intent(in) :: x(:), confidence
    integer, intent(in) :: rounds
    integer(int64), intent(in) :: seed
    real(real64), intent(out) :: lo, hi
    real(real64), allocatable :: means(:)
    integer(int64), allocatable :: dummy(:)
    integer(int64) :: state, n, pick, i
    integer :: r
    real(real64) :: acc, alpha

    n = size(x, kind=int64)
    if (n == 0 .or. rounds < 1) then
      lo = 0
      hi = 0
      if (n > 0) then
        lo = sum(x) / n
        hi = lo
      end if
      return
    end if
    state = seed
    if (state == 0) state = 88172645463325252_int64
    allocate(means(rounds), dummy(rounds))
    do r = 1, rounds
      acc = 0
      do i = 1, n
        call xorshift64(state)
        pick = modulo(ishft(state, -11), n) + 1
        acc = acc + x(pick)
      end do
      means(r) = acc / n
      dummy(r) = r
    end do
    call sort_real_with_index(means, dummy)
    alpha = 1 - confidence
    lo = quantile_sorted(means, alpha / 2)
    hi = quantile_sorted(means, 1 - alpha / 2)
  end subroutine bootstrap_mean_ci

end module ann_recall_core


program ann_recall_auditor
  use, intrinsic :: iso_fortran_env, only: int64, real32, real64, output_unit, error_unit
  use ann_npy_io
  use ann_recall_core
  !$ use omp_lib
  implicit none

  character(len=:), allocatable :: base_path, query_path, result_path, truth_path, write_truth_path
  character(len=:), allocatable :: json_path, metric_name, gate_on
  integer :: k, margin, qblock, bblock, rounds, id_base, worst_n, metric, threads
  integer(int64) :: seed
  real(real64) :: tie_rel, tie_abs, confidence, min_recall, max_invalid_rate
  logical :: have_gate

  real(real32), allocatable :: base(:,:), queries(:,:)
  real(real64), allocatable :: bnorm(:), qnorm(:)
  integer(int64), allocatable :: results(:,:), truth_raw(:,:), truth_out(:,:)
  type(ground_truth) :: gt
  type(query_result), allocatable :: per(:)
  character(len=512) :: problem

  integer(int64) :: nb, nq, d, q, n_eval, n_skipped, n_ratio, slots
  integer(int64) :: tot_missing, tot_oor, tot_dup, tot_tie_hits, tot_id_hits, tot_probe_minus, tot_probe_plus
  integer(int64) :: n_certified, n_fallback, n_perfect, n_zero, zero_base
  real(real64) :: mean_tie, mean_id, mean_ratio, ci_lo, ci_hi, p50, p10, p01, oor_rate, gate_value
  real(real64) :: probe_minus_rate, probe_plus_rate
  real(real64), allocatable :: rec(:), rec_sorted(:), keys(:)
  integer(int64), allocatable :: order(:), eval_index(:)
  logical :: gate_failed, offset_suspect
  character(len=:), allocatable :: reasons, offset_hint
  integer :: i

  threads = 1
  !$ threads = omp_get_max_threads()

  call parse_arguments()
  if (.not. host_is_little_endian()) call io_fail('this build assumes a little endian host')

  call npy_read_vectors(base_path, 'base', base)
  call npy_read_vectors(query_path, 'queries', queries)
  d = size(base, 1, kind=int64)
  nb = size(base, 2, kind=int64)
  nq = size(queries, 2, kind=int64)
  if (size(queries, 1, kind=int64) /= d) then
    write(problem, '(a,i0,a,i0)') 'dimension mismatch: base has ', d, ', queries have ', size(queries, 1, kind=int64)
    call io_fail(problem)
  end if
  if (int(k, int64) > nb) then
    write(problem, '(a,i0,a,i0,a)') 'k = ', k, ' but the base only has ', nb, ' vectors'
    call io_fail(problem)
  end if

  allocate(bnorm(nb), qnorm(nq))
  call vector_norms(base, bnorm)
  call vector_norms(queries, qnorm)
  zero_base = count(bnorm == 0)
  if (metric == METRIC_COS .and. zero_base > 0) then
    write(problem, '(i0,a)') zero_base, ' base vectors have zero norm; cosine treats them as orthogonal to everything'
    call io_warn(problem)
  end if

  if (allocated(truth_path)) then
    call npy_read_ids(truth_path, 'truth', truth_raw)
    if (size(truth_raw, 2, kind=int64) /= nq) call io_fail('truth file row count does not match the queries')
    if (size(truth_raw, 1) < k) call io_fail('truth file has fewer columns than k')
    problem = ''
    call load_ground_truth(truth_raw(1:k, :), id_base, base, queries, bnorm, qnorm, metric, k, gt, problem)
    if (len_trim(problem) > 0) call io_fail(problem)
    deallocate(truth_raw)
  else
    call exact_ground_truth(base, queries, bnorm, qnorm, metric, k, margin, qblock, bblock, gt)
  end if
  n_certified = count(gt%certified)
  n_fallback = count(gt%fell_back)

  if (allocated(write_truth_path)) then
    allocate(truth_out(k, nq))
    truth_out = gt%ids - 1 + id_base
    call npy_write_ids(write_truth_path, truth_out)
    deallocate(truth_out)
  end if

  if (.not. allocated(result_path)) then
    write(output_unit, '(a)') 'ANN Recall Auditor: ground truth only'
    write(output_unit, '(a,i0,a,i0,a,i0)') '  base ', nb, ' x ', d, '   queries ', nq
    write(output_unit, '(a,a,a,i0,a,i0)') '  metric ', metric_name, '   k ', k, '   threads ', threads
    write(output_unit, '(a,i0,a,i0,a,a,a)') '  certified ', n_certified, '   float64 rescans ', n_fallback, &
      '   ', fmt(gt%seconds, 2), ' s'
    if (allocated(write_truth_path)) write(output_unit, '(a)') '  wrote '//write_truth_path
    stop EXIT_PASS, quiet=.true.
  end if

  call npy_read_ids(result_path, 'results', results)
  if (size(results, 2, kind=int64) /= nq) then
    write(problem, '(a,i0,a,i0,a)') 'results have ', size(results, 2, kind=int64), ' rows but there are ', nq, ' queries'
    call io_fail(problem)
  end if
  if (size(results, 1) < k) then
    write(problem, '(a,i0,a,i0)') 'results have ', size(results, 1), ' columns, fewer than k = ', k
    call io_fail(problem)
  end if

  allocate(per(nq))
  !$omp parallel do private(q) schedule(dynamic, 32)
  do q = 1, nq
    if (metric == METRIC_COS .and. qnorm(q) == 0) then
      per(q)%evaluated = .false.
    else
      per(q) = evaluate_query(results(1:k, q), id_base, gt%ids(:, q), gt%dist(:, q), queries(:, q), base, &
                              qnorm(q), bnorm, metric, tie_rel, tie_abs)
    end if
  end do
  !$omp end parallel do

  n_eval = count(per%evaluated)
  n_skipped = nq - n_eval
  if (n_eval == 0) call io_fail('no query could be evaluated (all queries have zero norm under cosine)')
  allocate(rec(n_eval), eval_index(n_eval))
  n_eval = 0
  tot_missing = 0; tot_oor = 0; tot_dup = 0; tot_tie_hits = 0; tot_id_hits = 0
  tot_probe_minus = 0; tot_probe_plus = 0; n_ratio = 0; mean_ratio = 0
  mean_id = 0; n_perfect = 0; n_zero = 0
  do q = 1, nq
    if (.not. per(q)%evaluated) cycle
    n_eval = n_eval + 1
    rec(n_eval) = per(q)%recall_tie
    eval_index(n_eval) = q
    mean_id = mean_id + per(q)%recall_id
    tot_missing = tot_missing + per(q)%missing
    tot_oor = tot_oor + per(q)%out_of_range
    tot_dup = tot_dup + per(q)%duplicates
    tot_tie_hits = tot_tie_hits + per(q)%hits_tie
    tot_id_hits = tot_id_hits + per(q)%hits_id
    tot_probe_minus = tot_probe_minus + per(q)%probe_minus
    tot_probe_plus = tot_probe_plus + per(q)%probe_plus
    if (per(q)%hits_tie == k) n_perfect = n_perfect + 1
    if (per(q)%hits_tie == 0) n_zero = n_zero + 1
    if (per(q)%ratio_ok) then
      n_ratio = n_ratio + 1
      mean_ratio = mean_ratio + per(q)%ratio
    end if
  end do
  slots = n_eval * k
  mean_tie = sum(rec) / n_eval
  mean_id = mean_id / n_eval
  if (n_ratio > 0) mean_ratio = mean_ratio / n_ratio
  oor_rate = real(tot_oor, real64) / slots
  probe_minus_rate = real(tot_probe_minus, real64) / slots
  probe_plus_rate = real(tot_probe_plus, real64) / slots

  rec_sorted = rec
  allocate(order(n_eval))
  order = [(q, q = 1, n_eval)]
  call sort_real_with_index(rec_sorted, order)
  p50 = quantile_sorted(rec_sorted, 0.50_real64)
  p10 = quantile_sorted(rec_sorted, 0.10_real64)
  p01 = quantile_sorted(rec_sorted, 0.01_real64)
  call bootstrap_mean_ci(rec, rounds, confidence, seed, ci_lo, ci_hi)

  ! An off by one id mapping (internal labels against external ids, or a
  ! 1 based export) shows up as shifted ids landing on the true neighbours.
  offset_suspect = .false.
  offset_hint = ''
  if (probe_minus_rate > mean_id + 0.25_real64 .and. probe_minus_rate > 0.5_real64) then
    offset_suspect = .true.
    offset_hint = 'results look shifted up by one: try --id-base '//itoa(id_base + 1)
  else if (probe_plus_rate > mean_id + 0.25_real64 .and. probe_plus_rate > 0.5_real64) then
    offset_suspect = .true.
    offset_hint = 'results look shifted down by one: try --id-base '//itoa(id_base - 1)
  end if

  ! Worst queries: lowest recall first, then largest distance ratio.
  allocate(keys(n_eval))
  do i = 1, int(n_eval)
    q = eval_index(i)
    keys(i) = rec(i) * 1.0e6_real64
    if (per(q)%ratio_ok .and. metric /= METRIC_IP) keys(i) = keys(i) - min(per(q)%ratio, 1.0e5_real64)
  end do
  deallocate(order)
  allocate(order(n_eval))
  order = eval_index
  call sort_real_with_index(keys, order)

  gate_failed = .false.
  reasons = ''
  gate_value = mean_tie
  if (gate_on == 'lower') gate_value = ci_lo
  if (have_gate .and. gate_value < min_recall) then
    gate_failed = .true.
    reasons = reasons//'recall '//gate_on//' '//fmt(gate_value, 4)//' is below --min-recall '//fmt(min_recall, 4)//'; '
  end if
  if (oor_rate > max_invalid_rate) then
    gate_failed = .true.
    reasons = reasons//'out of range id rate '//fmt(oor_rate, 6)//' exceeds --max-invalid-rate '// &
              fmt(max_invalid_rate, 6)//'; '
  end if
  if (offset_suspect) then
    gate_failed = .true.
    reasons = reasons//offset_hint//'; '
  end if

  if (len(reasons) > 2) reasons = reasons(:len(reasons) - 2)
  call print_report()
  if (allocated(json_path)) call write_json()

  if (gate_failed) then
    stop EXIT_GATE_FAIL, quiet=.true.
  end if
  stop EXIT_PASS, quiet=.true.

contains

  function itoa(v) result(s)
    integer, intent(in) :: v
    character(len=:), allocatable :: s
    character(len=16) :: buf
    write(buf, '(i0)') v
    s = trim(buf)
  end function itoa

  function ltoa(v) result(s)
    integer(int64), intent(in) :: v
    character(len=:), allocatable :: s
    character(len=24) :: buf
    write(buf, '(i0)') v
    s = trim(buf)
  end function ltoa

  ! Fixed point with a guaranteed leading zero, so the text is valid JSON.
  function fmt(v, digits) result(s)
    real(real64), intent(in) :: v
    integer, intent(in) :: digits
    character(len=:), allocatable :: s
    character(len=64) :: buf
    character(len=16) :: spec
    if (digits <= 0) then
      write(buf, '(i0)') nint(v, int64)
      s = trim(adjustl(buf))
      return
    end if
    write(spec, '(a,i0,a)') '(f0.', digits, ')'
    write(buf, spec) v
    s = trim(adjustl(buf))
    if (s(1:1) == '.') then
      s = '0'//s
    else if (len(s) > 1) then
      if (s(1:2) == '-.') s = '-0'//s(2:)
    end if
  end function fmt

  function json_str(v) result(s)
    character(len=*), intent(in) :: v
    character(len=:), allocatable :: s
    integer :: j
    s = '"'
    do j = 1, len_trim(v)
      select case (v(j:j))
      case ('"')
        s = s//'\"'
      case ('\')
        s = s//'\\'
      case default
        if (iachar(v(j:j)) < 32) then
          s = s//' '
        else
          s = s//v(j:j)
        end if
      end select
    end do
    s = s//'"'
  end function json_str

  subroutine usage()
    write(output_unit, '(a)') 'usage: ann-recall-auditor --base BASE.npy --queries QUERIES.npy [--results ANN.npy] [options]'
    write(output_unit, '(a)') ''
    write(output_unit, '(a)') '  --k N                 neighbours per query to score (default 10)'
    write(output_unit, '(a)') '  --metric l2|ip|cos    distance the index was built for (default l2)'
    write(output_unit, '(a)') '  --id-base 0|1         id of the first base row in results and truth files (default 0)'
    write(output_unit, '(a)') '  --truth FILE.npy      reuse cached exact neighbour ids instead of searching'
    write(output_unit, '(a)') '  --write-truth FILE    save exact neighbour ids as int64 .npy for later runs'
    write(output_unit, '(a)') '  --min-recall R        fail (exit 1) when recall is below R'
    write(output_unit, '(a)') '  --gate-on lower|mean  gate on the bootstrap lower bound or the mean (default lower)'
    write(output_unit, '(a)') '  --max-invalid-rate R  allowed share of out of range ids (default 0)'
    write(output_unit, '(a)') '  --confidence C        bootstrap interval level (default 0.95)'
    write(output_unit, '(a)') '  --bootstrap N         bootstrap rounds, 0 disables (default 2000)'
    write(output_unit, '(a)') '  --seed N              bootstrap seed (default 20261121)'
    write(output_unit, '(a)') '  --tie-rel R           relative tolerance for equal distance ties (default 1e-7)'
    write(output_unit, '(a)') '  --tie-abs A           absolute tolerance for ties (default 0)'
    write(output_unit, '(a)') '  --margin N            extra float32 candidates kept per query (default 32)'
    write(output_unit, '(a)') '  --query-block N       queries per matmul block (default 256)'
    write(output_unit, '(a)') '  --base-block N        base vectors per matmul block (default 8192)'
    write(output_unit, '(a)') '  --worst N             worst queries to list (default 10)'
    write(output_unit, '(a)') '  --json FILE           also write the full report as JSON'
    write(output_unit, '(a)') ''
    write(output_unit, '(a)') 'exit codes: 0 pass, 1 gate failed, 2 bad input or usage'
  end subroutine usage

  subroutine parse_arguments()
    integer :: argc, ai, ios, alen
    character(len=:), allocatable :: flag, val

    k = DEFAULT_K
    margin = DEFAULT_MARGIN
    qblock = DEFAULT_QUERY_BLOCK
    bblock = DEFAULT_BASE_BLOCK
    rounds = DEFAULT_BOOTSTRAP
    seed = DEFAULT_SEED
    tie_rel = DEFAULT_TIE_REL
    tie_abs = 0
    confidence = DEFAULT_CONFIDENCE
    id_base = 0
    worst_n = 10
    metric_name = 'l2'
    gate_on = 'lower'
    have_gate = .false.
    min_recall = 0
    max_invalid_rate = 0

    argc = command_argument_count()
    if (argc == 0) then
      call usage()
      stop EXIT_INPUT_ERROR, quiet=.true.
    end if
    ai = 1
    do while (ai <= argc)
      call get_command_argument(ai, length=alen)
      allocate(character(len=alen) :: flag)
      call get_command_argument(ai, flag)
      if (flag == '--help' .or. flag == '-h') then
        call usage()
        stop EXIT_PASS, quiet=.true.
      end if
      if (ai == argc) call io_fail('missing value after '//flag)
      call get_command_argument(ai + 1, length=alen)
      allocate(character(len=alen) :: val)
      call get_command_argument(ai + 1, val)
      ios = 0
      select case (flag)
      case ('--base');            base_path = val
      case ('--queries');         query_path = val
      case ('--results');         result_path = val
      case ('--truth');           truth_path = val
      case ('--write-truth');     write_truth_path = val
      case ('--json');            json_path = val
      case ('--metric');          metric_name = val
      case ('--gate-on');         gate_on = val
      case ('--k');               read(val, *, iostat=ios) k
      case ('--margin');          read(val, *, iostat=ios) margin
      case ('--query-block');     read(val, *, iostat=ios) qblock
      case ('--base-block');      read(val, *, iostat=ios) bblock
      case ('--bootstrap');       read(val, *, iostat=ios) rounds
      case ('--seed');            read(val, *, iostat=ios) seed
      case ('--tie-rel');         read(val, *, iostat=ios) tie_rel
      case ('--tie-abs');         read(val, *, iostat=ios) tie_abs
      case ('--confidence');      read(val, *, iostat=ios) confidence
      case ('--id-base');         read(val, *, iostat=ios) id_base
      case ('--worst');           read(val, *, iostat=ios) worst_n
      case ('--max-invalid-rate'); read(val, *, iostat=ios) max_invalid_rate
      case ('--min-recall')
        read(val, *, iostat=ios) min_recall
        have_gate = .true.
      case default
        call io_fail('unknown option '//flag//' (see --help)')
      end select
      if (ios /= 0) call io_fail('bad value for '//flag//': '//val)
      deallocate(flag, val)
      ai = ai + 2
    end do

    if (.not. allocated(base_path)) call io_fail('--base is required')
    if (.not. allocated(query_path)) call io_fail('--queries is required')
    if (.not. allocated(result_path) .and. .not. allocated(write_truth_path)) &
      call io_fail('give --results to audit an index, or --write-truth to only build ground truth')
    select case (metric_name)
    case ('l2');  metric = METRIC_L2
    case ('ip');  metric = METRIC_IP
    case ('cos'); metric = METRIC_COS
    case default
      call io_fail('--metric must be l2, ip or cos')
    end select
    if (gate_on /= 'lower' .and. gate_on /= 'mean') call io_fail('--gate-on must be lower or mean')
    if (k < 1) call io_fail('--k must be at least 1')
    if (margin < 0) call io_fail('--margin must not be negative')
    if (qblock < 1 .or. bblock < 1) call io_fail('block sizes must be positive')
    if (rounds < 0) call io_fail('--bootstrap must not be negative')
    if (confidence <= 0 .or. confidence >= 1) call io_fail('--confidence must lie strictly between 0 and 1')
    if (tie_rel < 0 .or. tie_abs < 0) call io_fail('tie tolerances must not be negative')
    if (max_invalid_rate < 0) call io_fail('--max-invalid-rate must not be negative')
    if (worst_n < 0) worst_n = 0
  end subroutine parse_arguments

  subroutine print_report()
    integer :: w
    integer(int64) :: qi
    character(len=:), allocatable :: truth_line, ratio_label
    write(output_unit, '(a)') 'ANN Recall Auditor'
    write(output_unit, '(a)') '  base       '//ltoa(nb)//' x '//ltoa(d)//'  '//base_path
    write(output_unit, '(a)') '  queries    '//ltoa(nq)//'  '//query_path
    write(output_unit, '(a)') '  results    '//ltoa(size(results, 2, kind=int64))//' x '// &
                              ltoa(size(results, 1, kind=int64))//' (scoring first '//itoa(k)//')  '//result_path
    write(output_unit, '(a)') '  metric     '//metric_name//'   k '//itoa(k)//'   id base '//itoa(id_base)// &
                              '   threads '//itoa(threads)
    if (gt%from_file) then
      truth_line = 'loaded from '//truth_path//' (ids trusted, distances recomputed in float64)'
    else
      truth_line = 'exact, '//ltoa(n_certified)//' of '//ltoa(nq)//' certified, '//ltoa(n_fallback)// &
                   ' float64 rescans, '//fmt(gt%seconds, 2)//' s'
    end if
    write(output_unit, '(a)') '  truth      '//truth_line
    if (n_skipped > 0) write(output_unit, '(a)') '  skipped    '//ltoa(n_skipped)//' zero norm queries under cosine'
    write(output_unit, '(a)') ''
    write(output_unit, '(a)') '  recall@'//itoa(k)//' tie aware   '//fmt(mean_tie, 4)//'   '// &
                              fmt(100 * confidence, 0)//'% CI ['//fmt(ci_lo, 4)//', '//fmt(ci_hi, 4)//']'
    write(output_unit, '(a)') '  recall@'//itoa(k)//' id match    '//fmt(mean_id, 4)
    write(output_unit, '(a)') '  per query  p50 '//fmt(p50, 4)//'   p10 '//fmt(p10, 4)//'   p1 '//fmt(p01, 4)
    write(output_unit, '(a)') '  perfect    '//fmt(100 * real(n_perfect, real64) / n_eval, 2)//'%   zero '// &
                              fmt(100 * real(n_zero, real64) / n_eval, 2)//'%'
    if (metric == METRIC_IP) then
      ratio_label = '  score lost per slot '
    else
      ratio_label = '  distance ratio '
    end if
    if (n_ratio > 0) then
      write(output_unit, '(a)') ratio_label//fmt(mean_ratio, 6)//'  over '//ltoa(n_ratio)//' complete lists'
    else
      write(output_unit, '(a)') ratio_label//'n/a (no query returned k distinct valid ids)'
    end if
    write(output_unit, '(a)') '  slots      '//ltoa(slots)//' total, '//ltoa(tot_missing)//' padding, '// &
                              ltoa(tot_oor)//' out of range, '//ltoa(tot_dup)//' duplicate'
    if (tot_tie_hits > tot_id_hits) write(output_unit, '(a)') '  ties       '//ltoa(tot_tie_hits - tot_id_hits)// &
      ' hits are equal distance neighbours that strict id matching would have counted as misses'
    if (offset_suspect) write(output_unit, '(a)') '  warning    '//offset_hint
    if (worst_n > 0) then
      write(output_unit, '(a)') ''
      write(output_unit, '(a)') '  worst queries (0 based row, recall, k-th exact distance)'
      do w = 1, int(min(int(worst_n, int64), n_eval))
        qi = order(w)
        write(output_unit, '(a)') '    '//ltoa(qi - 1)//'  '//fmt(per(qi)%recall_tie, 4)//'  '// &
                                  fmt(gt%dist(k, qi), 6)
      end do
    end if
    write(output_unit, '(a)') ''
    if (gate_failed) then
      write(output_unit, '(a)') '  gate       FAIL: '//reasons
    else if (have_gate) then
      write(output_unit, '(a)') '  gate       PASS (recall '//gate_on//' '//fmt(gate_value, 4)//' >= '// &
                                fmt(min_recall, 4)//')'
    else
      write(output_unit, '(a)') '  gate       none set (pass --min-recall to enforce one)'
    end if
  end subroutine print_report

  subroutine write_json()
    integer :: u, ios, w
    integer(int64) :: qi
    character(len=:), allocatable :: sep
    open(newunit=u, file=json_path, status='replace', action='write', iostat=ios)
    if (ios /= 0) call io_fail('cannot create '//json_path)
    write(u, '(a)') '{'
    write(u, '(a)') '  "tool": "ann-recall-auditor",'
    write(u, '(a)') '  "base": '//json_str(base_path)//', "queries": '//json_str(query_path)// &
                    ', "results": '//json_str(result_path)//','
    write(u, '(a)') '  "metric": '//json_str(metric_name)//', "k": '//itoa(k)//', "id_base": '//itoa(id_base)// &
                    ', "dim": '//ltoa(d)//', "base_count": '//ltoa(nb)//', "query_count": '//ltoa(nq)//','
    if (gt%from_file) then
      write(u, '(a)') '  "truth": {"source": "file", "path": '//json_str(truth_path)//'},'
    else
      write(u, '(a)') '  "truth": {"source": "exact", "certified": '//ltoa(n_certified)//', "float64_rescans": '// &
                      ltoa(n_fallback)//', "seconds": '//fmt(gt%seconds, 3)//'},'
    end if
    write(u, '(a)') '  "evaluated": '//ltoa(n_eval)//', "skipped_zero_norm": '//ltoa(n_skipped)//','
    write(u, '(a)') '  "recall_tie": '//fmt(mean_tie, 6)//', "recall_id": '//fmt(mean_id, 6)//','
    write(u, '(a)') '  "ci": {"confidence": '//fmt(confidence, 4)//', "lower": '//fmt(ci_lo, 6)// &
                    ', "upper": '//fmt(ci_hi, 6)//', "rounds": '//itoa(rounds)//', "seed": '//ltoa(seed)//'},'
    write(u, '(a)') '  "quantiles": {"p50": '//fmt(p50, 6)//', "p10": '//fmt(p10, 6)//', "p1": '//fmt(p01, 6)//'},'
    write(u, '(a)') '  "perfect_queries": '//ltoa(n_perfect)//', "zero_recall_queries": '//ltoa(n_zero)//','
    if (n_ratio > 0) then
      write(u, '(a)') '  "distance_ratio": '//fmt(mean_ratio, 8)//', "ratio_queries": '//ltoa(n_ratio)//','
    else
      write(u, '(a)') '  "distance_ratio": null, "ratio_queries": 0,'
    end if
    write(u, '(a)') '  "slots": {"total": '//ltoa(slots)//', "padding": '//ltoa(tot_missing)//', "out_of_range": '// &
                    ltoa(tot_oor)//', "duplicate": '//ltoa(tot_dup)//'},'
    write(u, '(a)') '  "offset_probe": {"minus_one": '//fmt(probe_minus_rate, 6)//', "plus_one": '// &
                    fmt(probe_plus_rate, 6)//', "suspect": '//trim(merge('true ', 'false', offset_suspect))//'},'
    write(u, '(a)') '  "worst": ['
    do w = 1, int(min(int(worst_n, int64), n_eval))
      qi = order(w)
      sep = ','
      if (w == int(min(int(worst_n, int64), n_eval))) sep = ''
      write(u, '(a)') '    {"query": '//ltoa(qi - 1)//', "recall": '//fmt(per(qi)%recall_tie, 6)// &
                      ', "kth_distance": '//fmt(gt%dist(k, qi), 8)//'}'//sep
    end do
    write(u, '(a)') '  ],'
    if (have_gate) then
      write(u, '(a)') '  "gate": {"min_recall": '//fmt(min_recall, 6)//', "on": '//json_str(gate_on)// &
                      ', "value": '//fmt(gate_value, 6)//', "passed": '//trim(merge('true ', 'false', .not. gate_failed))// &
                      ', "reasons": '//json_str(reasons)//'}'
    else
      write(u, '(a)') '  "gate": {"min_recall": null, "passed": '//trim(merge('true ', 'false', .not. gate_failed))// &
                      ', "reasons": '//json_str(reasons)//'}'
    end if
    write(u, '(a)') '}'
    close(u)
  end subroutine write_json

end program ann_recall_auditor
