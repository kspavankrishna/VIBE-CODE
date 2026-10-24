! CarbonBufferPoolSimulator.f90
! Build: gfortran -O2 -fopenmp -fwrapv -std=f2018 CarbonBufferPoolSimulator.f90 -o CarbonBufferPoolSimulator
! Run:   ./CarbonBufferPoolSimulator SamplePortfolio.csv years=40 trials=200000 confidence=0.95,0.99

module BufferRng
  use, intrinsic :: iso_fortran_env, only: int64, real64
  implicit none
  private
  public :: RngState, SeedStream, NextUniform, DrawBeta

  type :: RngState
    integer(int64) :: s(4) = 0_int64
  end type RngState

  integer(int64), parameter :: GoldenGamma = -7046029254386353131_int64
  real(real64), parameter :: TwoPowMinus53 = 1.0_real64 / 9007199254740992.0_real64
  integer, parameter :: MaxRejection = 1000

contains

  pure function Rotl(x, k) result(r)
    integer(int64), intent(in) :: x
    integer, intent(in) :: k
    integer(int64) :: r
    r = ior(ishft(x, k), ishft(x, k - 64))
  end function Rotl

  function SplitMix(z) result(out)
    integer(int64), intent(inout) :: z
    integer(int64) :: out, t
    z = z + GoldenGamma
    t = z
    t = ieor(t, ishft(t, -30)) * (-4658895280553007687_int64)
    t = ieor(t, ishft(t, -27)) * (-7723592293110705685_int64)
    out = ieor(t, ishft(t, -31))
  end function SplitMix

  ! Every trial owns an independent stream derived from (seed, trial).
  ! Results therefore do not depend on thread count or scheduling.
  subroutine SeedStream(st, seed, trial)
    type(RngState), intent(out) :: st
    integer(int64), intent(in) :: seed, trial
    integer(int64) :: z
    integer :: i
    z = seed + (trial + 1_int64) * GoldenGamma
    do i = 1, 4
      st%s(i) = SplitMix(z)
    end do
    if (all(st%s == 0_int64)) st%s(1) = 1_int64
  end subroutine SeedStream

  function NextU64(st) result(res)
    type(RngState), intent(inout) :: st
    integer(int64) :: res, t
    res = Rotl(st%s(2) * 5_int64, 7) * 9_int64
    t = ishft(st%s(2), 17)
    st%s(3) = ieor(st%s(3), st%s(1))
    st%s(4) = ieor(st%s(4), st%s(2))
    st%s(2) = ieor(st%s(2), st%s(3))
    st%s(1) = ieor(st%s(1), st%s(4))
    st%s(3) = ieor(st%s(3), t)
    st%s(4) = Rotl(st%s(4), 45)
  end function NextU64

  ! Open interval (0,1): never returns exactly 0 or 1, so log() is always safe.
  function NextUniform(st) result(u)
    type(RngState), intent(inout) :: st
    real(real64) :: u
    u = (real(ishft(NextU64(st), -11), real64) + 0.5_real64) * TwoPowMinus53
  end function NextUniform

  function NextNormal(st) result(z)
    type(RngState), intent(inout) :: st
    real(real64) :: z, u1, u2
    u1 = NextUniform(st)
    u2 = NextUniform(st)
    z = sqrt(-2.0_real64 * log(u1)) * cos(6.283185307179586_real64 * u2)
  end function NextNormal

  ! Marsaglia and Tsang. Shape below one is boosted: G(a) = G(a+1) * U**(1/a).
  recursive function DrawGamma(st, shape) result(g)
    type(RngState), intent(inout) :: st
    real(real64), intent(in) :: shape
    real(real64) :: g, a, d, c, x, v, u
    integer :: iter
    a = shape
    if (a < 1.0_real64) then
      g = DrawGamma(st, a + 1.0_real64) * NextUniform(st)**(1.0_real64 / a)
      return
    end if
    d = a - 1.0_real64 / 3.0_real64
    c = 1.0_real64 / sqrt(9.0_real64 * d)
    g = d
    do iter = 1, MaxRejection
      x = NextNormal(st)
      v = 1.0_real64 + c * x
      if (v <= 0.0_real64) cycle
      v = v * v * v
      u = NextUniform(st)
      if (log(u) < 0.5_real64 * x * x + d - d * v + d * log(v)) then
        g = d * v
        return
      end if
    end do
  end function DrawGamma

  ! Beta(alpha, beta) as X/(X+Y). If both gammas underflow (tiny shapes) fall back to the mean.
  function DrawBeta(st, alpha, beta) result(f)
    type(RngState), intent(inout) :: st
    real(real64), intent(in) :: alpha, beta
    real(real64) :: f, x, y
    x = DrawGamma(st, alpha)
    y = DrawGamma(st, beta)
    if (x + y <= tiny(1.0_real64)) then
      f = alpha / (alpha + beta)
    else
      f = x / (x + y)
    end if
  end function DrawBeta

end module BufferRng

module BufferModel
  use, intrinsic :: iso_fortran_env, only: int64, real64, error_unit
  use BufferRng
  implicit none
  private
  public :: Portfolio, Config, MaxLevels, ExitUsage, ExitInput, ExitFile, ExitGate
  public :: Die, ParseArgs, LoadPortfolio, SimulateTrial, HeapSort, Fmt, ToLower

  integer, parameter :: MaxLevels = 8
  integer, parameter :: ExitGate = 1, ExitUsage = 2, ExitInput = 3, ExitFile = 4
  integer, parameter :: MaxLine = 2048, MaxFields = 16, FieldLen = 128
  integer, parameter :: MaxYears = 200, MaxTrials = 20000000

  type :: Portfolio
    integer :: nProj = 0, nRegion = 0
    character(len=FieldLen), allocatable :: id(:), regionName(:)
    integer, allocatable :: region(:)
    real(real64), allocatable :: stock(:), hazard(:), alpha(:), beta(:), regionStock(:)
    real(real64) :: totalStock = 0.0_real64
  end type Portfolio

  type :: Config
    character(len=MaxLine) :: path = ''
    integer :: years = 40, trials = 200000, nLevels = 2
    integer(int64) :: seed = 20260101_int64
    real(real64) :: levels(MaxLevels) = [0.95_real64, 0.99_real64, 0.0_real64, 0.0_real64, &
                                         0.0_real64, 0.0_real64, 0.0_real64, 0.0_real64]
    real(real64) :: shockProb = 0.04_real64, shockMult = 5.0_real64
    real(real64) :: globalProb = 0.01_real64, globalMult = 2.0_real64
    real(real64) :: topup = 0.0_real64
    real(real64) :: poolPct = -1.0_real64
    real(real64) :: maxShortfallProb = -1.0_real64
    logical :: json = .false.
  end type Config

contains

  subroutine Die(code, msg)
    integer, intent(in) :: code
    character(len=*), intent(in) :: msg
    write (error_unit, '(A)') 'error: ' // trim(msg)
    stop code, quiet=.true.
  end subroutine Die

  function ToLower(s) result(t)
    character(len=*), intent(in) :: s
    character(len=len(s)) :: t
    integer :: i, c
    t = s
    do i = 1, len(s)
      c = iachar(s(i:i))
      if (c >= 65 .and. c <= 90) t(i:i) = achar(c + 32)
    end do
  end function ToLower

  function Fmt(x, decimals) result(s)
    real(real64), intent(in) :: x
    integer, intent(in) :: decimals
    character(len=:), allocatable :: s
    character(len=40) :: buf
    character(len=16) :: f
    write (f, '(A,I0,A,I0,A)') '(F', 30, '.', decimals, ')'
    write (buf, f) x
    s = trim(adjustl(buf))
    if (s(len(s):len(s)) == '.') s = s(1:len(s) - 1)
    if (s(1:1) == '.') s = '0' // s
    if (len(s) > 1) then
      if (s(1:2) == '-.') s = '-0' // s(2:)
    end if
  end function Fmt

  function IsNumber(s) result(ok)
    character(len=*), intent(in) :: s
    logical :: ok
    integer :: i
    logical :: digit
    ok = .false.
    digit = .false.
    if (len_trim(s) == 0) return
    do i = 1, len_trim(s)
      select case (s(i:i))
      case ('0':'9')
        digit = .true.
      case ('+', '-', '.', 'e', 'E')
      case default
        return
      end select
    end do
    ok = digit
  end function IsNumber

  subroutine ParseReal(s, v, ok)
    character(len=*), intent(in) :: s
    real(real64), intent(out) :: v
    logical, intent(out) :: ok
    integer :: ios
    v = 0.0_real64
    ok = IsNumber(adjustl(s))
    if (.not. ok) return
    read (s, *, iostat=ios) v
    ok = (ios == 0) .and. (v - v == 0.0_real64) .and. (abs(v) <= huge(1.0_real64))
  end subroutine ParseReal

  subroutine SplitCsv(line, fields, nf)
    character(len=*), intent(in) :: line
    character(len=FieldLen), intent(out) :: fields(MaxFields)
    integer, intent(out) :: nf
    integer :: i, start, n
    nf = 0
    fields = ''
    n = len_trim(line)
    start = 1
    do i = 1, n + 1
      if (i == n + 1 .or. line(min(i, n):min(i, n)) == ',') then
        if (i <= n .or. n > 0) then
          nf = nf + 1
          if (nf > MaxFields) call Die(ExitInput, 'too many columns in a CSV row')
          if (i - start > FieldLen) call Die(ExitInput, 'CSV field longer than 128 characters')
          if (i > start) fields(nf) = adjustl(line(start:i - 1))
        end if
        start = i + 1
      end if
    end do
  end subroutine SplitCsv

  subroutine ParseLevels(s, cfg)
    character(len=*), intent(in) :: s
    type(Config), intent(inout) :: cfg
    character(len=FieldLen) :: parts(MaxFields)
    integer :: nf, i
    logical :: ok
    real(real64) :: v
    call SplitCsv(s, parts, nf)
    if (nf < 1 .or. nf > MaxLevels) call Die(ExitUsage, 'confidence takes 1 to 8 comma separated values')
    do i = 1, nf
      call ParseReal(parts(i), v, ok)
      if (.not. ok .or. v <= 0.5_real64 .or. v >= 1.0_real64) &
        call Die(ExitUsage, 'confidence values must be numbers above 0.5 and below 1')
      cfg%levels(i) = v
    end do
    cfg%nLevels = nf
  end subroutine ParseLevels

  subroutine ParseArgs(cfg)
    type(Config), intent(out) :: cfg
    character(len=MaxLine) :: arg, key, val
    integer :: i, n, eq, ios
    real(real64) :: v
    logical :: ok
    n = command_argument_count()
    if (n < 1) call Die(ExitUsage, 'usage: CarbonBufferPoolSimulator portfolio.csv [years=N trials=N seed=N ' // &
      'confidence=0.95,0.99 shock_prob=P shock_mult=M global_prob=P global_mult=M topup=T pool_pct=P ' // &
      'max_shortfall_prob=P format=text|json]')
    do i = 1, n
      call get_command_argument(i, arg)
      eq = index(arg, '=')
      if (eq == 0) then
        if (len_trim(cfg%path) > 0) call Die(ExitUsage, 'only one portfolio path is allowed: ' // trim(arg))
        cfg%path = arg
        cycle
      end if
      key = ToLower(arg(1:eq - 1))
      val = arg(eq + 1:)
      call ParseReal(val, v, ok)
      select case (trim(key))
      case ('years')
        if (.not. ok .or. v < 1 .or. v > MaxYears .or. abs(v - anint(v)) > 0.0_real64) &
          call Die(ExitUsage, 'years must be an integer 1 to 200')
        cfg%years = nint(v)
      case ('trials')
        if (.not. ok .or. v < 1000 .or. v > MaxTrials .or. abs(v - anint(v)) > 0.0_real64) &
          call Die(ExitUsage, 'trials must be an integer 1000 to 20000000')
        cfg%trials = nint(v)
      case ('seed')
        read (val, *, iostat=ios) cfg%seed
        if (ios /= 0) call Die(ExitUsage, 'seed must be an integer')
      case ('confidence')
        call ParseLevels(val, cfg)
      case ('shock_prob')
        if (.not. ok .or. v < 0 .or. v > 1) call Die(ExitUsage, 'shock_prob must be in [0,1]')
        cfg%shockProb = v
      case ('shock_mult')
        if (.not. ok .or. v < 1) call Die(ExitUsage, 'shock_mult must be at least 1')
        cfg%shockMult = v
      case ('global_prob')
        if (.not. ok .or. v < 0 .or. v > 1) call Die(ExitUsage, 'global_prob must be in [0,1]')
        cfg%globalProb = v
      case ('global_mult')
        if (.not. ok .or. v < 1) call Die(ExitUsage, 'global_mult must be at least 1')
        cfg%globalMult = v
      case ('topup')
        if (.not. ok .or. v < 0) call Die(ExitUsage, 'topup must be zero or more tonnes per year')
        cfg%topup = v
      case ('pool_pct')
        if (.not. ok .or. v < 0 .or. v > 100) call Die(ExitUsage, 'pool_pct must be in [0,100]')
        cfg%poolPct = v
      case ('max_shortfall_prob')
        if (.not. ok .or. v < 0 .or. v > 1) call Die(ExitUsage, 'max_shortfall_prob must be in [0,1]')
        cfg%maxShortfallProb = v
      case ('format')
        select case (trim(ToLower(val)))
        case ('json')
          cfg%json = .true.
        case ('text')
          cfg%json = .false.
        case default
          call Die(ExitUsage, 'format must be text or json')
        end select
      case default
        call Die(ExitUsage, 'unknown option: ' // trim(key))
      end select
    end do
    if (len_trim(cfg%path) == 0) call Die(ExitUsage, 'missing portfolio CSV path')
    if (cfg%maxShortfallProb >= 0 .and. cfg%poolPct < 0) &
      call Die(ExitUsage, 'max_shortfall_prob needs pool_pct to be set')
  end subroutine ParseArgs

  function ColumnIndex(header, nf, name) result(ix)
    character(len=FieldLen), intent(in) :: header(MaxFields)
    integer, intent(in) :: nf
    character(len=*), intent(in) :: name
    integer :: ix, i
    ix = 0
    do i = 1, nf
      if (trim(ToLower(header(i))) == name) then
        if (ix /= 0) call Die(ExitInput, 'duplicate column in header: ' // name)
        ix = i
      end if
    end do
    if (ix == 0) call Die(ExitInput, 'missing required column: ' // name)
  end function ColumnIndex

  function IsSkippable(line) result(skip)
    character(len=*), intent(in) :: line
    logical :: skip
    character(len=len(line)) :: t
    t = adjustl(line)
    skip = (len_trim(t) == 0) .or. (t(1:1) == '#')
  end function IsSkippable

  subroutine StripCr(line)
    character(len=*), intent(inout) :: line
    integer :: n
    n = len_trim(line)
    if (n > 0) then
      if (line(n:n) == achar(13)) line(n:n) = ' '
    end if
  end subroutine StripCr

  subroutine LoadPortfolio(path, p)
    character(len=*), intent(in) :: path
    type(Portfolio), intent(out) :: p
    character(len=MaxLine) :: line
    character(len=FieldLen) :: f(MaxFields)
    integer :: u, ios, nf, lineNo, j, r, k
    integer :: cId, cReg, cStock, cHaz, cAlpha, cBeta
    logical :: haveHeader, ok
    real(real64) :: v
    character(len=64) :: where

    open (newunit=u, file=trim(path), status='old', action='read', iostat=ios)
    if (ios /= 0) call Die(ExitFile, 'cannot open portfolio file: ' // trim(path))

    ! First pass counts data rows so the arrays are sized exactly.
    p%nProj = 0
    haveHeader = .false.
    do
      read (u, '(A)', iostat=ios) line
      if (ios /= 0) exit
      call StripCr(line)
      if (IsSkippable(line)) cycle
      if (.not. haveHeader) then
        haveHeader = .true.
      else
        p%nProj = p%nProj + 1
      end if
    end do
    if (.not. haveHeader) call Die(ExitInput, 'portfolio file has no header row')
    if (p%nProj == 0) call Die(ExitInput, 'portfolio file has no project rows')
    rewind (u)

    allocate (p%id(p%nProj), p%region(p%nProj), p%stock(p%nProj), p%hazard(p%nProj), &
              p%alpha(p%nProj), p%beta(p%nProj))
    allocate (p%regionName(p%nProj), p%regionStock(p%nProj))
    p%regionStock = 0.0_real64
    p%nRegion = 0

    haveHeader = .false.
    k = 0
    lineNo = 0
    cId = 0; cReg = 0; cStock = 0; cHaz = 0; cAlpha = 0; cBeta = 0
    do
      read (u, '(A)', iostat=ios) line
      if (ios /= 0) exit
      lineNo = lineNo + 1
      call StripCr(line)
      if (IsSkippable(line)) cycle
      if (len_trim(line) >= MaxLine) call Die(ExitInput, 'line is too long')
      call SplitCsv(line, f, nf)
      write (where, '(A,I0)') 'line ', lineNo
      if (.not. haveHeader) then
        cId = ColumnIndex(f, nf, 'project_id')
        cReg = ColumnIndex(f, nf, 'region')
        cStock = ColumnIndex(f, nf, 'stock_tonnes')
        cHaz = ColumnIndex(f, nf, 'annual_hazard')
        cAlpha = ColumnIndex(f, nf, 'sev_alpha')
        cBeta = ColumnIndex(f, nf, 'sev_beta')
        haveHeader = .true.
        cycle
      end if
      if (nf < max(cId, cReg, cStock, cHaz, cAlpha, cBeta)) call Die(ExitInput, trim(where) // ': too few columns')
      k = k + 1
      if (len_trim(f(cId)) == 0) call Die(ExitInput, trim(where) // ': empty project_id')
      if (len_trim(f(cReg)) == 0) call Die(ExitInput, trim(where) // ': empty region')
      p%id(k) = f(cId)
      do j = 1, k - 1
        if (p%id(j) == p%id(k)) call Die(ExitInput, trim(where) // ': duplicate project_id ' // trim(p%id(k)))
      end do

      call ParseReal(f(cStock), v, ok)
      if (.not. ok .or. v <= 0) call Die(ExitInput, trim(where) // ': stock_tonnes must be a positive number')
      p%stock(k) = v
      call ParseReal(f(cHaz), v, ok)
      if (.not. ok .or. v < 0 .or. v > 1) call Die(ExitInput, trim(where) // ': annual_hazard must be in [0,1]')
      p%hazard(k) = v
      call ParseReal(f(cAlpha), v, ok)
      if (.not. ok .or. v <= 0) call Die(ExitInput, trim(where) // ': sev_alpha must be above zero')
      p%alpha(k) = v
      call ParseReal(f(cBeta), v, ok)
      if (.not. ok .or. v <= 0) call Die(ExitInput, trim(where) // ': sev_beta must be above zero')
      p%beta(k) = v

      r = 0
      do j = 1, p%nRegion
        if (trim(p%regionName(j)) == trim(f(cReg))) then
          r = j
          exit
        end if
      end do
      if (r == 0) then
        p%nRegion = p%nRegion + 1
        r = p%nRegion
        p%regionName(r) = f(cReg)
      end if
      p%region(k) = r
      p%regionStock(r) = p%regionStock(r) + p%stock(k)
    end do
    close (u)

    p%totalStock = sum(p%stock)
    if (.not. (p%totalStock > 0.0_real64 .and. p%totalStock <= huge(1.0_real64) / 4)) &
      call Die(ExitInput, 'total stock is not a usable number')
  end subroutine LoadPortfolio

  ! One independent trial. requiredPool is the smallest opening pool (tonnes) that never runs dry.
  ! firstFail is the first year the pool of size poolTonnes would have run dry, or 0 if it never does.
  subroutine SimulateTrial(p, cfg, trial, poolTonnes, requiredPool, firstFail, regionLoss)
    type(Portfolio), intent(in) :: p
    type(Config), intent(in) :: cfg
    integer, intent(in) :: trial
    real(real64), intent(in) :: poolTonnes
    real(real64), intent(out) :: requiredPool
    integer, intent(out) :: firstFail
    real(real64), intent(inout) :: regionLoss(:)
    type(RngState) :: st
    real(real64), allocatable :: stock(:)
    logical, allocatable :: shocked(:)
    real(real64) :: cum, h, mult, loss, deficit
    logical :: globalShock
    integer :: y, i

    allocate (stock(p%nProj), shocked(p%nRegion))
    stock = p%stock
    call SeedStream(st, cfg%seed, int(trial, int64))
    cum = 0.0_real64
    requiredPool = 0.0_real64
    firstFail = 0
    do y = 1, cfg%years
      do i = 1, p%nRegion
        shocked(i) = NextUniform(st) < cfg%shockProb
      end do
      globalShock = NextUniform(st) < cfg%globalProb
      do i = 1, p%nProj
        if (stock(i) <= 0.0_real64) cycle
        mult = 1.0_real64
        if (shocked(p%region(i))) mult = mult * cfg%shockMult
        if (globalShock) mult = mult * cfg%globalMult
        h = min(1.0_real64, p%hazard(i) * mult)
        if (NextUniform(st) < h) then
          loss = DrawBeta(st, p%alpha(i), p%beta(i)) * stock(i)
          stock(i) = stock(i) - loss
          if (stock(i) < 1.0e-9_real64 * p%stock(i)) then
            loss = loss + stock(i)
            stock(i) = 0.0_real64
          end if
          cum = cum + loss
          regionLoss(p%region(i)) = regionLoss(p%region(i)) + loss
        end if
      end do
      deficit = cum - cfg%topup * real(y, real64)
      if (deficit > requiredPool) requiredPool = deficit
      if (firstFail == 0 .and. poolTonnes >= 0.0_real64 .and. deficit > poolTonnes) firstFail = y
    end do
  end subroutine SimulateTrial

  subroutine HeapSort(a)
    real(real64), intent(inout) :: a(:)
    integer :: n, i
    real(real64) :: t
    n = size(a)
    do i = n / 2, 1, -1
      call SiftDown(a, i, n)
    end do
    do i = n, 2, -1
      t = a(1); a(1) = a(i); a(i) = t
      call SiftDown(a, 1, i - 1)
    end do
  end subroutine HeapSort

  subroutine SiftDown(a, start, last)
    real(real64), intent(inout) :: a(:)
    integer, intent(in) :: start, last
    integer :: root, child
    real(real64) :: t
    root = start
    do
      child = 2 * root
      if (child > last) exit
      if (child < last) then
        if (a(child) < a(child + 1)) child = child + 1
      end if
      if (a(root) >= a(child)) exit
      t = a(root); a(root) = a(child); a(child) = t
      root = child
    end do
  end subroutine SiftDown

end module BufferModel

program CarbonBufferPoolSimulator
  use, intrinsic :: iso_fortran_env, only: int64, real64
  use BufferModel
  implicit none

  type(Config) :: cfg
  type(Portfolio) :: p
  real(real64), allocatable :: req(:), sorted(:), tailLoss(:)
  integer, allocatable :: fail(:)
  real(real64) :: poolTonnes, thr, spread, pctV, tailTotal, pFail, se, unmet
  real(real64) :: quant(MaxLevels), quantLo(MaxLevels), quantHi(MaxLevels), cvarV(MaxLevels)
  integer :: t, i, k, kLo, kHi, nTail, nFail, medYear, topLevel, ord(256)
  logical :: gateFailed
  real(real64), parameter :: Z95 = 1.959964_real64

  call ParseArgs(cfg)
  call LoadPortfolio(cfg%path, p)

  poolTonnes = -1.0_real64
  if (cfg%poolPct >= 0.0_real64) poolTonnes = cfg%poolPct / 100.0_real64 * p%totalStock

  allocate (req(cfg%trials), fail(cfg%trials))

  !$omp parallel do default(shared) private(t) schedule(static)
  do t = 1, cfg%trials
    block
      real(real64), allocatable :: rl(:)
      allocate (rl(p%nRegion))
      rl = 0.0_real64
      call SimulateTrial(p, cfg, t, poolTonnes, req(t), fail(t), rl)
    end block
  end do
  !$omp end parallel do

  allocate (sorted(cfg%trials))
  sorted = req
  call HeapSort(sorted)

  do i = 1, cfg%nLevels
    k = min(cfg%trials, max(1, ceiling(real(cfg%trials, real64) * cfg%levels(i))))
    spread = Z95 * sqrt(real(cfg%trials, real64) * cfg%levels(i) * (1.0_real64 - cfg%levels(i)))
    kLo = max(1, k - ceiling(spread))
    kHi = min(cfg%trials, k + ceiling(spread))
    quant(i) = sorted(k)
    quantLo(i) = sorted(kLo)
    quantHi(i) = sorted(kHi)
    cvarV(i) = sum(sorted(k:cfg%trials)) / real(cfg%trials - k + 1, real64)
  end do

  ! Tail attribution: replay only the trials at or above the top confidence quantile.
  ! Streams are seeded per trial, so the replay reproduces the exact same losses.
  topLevel = cfg%nLevels
  do i = 1, cfg%nLevels
    if (cfg%levels(i) > cfg%levels(topLevel)) topLevel = i
  end do
  thr = quant(topLevel)
  allocate (tailLoss(p%nRegion))
  tailLoss = 0.0_real64
  nTail = 0
  !$omp parallel do default(shared) private(t) reduction(+:tailLoss, nTail) schedule(dynamic, 256)
  do t = 1, cfg%trials
    if (req(t) >= thr) then
      block
        real(real64), allocatable :: rl(:)
        real(real64) :: rq
        integer :: ff
        allocate (rl(p%nRegion))
        rl = 0.0_real64
        call SimulateTrial(p, cfg, t, poolTonnes, rq, ff, rl)
        tailLoss = tailLoss + rl
        nTail = nTail + 1
      end block
    end if
  end do
  !$omp end parallel do
  tailTotal = sum(tailLoss)

  ! Optional pool test.
  pFail = 0.0_real64
  unmet = 0.0_real64
  nFail = 0
  medYear = 0
  if (poolTonnes >= 0.0_real64) then
    nFail = count(fail > 0)
    pFail = real(nFail, real64) / real(cfg%trials, real64)
    se = sqrt(pFail * (1.0_real64 - pFail) / real(cfg%trials, real64))
    unmet = sum(max(req - poolTonnes, 0.0_real64)) / real(cfg%trials, real64)
    if (nFail > 0) then
      block
        real(real64), allocatable :: fy(:)
        allocate (fy(nFail))
        fy = real(pack(fail, fail > 0), real64)
        call HeapSort(fy)
        medYear = nint(fy((nFail + 1) / 2))
      end block
    end if
  end if

  gateFailed = .false.
  if (cfg%maxShortfallProb >= 0.0_real64 .and. poolTonnes >= 0.0_real64) &
    gateFailed = pFail > cfg%maxShortfallProb

  ! Order regions by tail loss share, largest first.
  do i = 1, p%nRegion
    ord(min(i, 256)) = i
  end do
  call OrderRegions(tailLoss, ord, min(p%nRegion, 256))

  if (cfg%json) then
    call WriteJson()
  else
    call WriteText()
  end if

  if (gateFailed) stop ExitGate, quiet=.true.

contains

  subroutine OrderRegions(v, o, n)
    real(real64), intent(in) :: v(:)
    integer, intent(inout) :: o(:)
    integer, intent(in) :: n
    integer :: a, b, tmp
    do a = 2, n
      tmp = o(a)
      b = a - 1
      do while (b >= 1)
        if (v(o(b)) >= v(tmp)) exit
        o(b + 1) = o(b)
        b = b - 1
      end do
      o(b + 1) = tmp
    end do
  end subroutine OrderRegions

  subroutine WriteText()
    integer :: a, r, shown
    real(real64) :: tShare, sShare
    write (*, '(A)') 'Carbon buffer pool simulation'
    write (*, '(A)') '============================='
    write (*, '(A,I0,A,I0,A,A,A)') 'Projects: ', p%nProj, '   Regions: ', p%nRegion, &
      '   Stock: ', Fmt(p%totalStock, 0), ' tCO2e'
    write (*, '(A,I0,A,I0,A,I0)') 'Horizon: ', cfg%years, ' years   Trials: ', cfg%trials, '   Seed: ', cfg%seed
    write (*, '(A,A,A,A,A,A,A,A)') 'Regional shock: p=', Fmt(cfg%shockProb, 4), ' x', Fmt(cfg%shockMult, 2), &
      '   Global shock: p=', Fmt(cfg%globalProb, 4), ' x', Fmt(cfg%globalMult, 2)
    write (*, '(A,A,A)') 'Top-up: ', Fmt(cfg%topup, 2), ' tonnes per year'
    write (*, '(A)') ''
    write (*, '(A)') 'Required opening buffer (smallest pool that never runs dry over the horizon)'
    do a = 1, cfg%nLevels
      pctV = 100.0_real64 * quant(a) / p%totalStock
      write (*, '(A,A,A,A,A,A,A,A,A,A,A,A)') '  ', Fmt(100.0_real64 * cfg%levels(a), 2), '%: ', &
        Fmt(quant(a), 0), ' t (', Fmt(pctV, 3), '% of stock)  95% CI ', &
        Fmt(100.0_real64 * quantLo(a) / p%totalStock, 3), '% to ', &
        Fmt(100.0_real64 * quantHi(a) / p%totalStock, 3), '%  tail mean ', &
        Fmt(100.0_real64 * cvarV(a) / p%totalStock, 3), '%'
    end do
    if (poolTonnes >= 0.0_real64) then
      write (*, '(A)') ''
      write (*, '(A,A,A,A,A)') 'Pool test: ', Fmt(cfg%poolPct, 3), '% = ', Fmt(poolTonnes, 0), ' t'
      write (*, '(A,A,A,A,A,A)') '  Shortfall probability: ', Fmt(100.0_real64 * pFail, 3), '% (+/- ', &
        Fmt(100.0_real64 * Z95 * se, 3), '%)'
      write (*, '(A,A,A)') '  Mean unmet reversal per trial: ', Fmt(unmet, 1), ' t'
      if (nFail > 0) write (*, '(A,I0)') '  Median year of first shortfall: ', medYear
      if (cfg%maxShortfallProb >= 0.0_real64) then
        if (gateFailed) then
          write (*, '(A,A)') '  Gate: FAIL, limit ', Fmt(100.0_real64 * cfg%maxShortfallProb, 3)
        else
          write (*, '(A,A)') '  Gate: pass, limit ', Fmt(100.0_real64 * cfg%maxShortfallProb, 3)
        end if
      end if
    end if
    write (*, '(A)') ''
    write (*, '(A,A,A,I0,A)') 'Where the ', Fmt(100.0_real64 * cfg%levels(topLevel), 2), &
      '% tail comes from (', nTail, ' trials)'
    shown = min(p%nRegion, 10, 256)
    do a = 1, shown
      r = ord(a)
      tShare = 0.0_real64
      if (tailTotal > 0.0_real64) tShare = 100.0_real64 * tailLoss(r) / tailTotal
      sShare = 100.0_real64 * p%regionStock(r) / p%totalStock
      write (*, '(A,A,A,A,A,A,A)') '  ', trim(p%regionName(r)), ': ', Fmt(tShare, 1), '% of tail loss vs ', &
        Fmt(sShare, 1), '% of stock'
    end do
  end subroutine WriteText

  subroutine WriteJson()
    integer :: a, r, shown
    real(real64) :: tShare
    write (*, '(A)') '{'
    write (*, '(A,I0,A,I0,A,A,A)') '  "projects": ', p%nProj, ', "regions": ', p%nRegion, &
      ', "stock_tonnes": ', Fmt(p%totalStock, 3), ','
    write (*, '(A,I0,A,I0,A,I0,A)') '  "years": ', cfg%years, ', "trials": ', cfg%trials, ', "seed": ', cfg%seed, ','
    write (*, '(A)') '  "required_buffer": ['
    do a = 1, cfg%nLevels
      write (*, '(A,A,A,A,A,A,A,A,A,A,A,A,A,A)') '    {"confidence": ', Fmt(cfg%levels(a), 6), &
        ', "tonnes": ', Fmt(quant(a), 3), ', "pct_of_stock": ', Fmt(100.0_real64 * quant(a) / p%totalStock, 4), &
        ', "ci_lo_pct": ', Fmt(100.0_real64 * quantLo(a) / p%totalStock, 4), &
        ', "ci_hi_pct": ', Fmt(100.0_real64 * quantHi(a) / p%totalStock, 4), &
        ', "tail_mean_pct": ', Fmt(100.0_real64 * cvarV(a) / p%totalStock, 4), &
        merge('},', '} ', a < cfg%nLevels)
    end do
    write (*, '(A)') '  ],'
    if (poolTonnes >= 0.0_real64) then
      write (*, '(A,A,A,A,A,A,A,I0,A,A,A)') '  "pool_test": {"pool_pct": ', Fmt(cfg%poolPct, 4), &
        ', "pool_tonnes": ', Fmt(poolTonnes, 3), ', "shortfall_probability": ', Fmt(pFail, 6), &
        ', "median_first_shortfall_year": ', medYear, ', "gate_failed": ', &
        merge('true ', 'false', gateFailed), '},'
    end if
    write (*, '(A)') '  "tail_attribution": ['
    shown = min(p%nRegion, 10, 256)
    do a = 1, shown
      r = ord(a)
      tShare = 0.0_real64
      if (tailTotal > 0.0_real64) tShare = tailLoss(r) / tailTotal
      write (*, '(A,A,A,A,A,A,A)') '    {"region": "', trim(p%regionName(r)), '", "tail_share": ', Fmt(tShare, 6), &
        ', "stock_share": ', Fmt(p%regionStock(r) / p%totalStock, 6), &
        merge('},', '} ', a < shown)
    end do
    write (*, '(A)') '  ]'
    write (*, '(A)') '}'
  end subroutine WriteJson

end program CarbonBufferPoolSimulator
