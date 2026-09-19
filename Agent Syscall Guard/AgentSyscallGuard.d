import core.stdc.errno : errno, EINTR, EPERM;
import core.stdc.stdio : perror;
import core.sys.posix.signal : SIGKILL, SIGSTOP, SIGTRAP, SIGALRM, SIGINT, SIGTERM, kill,
    sigaction, sigaction_t, sigemptyset;
import core.sys.posix.sys.types : pid_t;
import core.sys.posix.sys.wait : waitpid, WIFEXITED, WEXITSTATUS, WIFSIGNALED,
    WTERMSIG, WIFSTOPPED, WSTOPSIG;
import core.sys.posix.unistd : fork, execvp, alarm, _exit;
import std.algorithm : startsWith;
import std.array : appender, split;
import std.conv : to;
import std.datetime.systime : Clock;
import std.format : format;
import std.range : enumerate;
import std.stdio : File, stderr, writeln, writefln;
import std.string : strip, toLower, toStringz;

// ---------------------------------------------------------------------------
// Raw ptrace(2) binding. Neither DMD nor LDC ship core.sys.linux.sys.ptrace,
// so this declares exactly the subset used below. Calling glibc's variadic
// ptrace() with a fixed four argument signature is the same approach taken
// by Go's x/sys/unix and Rust's libc crate: on the x86_64 SysV ABI the first
// four integer/pointer arguments land in registers regardless of how the
// callee's prototype was spelled, so this is safe in practice as well as in
// the ABI on paper.
// ---------------------------------------------------------------------------
private extern (C) nothrow @nogc long ptrace(int request, pid_t pid, void* addr, void* data);

private enum : int
{
    PTRACE_TRACEME     = 0,
    PTRACE_PEEKDATA    = 2,
    PTRACE_CONT        = 7,
    PTRACE_GETREGS     = 12,
    PTRACE_SETREGS     = 13,
    PTRACE_SYSCALL     = 24,
    PTRACE_SETOPTIONS  = 0x4200,
    PTRACE_GETEVENTMSG = 0x4201,
}

private enum uint
    PTRACE_O_TRACESYSGOOD = 0x0001,
    PTRACE_O_TRACEFORK    = 0x0002,
    PTRACE_O_TRACEVFORK   = 0x0004,
    PTRACE_O_TRACECLONE   = 0x0008,
    PTRACE_O_TRACEEXEC    = 0x0010,
    PTRACE_O_EXITKILL     = 0x0010_0000;

/// waitpid() flag needed to reap raw clone()-created threads as well as
/// ordinary children; without it a CLONE_THREAD tracee can go unreaped.
private enum int __WALL = 0x4000_0000;

/// Layout of struct user_regs_struct from <sys/user.h> on x86_64 Linux.
/// Field order is part of the kernel/glibc ABI and has been stable since
/// the syscall table itself was introduced.
private struct UserRegsStruct
{
    ulong r15, r14, r13, r12, rbp, rbx, r11, r10, r9, r8,
          rax, rcx, rdx, rsi, rdi, orig_rax, rip, cs, eflags,
          rsp, ss, fs_base, gs_base, ds, es, fs, gs;
}

// ---------------------------------------------------------------------------
// x86_64 Linux syscall table. Numbers are part of the kernel ABI and never
// change once assigned, so this table is safe to hardcode. A policy rule may
// always name a syscall by its raw number instead of by name, so a gap or
// mistake in this table can never make an unrecognised syscall slip past the
// default policy: see resolveSyscall() below.
// ---------------------------------------------------------------------------
private struct SyscallEntry { int num; string name; }

private immutable SyscallEntry[] syscallTable = [
    SyscallEntry(0, "read"), SyscallEntry(1, "write"), SyscallEntry(2, "open"),
    SyscallEntry(3, "close"), SyscallEntry(4, "stat"), SyscallEntry(5, "fstat"),
    SyscallEntry(6, "lstat"), SyscallEntry(7, "poll"), SyscallEntry(8, "lseek"),
    SyscallEntry(9, "mmap"), SyscallEntry(10, "mprotect"), SyscallEntry(11, "munmap"),
    SyscallEntry(12, "brk"), SyscallEntry(13, "rt_sigaction"), SyscallEntry(14, "rt_sigprocmask"),
    SyscallEntry(15, "rt_sigreturn"), SyscallEntry(16, "ioctl"), SyscallEntry(17, "pread64"),
    SyscallEntry(18, "pwrite64"), SyscallEntry(19, "readv"), SyscallEntry(20, "writev"),
    SyscallEntry(21, "access"), SyscallEntry(22, "pipe"), SyscallEntry(23, "select"),
    SyscallEntry(24, "sched_yield"), SyscallEntry(25, "mremap"), SyscallEntry(26, "msync"),
    SyscallEntry(27, "mincore"), SyscallEntry(28, "madvise"), SyscallEntry(29, "shmget"),
    SyscallEntry(30, "shmat"), SyscallEntry(31, "shmctl"), SyscallEntry(32, "dup"),
    SyscallEntry(33, "dup2"), SyscallEntry(34, "pause"), SyscallEntry(35, "nanosleep"),
    SyscallEntry(36, "getitimer"), SyscallEntry(37, "alarm"), SyscallEntry(38, "setitimer"),
    SyscallEntry(39, "getpid"), SyscallEntry(40, "sendfile"), SyscallEntry(41, "socket"),
    SyscallEntry(42, "connect"), SyscallEntry(43, "accept"), SyscallEntry(44, "sendto"),
    SyscallEntry(45, "recvfrom"), SyscallEntry(46, "sendmsg"), SyscallEntry(47, "recvmsg"),
    SyscallEntry(48, "shutdown"), SyscallEntry(49, "bind"), SyscallEntry(50, "listen"),
    SyscallEntry(51, "getsockname"), SyscallEntry(52, "getpeername"), SyscallEntry(53, "socketpair"),
    SyscallEntry(54, "setsockopt"), SyscallEntry(55, "getsockopt"), SyscallEntry(56, "clone"),
    SyscallEntry(57, "fork"), SyscallEntry(58, "vfork"), SyscallEntry(59, "execve"),
    SyscallEntry(60, "exit"), SyscallEntry(61, "wait4"), SyscallEntry(62, "kill"),
    SyscallEntry(63, "uname"), SyscallEntry(64, "semget"), SyscallEntry(65, "semop"),
    SyscallEntry(66, "semctl"), SyscallEntry(67, "shmdt"), SyscallEntry(68, "msgget"),
    SyscallEntry(69, "msgsnd"), SyscallEntry(70, "msgrcv"), SyscallEntry(71, "msgctl"),
    SyscallEntry(72, "fcntl"), SyscallEntry(73, "flock"), SyscallEntry(74, "fsync"),
    SyscallEntry(75, "fdatasync"), SyscallEntry(76, "truncate"), SyscallEntry(77, "ftruncate"),
    SyscallEntry(78, "getdents"), SyscallEntry(79, "getcwd"), SyscallEntry(80, "chdir"),
    SyscallEntry(81, "fchdir"), SyscallEntry(82, "rename"), SyscallEntry(83, "mkdir"),
    SyscallEntry(84, "rmdir"), SyscallEntry(85, "creat"), SyscallEntry(86, "link"),
    SyscallEntry(87, "unlink"), SyscallEntry(88, "symlink"), SyscallEntry(89, "readlink"),
    SyscallEntry(90, "chmod"), SyscallEntry(91, "fchmod"), SyscallEntry(92, "chown"),
    SyscallEntry(93, "fchown"), SyscallEntry(94, "lchown"), SyscallEntry(95, "umask"),
    SyscallEntry(96, "gettimeofday"), SyscallEntry(97, "getrlimit"), SyscallEntry(98, "getrusage"),
    SyscallEntry(99, "sysinfo"), SyscallEntry(100, "times"), SyscallEntry(101, "ptrace"),
    SyscallEntry(102, "getuid"), SyscallEntry(103, "syslog"), SyscallEntry(104, "getgid"),
    SyscallEntry(105, "setuid"), SyscallEntry(106, "setgid"), SyscallEntry(107, "geteuid"),
    SyscallEntry(108, "getegid"), SyscallEntry(109, "setpgid"), SyscallEntry(110, "getppid"),
    SyscallEntry(111, "getpgrp"), SyscallEntry(112, "setsid"), SyscallEntry(113, "setreuid"),
    SyscallEntry(114, "setregid"), SyscallEntry(115, "getgroups"), SyscallEntry(116, "setgroups"),
    SyscallEntry(117, "setresuid"), SyscallEntry(118, "getresuid"), SyscallEntry(119, "setresgid"),
    SyscallEntry(120, "getresgid"), SyscallEntry(121, "getpgid"), SyscallEntry(122, "setfsuid"),
    SyscallEntry(123, "setfsgid"), SyscallEntry(124, "getsid"), SyscallEntry(125, "capget"),
    SyscallEntry(126, "capset"), SyscallEntry(127, "rt_sigpending"), SyscallEntry(128, "rt_sigtimedwait"),
    SyscallEntry(129, "rt_sigqueueinfo"), SyscallEntry(130, "rt_sigsuspend"), SyscallEntry(131, "sigaltstack"),
    SyscallEntry(132, "utime"), SyscallEntry(133, "mknod"), SyscallEntry(134, "uselib"),
    SyscallEntry(135, "personality"), SyscallEntry(136, "ustat"), SyscallEntry(137, "statfs"),
    SyscallEntry(138, "fstatfs"), SyscallEntry(139, "sysfs"), SyscallEntry(140, "getpriority"),
    SyscallEntry(141, "setpriority"), SyscallEntry(142, "sched_setparam"), SyscallEntry(143, "sched_getparam"),
    SyscallEntry(144, "sched_setscheduler"), SyscallEntry(145, "sched_getscheduler"),
    SyscallEntry(146, "sched_get_priority_max"), SyscallEntry(147, "sched_get_priority_min"),
    SyscallEntry(148, "sched_rr_get_interval"), SyscallEntry(149, "mlock"), SyscallEntry(150, "munlock"),
    SyscallEntry(151, "mlockall"), SyscallEntry(152, "munlockall"), SyscallEntry(153, "vhangup"),
    SyscallEntry(154, "modify_ldt"), SyscallEntry(155, "pivot_root"), SyscallEntry(156, "_sysctl"),
    SyscallEntry(157, "prctl"), SyscallEntry(158, "arch_prctl"), SyscallEntry(159, "adjtimex"),
    SyscallEntry(160, "setrlimit"), SyscallEntry(161, "chroot"), SyscallEntry(162, "sync"),
    SyscallEntry(163, "acct"), SyscallEntry(164, "settimeofday"), SyscallEntry(165, "mount"),
    SyscallEntry(166, "umount2"), SyscallEntry(167, "swapon"), SyscallEntry(168, "swapoff"),
    SyscallEntry(169, "reboot"), SyscallEntry(170, "sethostname"), SyscallEntry(171, "setdomainname"),
    SyscallEntry(172, "iopl"), SyscallEntry(173, "ioperm"), SyscallEntry(174, "create_module"),
    SyscallEntry(175, "init_module"), SyscallEntry(176, "delete_module"), SyscallEntry(177, "get_kernel_syms"),
    SyscallEntry(178, "query_module"), SyscallEntry(179, "quotactl"), SyscallEntry(180, "nfsservctl"),
    SyscallEntry(183, "afs_syscall"), SyscallEntry(186, "gettid"), SyscallEntry(187, "readahead"),
    SyscallEntry(188, "setxattr"), SyscallEntry(189, "lsetxattr"), SyscallEntry(190, "fsetxattr"),
    SyscallEntry(191, "getxattr"), SyscallEntry(192, "lgetxattr"), SyscallEntry(193, "fgetxattr"),
    SyscallEntry(194, "listxattr"), SyscallEntry(195, "llistxattr"), SyscallEntry(196, "flistxattr"),
    SyscallEntry(197, "removexattr"), SyscallEntry(198, "lremovexattr"), SyscallEntry(199, "fremovexattr"),
    SyscallEntry(200, "tkill"), SyscallEntry(201, "time"), SyscallEntry(202, "futex"),
    SyscallEntry(203, "sched_setaffinity"), SyscallEntry(204, "sched_getaffinity"),
    SyscallEntry(206, "io_setup"), SyscallEntry(207, "io_destroy"), SyscallEntry(208, "io_getevents"),
    SyscallEntry(209, "io_submit"), SyscallEntry(210, "io_cancel"), SyscallEntry(212, "lookup_dcookie"),
    SyscallEntry(213, "epoll_create"), SyscallEntry(216, "remap_file_pages"), SyscallEntry(217, "getdents64"),
    SyscallEntry(218, "set_tid_address"), SyscallEntry(219, "restart_syscall"), SyscallEntry(220, "semtimedop"),
    SyscallEntry(221, "fadvise64"), SyscallEntry(222, "timer_create"), SyscallEntry(223, "timer_settime"),
    SyscallEntry(224, "timer_gettime"), SyscallEntry(225, "timer_getoverrun"), SyscallEntry(226, "timer_delete"),
    SyscallEntry(227, "clock_settime"), SyscallEntry(228, "clock_gettime"), SyscallEntry(229, "clock_getres"),
    SyscallEntry(230, "clock_nanosleep"), SyscallEntry(231, "exit_group"), SyscallEntry(232, "epoll_wait"),
    SyscallEntry(233, "epoll_ctl"), SyscallEntry(234, "tgkill"), SyscallEntry(235, "utimes"),
    SyscallEntry(237, "mbind"), SyscallEntry(238, "set_mempolicy"), SyscallEntry(239, "get_mempolicy"),
    SyscallEntry(240, "mq_open"), SyscallEntry(241, "mq_unlink"), SyscallEntry(242, "mq_timedsend"),
    SyscallEntry(243, "mq_timedreceive"), SyscallEntry(244, "mq_notify"), SyscallEntry(245, "mq_getsetattr"),
    SyscallEntry(246, "kexec_load"), SyscallEntry(247, "waitid"), SyscallEntry(248, "add_key"),
    SyscallEntry(249, "request_key"), SyscallEntry(250, "keyctl"), SyscallEntry(251, "ioprio_set"),
    SyscallEntry(252, "ioprio_get"), SyscallEntry(253, "inotify_init"), SyscallEntry(254, "inotify_add_watch"),
    SyscallEntry(255, "inotify_rm_watch"), SyscallEntry(256, "migrate_pages"), SyscallEntry(257, "openat"),
    SyscallEntry(258, "mkdirat"), SyscallEntry(259, "mknodat"), SyscallEntry(260, "fchownat"),
    SyscallEntry(261, "futimesat"), SyscallEntry(262, "newfstatat"), SyscallEntry(263, "unlinkat"),
    SyscallEntry(264, "renameat"), SyscallEntry(265, "linkat"), SyscallEntry(266, "symlinkat"),
    SyscallEntry(267, "readlinkat"), SyscallEntry(268, "fchmodat"), SyscallEntry(269, "faccessat"),
    SyscallEntry(270, "pselect6"), SyscallEntry(271, "ppoll"), SyscallEntry(272, "unshare"),
    SyscallEntry(273, "set_robust_list"), SyscallEntry(274, "get_robust_list"), SyscallEntry(275, "splice"),
    SyscallEntry(276, "tee"), SyscallEntry(277, "sync_file_range"), SyscallEntry(278, "vmsplice"),
    SyscallEntry(279, "move_pages"), SyscallEntry(280, "utimensat"), SyscallEntry(281, "epoll_pwait"),
    SyscallEntry(282, "signalfd"), SyscallEntry(283, "timerfd_create"), SyscallEntry(284, "eventfd"),
    SyscallEntry(285, "fallocate"), SyscallEntry(286, "timerfd_settime"), SyscallEntry(287, "timerfd_gettime"),
    SyscallEntry(288, "accept4"), SyscallEntry(289, "signalfd4"), SyscallEntry(290, "eventfd2"),
    SyscallEntry(291, "epoll_create1"), SyscallEntry(292, "dup3"), SyscallEntry(293, "pipe2"),
    SyscallEntry(294, "inotify_init1"), SyscallEntry(295, "preadv"), SyscallEntry(296, "pwritev"),
    SyscallEntry(297, "rt_tgsigqueueinfo"), SyscallEntry(298, "perf_event_open"), SyscallEntry(299, "recvmmsg"),
    SyscallEntry(300, "fanotify_init"), SyscallEntry(301, "fanotify_mark"), SyscallEntry(302, "prlimit64"),
    SyscallEntry(303, "name_to_handle_at"), SyscallEntry(304, "open_by_handle_at"), SyscallEntry(305, "clock_adjtime"),
    SyscallEntry(306, "syncfs"), SyscallEntry(307, "sendmmsg"), SyscallEntry(308, "setns"),
    SyscallEntry(309, "getcpu"), SyscallEntry(310, "process_vm_readv"), SyscallEntry(311, "process_vm_writev"),
    SyscallEntry(312, "kcmp"), SyscallEntry(313, "finit_module"), SyscallEntry(314, "sched_setattr"),
    SyscallEntry(315, "sched_getattr"), SyscallEntry(316, "renameat2"), SyscallEntry(317, "seccomp"),
    SyscallEntry(318, "getrandom"), SyscallEntry(319, "memfd_create"), SyscallEntry(320, "kexec_file_load"),
    SyscallEntry(321, "bpf"), SyscallEntry(322, "execveat"), SyscallEntry(323, "userfaultfd"),
    SyscallEntry(324, "membarrier"), SyscallEntry(325, "mlock2"), SyscallEntry(326, "copy_file_range"),
    SyscallEntry(327, "preadv2"), SyscallEntry(328, "pwritev2"), SyscallEntry(329, "pkey_mprotect"),
    SyscallEntry(330, "pkey_alloc"), SyscallEntry(331, "pkey_free"), SyscallEntry(332, "statx"),
    SyscallEntry(333, "io_pgetevents"), SyscallEntry(334, "rseq"),
    SyscallEntry(424, "pidfd_send_signal"), SyscallEntry(425, "io_uring_setup"),
    SyscallEntry(426, "io_uring_enter"), SyscallEntry(427, "io_uring_register"),
    SyscallEntry(428, "open_tree"), SyscallEntry(429, "move_mount"), SyscallEntry(430, "fsopen"),
    SyscallEntry(431, "fsconfig"), SyscallEntry(432, "fsmount"), SyscallEntry(433, "fspick"),
    SyscallEntry(434, "pidfd_open"), SyscallEntry(435, "clone3"), SyscallEntry(436, "close_range"),
    SyscallEntry(437, "openat2"), SyscallEntry(438, "pidfd_getfd"), SyscallEntry(439, "faccessat2"),
    SyscallEntry(440, "process_madvise"), SyscallEntry(441, "epoll_pwait2"), SyscallEntry(442, "mount_setattr"),
    SyscallEntry(443, "quotactl_fd"), SyscallEntry(444, "landlock_create_ruleset"),
    SyscallEntry(445, "landlock_add_rule"), SyscallEntry(446, "landlock_restrict_self"),
    SyscallEntry(447, "memfd_secret"), SyscallEntry(448, "process_mrelease"),
    SyscallEntry(449, "futex_waitv"), SyscallEntry(450, "set_mempolicy_home_node"),
];

private __gshared string[int] nameByNumber;
private __gshared int[string] numberByName;

shared static this()
{
    foreach (e; syscallTable)
    {
        nameByNumber[e.num] = e.name;
        numberByName[e.name] = e.num;
    }
}

private string nameOf(int num)
{
    if (auto p = num in nameByNumber)
        return *p;
    return format("syscall_%d", num);
}

// ---------------------------------------------------------------------------
// Policy
// ---------------------------------------------------------------------------

private enum Action { allow, deny, kill_ }

private struct Rule
{
    Action action;
    int syscallNum;
    string syscallName;
    bool hasArgMatch;
    string argPrefix;
}

private struct Policy
{
    Action defaultAction = Action.deny;
    Rule[] rules;

    Action decide(int syscallNum, string resolvedArg0) const
    {
        foreach (r; rules)
        {
            if (r.syscallNum != syscallNum)
                continue;
            if (r.hasArgMatch && (resolvedArg0 is null || !resolvedArg0.startsWith(r.argPrefix)))
                continue;
            return r.action;
        }
        return defaultAction;
    }
}

private int resolveSyscall(string token, size_t lineNo)
{
    bool numeric = token.length > 0;
    foreach (c; token)
        if (c < '0' || c > '9')
        {
            numeric = false;
            break;
        }
    if (numeric)
        return token.to!int;
    if (auto p = token in numberByName)
        return *p;
    throw new Exception(format(
        "policy line %d: unknown syscall name '%s' (use the numeric id from your kernel's " ~
        "unistd_64.h if you are certain of it; a bare number is always accepted)", lineNo, token));
}

private Policy loadPolicy(string path)
{
    Policy policy;
    auto f = File(path, "r");
    foreach (lineNo, rawLine; f.byLine.enumerate(1))
    {
        string line = rawLine.idup.strip;
        if (line.length == 0 || line[0] == '#')
            continue;
        auto tokens = line.split;
        if (tokens.length == 0)
            continue;
        string head = tokens[0].toLower;

        if (head == "default")
        {
            if (tokens.length != 2)
                throw new Exception(format("policy line %d: 'default' needs exactly one word, allow or deny", lineNo));
            switch (tokens[1].toLower)
            {
                case "allow": policy.defaultAction = Action.allow; break;
                case "deny":  policy.defaultAction = Action.deny;  break;
                default:
                    throw new Exception(format("policy line %d: default must be 'allow' or 'deny', not '%s'", lineNo, tokens[1]));
            }
            continue;
        }

        Action action;
        switch (head)
        {
            case "allow": action = Action.allow; break;
            case "deny":  action = Action.deny;  break;
            case "kill":  action = Action.kill_; break;
            default:
                throw new Exception(format("policy line %d: expected allow, deny, kill or default, found '%s'", lineNo, head));
        }
        if (tokens.length < 2)
            throw new Exception(format("policy line %d: %s needs a syscall name or number", lineNo, head));

        int num = resolveSyscall(tokens[1], lineNo);
        string name = nameOf(num);
        Rule r;
        r.action = action;
        r.syscallNum = num;
        r.syscallName = name;
        if (tokens.length >= 3)
        {
            if (name != "execve" && name != "execveat")
                throw new Exception(format(
                    "policy line %d: a path argument is only meaningful for execve/execveat, not %s", lineNo, name));
            r.hasArgMatch = true;
            r.argPrefix = tokens[2].idup;
        }
        policy.rules ~= r;
    }
    return policy;
}

// ---------------------------------------------------------------------------
// Tracee memory access
// ---------------------------------------------------------------------------

/// Reads a NUL terminated string out of a traced process's address space,
/// one machine word at a time via PTRACE_PEEKDATA. Returns null if the
/// address is unreadable (already unmapped, wrong architecture, etc.)
/// rather than throwing, because a policy decision still has to be made
/// for the syscall even when the path cannot be recovered.
private string readTraceeCString(pid_t pid, ulong addr, size_t maxLen = 4096)
{
    if (addr == 0)
        return null;
    auto app = appender!(char[]);
    ulong wordAddr = addr;
    outer: while (app.data.length < maxLen)
    {
        errno = 0;
        long word = ptrace(PTRACE_PEEKDATA, pid, cast(void*) wordAddr, null);
        if (word == -1 && errno != 0)
            return null;
        ubyte[8] bytes = (cast(ubyte*) &word)[0 .. 8];
        foreach (b; bytes)
        {
            if (b == 0)
                break outer;
            app.put(cast(char) b);
        }
        wordAddr += 8;
    }
    return app.data.idup;
}

// ---------------------------------------------------------------------------
// Audit log
// ---------------------------------------------------------------------------

private string jsonEscape(string s)
{
    auto app = appender!(char[]);
    foreach (c; s)
    {
        switch (c)
        {
            case '"':  app.put(`\"`); break;
            case '\\': app.put(`\\`); break;
            case '\n': app.put(`\n`); break;
            case '\r': app.put(`\r`); break;
            case '\t': app.put(`\t`); break;
            default:
                if (c < 0x20)
                    app.put(format(`\u%04x`, cast(uint) c));
                else
                    app.put(c);
        }
    }
    return app.data.idup;
}

private void logEvent(File log, pid_t pid, string action, string syscallName, int num, string detail)
{
    if (!log.isOpen)
        return;
    log.writefln(
        `{"ts":"%s","pid":%d,"action":"%s","syscall":"%s","num":%d,"detail":"%s"}`,
        Clock.currTime.toISOExtString, pid, action, jsonEscape(syscallName), num, jsonEscape(detail));
    log.flush;
}

// ---------------------------------------------------------------------------
// Signal state shared with the async-signal-safe handlers below
// ---------------------------------------------------------------------------

private enum StopReason { none, timeout, interrupted }
private __gshared StopReason stopReason = StopReason.none;

extern (C) private void onAlarm(int) nothrow @nogc
{
    stopReason = StopReason.timeout;
}

extern (C) private void onInterrupt(int) nothrow @nogc
{
    stopReason = StopReason.interrupted;
}

/// Installs `handler` for `sig` via sigaction() with no SA_RESTART, so that
/// a blocking waitpid() in the tracer loop is guaranteed to return EINTR
/// instead of being transparently restarted by libc, which is exactly what
/// core.stdc.signal.signal() does by default on Linux and would otherwise
/// make --timeout and Ctrl-C silently do nothing while a tracee is quiet.
private alias SigHandler = extern (C) void function(int) nothrow @nogc;

private void installSignal(int sig, SigHandler handler)
{
    sigaction_t act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = handler;
    sigaction(sig, &act, null);
}

// ---------------------------------------------------------------------------
// The tracer
// ---------------------------------------------------------------------------

private struct TraceeState
{
    bool inSyscall;
    bool pendingDeny;
}

private int runTracer(Policy policy, string[] childArgv, File auditLog)
{
    pid_t child = fork();
    if (child < 0)
    {
        stderr.writeln("agent-syscall-guard: fork failed");
        return 1;
    }

    if (child == 0)
    {
        ptrace(PTRACE_TRACEME, 0, null, null);
        auto cargv = new const(char)*[childArgv.length + 1];
        foreach (i, a; childArgv)
            cargv[i] = a.toStringz;
        cargv[$ - 1] = null;
        execvp(cargv[0], cargv.ptr);
        perror("agent-syscall-guard: execvp");
        _exit(127);
    }

    TraceeState[pid_t] tracees;
    tracees[child] = TraceeState(false, false);
    bool[pid_t] optionsSet;

    long allowedCount, deniedCount, killedCount;
    int mainExitCode = -1;
    bool sawMainExit = false;

    while (tracees.length > 0)
    {
        int status;
        pid_t w = waitpid(-1, &status, __WALL);

        if (w == -1)
        {
            if (errno == EINTR)
            {
                if (stopReason != StopReason.none)
                {
                    foreach (pid, _; tracees)
                        kill(pid, SIGKILL);
                    stderr.writefln("agent-syscall-guard: stopping (%s), sent SIGKILL to %d tracee(s)",
                        stopReason == StopReason.timeout ? "timeout" : "interrupted", tracees.length);
                    break;
                }
                continue;
            }
            break;
        }

        if (WIFEXITED(status) || WIFSIGNALED(status))
        {
            if (w == child)
            {
                sawMainExit = true;
                mainExitCode = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
            }
            tracees.remove(w);
            continue;
        }

        if (!WIFSTOPPED(status))
            continue;

        int stopsig = WSTOPSIG(status);
        int event = status >> 16;

        if (w !in tracees)
            // Auto-attached grandchild from clone/fork/vfork, seen for the
            // first time via its own waitpid() report.
            tracees[w] = TraceeState(false, false);

        if (w !in optionsSet)
        {
            // Every tracee needs its own PTRACE_SETOPTIONS call, whether it
            // is the process we forked ourselves or one auto-attached later:
            // options never propagate from parent tracee to child tracee.
            optionsSet[w] = true;
            ptrace(PTRACE_SETOPTIONS, w, null, cast(void*) cast(size_t)(
                PTRACE_O_TRACESYSGOOD | PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK |
                PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC | PTRACE_O_EXITKILL));
        }

        if (stopsig == (SIGTRAP | 0x80))
        {
            auto st = w in tracees;
            if (!st.inSyscall)
            {
                UserRegsStruct regs;
                ptrace(PTRACE_GETREGS, w, null, &regs);
                int num = cast(int) regs.orig_rax;
                string name = nameOf(num);

                string arg0;
                if (name == "execve")
                    arg0 = readTraceeCString(w, regs.rdi);
                else if (name == "execveat")
                    arg0 = readTraceeCString(w, regs.rsi);

                final switch (policy.decide(num, arg0))
                {
                    case Action.allow:
                        allowedCount++;
                        break;
                    case Action.deny:
                        deniedCount++;
                        st.pendingDeny = true;
                        regs.orig_rax = cast(ulong) -1L;
                        ptrace(PTRACE_SETREGS, w, null, &regs);
                        logEvent(auditLog, w, "deny", name, num, arg0 is null ? "" : arg0);
                        break;
                    case Action.kill_:
                        killedCount++;
                        logEvent(auditLog, w, "kill", name, num, arg0 is null ? "" : arg0);
                        foreach (pid, _; tracees)
                            kill(pid, SIGKILL);
                        break;
                }
                st.inSyscall = true;
            }
            else
            {
                if (st.pendingDeny)
                {
                    UserRegsStruct regs;
                    ptrace(PTRACE_GETREGS, w, null, &regs);
                    regs.rax = cast(ulong) -(cast(long) EPERM);
                    ptrace(PTRACE_SETREGS, w, null, &regs);
                    st.pendingDeny = false;
                }
                st.inSyscall = false;
            }
            ptrace(PTRACE_SYSCALL, w, null, null);
            continue;
        }

        if (stopsig == SIGTRAP)
        {
            // Either a fork/vfork/clone/exec event stop (event != 0) or the
            // plain trap delivered right after PTRACE_TRACEME's execve.
            // Both mean "syscall phase resets"; the next stop for this pid
            // is a fresh syscall-entry.
            tracees[w].inSyscall = false;
            ptrace(PTRACE_SYSCALL, w, null, null);
            continue;
        }

        if (stopsig == SIGSTOP)
        {
            // The SIGSTOP the kernel delivers to a freshly auto-attached
            // clone/fork/vfork child right after PTRACE_O_TRACEFORK et al
            // attach it. Swallow it rather than forwarding it, or the
            // tracee would stop again immediately.
            ptrace(PTRACE_SYSCALL, w, null, null);
            continue;
        }

        // A genuine signal-delivery-stop: pass the signal through so the
        // tracee's own handlers and default dispositions still apply.
        ptrace(PTRACE_SYSCALL, w, null, cast(void*) cast(size_t) stopsig);
    }

    stderr.writefln(
        "agent-syscall-guard: summary allowed=%d denied=%d killed=%d",
        allowedCount, deniedCount, killedCount);

    if (killedCount > 0)
        return 128 + SIGKILL;
    if (sawMainExit)
        return mainExitCode;
    return 1;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

private void printUsage()
{
    stderr.writeln(`agent-syscall-guard - ptrace based syscall allowlist for one command tree

Usage:
  agent-syscall-guard --policy FILE [--log FILE] [--timeout SECONDS] -- COMMAND [ARGS...]

Options:
  --policy FILE     Policy file (required). See format below.
  --log FILE        Append a JSONL audit record for every denied or killed
                     syscall. Allowed syscalls are not logged.
  --timeout SECONDS Kill the whole traced process tree if it is still
                     running after SECONDS. 0 (default) means no timeout.
  -h, --help        Print this message.

Policy file format, one rule per line, first match wins, '#' starts a
comment:
  default deny
  allow open
  allow openat
  allow read
  allow write
  deny ptrace
  kill reboot
  allow execve /usr/bin/python3

A syscall may be named or given as its raw numeric id. This build only
understands the x86_64 syscall table. Only execve and execveat accept a
third column, matched as a prefix against the resolved path argument.`);
}

int main(string[] args)
{
    string policyPath;
    string logPath;
    long timeoutSeconds;

    size_t i = 1;
    for (; i < args.length; i++)
    {
        string a = args[i];
        if (a == "--")
        {
            i++;
            break;
        }
        else if (a == "--policy")
        {
            if (++i >= args.length) { stderr.writeln("agent-syscall-guard: --policy needs a value"); return 2; }
            policyPath = args[i];
        }
        else if (a == "--log")
        {
            if (++i >= args.length) { stderr.writeln("agent-syscall-guard: --log needs a value"); return 2; }
            logPath = args[i];
        }
        else if (a == "--timeout")
        {
            if (++i >= args.length) { stderr.writeln("agent-syscall-guard: --timeout needs a value"); return 2; }
            timeoutSeconds = args[i].to!long;
        }
        else if (a == "-h" || a == "--help")
        {
            printUsage;
            return 0;
        }
        else
        {
            stderr.writefln("agent-syscall-guard: unknown option '%s'", a);
            printUsage;
            return 2;
        }
    }

    string[] childArgv = args[i .. $];
    if (policyPath.length == 0)
    {
        stderr.writeln("agent-syscall-guard: --policy FILE is required");
        printUsage;
        return 2;
    }
    if (childArgv.length == 0)
    {
        stderr.writeln("agent-syscall-guard: no command given after --");
        printUsage;
        return 2;
    }

    Policy policy;
    try
        policy = loadPolicy(policyPath);
    catch (Exception e)
    {
        stderr.writefln("agent-syscall-guard: %s", e.msg);
        return 2;
    }

    File auditLog;
    if (logPath.length)
        auditLog = File(logPath, "a");

    installSignal(SIGINT, &onInterrupt);
    installSignal(SIGTERM, &onInterrupt);
    if (timeoutSeconds > 0)
    {
        installSignal(SIGALRM, &onAlarm);
        alarm(cast(uint) timeoutSeconds);
    }

    return runTracer(policy, childArgv, auditLog);
}
