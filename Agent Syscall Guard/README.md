# Agent Syscall Guard

You want to run an LLM agent's shell command or a piece of LLM-generated code without a container, without root and without waiting for a seccomp-bpf policy to compile. This is a single file D program that puts a syscall allowlist around one command tree using nothing but ptrace, and it kills, denies or lets through every syscall the process and everything it forks makes.

**Language:** D | **Lines:** 726 | **Added:** 2026-09-19

## What this solves

Running an AI agent's command or an LLM's generated script somewhere other than a full container is a real, everyday problem, not a hypothetical. A CI runner without `CAP_SYS_ADMIN` cannot set up seccomp-bpf. A shared box running several agents at once does not want a Docker daemon per task. A developer testing a coding agent locally wants to see exactly what syscalls it is making before deciding whether to trust it with a real project. In all three cases the honest options today are "run it raw and hope" or "stand up a whole container runtime for one command," and neither is what the situation calls for.

`ptrace` has been able to intercept every syscall a traced process makes since long before seccomp existed, and it needs no special capability beyond being the parent (or an attacher) of the traced process. What it does not come with is a policy engine, a way to keep watching the forks and clones an agent's shell spawns, or a way to turn a denial into a clean `EPERM` instead of a crash. Agent Syscall Guard is that missing layer: point it at a plain text policy file and a command, and it forks the command under `PTRACE_TRACEME`, decides allow, deny or kill for every syscall by number, and does the same for every child, grandchild and thread the command creates, automatically, without you having to reattach anything by hand.

The failure mode this replaces is the one where a sandboxing script traces the top level process, feels safe, and then the agent's shell forks a `python3` subprocess that runs completely untraced because nobody re-attached to it. Or the denial path itself crashes the sandboxed program with a signal instead of returning a normal error code, so a well-behaved agent that would have handled "permission denied" and moved on instead dies noisily and the whole run has to be retried. Both of those are fixed here by construction: `PTRACE_O_TRACEFORK`, `PTRACE_O_TRACEVFORK` and `PTRACE_O_TRACECLONE` are turned on for every tracee including ones we did not fork ourselves, and a denied syscall comes back to the caller as a normal `-EPERM` return value rather than a fault.

## Why I built it

Most of what I found under "run untrusted code safely" either wants a container runtime, wants CAP_SYS_ADMIN for seccomp-bpf, or is a hosted product that runs your code on someone else's machine. None of that fits the case of "I have one command, I want a fast no on eleven syscalls and I want it to run in a shell script in five minutes." ptrace-based syscall interception is old, well understood and needs nothing extra installed, but every example I could find online traces exactly one process and calls it done, which is not how a real agent's shell, its subprocess and that subprocess's own children actually behave.

The other reason to write this in D specifically: this is exactly the kind of code where you are calling raw kernel interfaces, poking at another process's registers and reading its memory word by word, and you want that to be explicit and typed rather than buried in a scripting language's FFI layer. D's `extern(C)` declarations, its struct layout control and its `@system`/`@safe` distinction make the boundary between "this touches raw memory" and "this is ordinary logic" visible in the source itself, which matters a lot in a security tool where a reviewer needs to be able to see exactly where the dangerous parts are.

## When to use it

- Running one shell command or script from an AI coding agent where you want a fast, capability-free no on syscalls like `ptrace`, `mount`, `reboot`, `init_module` or raw `bpf`, without standing up a container.
- CI runners and shared build boxes where you cannot get `CAP_SYS_ADMIN` for seccomp-bpf but can still fork and trace your own child processes.
- Auditing what an unfamiliar script or agent actually does at the syscall level before deciding whether to trust it, using `--log` to get a JSONL record of every syscall it was refused.
- Locking a subprocess to only exec a specific, known interpreter path (`allow execve /usr/bin/python3`) so an agent cannot shell out to something else even if its own code is compromised.
- Bounding a possibly-hanging agent task with `--timeout` so a stuck process tree gets `SIGKILL`ed instead of occupying a CI slot forever.

## How it works

`main` parses `--policy`, `--log`, `--timeout` and the `--` separated command, loads the policy with `loadPolicy`, installs signal handlers with `installSignal`, and calls `runTracer`. `loadPolicy` reads the file line by line, treats `#` as a comment, and recognizes three kinds of line: `default allow` or `default deny` sets `Policy.defaultAction`, and `allow`, `deny` or `kill` followed by a syscall name or number builds a `Rule` appended to `Policy.rules`. `resolveSyscall` accepts either form: an all-digit token is used directly as the syscall number, and a name is looked up in `numberByName`, a table built once at startup by `shared static this()` from the `syscallTable` array of `SyscallEntry(number, name)` pairs covering the x86_64 syscall table from `read` at 0 through `set_mempolicy_home_node` at 450. Because a bare number always works, a gap or a mistake in that name table can never let an unrecognized syscall slip past the default policy; naming is a convenience, numbers are ground truth. A rule may carry a third column only for `execve` or `execveat`, matched later as a prefix against the resolved path argument; `loadPolicy` rejects a third column on any other syscall at load time rather than silently ignoring it.

`runTracer` forks the target command. The child calls the raw `ptrace(PTRACE_TRACEME, ...)` binding declared at the top of the file (druntime ships no `core.sys.linux.sys.ptrace`, so this file declares the four-argument extern itself, the same approach Go's `x/sys/unix` and Rust's `libc` crate take) and then `execvp`s into the real command. The parent runs a `waitpid(-1, &status, __WALL)` loop, and for every stop it has not seen the pid for yet, calls `PTRACE_SETOPTIONS` with `PTRACE_O_TRACESYSGOOD | PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK | PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC | PTRACE_O_EXITKILL`, because options never propagate from a tracee to its own children; each one needs the call again at its own first stop. `PTRACE_O_EXITKILL` is the safety net: if this program crashes or is killed, the kernel `SIGKILL`s every tracee itself, so a dead guard can never leave an untraced process running loose.

Each tracee's state is a `TraceeState { inSyscall, pendingDeny }`, because a syscall-stop under `PTRACE_O_TRACESYSGOOD` alternates strictly between an entry stop and an exit stop for as long as the tracer keeps resuming with `PTRACE_SYSCALL`. On entry, `PTRACE_GETREGS` reads a `UserRegsStruct` matching the kernel's `user_regs_struct` layout, `orig_rax` gives the syscall number, and for `execve`/`execveat` the path argument is read out of the tracee's own memory by `readTraceeCString`, one machine word at a time via `PTRACE_PEEKDATA`, stopping at the first NUL byte. `Policy.decide` walks the rules in file order and returns the first match's action, or the default. An `allow` just resumes. A `deny` sets `pendingDeny`, overwrites `orig_rax` with an invalid syscall number so the kernel skips the real syscall entirely, and at the matching exit stop overwrites `rax` with `-EPERM`, so the sandboxed process sees a normal permission error instead of a fault or a silent hang. A `kill` sends `SIGKILL` to every tracee in the tree at once. Every `deny` and `kill` decision is written to the audit log by `logEvent` as one JSON line with a timestamp, pid, action, syscall name and number, and the resolved path when there was one; `allow` decisions are not logged, since a full trace would be an `strace`, not an allowlist.

Plain signal-delivery-stops, the kind that happen when the tracee receives a real signal like `SIGSEGV` or `SIGTERM`, are passed straight through by resuming with that signal as the fourth `ptrace` argument, so the tracee's own handlers and defaults still apply and a process that catches its own `SIGTERM` to exit cleanly keeps working even while fully traced. `--timeout` and Ctrl-C both rely on `installSignal`, which uses `sigaction` directly with no `SA_RESTART`, specifically because `core.stdc.signal.signal` on Linux installs with restart-on-interrupt semantics by default, which would otherwise let a blocking `waitpid` swallow the alarm and let a hung command run to completion anyway.

## Usage

```bash
ldc2 -O2 -of=agent-syscall-guard AgentSyscallGuard.d

cat > policy.txt <<'EOF'
default deny
allow read
allow write
allow open
allow openat
allow close
allow mmap
allow mprotect
allow munmap
allow brk
allow rt_sigaction
allow rt_sigprocmask
allow rt_sigreturn
allow access
allow execve /usr/bin/python3
allow arch_prctl
allow set_tid_address
allow exit_group
deny ptrace
kill reboot
EOF

./agent-syscall-guard --policy policy.txt -- python3 agent_task.py

./agent-syscall-guard --policy policy.txt --log audit.jsonl --timeout 30 -- ./run_agent.sh
```

A denied syscall makes the sandboxed process see `EPERM`, the same as if permission had really been refused, so anything that already handles a permission error keeps working. `--log` appends one JSON object per denied or killed syscall to the given file. `--timeout` sends `SIGKILL` to the whole traced tree if it is still running after that many seconds; omitted or `0` means no timeout. `agent-syscall-guard --help` prints the full option and policy reference.

## Notes

- x86_64 Linux only. The `UserRegsStruct` layout and the syscall table are both specific to that architecture; porting to aarch64 means a different register struct and a different syscall table, not a different design.
- The policy applies uniformly to every process in the tree. There is no per-child override, so if an agent's shell needs a wider allowlist than the interpreter it launches, write the policy for the union of both.
- Reading a tracee's memory with `PTRACE_PEEKDATA` one word at a time is simple and dependency free, not the fastest option available; for a program making millions of `execve` calls a second this would be a bottleneck, which is not the workload this was built for.
- A `kill` rule tears down the entire tree, not just the offending process, on the reasoning that a process attempting a syscall you consider a kill-worthy violation should not be trusted to keep any of its siblings, workers or already-forked children running either.
- This is a syscall filter, not a filesystem or network sandbox. Allowing `open` and `openat` still lets the process open any path it has permission for; pair this with a restricted user, a chroot or a mount namespace if you also need to bound the filesystem it can see.
