# Untrusted Code Resource Governor

Running code you did not write inside your own JVM used to be a `SecurityManager` problem. That class is gone, so an infinite loop or an allocation bomb from an LLM generated snippet now takes your Java service with it. This is the replacement layer: hard wall clock, CPU time and allocated memory budgets per run, plus a classloader that refuses to link banned APIs.

**Language:** Java | **Lines:** 465 | **Added:** 2026-09-03

## What this solves

Every team shipping an AI code execution feature on the JVM hits the same wall. You have a snippet from a model, a user submission in a coding eval platform or a third party plugin class, and you need to call it in process. The old answer was `SecurityManager`, deprecated for years and removed for good starting with JDK 24. A lot of internal "run this snippet" services were quietly built on it and nobody has replaced that layer yet.

Without a governor the failure modes are boring and expensive. An infinite loop pins a thread forever and your pool bleeds out one request at a time until the service stops accepting work. A snippet that allocates in a loop drives the shared heap into an `OutOfMemoryError` that lands on whatever unrelated request was allocating at that moment, so the stack trace you page on has nothing to do with the code that caused it. A snippet that shells out through `Runtime.exec` does it, because nothing is watching.

None of this requires malice. Ask a model to "process this data faster" and it will hand you an accidental O(n^2) loop or an unbounded cache. The person who notices is on call at 3am reading a heap dump. The common shortcut is to run the untrusted `Callable` on a plain thread, wrap it in a try/catch and call it sandboxed. That is not a sandbox, it is a coin flip. This file replaces it with three measured budgets, a classloader level blocklist and a `Result` object that says what a run cost and which limit it broke.

## Why I built it

The Java ecosystem has plenty of OS level isolation advice (containers, gVisor, a throwaway VM) and almost nothing for the layer inside that boundary. A container will not tell you one eval run burned 4 seconds of CPU, and it gives you no per call circuit breaker signal. Timeout libraries give you wall clock only, which lets a busy loop slide whenever the host is having a slow day.

So this is the in process half of the answer: real budgets on real per thread counters, honest telemetry on what each run consumed, and a documented limit on what in process defense can promise. Zero dependencies, one file, compiles with `javac`.

## When to use it

- An AI coding assistant with a "run my code" button executing generated Java in your service
- An automated grading or benchmarking harness scoring LLM generated solutions where one bad submission must not kill the batch
- A plugin system that loads third party Java classes into a long lived process
- Migrating a service off `SecurityManager` onto JDK 21+ and needing something concrete in its place
- Feeding a circuit breaker with per run cost signals instead of guessing at timeouts

## How it works

`UntrustedCodeResourceGovernor` is `AutoCloseable` and owns two executors. `pool` is `Executors.newThreadPerTaskExecutor` handing out daemon platform threads named `resource-governor-task`, one per governed run. `watchdog` is a single daemon scheduled thread. Platform threads over virtual threads is deliberate and documented inline: `ThreadMXBean`'s per thread CPU time and allocated bytes counters are tracked per carrier thread and return -1 for a virtual thread's own id, which would silently disable two of the three budgets while looking like it worked.

The constructor wires up telemetry and degrades cleanly. It calls `setThreadCpuTimeEnabled(true)`, records `isThreadCpuTimeSupported()`, then tries to cast the bean to the HotSpot specific `com.sun.management.ThreadMXBean` inside a `try`/`catch (ClassCastException)`. On a JVM that does not offer it, `sunThreadBean` stays null and `allocationSupported` stays false, so the memory budget stops being enforced instead of throwing. Defaults are a 15ms poll interval and a 500ms grace window.

`run(Callable<T> task, Budget budget)` is the entry point. It wraps the task so it first publishes its own `Thread` into an `AtomicReference` and counts down a `CountDownLatch`, submits it, then waits up to 2 seconds for that latch. Once it has the thread it schedules `pollOnce` on the watchdog with `scheduleAtFixedRate`, and blocks on `future.get` with a hard timeout of `wallClock + grace`.

`pollOnce` is the enforcement loop. Each tick it reads elapsed nanos from `System.nanoTime`, `threadBean.getThreadCpuTime(threadId)` and `sunThreadBean.getThreadAllocatedBytes(threadId)`, then checks wall clock, CPU and allocation in that order. A counter reporting a negative value is skipped, so an unsupported signal never produces a false violation. On the first breach it uses `compareAndSet` on an `AtomicReference<ViolationKind>` to claim the violation exactly once, snapshots the numbers into two `AtomicLong`s and interrupts the task thread. Kinds are `WALL_CLOCK`, `CPU_TIME`, `MEMORY` and `BLOCKED_API`. If `future.get` times out first, the caller side records `WALL_CLOCK`, interrupts and calls `future.cancel(true)`.

The fourth line of defense is `BlockingClassLoader`, which overrides both `findClass` and `loadClass`. `findClass` defines exactly one class, the target, from raw bytes, so the untrusted class's own references resolve through this loader instead of coming back already resolved from the system loader. `loadClass` checks every requested name against a set of blocked prefixes and throws `BlockedApiException` (a `RuntimeException`, so it escapes linking) on a match. `DEFAULT_BLOCKLIST` covers `java.lang.Runtime`, `java.lang.ProcessBuilder`, `java.lang.reflect.`, `java.lang.invoke.MethodHandles$Lookup`, `java.io.File` and friends, `java.nio.file.`, `java.nio.channels.`, `java.net.` and `sun.misc.Unsafe`. Because the check happens during linking, a snippet reaching for `Runtime` never executes a single instruction. `loadUntrustedTask(className, classBytes, blockedPrefixes)` builds that loader, calls `Class.forName`, instantiates through the no arg constructor and rejects anything that is not a `Callable`.

Everything comes back in `Result<T>`: `value`, `violation`, `failureCause`, `wallMillis`, `cpuMillis` and `allocatedBytes`. `ok()` is true only when both violation and cause are null. `close()` calls `shutdownNow` on both executors and waits a second for termination.

## Usage

```java
try (UntrustedCodeResourceGovernor governor = new UntrustedCodeResourceGovernor()) {

    // 1. Budget: 2s wall clock, 150ms CPU, 1 GB allocated
    Budget budget = Budget.of(Duration.ofSeconds(2), 150, 1_000_000_000L);

    Result<Object> r = governor.run(() -> compute(), budget);
    if (!r.ok()) {
        log.warn("rejected: {} after {}ms wall", r.violation, r.wallMillis);
    }

    // 2. Untrusted bytecode, defined through the blocklist classloader.
    //    The class must implement Callable and have a no-arg constructor.
    Callable<?> task = governor.loadUntrustedTask(
            "com.example.Submission", classBytes, DEFAULT_BLOCKLIST);
    Result<?> out = governor.run(task, Budget.of(Duration.ofSeconds(2), 2_000, 50_000_000L));
}
```

Custom poll and grace windows: `new UntrustedCodeResourceGovernor(Duration.ofMillis(5), Duration.ofMillis(200))`. Run the built in demo, which exercises a busy loop, a memory bomb and a `Runtime.exec` attempt:

```bash
javac UntrustedCodeResourceGovernor.java
java UntrustedCodeResourceGovernor
```

## Notes

- Enforcement is cooperative. The governor interrupts the task thread, and the JVM has no safe way to stop a thread that ignores interrupts. A tight loop with no interrupt check and no blocking call keeps burning a daemon thread after `run` returns with a violation.
- `cpuMillis` and `allocatedBytes` are only populated when the watchdog trips a violation. A clean run and a caller side wall clock timeout both report -1 there.
- If the task thread does not publish itself within 2 seconds, no watchdog is scheduled and only the `future.get` timeout applies.
- The memory budget depends on `com.sun.management.ThreadMXBean`, which ships in mainstream OpenJDK builds. On a JVM without it that budget is silently not enforced. Same for CPU time if `isThreadCpuTimeSupported()` is false.
- Allocated bytes is cumulative allocation on that thread, not live heap. A task that allocates and discards garbage fast trips the budget even with a small retained footprint. Good bomb detector, not a heap quota.
- The blocklist is prefix matching on class names. It stops code that names a banned API, which covers almost everything a model generates. It will not stop someone determined to reach native code through an unblocked path, and it is not an OS level security boundary. Keep the container.
- `loadUntrustedTask` can throw `BlockedApiException` before `run` is ever called, since linking happens there. Catch it at the load site as well as reading `Result.violation`.
