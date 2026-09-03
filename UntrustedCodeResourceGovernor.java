import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs untrusted or LLM-generated Java code in-process under a hard wall-clock,
 * CPU-time and allocated-memory budget, with a classloader-level API blocklist
 * as a defense-in-depth layer. Built for JDK 21+, where {@code SecurityManager}
 * no longer exists (see JEP 411/486).
 */
public final class UntrustedCodeResourceGovernor implements AutoCloseable {

    private final ExecutorService pool;
    private final ScheduledExecutorService watchdog;
    private final ThreadMXBean threadBean;
    private final com.sun.management.ThreadMXBean sunThreadBean;
    private final boolean cpuTimeSupported;
    private final boolean allocationSupported;
    private final Duration pollInterval;
    private final Duration grace;

    public UntrustedCodeResourceGovernor() {
        this(Duration.ofMillis(15), Duration.ofMillis(500));
    }

    public UntrustedCodeResourceGovernor(Duration pollInterval, Duration grace) {
        // Deliberately platform threads, not virtual threads: ThreadMXBean's per-thread
        // CPU-time and allocated-bytes counters are tracked per carrier thread and return
        // -1 for a virtual thread's own id, which would silently disable two of the three
        // budgets below. One short-lived platform thread per governed run is the right
        // trade here even though virtual threads are usually the 2026 default choice.
        this.pool = Executors.newThreadPerTaskExecutor(r -> {
            Thread t = new Thread(r, "resource-governor-task");
            t.setDaemon(true);
            return t;
        });
        this.watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "resource-governor-watchdog");
            t.setDaemon(true);
            return t;
        });
        this.threadBean = ManagementFactory.getThreadMXBean();
        this.threadBean.setThreadCpuTimeEnabled(true);
        this.cpuTimeSupported = threadBean.isThreadCpuTimeSupported();

        com.sun.management.ThreadMXBean sunBean;
        boolean allocSupported;
        try {
            sunBean = (com.sun.management.ThreadMXBean) threadBean;
            sunBean.setThreadAllocatedMemoryEnabled(true);
            allocSupported = sunBean.isThreadAllocatedMemorySupported();
        } catch (ClassCastException notHotSpot) {
            sunBean = null;
            allocSupported = false;
        }
        this.sunThreadBean = sunBean;
        this.allocationSupported = allocSupported;
        this.pollInterval = pollInterval;
        this.grace = grace;
    }

    /** A wall-clock / CPU-time / allocated-bytes ceiling for one run. */
    public static final class Budget {
        public final Duration wallClock;
        public final long cpuNanos;
        public final long allocatedBytes;

        private Budget(Duration wallClock, long cpuNanos, long allocatedBytes) {
            this.wallClock = wallClock;
            this.cpuNanos = cpuNanos;
            this.allocatedBytes = allocatedBytes;
        }

        public static Budget of(Duration wallClock, long cpuMillis, long allocatedBytesLimit) {
            if (wallClock == null || wallClock.isNegative() || wallClock.isZero()) {
                throw new IllegalArgumentException("wallClock must be positive");
            }
            if (cpuMillis <= 0) {
                throw new IllegalArgumentException("cpuMillis must be positive");
            }
            if (allocatedBytesLimit <= 0) {
                throw new IllegalArgumentException("allocatedBytesLimit must be positive");
            }
            return new Budget(wallClock, TimeUnit.MILLISECONDS.toNanos(cpuMillis), allocatedBytesLimit);
        }
    }

    /** Thrown by {@link BlockingClassLoader} when untrusted code touches a banned class. */
    public static final class BlockedApiException extends RuntimeException {
        public BlockedApiException(String className) {
            super("blocked API reference: " + className);
        }
    }

    public enum ViolationKind { WALL_CLOCK, CPU_TIME, MEMORY, BLOCKED_API }

    /** Outcome of one governed run: either a value, or the budget/violation that stopped it. */
    public static final class Result<T> {
        public final T value;
        public final ViolationKind violation;
        public final Throwable failureCause;
        public final long wallMillis;
        public final long cpuMillis;
        public final long allocatedBytes;

        Result(T value, ViolationKind violation, Throwable failureCause,
               long wallMillis, long cpuMillis, long allocatedBytes) {
            this.value = value;
            this.violation = violation;
            this.failureCause = failureCause;
            this.wallMillis = wallMillis;
            this.cpuMillis = cpuMillis;
            this.allocatedBytes = allocatedBytes;
        }

        public boolean ok() {
            return violation == null && failureCause == null;
        }

        @Override
        public String toString() {
            return "Result{ok=" + ok()
                    + ", violation=" + violation
                    + ", failureCause=" + (failureCause == null ? "-" : failureCause)
                    + ", wallMillis=" + wallMillis
                    + ", cpuMillis=" + cpuMillis
                    + ", allocatedBytes=" + allocatedBytes
                    + ", value=" + value + '}';
        }
    }

    /**
     * A classloader that (a) defines exactly one target class from raw bytes instead of
     * delegating it to the parent, so the target's own class references resolve through
     * this loader, and (b) refuses to resolve any class whose name starts with a blocked
     * prefix, from anywhere reachable through this loader.
     */
    public static final class BlockingClassLoader extends ClassLoader {
        private final Set<String> blockedPrefixes;
        private final String targetClassName;
        private final byte[] targetClassBytes;

        public BlockingClassLoader(ClassLoader parent, Set<String> blockedPrefixes,
                                    String targetClassName, byte[] targetClassBytes) {
            super(parent);
            this.blockedPrefixes = Set.copyOf(blockedPrefixes);
            this.targetClassName = targetClassName;
            this.targetClassBytes = targetClassBytes.clone();
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            if (name.equals(targetClassName)) {
                return defineClass(name, targetClassBytes, 0, targetClassBytes.length);
            }
            throw new ClassNotFoundException(name);
        }

        @Override
        protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            for (String prefix : blockedPrefixes) {
                if (name.startsWith(prefix)) {
                    throw new BlockedApiException(name);
                }
            }
            Class<?> result = findLoadedClass(name);
            if (result == null) {
                result = name.equals(targetClassName) ? findClass(name) : super.loadClass(name, false);
            }
            if (resolve) {
                resolveClass(result);
            }
            return result;
        }
    }

    /** A reasonable starting blocklist: process control, raw filesystem/network I/O, reflection escapes. */
    public static final Set<String> DEFAULT_BLOCKLIST = Set.of(
            "java.lang.Runtime",
            "java.lang.ProcessBuilder",
            "java.lang.reflect.",
            "java.lang.invoke.MethodHandles$Lookup",
            "java.io.File",
            "java.io.FileOutputStream",
            "java.io.FileWriter",
            "java.io.RandomAccessFile",
            "java.nio.file.",
            "java.nio.channels.",
            "java.net.",
            "sun.misc.Unsafe"
    );

    /**
     * Defines {@code className} from {@code classBytes} through a fresh {@link BlockingClassLoader}
     * and instantiates it. The class must implement {@code Callable<Object>} with a no-arg constructor.
     */
    public Callable<?> loadUntrustedTask(String className, byte[] classBytes, Set<String> blockedPrefixes)
            throws ReflectiveOperationException {
        BlockingClassLoader loader = new BlockingClassLoader(
                getClass().getClassLoader(), blockedPrefixes, className, classBytes);
        Class<?> clazz = Class.forName(className, true, loader);
        Object instance = clazz.getDeclaredConstructor().newInstance();
        if (!(instance instanceof Callable<?> callable)) {
            throw new IllegalArgumentException(className + " must implement java.util.concurrent.Callable");
        }
        return callable;
    }

    /** Runs {@code task} on a fresh platform thread, enforcing {@code budget} and returning how it ended. */
    public <T> Result<T> run(Callable<T> task, Budget budget) {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(budget, "budget");

        AtomicReference<Thread> threadRef = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<ViolationKind> violationKind = new AtomicReference<>();
        AtomicLong finalCpuNanos = new AtomicLong(-1);
        AtomicLong finalAllocatedBytes = new AtomicLong(-1);

        Callable<T> wrapped = () -> {
            threadRef.set(Thread.currentThread());
            started.countDown();
            return task.call();
        };

        long startNanos = System.nanoTime();
        Future<T> future = pool.submit(wrapped);

        try {
            started.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Thread taskThread = threadRef.get();
        ScheduledFuture<?> watch = null;
        if (taskThread != null) {
            long threadId = taskThread.threadId();
            watch = watchdog.scheduleAtFixedRate(() -> pollOnce(
                    budget, taskThread, threadId, startNanos,
                    violationKind, finalCpuNanos, finalAllocatedBytes
            ), pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS);
        }

        T value = null;
        Throwable failure = null;
        try {
            long hardTimeoutMillis = budget.wallClock.toMillis() + grace.toMillis();
            value = future.get(hardTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            violationKind.compareAndSet(null, ViolationKind.WALL_CLOCK);
            if (taskThread != null) {
                taskThread.interrupt();
            }
            future.cancel(true);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof BlockedApiException) {
                violationKind.compareAndSet(null, ViolationKind.BLOCKED_API);
            }
            failure = cause;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = e;
        } finally {
            if (watch != null) {
                watch.cancel(false);
            }
        }

        long wallMillis = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
        long cpuNanosFinal = finalCpuNanos.get();
        long cpuMillis = cpuNanosFinal >= 0 ? Duration.ofNanos(cpuNanosFinal).toMillis() : -1;
        return new Result<>(value, violationKind.get(), failure, wallMillis, cpuMillis, finalAllocatedBytes.get());
    }

    private void pollOnce(Budget budget, Thread taskThread, long threadId, long startNanos,
                           AtomicReference<ViolationKind> violationKind,
                           AtomicLong finalCpuNanos, AtomicLong finalAllocatedBytes) {
        if (violationKind.get() != null) {
            return;
        }
        long wallNanos = System.nanoTime() - startNanos;
        long cpuNanos = cpuTimeSupported ? threadBean.getThreadCpuTime(threadId) : -1;
        long allocBytes = allocationSupported ? sunThreadBean.getThreadAllocatedBytes(threadId) : -1;

        ViolationKind kind = null;
        if (wallNanos > budget.wallClock.toNanos()) {
            kind = ViolationKind.WALL_CLOCK;
        } else if (cpuNanos >= 0 && cpuNanos > budget.cpuNanos) {
            kind = ViolationKind.CPU_TIME;
        } else if (allocBytes >= 0 && allocBytes > budget.allocatedBytes) {
            kind = ViolationKind.MEMORY;
        }
        if (kind != null && violationKind.compareAndSet(null, kind)) {
            finalCpuNanos.set(cpuNanos);
            finalAllocatedBytes.set(allocBytes);
            taskThread.interrupt();
        }
    }

    @Override
    public void close() {
        watchdog.shutdownNow();
        pool.shutdownNow();
        try {
            pool.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** A demo "untrusted" class, compiled normally, whose bytecode we re-load through the blocklist. */
    public static final class DemoRuntimeExecTask implements Callable<Object> {
        @Override
        public Object call() throws Exception {
            Runtime.getRuntime().exec(new String[] {"echo", "you-should-not-see-this"});
            return "escaped the sandbox";
        }
    }

    private static byte[] readClassBytes(Class<?> clazz) throws IOException {
        String resource = clazz.getName().replace('.', '/') + ".class";
        try (InputStream in = clazz.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("class bytes not found on classpath: " + resource);
            }
            return in.readAllBytes();
        }
    }

    public static void main(String[] args) throws Exception {
        try (UntrustedCodeResourceGovernor governor = new UntrustedCodeResourceGovernor()) {

            Callable<Object> spinTask = () -> {
                long iterations = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    iterations++;
                }
                return iterations;
            };
            Result<Object> cpuResult = governor.run(
                    spinTask, Budget.of(Duration.ofSeconds(2), 150, 1_000_000_000L));
            System.out.println("busy-loop task      -> " + cpuResult);

            Callable<Object> memoryBombTask = () -> {
                List<byte[]> hoard = new ArrayList<>();
                while (!Thread.currentThread().isInterrupted()) {
                    hoard.add(new byte[1_000_000]);
                }
                return hoard.size();
            };
            Result<Object> memResult = governor.run(
                    memoryBombTask, Budget.of(Duration.ofSeconds(2), 5_000, 20_000_000L));
            System.out.println("memory-bomb task     -> " + memResult);

            byte[] bytes = readClassBytes(DemoRuntimeExecTask.class);
            Callable<?> blockedTask = governor.loadUntrustedTask(
                    DemoRuntimeExecTask.class.getName(), bytes, DEFAULT_BLOCKLIST);
            Result<?> blockedResult = governor.run(
                    blockedTask, Budget.of(Duration.ofSeconds(2), 2_000, 50_000_000L));
            System.out.println("blocked-runtime task -> " + blockedResult);
        }
    }
}

/*
 * ============================================================================
 * WHAT THIS IS AND WHY IT EXISTS
 * ============================================================================
 *
 * This solves a problem every team building an AI code-execution feature runs
 * into on the JVM in 2026: how do you run code you did not write (an LLM's
 * generated snippet, a user's submission in a coding-eval platform, a plugin)
 * inside your own Java process, without one bad or malicious run taking the
 * whole service down. The old answer used to be SecurityManager. That is gone
 * now (deprecated for years, removed for good starting with JDK 24, and the
 * newer LTS releases everyone is migrating onto in 2026 do not have it). A lot
 * of internal "run this snippet" services out there were quietly built on top
 * of SecurityManager and nobody has replaced that layer yet. This file is that
 * replacement layer, done properly instead of hacked together under deadline.
 *
 * Built because I kept seeing the same shortcut in code-execution services:
 * just run the untrusted callable on a plain thread and hope for the best, or
 * wrap it in a try/catch and call it "sandboxed." That is not a sandbox, it is
 * a coin flip. A snippet with an infinite loop pins a thread forever. A
 * snippet that allocates in a loop takes down the JVM with an OutOfMemoryError
 * that can affect unrelated requests sharing the heap. A snippet that shells
 * out or opens a socket does whatever it wants because nothing is watching.
 * None of that requires malice, an LLM asked to "process this data faster"
 * will happily hand you an accidental O(n^2) loop or an unbounded cache.
 *
 * Use it when you are building anything that executes code you do not fully
 * trust inside a JVM process: an AI coding assistant's "run my code" button,
 * an automated grading or benchmarking harness for LLM-generated solutions, a
 * plugin system that loads third-party Java classes, or an internal tool that
 * evaluates user-submitted transformations. It is not a replacement for OS
 * level isolation (containers, gVisor, a throwaway VM) if you need a hard
 * security boundary against a genuinely adversarial attacker with time on
 * their hands, that still belongs in your deployment, not just your code. What
 * this gives you is the layer that goes inside that boundary: real budgets,
 * real telemetry on what a run actually cost, and a documented, honest limit
 * on what in-process defense can and cannot guarantee.
 *
 * The trick: three independent signals, checked together, on a dedicated
 * thread for exactly one run. Wall-clock time catches a hang. Thread-level
 * CPU time (via ThreadMXBean.getThreadCpuTime, not the process-wide number)
 * catches a busy loop that a wall-clock budget alone would let slide if the
 * host machine is just generally slow that day. Thread-level allocated bytes
 * (via the HotSpot-specific com.sun.management.ThreadMXBean, guarded so it
 * degrades cleanly on JVMs that lack it) catches a memory bomb before it ever
 * reaches the point of forcing a GC pause or an OutOfMemoryError on the whole
 * heap. That thread is a plain platform thread on purpose, not a virtual one:
 * I tried virtual threads first, since that is the reflexive answer to
 * "one thread per short task" on modern Java, and both of those per-thread
 * counters quietly returned -1 for every virtual thread id I tested. They are
 * tracked per carrier thread underneath, not per virtual thread, so a virtual
 * thread would have silently disabled two of the three budgets while looking
 * like it worked. That is exactly the kind of gap that only shows up once a
 * real snippet burns real CPU in production, so this file pins platform
 * threads and says why instead of leaving it as a surprise. All three signals
 * get polled by one lightweight watchdog thread, and the moment any one
 * trips, the offending thread gets interrupted and the exact reason comes
 * back in the Result. On top of the three budgets, a custom
 * ClassLoader gives you a fourth, cheaper line of defense: define the
 * untrusted class fresh through that loader instead of letting the system
 * classloader hand you an already-resolved copy, and any reference it makes
 * to a blocklisted class (Runtime, ProcessBuilder, File, Socket, raw
 * reflection) throws before the JVM even finishes linking it, so the "escape"
 * attempt never executes a single instruction. That last part will not stop
 * someone determined to reach native code through an unblocked path, and this
 * file says so plainly rather than pretending otherwise, but it stops the
 * overwhelming majority of code that reaches for a banned API by name, which
 * covers almost everything an LLM actually generates.
 *
 * Drop this into any Java 21+ service that runs a "call()" it did not author.
 * Compile it standalone with javac, there are zero third-party dependencies,
 * only java.base and the
 * com.sun.management extension that ships in every mainstream OpenJDK build.
 * Wire your real untrusted bytecode into loadUntrustedTask instead of the demo
 * class in main, tune the Budget numbers to your workload, and treat a
 * non-null Result.violation as your circuit breaker signal into whatever
 * queue or rate limiter sits in front of this. Same idea works past pure Java:
 * anywhere you are one hop away from removing SecurityManager and have not
 * decided what replaces it yet, start here.
 * ============================================================================
 */
