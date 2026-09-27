# Deterministic Gradient Reduction

Sum a gradient buffer with more threads or fewer and get the exact same bits back, down to the last mantissa digit, while also landing closer to the true mathematical answer than a naive loop ever will.

**Language:** Fortran | **Lines:** 462 (284 module, 178 driver) | **Added:** 2026-09-27

## What this solves

Floating point addition is not associative. `(a + b) + c` and `a + (b + c)` can round to different values, and once you are summing millions of numbers the order those additions happen in is decided by whatever the runtime scheduler feels like doing that day. That is exactly what happens inside a parallel reduction: OpenMP, MPI all-reduce, and every GPU collective library split the array across workers, add up each worker's slice, and then combine the partial sums. Change the worker count, change a batch size, run on a different node shape, and the partial sums get sliced differently, added in a different order, and the final total comes out a few ULPs off from last time.

For a one-off number that barely matters. For a training loop it is a real problem. A model trained on 8 GPUs and the same model trained on 16 GPUs with the same data, same seed and same hyperparameters will diverge in the loss curve after enough steps, because gradient reduction rounds differently each time, and each rounding difference compounds through the optimizer step, the next forward pass and the next backward pass. Debugging "why did my loss curve change when I only changed the node count" wastes days because the natural assumption is a code bug, not floating point rounding. The same non-determinism breaks bitwise reproducibility for compliance and audit trails, breaks "resume from checkpoint on a different cluster" workflows when a discrepancy check compares checksums, and turns flaky test failures in CI into unreproducible ones because a "flaky" numerical assertion actually depends on how many workers the CI runner happened to schedule.

The usual fixes make it worse or don't fix it. Summing everything on a single thread in a fixed order is deterministic but throws away the whole reason you parallelized. Kahan summation on each worker's slice cuts the rounding error inside that slice but does nothing about the fact that the slice boundaries themselves depend on the worker count, so two different worker counts still walk a different tree and still land on different bits. What actually fixes it is making the *shape of the reduction tree* a pure function of the input length alone, never of how many workers happen to be available, and then making each node of that tree numerically strong enough that combining two subtree results does not throw away precision either.

This module does both halves of that at once. `DeterministicGradientReduction.f90` builds a binary reduction tree whose split points come only from array indices (`mid = lo + (hi - lo) / 2`), so the pairing of every addition is fixed before a single OpenMP thread is spawned. Each leaf and each internal node carries its running total as a double-double pair instead of a single rounded number, so the correction term that would normally be discarded at every level survives all the way to the root.

## Why I built it

I kept seeing the same complaint in different clothes: a training run that will not reproduce across cluster sizes, a batch job whose checksum changes when the shard count changes, a scientific simulation whose regression test fails only on the CI runner with a different core count. Every time, the root cause was the same one line of received wisdom nobody double checks: "floating point addition is basically associative for practical purposes." It is not, and once a reduction runs across a variable number of workers that "basically" gap turns into a real bug report.

Fortran is still where a huge amount of this reduction work actually happens, underneath a Python or C++ training framework, inside a physics or climate solver, inside a legacy HPC codebase that a newer AI pipeline now calls into. It is also a language where OpenMP tasking, IEEE-754 intrinsics and array slicing all sit natively in the standard library, so a tree reduction like this needs no external dependency, no vendor math library and no build system beyond a Fortran compiler with `-fopenmp`. I wanted a version of this that a scientific computing team or an ML infra team could drop into an existing Fortran-based collective or a custom all-reduce kernel without pulling in anything else.

## When to use it

Reach for this when a sum has to come out identical no matter how many threads, processes or machines computed it, and when the values being summed span a wide range of magnitudes so a naive accumulator would lose the small ones. That covers gradient or activation reduction in a distributed training step written in or called from Fortran, reproducibility checks that compare a "did the refactor change the math" checksum across different hardware, scientific simulations that need the same run to reproduce bit for bit on a laptop and on a cluster for review, and any CI gate that currently fails intermittently because a summation's answer quietly depends on how many cores the runner gave it that day.

Do not reach for it if approximate results are fine and reduction cost is the bottleneck: the double-double bookkeeping is real extra arithmetic per element, and a plain `sum()` intrinsic will always be faster when a few ULPs of drift across runs is not something anyone will ever check. It also will not fix non-determinism coming from anywhere other than summation order: a random number generator seeded from wall clock time, non-deterministic GPU kernels, or algorithms that branch on floating point comparisons will still be non-deterministic even with a perfectly deterministic reduction underneath them.

## How it works

`reduce_sum` is the public entry point, exposed as a generic interface over `reduce_sum_r32` (for `real32` gradient buffers, promoted internally to double precision) and `reduce_sum_r64` (for `real64` buffers directly). Both return the total and an integer status built from `STATUS_OK`, `STATUS_EMPTY`, `STATUS_HAS_NAN` and `STATUS_HAS_INF`, which `classify_finiteness` computes up front with an OpenMP-parallel scan using the `ieee_is_nan` and `ieee_is_finite` intrinsics. That scan exists because a NaN or an Inf hiding in a gradient buffer should come back as an explicit, named reason, not as a silently propagated NaN total that a caller has to reverse-engineer.

The actual summation happens in `tree_sum`, a recursive subroutine that keeps splitting `x(lo:hi)` at `mid = lo + (hi - lo) / 2` until a segment is smaller than `LEAF_SIZE` (2048 elements), at which point `leaf_sum` takes over. `leaf_sum` runs the Ogita-Rump-Oishi "Sum2" compensated summation left to right across the leaf: it calls `two_sum` on the running total and each new element, which is Knuth's TwoSum algorithm, an exact way to recover both the rounded sum and the rounding error of `a + b` for any two IEEE-754 doubles under round-to-nearest. The accumulated errors go into a correction term `c`, and `quick_two_sum` (Dekker's version of TwoSum, valid when the first argument dominates in magnitude) folds `s` and `c` back into a single double-double pair `(sum_hi, sum_lo)` to hand up the tree.

Internal nodes never touch a raw array element. They only ever combine two already-computed double-double pairs using `dd_add`, which runs `two_sum` on the two high parts, folds both low parts into the resulting error term, and renormalizes with another `quick_two_sum`. Because that folding step keeps the correction term alive instead of collapsing each subtree back into one rounded double before combining, precision that a plain hierarchical Kahan sum would lose at every internal node survives all the way to the root.

Parallelism comes from `!$omp task`, spawned only when a subtree is at least `TASK_GRANULARITY` elements wide (32 times the leaf size), with the smaller branch recursed into directly and the larger branch handed to a task, followed by `!$omp taskwait` before `dd_add` combines the two results. The key property is that `lo` and `hi` alone decide the split, so the tree a single thread walks and the tree eight threads walk are the identical tree; OpenMP only changes which core executes which subtree, never which numbers get added to which. `reduce_sum_r64` opens the parallel region with `!$omp parallel` / `!$omp single` so `tree_sum`'s tasking has a thread team to schedule onto even when called from ordinary sequential code.

Two more pieces guard the "trust but verify" side of a determinism claim. `bit_fingerprint` runs a splitmix64-style mixer over the raw IEEE bit pattern of a double-double result (via `transfer`), turning "are these two floating point answers equal" into "are these two 64-bit integers equal," which is much harder to accidentally relax into an epsilon comparison during a later refactor. `fast_math_canary_ok` checks a known IEEE-754 identity, that adding 1.0 to 2^53 and recovering both the rounded result and the exact rounding error through `two_sum` must give back exactly `2^53` and `1.0`, a round trip that `-ffast-math` or `-Ofast` are permitted to break by reassociating the arithmetic. Call it once at startup: if it fails, the compensated summation guarantees in this file no longer hold, and the driver in this folder treats that as a hard failure rather than a warning.

## Usage

Compile the module before anything that uses it, with OpenMP enabled and without any fast-math flag:

```
gfortran -O2 -fopenmp -c DeterministicGradientReduction.f90
gfortran -O2 -fopenmp DeterministicGradientReductionDriver.f90 DeterministicGradientReduction.o -o driver
./driver
```

From your own program:

```fortran
use deterministic_gradient_reduction
real(real64) :: gradients(n), total
integer :: status

if (.not. fast_math_canary_ok()) stop 'rebuild without -ffast-math'

call reduce_sum(gradients, total, status)
if (iand(status, STATUS_HAS_NAN) /= 0) then
  print *, 'gradient buffer contains NaN: ', reduction_status(status)
end if
```

`reduce_sum` also accepts a `real32` array directly, useful when the buffer being reduced is already single precision. Set the thread count beforehand with `omp_set_num_threads` (or the `OMP_NUM_THREADS` environment variable) the same way you would for any other OpenMP program; the result will not change when you do.

`DeterministicGradientReductionDriver.f90` is the test harness and the proof, not a toy example. It builds a 2,000,003 element buffer shaped like a real gradient tensor (small values clustered near zero with periodic large, alternating-sign outliers to force cancellation), runs `reduce_sum` at 1, 2, 4 and 8 threads, and checks that `bit_fingerprint` comes back identical every time. It computes a quad precision (`real128`) ground truth and reports the relative error of both a naive left to right double precision sum and this module's result against it, so the accuracy gain is a measured number rather than a claim. It also exercises the empty-array path, seeds a NaN and an Inf into separate copies of the buffer to confirm `STATUS_HAS_NAN` and `STATUS_HAS_INF` fire correctly, and runs the fast-math canary first. On the reference build in this repository it reports bit-identical fingerprints across all four thread counts, a naive-sum relative error of about 1.3e-14, and a deterministic-sum relative error of about 2.2e-17, roughly three orders of magnitude tighter. Building the same module with `-ffast-math` makes the canary trip and the driver exit with status 1 before it runs a single reduction, which is the intended behavior: a build that cannot honor the arithmetic this file depends on should refuse to claim it is deterministic.

## Notes

`LEAF_SIZE` and `TASK_GRANULARITY` are compile time constants, not runtime knobs, on purpose: if they depended on thread count or were tunable per call, the reduction tree would stop being a pure function of the input length and the whole determinism guarantee would quietly disappear. Change them if a different leaf width suits your data better, but change them for everyone, not per run.

The double-double representation used here is Dekker and Knuth's classic error-free transformation technique, the same building block behind XBLAS and the accurate summation algorithms in Ogita, Rump and Oishi's 2005 paper "Accurate Sum and Dot Product." Nothing in this module invents new numerical theory; the contribution is wiring a well known compensated summation into a tree whose shape does not move when the parallelism does, which is the part that actually matters for reproducible distributed reduction.

This is a summation primitive, not a full collective communication library. It does not do network transport, does not handle GPU-resident buffers, and assumes the whole array to reduce already lives in one process's memory. Wiring it into an actual multi-node all-reduce (for example, as the local reduction step before or after a network exchange) is straightforward but is left to the caller, since that wiring is specific to whatever communication layer, NCCL, MPI or otherwise, a given training or simulation stack already uses.
