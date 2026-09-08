# GPU Lease Broker

Two jobs launch on the same box at the same time, both grab `cuda:0` and both die with CUDA out of memory. This is a single file C utility that hands out TTL based leases on GPU devices so one process owns a device at a time, with automatic renewal, stale owner cleanup and a `CUDA_VISIBLE_DEVICES` bridge.

**Language:** C | **Lines:** 1377 | **Added:** 2026-04-17

## What this solves

Most GPU contention outside a real cluster is a coordination problem, not a scheduling problem. One machine, four cards, six people or six cron jobs. Someone starts a fine tune at 14:00. A benchmark harness fires at 14:02 because the CI runner is on the same host. Both default to device 0, and the second one dies at the memory ceiling. Worse, both fit and both run at a third of the throughput while your numbers turn to garbage, unnoticed for an hour because the job is still "running".

The usual workarounds leak. A pidfile written by a shell script races between the check and the write, so two launchers a millisecond apart both think the GPU is free. It also lies after a hard kill or an OOM kill: the file is there, the process is gone and the device sits idle while every other job queues behind a ghost. The other end of the spectrum is heavier than the problem: Slurm, Kubernetes with the NVIDIA device plugin or a Redis backed semaphore is a lot of moving parts to stop two Python processes colliding, and you inherit the scheduler's failure modes too.

The bill without something in the middle: wasted GPU hours, benchmark runs thrown out because a neighbour process was resident, CI jobs red for reasons unrelated to the code, and stale locks cleaned by hand. On rented hardware that cost is direct. On a shared research machine it is trust, because people stop believing each other's numbers.

## Why I built it

`flock` is close but it dies with the shell that holds it and tells you nothing about who holds what or when it expires. `nvidia-smi` shows occupancy after the fact, not intent, and cannot arbitrate. Anything Redis or Postgres backed means a daemon, a network hop and credentials on a machine that only needed a mutex.

So this sits in the middle. A lease has an owner, a token, a TTL and a readable metadata file, it survives across unrelated processes on the host, it reaps itself when the holder dies, and it compiles from one file with nothing beyond POSIX.

## When to use it

- A shared workstation or lab box with two to eight GPUs and several people or agents launching jobs on it.
- A self hosted CI runner where the build agent and a scheduled benchmark can land on the same device.
- A benchmark harness where a neighbouring process silently invalidates results, so you need the card to yourself for the whole run.
- Any launcher script that writes `/tmp/gpu0.lock` and hopes, or that needs `CUDA_VISIBLE_DEVICES` set from whichever card was free.
- Container images and remote devboxes where lease state belongs in a bind mounted directory, not a service.

## How it works

The lock primitive is `mkdir`. `try_acquire_single()` calls `mkdir(path.lease_dir, 0700)` on `<state_dir>/<resource>.lease` and treats success as ownership. Directory creation is atomic on POSIX filesystems, so two racing acquirers cannot both win and there is no check then act window, which is what a pidfile gets wrong. On success it fills a `LeaseMeta` via `fill_new_meta()` and writes `lease.meta` with `write_file_atomic()`: temp file, `fsync`, `rename`, so a reader never sees a half written record.

That metadata is a flat `key=value` file escaped by `kv_escape()`, holding resource, token, host, user, owner, note, pid, uid and three timestamps. Ownership is proven by a token, not identity. `random_hex_token()` pulls 24 bytes from `/dev/urandom` into 48 hex characters, falling back to a splitmix style `hash_u64()` mix of time, pid and ppid. `renew_lease()` and `release_lease()` both `strcmp` the supplied token against the stored one and fail with `EPERM` on mismatch, so nobody frees your device by guessing the resource name.

Staleness is the interesting part, and `lease_is_stale()` uses three signals rather than one. Expiry: `expires_at_ms` in the past means dead. Same host PID liveness: if the lease carries `pid_checked` and its `host` matches this hostname, `is_process_alive()` does `kill(pid, 0)` and counts `EPERM` as alive so another user's lease is not falsely reaped. Directory age: if `lease.meta` is missing or unparseable, the directory mtime is checked against `--stale-grace-ms` so a half created lease from a crashed acquirer does not wedge the device forever. `pid_checked` is set only for `run`, since an `acquire` caller exits immediately and its pid means nothing.

Reaping is `move_lease_dir()` then `remove_dir_recursive()`. The directory is `rename`d to a unique hidden name like `.gpu0.lease.reaped.<pid>.<ms>` and only then deleted. The rename is the atomic claim on the corpse, the delete is cleanup, and the hidden name no longer ends in `.lease` so `scan_leases()` skips it. After a reap `try_acquire_single()` recurses to retry, and `ENOENT` from the rename means somebody reaped it first, also a retry. `acquire_any()` handles the any of case: repeat `--resource` and it walks the list from a pseudorandom index derived from `hash_u64(start_time, pid)`, spreading launchers across cards instead of stampeding device 0, then sleeps a jittered `50 + (seed % 101)` milliseconds between sweeps until `--wait-ms` runs out.

`run_command_with_lease()` is the mode you want in a launcher. It acquires, and with `--cuda-from-resource` derives a device index via `parse_cuda_index()` from names like `gpu0`, `cuda3` or `device1` and exports `CUDA_VISIBLE_DEVICES`, always exports `GPU_LEASE_RESOURCE`, `GPU_LEASE_TOKEN` and `GPU_LEASE_DIR`, installs handlers for SIGINT, SIGTERM and SIGHUP, then forks and `execvp`s your command. The parent polls `waitpid(WNOHANG)` every `GLB_POLL_MS` (200 ms), forwards caught signals to the child, and renews every `--renew-ms`. If a renewal fails, meaning somebody reaped or stole the lease, it sends SIGTERM, waits five seconds and escalates to SIGKILL rather than let an unauthorised process keep the card. On exit it releases and passes the child's status through.

## Usage

```sh
# build: one file, no dependencies beyond POSIX
cc -O2 -Wall -Wextra -o GpuLeaseBroker GpuLeaseBroker.c

# wrap a job: acquire, export CUDA_VISIBLE_DEVICES, renew while it runs, release on exit
./GpuLeaseBroker run \
  --resource gpu0 --resource gpu1 --resource gpu2 --resource gpu3 \
  --wait-ms 600000 --ttl-ms 60000 --renew-ms 20000 \
  --cuda-from-resource --owner ci --note "nightly finetune" \
  -- python train.py --epochs 3

# manual lease: prints "gpu0 <token>", or a JSON object with --json
TOKEN=$(./GpuLeaseBroker acquire --resource gpu0 --ttl-ms 300000 | awk '{print $2}')
./GpuLeaseBroker renew   --resource gpu0 --token "$TOKEN" --ttl-ms 300000 --quiet
./GpuLeaseBroker release --resource gpu0 --token "$TOKEN"

# inspect and clean up
./GpuLeaseBroker list --json
./GpuLeaseBroker gc --stale-grace-ms 5000

# state dir defaults to /tmp/gpu-lease-broker, override per call or by env
export GPU_LEASE_BROKER_DIR=/var/lib/gpu-leases
./GpuLeaseBroker list --dir /var/lib/gpu-leases
```

Commands: `acquire`, `renew`, `release`, `list`, `gc`, `run`. Flags: `--dir`, `--resource` (repeatable, max 128), `--ttl-ms` (60000), `--wait-ms` (0), `--renew-ms` (20000), `--stale-grace-ms` (5000), `--owner`, `--note`, `--token`, `--json`, `--quiet`, `--cuda-from-resource`, `--help`.

## Notes

- It brokers intent, not hardware. Nothing counts VRAM or stops a process that ignores the broker. Every participant has to go through it.
- Coordination is per host by default. Point `--dir` at a shared filesystem and several hosts can contend, but PID liveness only applies when the recorded `host` matches the local hostname, so a remote lease expires by TTL alone.
- Stale detection and reaping are not one atomic step, so a lease that expires and is renewed in the same instant can still be reaped by a racing acquirer. Keep `--renew-ms` well below `--ttl-ms`; the parser clamps it to `ttl/2` with a 1000 ms floor if you set it too high.
- Exit codes: 0 on success, 2 for usage errors, 75 from `acquire` when everything is busy, 70 from `run` when renewal failed and the child was killed, 127 if `execvp` could not start the command, 125 for an unexpected `waitpid` state, otherwise the child's exit code or `128 + signal`.
- `write_file_atomic()` fsyncs the temp file but not the parent directory, so power loss right after a rename can lose the metadata. Lease dirs are 0700, metadata files 0600, and resource names are sanitized to `[A-Za-z0-9._-]`, so `gpu:0` and `gpu_0` collide by design.
- POSIX only: `fork`, `execvp`, `sigaction`, `getopt_long` and `/dev/urandom`. Builds on Linux and macOS, needs a compatibility layer on Windows.
