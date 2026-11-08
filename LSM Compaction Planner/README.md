# LSM Compaction Planner

A pure Gleam planner that decides which LSM tree compaction to run next, how much it will write and whether it is safe to start. It takes a list of tables and returns one job with an honest write amplification number, or tells you exactly why nothing could run.

**Language:** Gleam | **Lines:** 907 | **Added:** 2026-11-08

## What this solves

If you build a storage engine, a key value store, an embedded index or a metrics database on an LSM tree, you eventually write a compaction picker. The first version is always the same: when level 0 has four files, merge them into level 1. That works in a demo. In production it falls apart in a handful of predictable ways.

Level 0 files overlap each other, so grabbing "the oldest file" leaves a newer overlapping file behind and a key ends up with two versions in the wrong order. Deeper levels grow past their target and the picker chooses the first file it sees, which happens to overlap ten files in the next level and writes ten times more than it needed to. A job that touches half the database blocks every other job. Tombstones get copied forward for months because nobody checked whether the data they shadow had already been compacted away. Two background workers pick overlapping inputs because the picker did not know what was already running. And when level 0 piles up and writers stall, the byte budget you set to protect disks stops the one job that could end the stall.

This entry is a picker that handles all of those cases and keeps the logic pure. You give it the table list, the config and the ids that are busy. It gives you a plan. There is no I/O, no clock and no process state, so you can test it with plain data and replay a production incident from a table dump.

## Why I built it

I kept seeing compaction logic written as a few if statements inside the engine, welded to the file format and the thread pool. That makes it impossible to test the interesting cases. How do you check the "level 0 stopped, so ignore the budget" rule if it only fires when real files pile up on a real disk?

Gleam is a good fit for pulling that logic out. The type system forces every case to be named, the planner is a set of small total functions and the same file runs on the Erlang VM and on JavaScript. I wanted something a person could read in one sitting, fork and wire into their own engine by mapping their file metadata to a six field record.

I also wanted the planner to check the engine's work. The `commit` function validates the tree that a merge produced before you publish it. A bug in a merge writer then shows up as a rejected commit with a reason, not as a corrupted level that you discover a week later.

## When to use it

Use it when you are writing or reviewing a leveled LSM engine and want a compaction picker you can reason about. It also works as a simulator front end: feed it table metadata from a real engine dump and see which job it would choose and what that job costs. If you operate RocksDB or a similar engine and wonder why write amplification looks high, the scoring and overlap ranking here show the trade off in a form you can read.

Do not use it as a drop in replacement for the picker inside an existing engine without mapping the details. It models leveled compaction with a fixed number of levels. It does not model universal or FIFO compaction, subcompactions, column families or per key range locks.

## How it works

The data model is small. A `Table` has an `id`, a `level`, a `smallest` and `largest` key, `bytes`, `entries`, `tombstones` and an `age` sequence number. A `Config` holds the level 0 thresholds (`level0_trigger`, `level0_slowdown`, `level0_stop`), the level sizing (`base_level_bytes`, `multiplier`, `level_count`), the budget `max_compaction_bytes`, the output cut size `target_table_bytes` and `tombstone_permille`. `default_config` gives a RocksDB style setup and `validate_config` rejects values that cannot work, such as a stop threshold below the slowdown threshold.

**Validation first.** `validate_tables` runs before any planning. It checks that ids are unique, that each range is ordered, that tombstone counts do not exceed entry counts and that levels 1 and deeper contain no overlapping tables. The overlap check sorts each level by smallest key and walks adjacent pairs in `first_overlap`. One pass is enough because if any pair overlaps then some adjacent pair does. A bad tree returns a typed `PlanError` such as `OverlapInLevel` instead of a job built on sand.

**Scores.** `level_scores` gives each level a score in parts per thousand. Level 0 is table count over `level0_trigger`. Deeper levels are bytes over `level_target`, which is `base_level_bytes` times `multiplier` to the power of level minus one, capped so it never overflows a JavaScript safe integer. A score of 1000 or more means the level wants work. `write_state` turns the level 0 count into `Normal`, `Slowdown` or `Stopped` so your write path can apply back pressure. The plan also reports `pending_bytes`, the amount over target summed across levels.

**Candidates.** The private `candidates` function builds the ordered list: size overflows by descending score, then tombstone heavy levels from the bottom up. The `plan` function tries them in order through `pick` and keeps the first that works. Everything it skipped is returned in `deferred` with a `DeferReason` of `OverBudget`, `TouchesBusy` or `NothingEligible`. That list is the answer to "why is nothing compacting".

**Level 0 closure.** For level 0 the seed is the oldest table, and `l0_closure` grows the input set with `grow` until no remaining level 0 table overlaps the combined key range. That includes chains, so tables covering a to b, b to c and c to d all come along. This is the rule that stops a stale version of a key surviving in level 0 after its newer copy lands in level 1.

**Seed ranking for deeper levels.** `build_ranked` ranks each table in an over target level by how many bytes of the next level it overlaps divided by its own size. The cheapest table to push down goes first, with age and id as tie breakers. If the best seed touches a busy table it tries the next seed, so one running job does not freeze a whole level. The cost is quadratic in tables per level pair, which is fine for thousands of tables and worth knowing if you run millions.

**Budget and forcing.** `assemble` rejects an input set larger than `max_compaction_bytes` unless the job is forced. A level 0 job is forced only when the level 0 count has reached `level0_stop`. A stalled engine has to make progress, so the budget gives way and the job reports `forced: True`.

**Trivial moves and tombstones.** If one table has no overlap in the next level, `make_job` returns a `TrivialMove` that writes zero bytes. If nothing deeper overlaps the input range, or the target is the bottom level, `drops_tombstones` is true and the estimated output shrinks by the tombstone share of the inputs. A tombstone heavy table in the bottom level is handled by `rewrite_bottom`, which rewrites it in place because there is nowhere further down to push it.

**Cost numbers.** Each `Job` carries `bytes_read`, `bytes_written`, `tables_written` and `write_amp_permille`, which is bytes written per byte of upper level data. A move is 0. Merging one table into an equal sized neighbour is 2000. These are estimates, since the real output depends on how many keys are shadowed, but they rank jobs correctly and they are stable.

**Commit.** `commit` takes the tree, the job and the tables your engine wrote. It checks that every input still exists unchanged, that outputs sit at `to_level`, that they stay inside the key span of the inputs and that the final tree passes `validate_tables`. A `TrivialMove` needs no outputs. An empty output is accepted only when the job drops tombstones. `describe_job` and `describe_error` produce one line log text.

## Usage

Copy `lsm_compaction_planner.gleam` into `src/` of a Gleam project. It needs only `gleam_stdlib`. The test file uses `gleeunit`.

```gleam
import gleam/io
import gleam/option.{None, Some}
import lsm_compaction_planner as lsm

pub fn next_compaction(tables: List(lsm.Table), running: List(Int)) {
  let config = lsm.default_config()
  case lsm.plan(config, tables, running) {
    Error(error) -> io.println(lsm.describe_error(error))
    Ok(plan) -> {
      case plan.write_state {
        lsm.Stopped -> io.println("writers must wait")
        lsm.Slowdown -> io.println("slow writers down")
        lsm.Normal -> Nil
      }
      case plan.job {
        Some(job) -> io.println(lsm.describe_job(job))
        None -> io.println("nothing to run, deferred: see plan.deferred")
      }
    }
  }
}

pub fn finish(tables, job, written) {
  // written is the list of lsm.Table values your merge produced
  lsm.commit(lsm.default_config(), tables, job, written)
}
```

Run the tests with `gleam test`. Keys are compared with `string.compare`, so use ASCII or hex encoded keys, or make sure your engine sorts in the same order the target runtime does.

## Notes

The planner only knows which tables are busy, not which key ranges running jobs will write into. If you run jobs in parallel, serialise the choice of output level ranges in your engine or pass the output tables of running jobs as busy once they exist.

Level 0 always compacts into level 1. There is no dynamic base level, so a tree with a nearly empty level 1 will still pay a full level 1 merge. Tombstone byte sizing assumes entries inside a table are of similar size. The estimates are for ranking and capacity planning, not for billing.

I wrote this without being able to run the Gleam compiler in the environment that produced it, so treat the first `gleam test` run as part of reviewing it. If something does not compile on your toolchain version, the fix should be local to a function.
