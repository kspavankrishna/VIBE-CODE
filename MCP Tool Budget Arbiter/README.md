# MCP Tool Budget Arbiter

A shared cost budget for a fleet of concurrent MCP or agent tool call tasks that cannot go negative, cannot be double spent by two tasks racing each other, and cannot be starved forever by one task that reserves a slice of the budget and then crashes without ever settling it.

**Language:** Ada | **Lines:** 613 | **Added:** 2026-09-28

## What this solves

Once an MCP server or an agent runtime stops being a single request handled by a single task, it turns into several agent sessions calling tools at the same time against one shared spend ceiling: one API key's rate plan, one team's monthly token budget, one sandbox's GPU second allowance. Two problems show up the moment there is more than one caller.

The first is the race. Two tasks both check "is there enough budget left" at nearly the same instant, both see yes, both proceed, and the budget goes negative or the real spend ends up higher than the cap that was supposed to be a hard limit. In most languages the fix is a mutex around a shared counter, which is correct but brings a second, quieter problem with it: an ordinary lock has no idea about task priority. A low priority housekeeping task can grab the lock to do bookkeeping, get preempted by a medium priority task that has nothing to do with the budget, and a high priority interactive tool call ends up waiting behind both of them even though it should never have to wait on the medium priority one at all. This is priority inversion, and it is exactly the kind of bug that only shows up under real load, not in a quick local test.

The second problem is the crash. A task reserves a slice of the budget for a tool call, and the tool call wedges on a socket read, the task gets killed, the process restarts, whatever. If reserving budget and spending it are the same step, that slice is gone forever and the shared budget slowly leaks away to zero even though nothing was actually spent. Any system that hands out a budget slice ahead of a costly, possibly slow, possibly failing operation needs a way to claw that slice back automatically.

MCP_Tool_Budget_Arbiter solves both with a single Ada protected type, `Arbiter`. Reserving budget, committing what was really spent, rolling back what was not spent and reclaiming what a dead task left behind are four separate operations on one protected object, so the mutual exclusion and the priority protection both come from the language runtime itself rather than from a hand rolled lock. Every state change is also appended to a fixed size, hash chained ledger inside the same protected object, so after a stress run you can point at exactly which reservation, commit, rollback or expiry happened in what order and confirm none of those records were altered or dropped out of sequence.

## Why I built it

Most of the budget guards and rate limiters people reach for treat "how much is left" as a number you check and then update in two separate steps, protected by whatever locking primitive that language happens to offer. That is fine until you actually need the guarantee to hold under concurrency and under failure at the same time, and most mainstream languages make you build both halves yourself: the mutual exclusion is your problem, and so is remembering to release whatever you reserved when the code path that was supposed to release it never runs.

Ada already solved the first half in the language definition. A protected type gives you a monitor with entries, procedures and functions instead of a bag of functions plus a mutex you have to remember to lock and unlock in the right order, and when the Ceiling_Locking policy is in effect, which is what `gnat.adc` in this folder turns on with `pragma Locking_Policy (Ceiling_Locking)`, a task executing inside one of the Arbiter's operations runs at the object's ceiling priority for the duration of that call. That is what actually prevents priority inversion: a lower priority task cannot be preempted mid operation by something that would otherwise cut in front of a higher priority waiter, because for as long as it holds the object it is not running at its own low priority any more. You get this by declaring the type correctly, not by writing scheduling code.

The crash half I built on top with an explicit two phase protocol, because no language gives you that for free. `Reserve` holds a slice of the budget and hands back a `Reservation_Id`. `Commit` settles it at the real cost, refunding whatever was reserved beyond what was needed. `Rollback` gives back the whole slice if the tool call never happened. `Expire_Stale` is the safety net underneath all three: anything still holding a slice past its lease is reclaimed automatically, which is the direct fix for a task that reserves and then disappears.

I also wanted the audit trail to be something you could actually check rather than just trust. `Verify_Ledger_Integrity` walks the FNV-1a style hash chain that `Fold` builds one link at a time as events happen, and confirms every record still in the ring buffer folds forward into the next one exactly as recorded. It is a plain, dependency free integrity check, not a cryptographic signature, and the README is explicit about that rather than overselling it: see Notes for exactly what it does and does not prove.

## When to use it

Reach for this when several concurrent tasks, whether that is agent sessions, MCP tool handlers or worker processes in an Ada based service, share one hard spend ceiling and you need that ceiling to actually hold under real concurrency, not just in a single threaded test. It fits an internal AI gateway enforcing a provider's monthly token allowance across many simultaneous chat sessions, a sandboxed agent runtime capping GPU seconds per tenant, or any Ada or SPARK adjacent embedded or safety critical system that happens to be coordinating a shared consumable resource, not just money, across concurrent tasks with different priorities.

Do not reach for it if you only have one task touching the budget at a time; a plain counter is simpler and there is nothing here for the protected type to protect you from. It also is not a rate limiter in the token bucket sense: `Capacity` is a fixed total that only comes back through `Commit` refunds, `Rollback` and `Expire_Stale`, so once real spend has used it up, it stays used up. If you want a budget that refills every second or every minute, wrap this with your own refill task that periodically raises the ceiling, or reach for a different tool built specifically around a refilling window.

## How it works

Everything lives in one protected type, `Arbiter`, declared with two discriminants. `Capacity` is the total budget, in `Budget_Units`. `Lease_Milliseconds` is how long a reservation may sit uncommitted before it can be reclaimed. `Budget_Units` is `range 0 .. 2 ** 62`, and that lower bound of zero is doing real work: any bug that tries to push the balance below zero raises `Constraint_Error` at the exact line that did it, instead of wrapping around or drifting negative the way a plain signed integer would.

`Reserve` takes a `Tenant_Id` (a bounded string capped at `Max_Tenant_Length`, built with `To_Tenant_Id`) and an `Amount`, and never blocks. It checks `Amount` against the current `Balance`, finds a free row in the fixed size `Slot_Table` (sized by `Max_Outstanding_Reservations`, 256 by default), and either grants a `Reservation_Id` and deducts the amount, or comes back immediately with one of `Rejected_Invalid_Amount`, `Rejected_Insufficient_Budget` or `Rejected_Table_Full` inside a `Reservation_Result`. Because it never queues a caller, a session under load gets a fast, explicit answer and decides its own retry or backoff policy instead of blocking invisibly inside the arbiter.

`Commit` settles a reservation at `Actual_Cost`. If the real cost came in lower than what was held, the difference goes straight back to `Balance`. If it came in higher, the charge is capped at what was actually reserved, so one caller that quoted itself too low can never eat into budget that was never set aside for it; reconciling that overrun is left to the caller's own accounting, which is a deliberate choice documented right in the spec. `Rollback` returns a reservation's full amount with no charge at all, for a tool call that was granted a slot but never ran.

`Expire_Stale` is the reaper hook: called with the current `Ada.Real_Time.Time`, it walks every slot, and any reservation whose `Expires_At` (set at `Reserve` time from `Lease_Milliseconds`) is in the past gets its budget returned and its slot freed, whether or not anyone ever calls `Commit` or `Rollback` on it. `Available` and `Outstanding_Count` are plain protected functions for observability, and because Ada lets multiple protected functions run concurrently with each other, several tasks can poll them at once without blocking each other or the writers waiting behind them any longer than necessary.

Every `Reserve`, `Commit`, `Rollback` and `Expire_Stale` event also runs through the private helper `Append_Ledger`, which calls `Fold` to mix the event kind, sequence number, tenant and amount into a running 64 bit FNV-1a style hash seeded by `Initial_Chain_Seed`, and stores the result in the `Ledger_Table`, a fixed size ring buffer holding the most recent `Max_Ledger_Entries` records (1024 by default). `Copy_Ledger` exports that ring buffer, oldest entry first, into a caller supplied `Ledger_Window`. `Verify_Ledger_Integrity` recomputes the chain over whatever is still retained and confirms it matches; if the buffer has wrapped since the oldest surviving record was written, that record's own chain value is trusted as the starting anchor instead of being recomputed, since its preimage has already been overwritten, so the check proves the retained window is internally consistent, not that nothing was ever evicted.

Two Ada language features do work here that would otherwise be hand written: `Reservation_Slot`, `Slot_Table` and `Ledger_Table` are declared in the package's visible part rather than inside the protected type's own private section, because a protected type's private section may declare components of an existing type but cannot define a brand new record type in place; nothing outside the package can still read or write one, since the only objects of these types are an `Arbiter`'s own private components. And nothing here allocates from the heap. Both tables are fixed size arrays sized at compile time from `Max_Outstanding_Reservations` and `Max_Ledger_Entries`, which is why this type works unchanged on a restricted, no heap Ada runtime, not only on a native one.

## Usage

Build with GNAT and the project file in this folder, which also maps the PascalCase file names to their Ada unit names since GNAT's default naming scheme forces lower case:

```
gprbuild -P McpToolBudgetArbiter.gpr
./obj/McpToolBudgetArbiterDemo
```

`gnat.adc` turns on `pragma Locking_Policy (Ceiling_Locking)` for the whole build; keep it alongside the sources if you build with a plain `gnatmake` instead of the project file, or the ceiling protocol described above will not be in effect.

From your own code:

```ada
with MCP_Tool_Budget_Arbiter; use MCP_Tool_Budget_Arbiter;

Governor : Arbiter (Capacity => 30_000, Lease_Milliseconds => 150);

Tenant  : constant Tenant_Id := To_Tenant_Id ("session-42");
Outcome : Reservation_Result;
Ok      : Boolean;
begin
   Governor.Reserve (Tenant, 900, Outcome);
   case Outcome.Status is
      when Granted =>
         --  run the tool call, then either:
         Governor.Commit (Outcome.Id, 640, Ok);       -- settle at real cost
         --  or, if it never ran:
         Governor.Rollback (Outcome.Id, Ok);
      when others =>
         null; -- back off and retry later
   end case;
end;
```

Run `Governor.Expire_Stale (Ada.Real_Time.Clock, Reclaimed)` from a periodic task to reclaim anything a dead or hung caller left behind; `McpToolBudgetArbiterDemo.adb` runs one every 25 milliseconds as an example. That demo drives four concurrent `Agent_Session` tasks against one `Arbiter`, each making 40 reservations of a random size and either committing, rolling back or deliberately abandoning it to simulate a crash, then prints `Governor.Available`, `Governor.Outstanding_Count` and `Governor.Verify_Ledger_Integrity` once every task has finished; Ada awaits every task declared inside a block automatically at that block's `end`, so the report only prints after all of them, plus the reaper, have actually terminated, with no manual join required.

## Notes

The two Post aspects this type would ideally carry, that `Reserve` only ever returns `Rejected_Invalid_Amount` when `Amount` is zero, and that `Available` never exceeds `Capacity`, are documented in comments right above the two operations in `McpToolBudgetArbiter.ads` instead of being written as checkable contracts. I wrote them as real Ada 2012 Post aspects first, and GNAT 13.3.0 crashed compiling both, reproducibly, in a minimal test case outside this package: a discriminated protected type combined with a postcondition that reads either a component of an out mode record parameter or a discriminant. Both guarantees are still enforced, just by the body's own arithmetic and control flow rather than by a compiler checked assertion, and the demo's final report exercises both paths under real concurrent load as the practical proof.

`Verify_Ledger_Integrity` is an integrity check, not a security control. It will catch corruption, truncation or reordering of the ledger records still held in the ring buffer, because tampering with a stored `Ledger_Record` in place breaks the folded chain value that depends on it and on everything after it. It will not catch someone who can read this source, recompute `Fold` themselves, and rewrite a whole suffix of the chain consistently; there is no secret key involved anywhere. If you need a tamper evident log that resists an adversary who can read the code, chain it with a keyed MAC instead of a plain hash.

`Max_Outstanding_Reservations` and `Max_Ledger_Entries` are fixed at compile time on purpose, the same way the leaf size is fixed in a deterministic reduction tree: making either one a runtime parameter would mean the reservation table or the ledger ring buffer could grow off the heap, which defeats the point of a type meant to run unchanged on a restricted, no heap Ada runtime. Raise them if 256 outstanding reservations or 1024 retained ledger entries is not enough for your workload, but raise them for the whole build, not per instance.
