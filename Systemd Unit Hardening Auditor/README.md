# Systemd Unit Hardening Auditor

A Tcl command line tool that reads systemd service units the way systemd really merges them, scores how exposed each service is and fails CI when a unit gets worse. It also catches sandbox settings that look safe in the file but do nothing at runtime.

**Language:** Tcl | **Lines:** 1308 | **Added:** 2026-10-30

## What this solves

Most teams know about `systemd-analyze security`. It is a good tool, but it only works on a live machine with the unit loaded. You cannot run it in a pull request against a directory of unit files, you cannot diff its score between two commits without a booted host and it will not tell you that the hardening line you added last week is silently cancelled by a drop in file somebody else owns.

This tool audits `.service` files straight from disk. It merges the main file with `<unit>.service.d/*.conf` in filename order, applies the empty assignment reset that drop ins rely on, and then scores the result. That covers the common service hardening problems: services that run as root, no `NoNewPrivileges`, an open capability bounding set, no syscall filter, a wide open `RestrictAddressFamilies`, `ReadWritePaths=/etc/ssh` quietly undoing `ProtectSystem=strict` and secrets pasted into `Environment=`.

The part I care about most is the dead and conflicting configuration. Examples it finds:

- `AmbientCapabilities=CAP_NET_BIND_SERVICE` combined with a `CapabilityBoundingSet` that removed that capability. The service starts, binds nothing below 1024 and the logs say permission denied.
- `ReadWritePaths=` set while `ProtectSystem` and `ProtectHome` are both off. Nothing is read only, so the line does nothing.
- `DynamicUser=yes` together with `ReadWritePaths=/var/lib/app`. The uid changes between restarts and ownership drifts. `StateDirectory=` is the fix.
- `RestrictAddressFamilies=` next to `PrivateNetwork=yes`, which is redundant.
- A `+` prefix on an `ExecStart` line, which runs that command outside the sandbox entirely.
- `MemoryDenyWriteExecute` on a Node, Java or dotnet service, which will crash at the first JIT page.

## Why I built it

I kept seeing the same review comment on infrastructure repos: "did you harden this unit?" Nobody could answer with a number, and nobody wanted to boot a VM per pull request just to run `systemd-analyze security`. People also copy a hardening block from a blog post, paste it into a unit and assume it works. The three failures I hit again and again were a drop in that reset a list and lost the original intent, `DynamicUser` implying settings nobody had noticed and an ambient capability that the bounding set threw away.

I wanted something that runs anywhere Tcl runs. Tcl is already on most Linux build images, Alpine included, so a single file with no packages is the right shape for a CI gate. It also let me keep the parser honest, because systemd's file format has real corner cases: line continuation with a trailing backslash, comment lines inside a continuation, `;` comments, repeated keys that accumulate and empty values that reset.

## When to use it

- As a pull request check on a repo that ships unit files, with `--max-score` as the budget.
- As a regression gate on a fleet baseline, with `--write-baseline` on main and `--baseline` on branches, so a new service cannot land worse than the one it was copied from.
- Before turning on hardening for an old service. Run it with `--emit-dropin` and the `safe` tier, deploy the drop in, then climb to `likely` and `risky` one step at a time.
- When you inherit a machine and want a ranked list of which units to fix first. The score is sorted by weight times exposure, so the biggest win is on top.
- When a hardened service fails in odd ways and you want to know whether the drop in directory changed what you think it changed.

It is not a replacement for running `systemd-analyze security` on the real host. It cannot see generated units, template instances with specifier tricks, units from `/usr/lib` that you did not pass in or what the service actually does at runtime. Treat the score as a review aid, not a certificate.

## How it works

**Parsing.** `parseText` walks the file line by line. Comments starting with `#` or `;` are skipped, even in the middle of a continuation. A trailing backslash joins the next line with a space, and a continuation that runs off the end of the file is reported. `applyStatement` splits `Key=Value`, tracks the current section and stores values in one dict keyed `Section/Key`. An empty value stores an empty list, which is how a reset is represented, and a repeated key appends. Lines that are not `Key=Value`, or that appear before any section, are recorded as problems and not guessed at.

**Merging.** `unitFiles` returns the unit file followed by every `*.conf` under `<unit>.d` sorted by name. `auditUnit` parses optional global drop ins first, then those files in order. `--global-dropins` is for the `service.d` style directory that applies to every unit.

**Effective values.** `get` returns the last value, `vals` returns all of them and `bool` accepts the systemd spellings for yes and no and reports bad ones. `effBool` and `effProtect` add what `DynamicUser=yes` implies: `PrivateTmp`, `RemoveIPC`, `RestrictSUIDSGID`, `ProtectSystem=strict` and `ProtectHome=read-only`. An explicit value in the unit always wins over the implied one.

**List directives.** `setEval` implements the real semantics for `CapabilityBoundingSet`, `RestrictAddressFamilies` and `RestrictNamespaces`. Lines add up. A line starting with `~` removes. If the first line is inverted the starting point is the full universe, which is `allCaps`, `allFamilies` or `allNamespaces`. An absent directive returns the word `unset` so the caller can tell it apart from a deliberately empty set. `capNorm` rejects unknown capability names and `afNorm` treats `none` as the empty set.

**Scoring.** Each rule is registered with `defRule`, which creates a `check_<id>` and a `fix_<id>` procedure. `boolRule` is the shortcut for plain yes or no settings. A check returns `ok`, `bad`, `part` with a fraction between 0 and 1 or `na`. The score in `auditUnit` is ten times the weighted exposure divided by the total weight of applicable rules, so rules that cannot apply do not pad the denominator. `bandFor` maps the number to TIGHT up to 2.0, OK up to 4.5, EXPOSED up to 7.0 and UNSAFE above. These weights and bands are my own, not systemd's. They are in the file and you can change them.

Some rules are graded and not binary. `capsExposure` sums `capWeight` for every capability left in the set and caps the total at 1, so `CAP_SYS_ADMIN` alone is a heavy hit while `CAP_NET_BIND_SERVICE` is nearly free. `RestrictNamespaces` uses `nsWeight` with user namespaces counted heaviest. `SystemCallFilter` distinguishes an allowlist from a denylist and credits `~@privileged` and `~@resources`. `ProtectSystem` gives partial credit to `full` and `yes`.

**Warnings that do not score.** `collectWarnings` handles the findings that are about correctness. It reads `deprecated` for `PermissionsStartOnly`, `MemoryLimit` and `CPUShares`. It uses `jitRe` to match the first two words of `ExecStart` against known JIT runtimes. It uses `splitQuoted` to read `Environment=` the way a shell would and `secretNameRe` to flag literal secrets, skipping names ending in `_FILE`, `_PATH` or `_DIR` and values that start with a slash. `isSensitivePath` decides whether a writable or bind mounted path is worth flagging.

**Suppressions.** A comment of the form `# hardening-ignore: RuleId reason` in the unit or a drop in suppresses that rule for that unit and removes it from both sides of the score. The reason is mandatory. A suppression with no reason is refused and reported, and an unknown rule id is reported too, so a typo cannot hide a finding. `--ignore RuleId` does the same for every unit.

**Gating.** `--max-score` fails any unit above the number. `readBaseline` loads tab separated `unit` and `score` rows and a unit fails if it scores more than its baseline plus `--tolerance`, which defaults to 0.05. Exit codes are 0 for pass, 1 for a failed gate and 2 for bad input such as a missing path, an unreadable baseline or no service units found.

**Drop in generation.** `dropinText` turns failing rules into a drop in file. Each rule has a tier: `safe` for settings that almost never break a service, `likely` for settings that need a quick check, `risky` for settings that need real testing such as an empty capability bounding set or `IPAddressDeny=any`. List directives are emitted with an empty reset line first so they replace instead of extending what was there. `--emit-dropin DIR` writes `DIR/<unit>.d/10-hardening.conf` and refuses to overwrite an existing file.

**Discovery.** `collect` walks directories recursively, skips units masked to `/dev/null` and skips symlink aliases to another service file, then reports both in notes.

## Usage

Needs Tcl 8.6 and nothing else.

```sh
# Audit a directory and print failing rules, worst first
tclsh SystemdUnitHardeningAuditor.tcl /etc/systemd/system

# Show passing rules as well
tclsh SystemdUnitHardeningAuditor.tcl --verbose Examples/MixedApp.service

# CI gate: every unit must score 4.5 or lower
tclsh SystemdUnitHardeningAuditor.tcl --max-score 4.5 --format tsv units/

# Freeze today's scores on main, then block regressions on branches
tclsh SystemdUnitHardeningAuditor.tcl --write-baseline scores.tsv units/
tclsh SystemdUnitHardeningAuditor.tcl --baseline scores.tsv --tolerance 0.1 units/

# Machine readable output
tclsh SystemdUnitHardeningAuditor.tcl --format json units/ > audit.json

# Generate drop ins for the safe tier, then widen to likely later
tclsh SystemdUnitHardeningAuditor.tcl --emit-dropin out/ --tier safe units/

# Apply a fleet wide drop in directory first, skip one rule everywhere
tclsh SystemdUnitHardeningAuditor.tcl --global-dropins /etc/systemd/system/service.d --ignore Delegate units/

# List every rule with its weight, tier and reason
tclsh SystemdUnitHardeningAuditor.tcl --list-rules
```

Options are `--format text|json|tsv`, `--verbose`, `--max-score N`, `--baseline FILE`, `--write-baseline FILE`, `--tolerance N`, `--ignore RULEID`, `--global-dropins DIR`, `--emit-dropin DIR`, `--tier safe|likely|risky` and `--list-rules`.

Run the tests with `tclsh TestSystemdUnitHardeningAuditor.tcl`. They cover continuation lines, reset semantics, drop in override order, `DynamicUser` implications, inverted capability sets, ambient versus bounding set conflicts, syscall filter scoring, umask direction, `+` exec prefixes, sensitive paths, secret detection, suppression rules, JSON escaping, baseline regression and the exit codes.

The `Examples` folder has three units. `BareApp.service` has no sandboxing and scores 8.94. `HardenedApp.service` scores 0.00. `MixedApp.service` has a drop in that resets and redefines the capability set and address families, a reasoned suppression, a refused suppression with no reason and an ambient capability that never gets granted. It scores 6.53.

## Notes

- The weights, tiers and score bands are judgement calls. They are tuned so that a bare unit lands near 9 and a unit with a full sandbox lands near 0. If your estate has different priorities, edit the numbers in `defineRules` and `capWeight`.
- It reads the files you give it. It does not run `systemctl`, does not expand specifiers such as `%i` and does not resolve units that systemd generates at boot.
- Only `[Service]` is audited. Socket, timer, mount and target units are skipped with a note.
- Drop ins from several directories (`/usr/lib`, `/etc`, `/run`) are not merged by precedence. Point it at the directory that holds the files you want checked, or use `--global-dropins` for the shared ones.
- `Environment=` parsing handles single and double quotes but not backslash escapes. A value with an escaped quote could be read wrongly. It only affects the secret warning.
- `ProtectProc` and `ProcSubset` need kernel 5.8 or newer. `ProtectClock` and `ProtectKernelLogs` need a recent systemd. A directive that your systemd does not know is ignored at runtime, so check `systemctl --version` before you trust a score of zero.
- Generated drop ins are a starting point. A service that writes outside its state directory will fail under `ProtectSystem=strict` until you add `StateDirectory=` or `ReadWritePaths=`. Restart one instance first and read `journalctl -u` before rolling out.
- Exit code 2 means the tool could not do its job. Do not treat it like a failed gate, fix the input.
