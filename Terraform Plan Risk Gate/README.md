# Terraform Plan Risk Gate

A Perl CLI that reads `terraform show -json` output and fails your pipeline when a plan destroys stateful resources, widens IAM access, opens a firewall to the internet or quietly turns off a protection. It needs nothing beyond the Perl that ships on every CI runner.

**Language:** Perl | **Lines:** 803 | **Added:** 2026-10-16

## What this solves

Most teams review a Terraform plan by scrolling a few hundred lines of coloured diff in a pull request. The dangerous line is rarely the one that stands out. A database shows `must be replaced` three screens above the fold. A policy document changes from one action to `ec2:*` inside an escaped JSON string that the diff shows as a single long line. A rename of a resource block looks like one destroy and one create, and nobody notices the destroy is a production database.

Policy engines exist, but they usually mean Rego, a server or a separate toolchain, and they work on the raw resource attributes. They are great once someone has written the rules. Most small teams never get that far, so the plan goes through on a quick approval.

This tool is the plan reviewer I wanted for terraform plan risk checks without any setup. You pipe the JSON plan into one Perl file and get a pass or fail with the reasons written in plain language. It works for AWS, Google Cloud and Azure resources, and it ignores anything it does not understand instead of guessing.

## Why I built it

I kept seeing the same four incidents in different companies: a replaced RDS instance, an IAM policy that grew a wildcard, a security group opened to `0.0.0.0/0` for "just a minute" and a destroy count that was ten times what the author expected because the wrong workspace was selected. Every one of them was visible in the plan. Nobody read it closely enough because the plan is long and the risky part is small.

I also wanted something that survives real plans, not a demo. That meant handling the cases that break naive scripts: policy documents stored as JSON strings, values that are unknown until apply, `after_sensitive` markers, replace paths, resources that Terraform drifted on and plan files that are binary or streamed. I chose Perl because it is on every Linux runner already, `JSON::PP` and `Getopt::Long` are core modules and there is nothing to install or pin.

## When to use it

Use it as a required check on any pull request that touches Terraform. It fits well in these places:

- A CI step between `terraform plan` and `terraform apply` that blocks the apply on high or critical findings.
- A pull request comment, using `--format markdown`, so the reviewer sees the risks before they open the diff.
- A pre apply check on a laptop when you are about to run a plan against production.
- A guard for teardown jobs, using `--max-destroy` to say how many destroys you actually expect.

It is not a replacement for a full policy engine or a security scanner that reads your source code. It reads the plan, which is what will actually happen, and that is the thing most scanners cannot see. If you already run OPA or Sentinel, keep them. This tool catches the plan level mistakes that those rules usually do not cover, and it takes a minute to adopt.

## How it works

The entry point is `run`. It parses options with `GetOptionsFromArray`, loads the plan with `read_plan`, loads waivers with `load_waivers`, calls `analyse`, filters through `apply_waivers`, decides with `verdict` and prints with one of `render_text`, `render_markdown` or `render_json`.

**Reading the plan.** `read_plan` accepts a file path or `-` for standard input. It refuses a zip file (the binary `tfplan`), recognises `terraform plan -json` streaming output and a state document, and says what command to run instead. It checks `format_version` is 1.x and stops if the plan is marked `errored`. Every one of those is exit code 2, so a broken input never looks like a pass.

**Classifying actions.** `kind_of` turns the `actions` array into one of replace, delete, create, update, forget, read or noop. A replace is any plan that has both delete and create. The order matters and is reported: delete first means a window with nothing running, create first can collide with a fixed name.

**The rules.** `analyse` runs these checks on every managed resource change:

- `TFR001` in `check_stateful_destroy`: destroying or replacing a stateful type from `@STATEFUL_TYPES`, such as databases, buckets, KMS keys, disks and key vaults. It adds specifics, for example `skip_final_snapshot` being true on an RDS instance, `force_destroy` on a bucket or a KMS key whose ciphertext will become unreadable.
- `TFR002` in `check_replace_reason`: any other replacement, with the Terraform `action_reason` and the `replace_paths` that forced it.
- `TFR003` in `check_iam`: parses policy documents out of `policy`, `assume_role_policy` and `inline_policy`, then compares the dangerous grants before and after through `policy_grants`. Only newly introduced grants are reported. It covers `Action: "*"`, `service:*` on all resources, `NotAction`, `Principal: "*"` without a condition, privilege escalation actions such as `iam:PassRole` and attachments of AdministratorAccess. A policy that is unknown until apply is reported as low severity, because it cannot be checked.
- `TFR004` in `check_network`: world open ingress on AWS security groups and rules, Google firewalls and Azure network security rules. `open_ingress_rules` reduces each rule to a signature so a rule that was already open does not repeat. `exposure_text` maps the port range onto the `%SENSITIVE_PORTS` table such as SSH, RDP, Postgres and Redis. A rule that only exposes port 443 is ignored on purpose.
- `TFR005` in `check_protection`: data driven from `@PROTECTION_RULES`. Deletion protection off, encryption off, backups set to zero, public access blocks relaxed, versioning suspended and public ACLs. Each rule says whether it also applies on create.
- `TFR006` in `check_secrets`: attributes named like `password` or `token` that hold a literal value and are not covered by `after_sensitive`. `is_marked` walks the sensitive marker in step with the value. The report names the path and never prints the value.
- `TFR007` in `check_blast_radius`: more destroys than `--max-destroy` or a larger share than `--max-destroy-pct`. The percentage rule only applies from `MIN_DESTROYS_FOR_PCT` destroys so a tiny stack does not trip it.
- `TFR008` in `check_drift`: a resource that was deleted outside Terraform, which the plan will recreate empty.
- `TFR009` in `check_probable_renames`: a delete plus a create of the same type where at least `RENAME_SIMILARITY` of the configured attributes match. It pairs them one to one, best score first, and prints the exact `moved` block to add. This is the single most useful check in my experience.
- `TFR010`: the plan reports `complete` as false, so a second plan will be needed.

**Waivers.** `load_waivers` demands a rule, an address (with `*` globs), an expiry date and a reason. There are no permanent waivers. An expired waiver does not suppress anything and is printed loudly. A waiver that matches nothing is reported as stale so the file does not rot.

**Severity and exit codes.** Findings are sorted by severity then address. `verdict` compares them against `--fail-on`. Exit 0 is pass, 1 is findings at or above the threshold and 2 is bad input or usage.

## Usage

Create the plan and convert it:

```
terraform plan -out=tfplan
terraform show -json tfplan > plan.json
```

Run the gate:

```
perl TerraformPlanRiskGate.pl plan.json
perl TerraformPlanRiskGate.pl --fail-on critical --format markdown plan.json
terraform show -json tfplan | perl TerraformPlanRiskGate.pl --format json -
perl TerraformPlanRiskGate.pl --waivers ExampleWaivers.json --max-destroy 2 plan.json
```

Options as implemented: `--fail-on` (low, medium, high or critical, default high), `--format` (text, markdown or json), `--waivers FILE`, `--max-destroy N` (default 5), `--max-destroy-pct N` (default 25), `--today YYYY-MM-DD` for repeatable waiver tests, `--output FILE`, `--version` and `--help`.

A waiver file looks like this:

```
{"waivers": [
  {"rule": "TFR005", "address": "aws_db_instance.orders", "expires": "2026-12-31", "reason": "Migration window approved in change ticket 4471"}
]}
```

`ExamplePlan.json` in this folder is a small plan that trips several rules. Run `perl TerraformPlanRiskGate.pl --today 2026-10-16 --waivers ExampleWaivers.json ExamplePlan.json` to see the output and the exit code 1.

A simple CI step:

```
terraform show -json tfplan > plan.json
perl TerraformPlanRiskGate.pl --format markdown --output risk.md plan.json || exit 1
```

## Notes

- It reads only the plan. It makes no network calls and never runs Terraform.
- Secret values are never printed. Findings name paths and attribute names only.
- Data sources and no-op changes are skipped. Resources under `resource_drift` are only checked for deletion.
- Policies built from unknown values cannot be evaluated, so they produce a low severity note. Pair the gate with a review of the final policy after apply where that matters.
- The stateful type list and the protection table are plain Perl arrays near the top of the file. Add your own provider resources there. It is a one line change each.
- Rename detection is a heuristic. A false positive costs you a look at the diff. A false negative costs you a database, so I chose to err on the side of reporting.
- The script exits cleanly when loaded with `require`, so you can unit test the helper functions from a `.t` file.
- Tested with Perl 5.38 and core modules only.
