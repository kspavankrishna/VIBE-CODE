# Kube Context Guard

You have credentials for six clusters and your shell remembers only one context. A single `kubectl delete deployment api` typed against the wrong current-context takes production down, and nothing in kubectl asks you to confirm. This is a Bash wrapper that sits in front of kubectl, classifies what the command actually does, and refuses risky mutations in protected clusters unless you prove intent.

**Language:** Bash | **Lines:** 1160 | **Added:** 2026-04-22

## What this solves

The failure mode is not missing RBAC. It is a valid command run by a valid identity against the wrong target. Your kubeconfig has prod, staging and three dev clusters in it. You switched to prod twenty minutes ago to read logs, got pulled into a thread, came back and ran `kubectl rollout restart deployment/checkout`. That command is syntactically perfect, fully authorized and completely wrong. Kubernetes executes it without a single question. The pager fires ninety seconds later and the postmortem line reads "human error, wrong context".

The same thing happens without a human in the loop. CI jobs, release bots and AI coding agents inherit a kubeconfig and a shell, then generate kubectl invocations from a prompt or a template. An agent that pipes YAML into `kubectl apply -f -` bypasses every review habit your team has, because there is no file, no diff and no PR. Nothing on disk records what was applied. When something breaks two hours later you are reconstructing the change from terminal scrollback that has already scrolled away.

Then there is concurrency. During an incident three engineers open three terminals against the same production namespace. One scales a deployment down, another scales it up, a third applies a manifest that reverts both. Kubernetes accepts all three because each caller is authorized, and the cluster ends in a state nobody intended. Admission controllers, OPA and Kyverno catch policy violations, but they run after the request leaves your machine and cannot tell "deliberate production change with a ticket" from "wrong window". That distinction lives on the client side, before the API call.

## Why I built it

Everything that exists here is either too heavy or in the wrong place. Policy engines are cluster side and need install rights, a control plane and a team to maintain the rules. `kubectx` and shell prompt plugins show you the context but never block anything, and a prompt string is useless to a CI runner or an agent. Wrapping kubectl in a five line alias that greps for "prod" breaks the moment someone passes `--context` explicitly or uses `-A`.

I wanted one portable file with no runtime, no daemon and no cluster install that I could drop into a repo, alias in an agent shell and have it fail closed on anything it does not understand. Unknown verb becomes mutating. Unrecognized rollout subcommand becomes mutating. Missing information never resolves into permission.

## When to use it

- Aliasing kubectl inside an AI coding agent or automation shell that has real cluster credentials and no supervision
- CI and GitHub Actions steps that run kubectl against production and need a ticket ID attached to every write
- Local SRE shells where you switch contexts constantly and a stale current-context is one command away from an outage
- Incident response where several people are operating on the same namespace and you need overlapping writes to fail loudly
- Any workflow where generated YAML reaches the cluster over `-f -` and you want that path shut off by default
- Compliance situations where you need a machine readable record of who changed what, in which cluster, under which change ticket

## How it works

Argument handling splits in `parse_args`. Anything starting with `--guard-` is consumed as guard configuration, in both `--flag value` and `--flag=value` form, and everything else lands in the `KUBECTL_ARGS` array. A bare `--` sends the remainder to kubectl verbatim. Every guard option also has a `KCG_*` environment variable equivalent, so CI can set policy once instead of threading flags through every step.

`parse_kubectl_shape` walks the kubectl argv with a small state machine rather than a regex. It tracks flags that consume the next token (`--context`, `-n`, `-f`, plus a list of value taking flags like `-o`, `--kubeconfig` and `--as`) so their values are never mistaken for positional tokens. What survives becomes the verb, subverb and resource. It sets `USES_STDIN_MANIFEST` on `-f -` and `ALL_NAMESPACES` on `-A`. The target is resolved next: `resolve_effective_context` prefers an explicit `--context` and otherwise shells out to `kubectl config current-context`, and `resolve_effective_namespace` prefers `-n`, falls back to a `config view --minify` jsonpath lookup and finally to `default`.

`classify_command` maps the verb into read, config, mutating, interactive or unknown. Reads are an explicit allowlist (`get`, `describe`, `logs`, `top`, `diff`, `explain` and friends). `auth` is read only when the subverb is `can-i`. `rollout status` and `rollout history` are reads, while `restart`, `undo`, `pause` and `resume` are mutations. `exec`, `attach`, `cp`, `debug` and `port-forward` are treated as mutating because interactive pod access changes live state without leaving a declarative trail. Anything unrecognized falls through to mutating with risk `unknown`. Along the way it appends human readable strings to a `REASONS` array: `--all` on a delete, a delete against namespaces, nodes, CRDs, PVs or webhook configurations, or a stdin manifest.

`compute_protection_state` decides whether the target is protected by matching the effective context against `PROTECT_CONTEXT_REGEX` (default catches prod, production, live and shared-prod as a delimited token) and the namespace against `PROTECT_NAMESPACE_REGEX` (kube-system, argocd, cert-manager, flux-system, istio-system and similar). A mutation with `-A` is protected unconditionally, since it can reach a protected namespace regardless of the current one. `enforce_context_policy` runs the independent `--guard-expect-context` and `--guard-allow-namespace` checks, which block regardless of protection state.

`compute_fingerprint` hashes `context`, `namespace` and the shell quoted command line through `sha256_text`, which tries shasum, sha256sum then openssl and exits 69 if none exist. That digest is the confirmation token. To run a protected mutation you need `--guard-allow-protected`, a `--guard-ticket`, and `--guard-confirm` carrying exactly that fingerprint. Change one character of the command and the token no longer matches, so a confirmation cannot be pasted forward from a previous, different operation. When a command is blocked, `emit_block_report` prints the context, namespace, fingerprint, reasons and a ready to paste rerun line built by `render_override_hint`.

Concurrency is a TTL lease, taken only for protected mutations. `acquire_mutation_lease` serializes on a `mkdir` mutex (atomic on POSIX filesystems, retried 200 times at 100 ms) and reads a key=value lease file keyed by sanitized context and namespace. If a different owner holds a lease newer than `LEASE_TTL_SECONDS`, the run exits 41. Otherwise the lease is claimed and `lease_start_heartbeat` forks a background loop that rewrites the timestamp every TTL/3 seconds, so a long apply keeps its claim while a crashed shell lets the lease expire on its own. An EXIT, INT and TERM trap calls `lease_release`, which deletes only a lease still matching this owner and fingerprint.

Every decision is appended to a JSONL audit log by `audit_event`: timestamp, status (allowed, blocked, completed, failed), exit code, context, namespace, verb, resource, risk, protected flag, owner, ticket, approver, fingerprint, the reconstructed command and the reason array. `--guard-self-test` runs six end to end tests against a generated fake kubectl, covering an allowed read, a blocked protected delete, the same delete succeeding with a valid fingerprint, the allow-namespace rule rejecting `-A`, a blocked stdin manifest and a lease conflict.

## Usage

```bash
chmod +x KubeContextGuard.sh
./KubeContextGuard.sh --guard-self-test

# Reads pass through untouched
./KubeContextGuard.sh -- get pods -n payments

# Pin the shell to dev clusters, block anything else
./KubeContextGuard.sh --guard-expect-context '^dev-' -- get pods -n payments

# A protected mutation is blocked and prints its fingerprint
./KubeContextGuard.sh -- delete deployment api -n payments   # exit 40

# Get the fingerprint on its own, then run for real
FP=$(./KubeContextGuard.sh --guard-fingerprint-only -- delete deployment api -n payments)
./KubeContextGuard.sh --guard-allow-protected --guard-ticket CHG-4821 \
  --guard-approved-by ops-lead --guard-confirm "$FP" -- delete deployment api -n payments

# Piped YAML needs its own override
cat deploy.yaml | ./KubeContextGuard.sh --guard-allow-protected --guard-ticket CHG-4821 \
  --guard-allow-stdin-manifest --guard-no-confirm -- apply -f - -n payments

# CI: set policy once by environment
export KCG_PROTECT_CONTEXT='^(prod|prod-eu1)$' KCG_ALLOW_NAMESPACE='^payments$' \
       KCG_CHANGE_TICKET="$CHANGE_ID" KCG_AUDIT_LOG=/var/log/kcg.jsonl
alias kubectl='/opt/bin/KubeContextGuard.sh --'
```

## Notes

- Exit codes: 40 blocked by policy, 41 lease conflict or lock timeout, 64 usage error, 69 missing kubectl or missing sha256 tool. Otherwise kubectl's own exit code is passed straight through.
- This is client side only. It guards a specific kubectl invocation, not the cluster. Anyone can call kubectl directly and bypass it entirely, so treat it as a seatbelt, not RBAC or admission control.
- `kubectl config` is classified as non mutating with risk `config`, so `config use-context` and `config set-credentials` are not blocked. They change your local kubeconfig, not the cluster.
- The lease is advisory and only taken for mutations that are both mutating and protected. Two people running unprotected changes, or bypassing the wrapper, will not see each other.
- Context and namespace resolution shells out to kubectl twice when they are not given explicitly, which adds latency to every read. Pass `--context` and `-n` in hot loops.
- `--guard-no-confirm` disables the fingerprint check but still requires `--guard-allow-protected` and a ticket. It exists for CI, not for interactive use.
- Requires bash with arrays and `[[ =~ ]]`, not sh or dash. Empty array expansion under `set -u` means very old bash builds (the macOS system 3.2) can misbehave, so run it under a current bash.
- Argv parsing knows the common value taking kubectl flags but not every one that exists. An unlisted value taking flag can push its value into the positional tokens, which at worst misreads the resource name. Verb classification is unaffected, and unknown verbs already fail closed.
