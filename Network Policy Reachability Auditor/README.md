# Network Policy Reachability Auditor

A Racket tool that replays Kubernetes NetworkPolicy semantics over a cluster snapshot or rendered manifests and tells you exactly who can reach whom, on which port and because of which rule. It lints the classic NetworkPolicy mistakes and turns "payments must never be reachable from monitoring" into a CI test that fails the build.

**Language:** Racket | **Lines:** 1132 | **Added:** 2026-11-25

## What this solves

Kubernetes NetworkPolicy looks simple and is not. A policy file is a few lines of YAML, but whether a given pod can open a connection to another pod depends on every policy in two namespaces, on the labels of both pods, on the labels of both namespaces, on named container ports and on two directions that must both say yes. Nobody keeps that in their head. The result is a familiar list of production surprises:

- A single YAML dash turns "pods labelled `app=api` in the `shop` namespace" into "anything in the `shop` namespace OR any pod labelled `app=api` in my own namespace". The policy applies cleanly, looks right in review and is much wider than intended.
- A team adds `policyTypes: [Ingress]` and an `egress` block in the same file. The egress rules are silently ignored.
- Someone rolls out a default deny egress policy and every pod in the namespace stops resolving DNS, because nothing allows 53/UDP to CoreDNS.
- `port: "8443"` in quotes is a port name, not a number. The API server rejects it, but it sits in a Helm chart for months because that chart never rendered that branch.
- An `ipBlock` of `10.0.0.0/8` was meant for the office VPN and also covers the pod network. Calico will match pod traffic against it, Cilium by default will not, so the same manifest gives different answers on different clusters.
- A policy still selects `app=checkout` after the service was renamed to `app=cart`. It selects nothing and protects nothing.

`kubectl describe networkpolicy` shows you the YAML back in prose. It does not answer the only question that matters during an incident or a security review: can this workload reach that workload on this port right now? If not, which policy is in the way?

This tool answers that question with the exact JSON path of the deciding rule, for example `payments/ledger-in spec.ingress[0].from[0]`. It gives a three way verdict: allow, deny or depends on the CNI. It also runs as a CI gate in two forms, an `audit` that fails on bad policies and a `check` that fails when a reachability expectation is broken.

## Why I built it

I kept seeing NetworkPolicy changes approved on the strength of "the YAML looks fine". Reviewers read the selector, nod and merge. Then a postmortem finds that the database was open to a whole namespace for three months because of one dash. Or that a namespace lost DNS for twenty minutes because the default deny landed before the DNS allow.

The existing options did not fit. Visual editors are good for writing a policy from scratch but they do not read your cluster. CNI specific tools such as Hubble or Calico flow logs show what happened, not what is allowed. They also only see it after traffic has flowed. Writing an integration test that spins up pods and runs `nc` works, but it is slow, flaky and needs a live cluster for every pull request.

What I wanted was the policy engine itself, written down once and carefully, that runs against JSON offline in a second. Racket suits this well: the problem is symbolic, the data is nested and the semantics are a handful of small precise functions that read almost like the Kubernetes API reference. The whole tool is one file and uses only the base Racket distribution.

## When to use it

- In CI on rendered manifests. Run `helm template` or `kustomize build`, convert to JSON and run `audit` plus `check` before merge. No cluster needed, workloads come from Deployment, StatefulSet, DaemonSet, ReplicaSet, Job, CronJob and ReplicationController templates.
- During a security review or a compliance audit, against a live snapshot from `kubectl get ns,pods,networkpolicies -A -o json`. You get the full reachability matrix for any port.
- During an incident when a service suddenly cannot connect. `explain` tells you which side refused, egress or ingress and which policies isolate it.
- Before rolling out a default deny policy. Add the policy to the snapshot locally and run `audit` to see whether DNS breaks and which workloads lose their callers.

It is not a replacement for your CNI. It models the upstream `networking.k8s.io/v1` NetworkPolicy API, which every conformant CNI implements. It tells you loudly when the snapshot contains objects it does not model.

## How it works

**Loading.** `objects-from-jsexprs` accepts anything kubectl or yq produce: a single object, a `List`, a typed list such as `PodList` whose items have no `kind` (the kind is inferred from the list name), a JSON array or a stream of concatenated JSON documents. `read-json-documents` reads documents until end of file and reports a clear error with a hint to convert YAML first.

**Workload classes.** `build-model` groups live pods into classes keyed by namespace, labels, container ports and host networking, because NetworkPolicy treats pods with the same values identically. A class is the `wl` struct. Pods in the `Succeeded` or `Failed` phase are skipped. Class names come from `pod-display-name`, which strips the `pod-template-hash` suffix from a ReplicaSet owner so a Deployment shows up as `shop/web`, not `shop/web-7d9f8b6c5`. Workload templates from manifests become classes too, unless a live pod in the same namespace already carries all of the template labels, so a mixed input does not double count. Pod IPs are kept on the class for `ipBlock` evaluation.

**Namespaces.** Every Namespace gets the `kubernetes.io/metadata.name` label (`NS-NAME-LABEL`), as the API server adds it automatically. If a policy or pod refers to a namespace that is not in the input, the tool synthesizes it with only that label and raises a `missing-namespace` finding, because any namespaceSelector on other labels will not match it.

**Parsing policies.** `parse-policy` applies the real `policyTypes` default: Ingress always, Egress only when the egress list is non empty. `parse-selector` handles `matchLabels` and `matchExpressions` with `In`, `NotIn`, `Exists` and `DoesNotExist`. A selector the API server would reject, such as an unknown operator or `In` with no values, becomes `NEVER-SELECTOR` so it matches nothing. You also get an `invalid-selector` error. `parse-peer` enforces that `ipBlock` cannot be mixed with selectors and that an empty peer is invalid. `parse-pspec` defaults the protocol to TCP, validates `endPort`, rejects quoted numbers and keeps named ports as strings.

**Addresses.** `parse-ip` handles IPv4 with strict octets, IPv6 with `::` compression, zone suffixes and an embedded IPv4 tail. `parse-cidr` masks host bits and remembers whether the input was canonical. `block-contains?` applies the `except` list. `block-overlaps-cidr?` is used with `--pod-cidr` for template workloads that have no IP yet.

**Evaluation.** `pair-eval` runs two sides through `side-eval`: egress on the source and ingress on the destination. `selecting-policies` finds policies in the subject namespace whose podSelector matches and whose types include the direction, memoised per class. No selecting policy means the direction is open. Otherwise each rule goes through `rule-eval`, which first checks ports with `port-match?` (named ports resolve against the destination container ports with matching protocol) and then peers with `peer-match`. A peer with only a podSelector matches pods in the policy namespace. A peer with only a namespaceSelector matches every pod in matching namespaces. A peer with both matches the intersection.

The three valued result is the important part. `peer-match` returns `yes`, `no` or `maybe`. `maybe` means an `ipBlock` covers a pod address: upstream Kubernetes leaves this implementation defined, so the tool refuses to guess. Host network pods also get `maybe`. `best` combines results, `side-allows` maps them to `allow`, `deny` or `depends` and the pair verdict is deny if either side denies.

**Audit.** `run-audit` calls `audit-policy`, `audit-isolation` and `audit-dns`. Finding codes include `dead-policy`, `or-not-and`, `ignored-rules`, `allow-any-source`, `internet-ingress`, `ipblock-covers-pods`, `unresolved-named-port`, `dead-peer`, `all-namespaces`, `hostnetwork-target`, `no-ingress-isolation`, `dns-blocked`, `dns-tcp-blocked`, `dns-cni-dependent`, `unmodeled-policy` and the parse errors. `audit-dns` checks every egress isolated workload against `DEFAULT-DNS-SPEC` (`kube-system:k8s-app=kube-dns`) plus any `--dns-ip`, for 53/UDP and 53/TCP. System namespaces in `SYSTEM-NAMESPACES` get info level isolation findings rather than warnings.

**Expectations.** `parse-expectations` reads one rule per line and `run-check` evaluates every source and destination pair. An expectation whose source or destination matches nothing fails, because a test that tests nothing is a broken test. `testable-pair?` skips a single pod talking to itself, since loopback is never policy controlled.

## Usage

Requires Racket 8 or newer. Options go before the command.

```bash
# live cluster snapshot
kubectl get namespaces,pods,networkpolicies -A -o json > cluster.json

# lint and gate: exit 1 on any error finding
racket NetworkPolicyReachabilityAuditor.rkt -f cluster.json audit

# stricter gate in CI, machine readable
racket NetworkPolicyReachabilityAuditor.rkt -f cluster.json --fail-on warn --format json audit

# why can or can't the API reach the ledger database?
racket NetworkPolicyReachabilityAuditor.rkt -f cluster.json explain shop:app=api payments/ledger-0 5432/TCP

# everything that can talk on port 5432
racket NetworkPolicyReachabilityAuditor.rkt -f cluster.json matrix 5432

# rendered manifests, no cluster
helm template ./chart | yq -o=json '.' > rendered.json
racket NetworkPolicyReachabilityAuditor.rkt -f rendered.json -n shop --pod-cidr 10.244.0.0/16 check reachability.txt
```

Sample `explain` output:

```
shop/api -> payments/ledger-0 5432/TCP: ALLOW
  egress (open)
    no policy isolates egress of shop/api
  ingress (yes)
    payments/ledger-in spec.ingress[0].from[0]
```

An expectations file, one rule per line, `#` starts a comment:

```
allow shop:app=web       -> shop:app=api         9090/TCP
deny  monitoring:*       -> payments:app=ledger  5432/TCP
allow ip:203.0.113.9     -> shop:app=web         8080
deny  *:app=prometheus   -> payments/ledger-0    5432
```

Endpoints are `ip:ADDR` for traffic from or to outside the pod network, `NAMESPACE/NAME` for a workload class or a pod by name and `NAMESPACE:SELECTOR` with terms `key=value`, `key!=value`, `key` and `!key` separated by commas. The namespace can be `*`.

Options: `-f` input file (repeatable, `-` for stdin, stdin is the default), `-n` default namespace, `--pod-cidr`, `--dns`, `--dns-ip`, `--ignore-namespace`, `--fail-on error|warn|info|none`, `--format text|json`, `--strict` (CNI dependent verdicts fail `check`) and `--limit` for `explain`.

Exit codes: 0 means clean, 1 means the gate failed (findings at or above `--fail-on`, a broken expectation or a denied pair in `explain`) and 2 means bad input or bad arguments.

## Notes

- AdminNetworkPolicy, BaselineAdminNetworkPolicy, Cilium, Calico and Istio policy objects are detected and reported as `unmodeled-policy`, but not evaluated. If you use them, treat an allow from this tool as "NetworkPolicy allows it" rather than "the packet gets through".
- Only TCP, UDP and SCTP ports are modelled. ICMP is outside the NetworkPolicy API.
- Live pods give exact IPs for `ipBlock` checks. Template workloads have no IP, so pass `--pod-cidr` if you want overlap warnings for them.
- A `depends` verdict is not a cop out. It marks the exact place where your result changes with the CNI. Replace that `ipBlock` with selectors and the ambiguity goes away.
- The `or-not-and` check is a heuristic on adjacent peers, which is where the YAML dash mistake lands. It can fire on intentional OR rules, in which case splitting them into two rules documents the intent and silences it.
- The model follows upstream NetworkPolicy semantics of the current `networking.k8s.io/v1` API: default policyTypes, endPort, namespace name label and named port resolution. If upstream changes, the functions to touch are `parse-policy`, `peer-match` and `port-match?`.
