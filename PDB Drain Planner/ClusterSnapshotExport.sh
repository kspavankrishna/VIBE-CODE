#!/usr/bin/env bash
# Exports a live cluster, or saved kubectl JSON, into the line format that
# PdbDrainPlanner reads. Needs bash 4, jq 1.6 or newer and, for live mode,
# kubectl with read access to nodes, pods, pdbs, replicasets, statefulsets
# and replicationcontrollers. It never writes to the cluster.
set -euo pipefail

usage() {
  cat <<'EOF'
usage: ClusterSnapshotExport.sh (--drain-selector K=V[,K=V] | --drain-nodes A,B) [options]

  --drain-selector K=V,...  nodes whose labels match every pair are drain targets
  --drain-nodes A,B         explicit comma separated drain target names
  --pool-label KEY          node label that names the pool (default: auto detect)
  --ready-seconds N         seconds a replacement pod needs to become ready (default 30)
  --context NAME            kubectl context to read
  --from-dir DIR            read nodes.json pods.json pdbs.json controllers.json
                            from DIR instead of calling kubectl
EOF
}

drain_selector=""
drain_nodes=""
pool_label=""
ready_seconds=30
context=""
from_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --drain-selector) drain_selector="${2:?--drain-selector needs a value}"; shift 2 ;;
    --drain-nodes)    drain_nodes="${2:?--drain-nodes needs a value}"; shift 2 ;;
    --pool-label)     pool_label="${2:?--pool-label needs a value}"; shift 2 ;;
    --ready-seconds)  ready_seconds="${2:?--ready-seconds needs a value}"; shift 2 ;;
    --context)        context="${2:?--context needs a value}"; shift 2 ;;
    --from-dir)       from_dir="${2:?--from-dir needs a value}"; shift 2 ;;
    -h|--help)        usage; exit 0 ;;
    *) echo "ClusterSnapshotExport: unknown argument $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$drain_selector" && -z "$drain_nodes" ]]; then
  echo "ClusterSnapshotExport: pass --drain-selector or --drain-nodes" >&2
  exit 2
fi
if ! [[ "$ready_seconds" =~ ^[0-9]+$ ]]; then
  echo "ClusterSnapshotExport: --ready-seconds must be a whole number" >&2
  exit 2
fi
command -v jq >/dev/null || { echo "ClusterSnapshotExport: jq is required" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

if [[ -n "$from_dir" ]]; then
  for f in nodes pods pdbs controllers; do
    [[ -r "$from_dir/$f.json" ]] || { echo "ClusterSnapshotExport: missing $from_dir/$f.json" >&2; exit 2; }
    cp "$from_dir/$f.json" "$work/$f.json"
  done
else
  command -v kubectl >/dev/null || { echo "ClusterSnapshotExport: kubectl is required" >&2; exit 2; }
  kc=(kubectl)
  [[ -n "$context" ]] && kc+=(--context "$context")
  "${kc[@]}" get nodes -o json > "$work/nodes.json"
  "${kc[@]}" get pods --all-namespaces -o json > "$work/pods.json"
  "${kc[@]}" get poddisruptionbudgets --all-namespaces -o json > "$work/pdbs.json"
  "${kc[@]}" get replicasets,statefulsets,replicationcontrollers --all-namespaces -o json > "$work/controllers.json"
fi

jq -r -n \
  --slurpfile nodes "$work/nodes.json" \
  --slurpfile pods "$work/pods.json" \
  --slurpfile pdbs "$work/pdbs.json" \
  --slurpfile ctrl "$work/controllers.json" \
  --arg selector "$drain_selector" \
  --arg names "$drain_nodes" \
  --arg poolkey "$pool_label" \
  --arg ready "$ready_seconds" '
  def pool_keys: if $poolkey != "" then [$poolkey]
    else ["karpenter.sh/nodepool", "cloud.google.com/gke-nodepool",
          "eks.amazonaws.com/nodegroup", "kubernetes.azure.com/agentpool",
          "agentpool"] end;
  def pool_of: (.metadata.labels // {}) as $l
    | ([pool_keys[] | $l[.] // empty] | first) // "default";
  def wanted: ($selector | if . == "" then [] else split(",") | map(split("=")) end);
  def target_names: ($names | if . == "" then [] else split(",") end);
  def is_target: . as $n
    | ((target_names | index($n.metadata.name)) != null)
      or ((wanted | length) > 0
          and all(wanted[]; ($n.metadata.labels // {})[.[0]] == (.[1] // "")));
  def ready_cond: ((.status.conditions // []) | map(select(.type == "Ready")) | first | .status) == "True";
  def or_dash: if . == "" or . == null then "-" else . end;
  def known_kind: IN("ReplicaSet", "StatefulSet", "ReplicationController");
  def owner: ((.metadata.ownerReferences // []) | map(select(.controller == true)) | first);
  def mirror: (.metadata.annotations // {}) | has("kubernetes.io/config.mirror");
  def request($r):
    (.spec.overhead[$r] // null) as $ovh
    | ([.spec.initContainers // [] | .[] | select(.restartPolicy == "Always") | .resources.requests[$r] // empty]) as $sidecars
    | ([.spec.containers[] | .resources.requests[$r] // empty] + $sidecars + [$ovh // empty]) as $app
    | ([.spec.initContainers // [] | .[] | select(.restartPolicy != "Always") | .resources.requests[$r] // empty]
       | map([.] + $sidecars + [$ovh // empty] | join("+"))) as $inits
    | ([($app | if length == 0 then "0" else join("+") end)] + $inits) | join("^");

  ($nodes[0].items | map(select(is_target) | .metadata.name)) as $targets
  | ($pods[0].items | map(select(.spec.nodeName != null
                                 and .metadata.deletionTimestamp == null
                                 and ((.status.phase // "") | IN("Succeeded", "Failed") | not)))) as $live

  | (if ($targets | length) == 0 then error("no node matched the drain selection") else empty end),

  "# exported by ClusterSnapshotExport.sh",
  ($nodes[0].items[]
    | "node \(.metadata.name) \(pool_of) \(.status.allocatable.cpu) \(.status.allocatable.memory) \(.status.allocatable.pods) \(if (.metadata.name | IN($targets[])) then "drain" else "keep" end) \(if (.spec.unschedulable // false) or (ready_cond | not) then "cordoned" else "schedulable" end)"),

  ($ctrl[0].items[] | select(.kind | known_kind)
    | "workload \(.metadata.namespace)/\(.kind):\(.metadata.name) \(.spec.replicas // 1) \($ready)"),

  ($live | map(select(owner != null and (owner.kind | known_kind | not)
                      and owner.kind != "DaemonSet" and owner.kind != "Node"))
         | group_by([.metadata.namespace, owner.kind, owner.name])[]
         | "workload \(.[0].metadata.namespace)/\(.[0] | owner.kind):\(.[0] | owner.name) \(length) \($ready)"),

  ($pdbs[0].items[]
    | (if .spec.minAvailable != null then "minAvailable \(.spec.minAvailable)"
       else "maxUnavailable \(.spec.maxUnavailable // 0)" end) as $budget
    | (if .spec.selector == null then "!"
       else ([(.spec.selector.matchLabels // {}) | to_entries[] | "\(.key)=\(.value)"]
             + [(.spec.selector.matchExpressions // [])[]
                | if .operator == "In" then "\(.key)@in:\(.values | join("|"))"
                  elif .operator == "NotIn" then "\(.key)@notin:\(.values | join("|"))"
                  elif .operator == "Exists" then "\(.key)@exists"
                  else "\(.key)@notexists" end])
            | if length == 0 then "*" else join(",") end end) as $sel
    | "pdb \(.metadata.namespace)/\(.metadata.name) \($budget) \(.spec.unhealthyPodEvictionPolicy // "IfHealthyBudget") \($sel)"),

  ($live[]
    | owner as $o
    | (if $o == null then "-"
       elif ($o.kind == "DaemonSet" or $o.kind == "Node") then "-"
       else "\(.metadata.namespace)/\($o.kind):\($o.name)" end) as $wl
    | ([ (if $o != null and $o.kind == "DaemonSet" then "ds" else empty end),
         (if mirror then "mirror" else empty end),
         (if any(.spec.volumes // [] | .[]; has("emptyDir")) then "emptydir" else empty end) ]
       | join(",") | or_dash) as $flags
    | ([(.metadata.labels // {}) | to_entries[] | "\(.key)=\(.value)"] | join(",") | or_dash) as $labels
    | ((.spec.nodeSelector // {}) as $ns | [pool_keys[] | $ns[.] // empty] | first | or_dash) as $pool
    | "pod \(.metadata.namespace)/\(.metadata.name) \(.spec.nodeName) \(if mirror then "-" else $wl end) \(request("cpu")) \(request("memory")) \(if ready_cond then "ready" else "unready" end) \($pool) \($labels) \($flags)")
'
