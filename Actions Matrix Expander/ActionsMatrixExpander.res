// Actions Matrix Expander. ReScript 12, Node target, zero dependencies.

type severity = SevError | SevWarning | SevInfo

type finding = {
  severity: severity,
  jobId: string,
  code: string,
  message: string,
}

type origin = FromBase | FromInclude(int)

type combo = {
  mutable pairs: array<(string, JSON.t)>,
  setBy: dict<int>,
  origin: origin,
}

type expansion = {
  axes: array<string>,
  combos: array<combo>,
  cartesian: int,
  excluded: int,
  merged: int,
  created: int,
}

type jobStatus =
  | Expanded(expansion)
  | Dynamic
  | Failed

type jobResult = {
  id: string,
  job: dict<JSON.t>,
  status: jobStatus,
  maxParallel: option<float>,
}

type options = {
  maxJobs: int,
  jobFilter: option<string>,
  asJson: bool,
  listAll: bool,
  strict: bool,
  failOnRemoved: bool,
  minutes: option<float>,
  weights: array<(string, float)>,
}

// GitHub rejects a matrix that expands past this many jobs.
let githubMatrixLimit = 256

// ---------- bindings ----------
@module("node:fs") external readFileSync: (string, string) => string = "readFileSync"
@val @scope("process") external argv: array<string> = "argv"
let setExitCode: int => unit = %raw("(c) => { process.exitCode = c }")

let plural = (n: int, one: string, many: string) => Int.toString(n) ++ " " ++ (n == 1 ? one : many)

// ---------- findings ----------
let add = (findings: array<finding>, severity, jobId, code, message) =>
  findings->Array.push({severity, jobId, code, message})

let severityName = s =>
  switch s {
  | SevError => "error"
  | SevWarning => "warning"
  | SevInfo => "info"
  }

// ---------- JSON helpers ----------
let rec jsonEq = (a: JSON.t, b: JSON.t): bool =>
  switch (a, b) {
  | (Null, Null) => true
  | (Boolean(x), Boolean(y)) => x == y
  | (Number(x), Number(y)) => x == y
  | (String(x), String(y)) => x == y
  | (Array(x), Array(y)) =>
    Array.length(x) == Array.length(y) &&
      x->Array.everyWithIndex((v, i) => jsonEq(v, y->Array.getUnsafe(i)))
  | (Object(x), Object(y)) =>
    Array.length(Dict.keysToArray(x)) == Array.length(Dict.keysToArray(y)) &&
      Dict.toArray(x)->Array.every(((k, v)) =>
        switch Dict.get(y, k) {
        | Some(w) => jsonEq(v, w)
        | None => false
        }
      )
  | _ => false
  }

// A map in an exclude entry is a partial match: every key it names must match,
// extra keys on the combination are ignored. Everything else is exact.
let rec jsonPartial = (pattern: JSON.t, actual: JSON.t): bool =>
  switch (pattern, actual) {
  | (Object(p), Object(a)) =>
    Dict.toArray(p)->Array.every(((k, v)) =>
      switch Dict.get(a, k) {
      | Some(w) => jsonPartial(v, w)
      | None => false
      }
    )
  | _ => jsonEq(pattern, actual)
  }

let rec canon = (j: JSON.t): string =>
  switch j {
  | Object(d) =>
    "{" ++
    Dict.keysToArray(d)
    ->Array.toSorted(String.compare)
    ->Array.map(k => JSON.stringify(String(k)) ++ ":" ++ canon(Dict.getUnsafe(d, k)))
    ->Array.join(",") ++ "}"
  | Array(a) => "[" ++ a->Array.map(canon)->Array.join(",") ++ "]"
  | other => JSON.stringify(other)
  }

let show = (j: JSON.t): string =>
  switch j {
  | String(s) => s
  | other => JSON.stringify(other)
  }

let typeName = (j: JSON.t) =>
  switch j {
  | Null => "null"
  | Boolean(_) => "boolean"
  | Number(_) => "number"
  | String(_) => "string"
  | Array(_) => "list"
  | Object(_) => "map"
  }

let isExpression = (s: string) => String.includes(s, "${{")

let comboKey = (c: combo) =>
  c.pairs
  ->Array.toSorted(((a, _), (b, _)) => String.compare(a, b))
  ->Array.map(((k, v)) => k ++ "=" ++ canon(v))
  ->Array.join("|")

let comboGet = (c: combo, k: string) =>
  switch c.pairs->Array.find(((key, _)) => key == k) {
  | Some((_, v)) => Some(v)
  | None => None
  }

// ---------- key similarity ----------
let normalizeKey = k =>
  k->String.toLowerCase->String.replaceAll("-", "")->String.replaceAll("_", "")

let levenshtein = (a: string, b: string): int => {
  let la = String.length(a)
  let lb = String.length(b)
  let prev = Array.fromInitializer(~length=lb + 1, i => i)
  for i in 1 to la {
    let cur = Array.make(~length=lb + 1, 0)
    cur->Array.setUnsafe(0, i)
    for j in 1 to lb {
      let cost = String.charAt(a, i - 1) == String.charAt(b, j - 1) ? 0 : 1
      let best = Math.Int.min(
        Math.Int.min(prev->Array.getUnsafe(j) + 1, cur->Array.getUnsafe(j - 1) + 1),
        prev->Array.getUnsafe(j - 1) + cost,
      )
      cur->Array.setUnsafe(j, best)
    }
    for j in 0 to lb {
      prev->Array.setUnsafe(j, cur->Array.getUnsafe(j))
    }
  }
  prev->Array.getUnsafe(lb)
}

let nearAxis = (key: string, axes: array<string>): option<string> =>
  axes->Array.find(ax =>
    ax != key &&
    (normalizeKey(ax) == normalizeKey(key) ||
    (String.length(key) >= 4 && String.length(ax) >= 4 && levenshtein(key, ax) <= 1))
  )

// ---------- axis checks ----------
let tenths = (x: float) => Math.round((x -. Math.floor(x)) *. 10.0)

let isOneDecimal = (x: float) => Math.abs(x *. 10.0 -. Math.round(x *. 10.0)) < 1e-9

// An unquoted 3.10 in YAML is the number 3.1. Spot an axis where 3.1 sits next to 3.8 or 3.9.
let checkVersionFloats = (findings, jobId, axis, vals: array<JSON.t>) => {
  let nums = vals->Array.filterMap(v =>
    switch v {
    | Number(x) if isOneDecimal(x) && x != Math.floor(x) => Some(x)
    | _ => None
    }
  )
  nums->Array.forEach(x => {
    if tenths(x) == 1.0 {
      let sibling = nums->Array.find(y => Math.floor(y) == Math.floor(x) && tenths(y) >= 2.0)
      switch sibling {
      | Some(y) =>
        findings->add(
          SevWarning,
          jobId,
          "M011",
          `axis "${axis}" holds ${Float.toString(x)} next to ${Float.toString(y)}. If the YAML said ${Float.toString(Math.floor(x))}.10 unquoted, the parser read it as the number ${Float.toString(x)} and version ${Float.toString(Math.floor(x))}.10 is gone. Quote the value.`,
        )
      | None => ()
      }
    }
  })
}

let checkAxisValues = (findings, jobId, axis, vals: array<JSON.t>) => {
  let seen = Dict.make()
  vals->Array.forEach(v => {
    let key = canon(v)
    switch Dict.get(seen, key) {
    | Some(n) => Dict.set(seen, key, n + 1)
    | None => Dict.set(seen, key, 1)
    }
  })
  Dict.toArray(seen)->Array.forEach(((key, n)) =>
    if n > 1 {
      findings->add(
        SevWarning,
        jobId,
        "M013",
        `axis "${axis}" lists ${key} ${Int.toString(n)} times, which yields duplicate jobs.`,
      )
    }
  )
  let kinds = vals->Array.map(typeName)->Set.fromArray
  if Set.size(kinds) > 1 {
    findings->add(
      SevWarning,
      jobId,
      "M012",
      `axis "${axis}" mixes ${Set.values(kinds)->Array.fromIterator->Array.join(" and ")} values. An include or exclude written with the other type will not match.`,
    )
  }
  checkVersionFloats(findings, jobId, axis, vals)
}

// ---------- matrix expansion ----------
let rec stringsEqualButTyped = (a: JSON.t, b: JSON.t) =>
  typeName(a) != typeName(b) && show(a) == show(b) && typeName(a) != "list" && typeName(a) != "map"

and describeMiss = (axes: array<string>, axisVals: dict<array<JSON.t>>, k: string, v: JSON.t) =>
  if !(axes->Array.includes(k)) {
    let hint = switch nearAxis(k, axes) {
    | Some(ax) => ` It looks like a typo of axis "${ax}".`
    | None => ""
    }
    `key "${k}" is not an axis.${hint}`
  } else {
    let vals = Dict.getUnsafe(axisVals, k)
    switch vals->Array.find(x => stringsEqualButTyped(x, v)) {
    | Some(x) =>
      `"${k}: ${JSON.stringify(v)}" is a ${typeName(v)} but the axis holds the ${typeName(x)} ${JSON.stringify(x)}.`
    | None => `value ${JSON.stringify(v)} never appears in axis "${k}".`
    }
  }

let expandMatrix = (findings, jobId, matrix: dict<JSON.t>, maxJobs: int): option<expansion> => {
  let axes = Dict.keysToArray(matrix)->Array.filter(k => k != "include" && k != "exclude")
  let axisVals: dict<array<JSON.t>> = Dict.make()
  let ok = ref(true)
  let dynamic = ref(false)
  let isDyn = (j: JSON.t) =>
    switch j {
    | String(s) => isExpression(s)
    | _ => false
    }
  Dict.toArray(matrix)->Array.forEach(((_, v)) =>
    if isDyn(v) {
      dynamic := true
    }
  )
  if dynamic.contents {
    findings->add(
      SevInfo,
      jobId,
      "M014",
      "matrix contains an expression such as fromJSON, so it is only known at run time and cannot be expanded here.",
    )
    None
  } else {
    axes->Array.forEach(ax =>
      switch Dict.getUnsafe(matrix, ax) {
      | Array(vals) =>
        if Array.length(vals) == 0 {
          findings->add(SevError, jobId, "M003", `axis "${ax}" is an empty list.`)
          ok := false
        } else {
          Dict.set(axisVals, ax, vals)
          checkAxisValues(findings, jobId, ax, vals)
        }
      | other =>
        findings->add(
          SevError,
          jobId,
          "M002",
          `axis "${ax}" must be a list but is a ${typeName(other)}.`,
        )
        ok := false
      }
    )
    let readEntries = (name: string): array<dict<JSON.t>> =>
      switch Dict.get(matrix, name) {
      | None => []
      | Some(Array(items)) =>
        items->Array.filterMapWithIndex((item, i) =>
          switch item {
          | Object(d) => Some(d)
          | other =>
            findings->add(
              SevError,
              jobId,
              "M019",
              `${name} entry ${Int.toString(i + 1)} must be a map but is a ${typeName(other)}.`,
            )
            ok := false
            None
          }
        )
      | Some(other) =>
        findings->add(
          SevError,
          jobId,
          "M019",
          `${name} must be a list of maps but is a ${typeName(other)}.`,
        )
        ok := false
        []
      }
    let excludes = readEntries("exclude")
    let includes = readEntries("include")
    if !ok.contents {
      None
    } else {
      let axisList = axes
      let base: array<combo> = if Array.length(axisList) == 0 {
        []
      } else {
        axisList
        ->Array.reduce([[]], (acc, ax) =>
          acc->Array.flatMap(prefix =>
            Dict.getUnsafe(axisVals, ax)->Array.map(v => Array.concat(prefix, [(ax, v)]))
          )
        )
        ->Array.map(pairs => {pairs, setBy: Dict.make(), origin: FromBase})
      }
      let cartesian = Array.length(base)
      // exclude: partial match against the original axes only
      let removed = Array.make(~length=cartesian, false)
      excludes->Array.forEachWithIndex((entry, ei) => {
        let n = ei + 1
        let pairs = Dict.toArray(entry)
        let matchesCombo = (c: combo) =>
          pairs->Array.every(((k, v)) =>
            switch comboGet(c, k) {
            | Some(cv) => axisList->Array.includes(k) && jsonPartial(v, cv)
            | None => false
            }
          )
        let matched = ref(0)
        let fresh = ref(0)
        base->Array.forEachWithIndex((c, ci) =>
          if matchesCombo(c) {
            matched := matched.contents + 1
            if !(removed->Array.getUnsafe(ci)) {
              fresh := fresh.contents + 1
              removed->Array.setUnsafe(ci, true)
            }
          }
        )
        if matched.contents == 0 {
          let why =
            pairs
            ->Array.filterMap(((k, v)) => {
              let present =
                axisList->Array.includes(k) &&
                  Dict.getUnsafe(axisVals, k)->Array.some(x => jsonPartial(v, x))
              present ? None : Some(describeMiss(axisList, axisVals, k, v))
            })
            ->Array.join(" ")
          let why = why == "" ? "the listed values never occur together." : why
          findings->add(
            SevWarning,
            jobId,
            "M006",
            `exclude ${Int.toString(n)} removes nothing: ${why} Exclude runs before include, so it cannot see keys that an include adds.`,
          )
        } else if fresh.contents == 0 {
          findings->add(
            SevInfo,
            jobId,
            "M018",
            `exclude ${Int.toString(n)} matches ${Int.toString(matched.contents)} combinations but earlier excludes already removed all of them.`,
          )
        }
      })
      let kept = base->Array.filterWithIndex((_, i) => !(removed->Array.getUnsafe(i)))
      let excludedCount = cartesian - Array.length(kept)
      // include: merge into surviving base combinations, else become a standalone job
      let extras: array<combo> = []
      let mergedInto: Set.t<combo> = Set.make()
      includes->Array.forEachWithIndex((entry, ii) => {
        let idx = ii + 1
        let pairs = Dict.toArray(entry)
        let compatible = (c: combo) =>
          pairs->Array.every(((k, v)) =>
            if axisList->Array.includes(k) {
              switch comboGet(c, k) {
              | Some(cv) => jsonEq(v, cv)
              | None => false
              }
            } else {
              true
            }
          )
        pairs->Array.forEach(((k, _)) =>
          if !(axisList->Array.includes(k)) {
            switch nearAxis(k, axisList) {
            | Some(ax) =>
              findings->add(
                SevWarning,
                jobId,
                "M010",
                `include ${Int.toString(idx)} key "${k}" is not an axis but resembles "${ax}". A typo here adds a new variable instead of selecting a combination.`,
              )
            | None => ()
            }
          }
        )
        let targets = kept->Array.filter(compatible)
        if Array.length(targets) == 0 {
          let reasons =
            pairs
            ->Array.filterMap(((k, v)) =>
              if axisList->Array.includes(k) {
                let vals = Dict.getUnsafe(axisVals, k)
                if vals->Array.some(x => jsonEq(x, v)) {
                  None
                } else {
                  Some(describeMiss(axisList, axisVals, k, v))
                }
              } else {
                None
              }
            )
            ->Array.join(" ")
          let hasTyped = reasons->String.includes("but the axis")
          let detail = if reasons != "" {
            reasons
          } else if Array.length(axisList) == 0 {
            "the matrix has no axes, so every include is a job of its own."
          } else {
            "no surviving combination carries all its axis values."
          }
          findings->add(
            hasTyped ? SevWarning : SevInfo,
            jobId,
            hasTyped ? "M012" : "M008",
            `include ${Int.toString(idx)} matches no base combination, so it becomes its own job holding only its own keys: ${detail}`,
          )
          extras->Array.push({
            pairs: pairs->Array.map(((k, v)) => (k, v)),
            setBy: Dict.make(),
            origin: FromInclude(idx),
          })
        } else {
          let anyChange = ref(false)
          let overwrites: dict<int> = Dict.make()
          targets->Array.forEach(c => {
            pairs->Array.forEach(((k, v)) => {
              if !(axisList->Array.includes(k)) {
                let pos = c.pairs->Array.findIndex(((key, _)) => key == k)
                if pos < 0 {
                  c.pairs->Array.push((k, v))
                  Dict.set(c.setBy, k, idx)
                  anyChange := true
                } else {
                  let (_, old) = c.pairs->Array.getUnsafe(pos)
                  if !jsonEq(old, v) {
                    c.pairs->Array.setUnsafe(pos, (k, v))
                    anyChange := true
                    switch Dict.get(c.setBy, k) {
                    | Some(prev) =>
                      let tag = Int.toString(prev) ++ "|" ++ k
                      Dict.set(overwrites, tag, Dict.get(overwrites, tag)->Option.getOr(0) + 1)
                    | None => ()
                    }
                    Dict.set(c.setBy, k, idx)
                  }
                }
              }
            })
            Set.add(mergedInto, c)
          })
          Dict.toArray(overwrites)->Array.forEach(((tag, n)) => {
            let parts = tag->String.split("|")
            findings->add(
              SevWarning,
              jobId,
              "M009",
              `include ${Int.toString(idx)} overwrites "${parts->Array.getUnsafe(1)}" set by include ${parts->Array.getUnsafe(0)} on ${Int.toString(n)} combinations. Later includes win, so order decides the result.`,
            )
          })
          if !anyChange.contents {
            findings->add(
              SevWarning,
              jobId,
              "M007",
              `include ${Int.toString(idx)} adds nothing: every key and value it carries is already present on the ${plural(Array.length(targets), "combination", "combinations")} it matches.`,
            )
          }
        }
      })
      let combos = Array.concat(kept, extras)
      let count = Array.length(combos)
      if count == 0 {
        findings->add(
          SevError,
          jobId,
          "M005",
          "matrix expands to zero jobs. GitHub fails the workflow when a matrix is empty.",
        )
      }
      if count > maxJobs {
        findings->add(
          SevError,
          jobId,
          "M004",
          `matrix expands to ${Int.toString(count)} jobs, above the limit of ${Int.toString(maxJobs)}.`,
        )
      }
      let seen = Dict.make()
      combos->Array.forEach(c => {
        let key = comboKey(c)
        Dict.set(seen, key, Dict.get(seen, key)->Option.getOr(0) + 1)
      })
      let dupes = Dict.valuesToArray(seen)->Array.filter(n => n > 1)->Array.length
      if dupes > 0 {
        findings->add(
          SevWarning,
          jobId,
          "M015",
          `${Int.toString(dupes)} job definitions appear more than once after expansion. They run as separate jobs with the same inputs.`,
        )
      }
      Some({
        axes: axisList,
        combos,
        cartesian,
        excluded: excludedCount,
        merged: Set.size(mergedInto),
        created: Array.length(extras),
      })
    }
  }
}

// ---------- workflow walking ----------
let asObject = (j: JSON.t) =>
  switch j {
  | Object(d) => Some(d)
  | _ => None
  }

let expandWorkflow = (findings, wf: JSON.t, opts: options): array<jobResult> => {
  let selected = (id: string) =>
    switch opts.jobFilter {
    | Some(f) => f == id
    | None => true
    }
  let fromMatrix = (id, job, matrixJson: JSON.t, maxParallel) =>
    switch matrixJson {
    | Object(m) =>
      switch expandMatrix(findings, id, m, opts.maxJobs) {
      | Some(exp) => {id, job, status: Expanded(exp), maxParallel}
      | None =>
        let dyn = findings->Array.some(f => f.jobId == id && f.code == "M014")
        {id, job, status: dyn ? Dynamic : Failed, maxParallel}
      }
    | String(s) if isExpression(s) =>
      findings->add(
        SevInfo,
        id,
        "M014",
        "strategy.matrix is an expression, so it is only known at run time and cannot be expanded here.",
      )
      {id, job, status: Dynamic, maxParallel}
    | other =>
      findings->add(
        SevError,
        id,
        "M001",
        `strategy.matrix must be a map but is a ${typeName(other)}.`,
      )
      {id, job, status: Failed, maxParallel}
    }
  let bareMatrix = (j: JSON.t) => fromMatrix("matrix", Dict.make(), j, None)
  switch wf {
  | Object(root) =>
    switch Dict.get(root, "jobs") {
    | Some(Object(jobs)) =>
      Dict.toArray(jobs)->Array.filterMap(((id, jv)) =>
        if !selected(id) {
          None
        } else {
          switch jv {
          | Object(job) =>
            switch Dict.get(job, "strategy") {
            | Some(Object(strategy)) =>
              switch Dict.get(strategy, "matrix") {
              | Some(m) =>
                let mp = switch Dict.get(strategy, "max-parallel") {
                | Some(Number(n)) => Some(n)
                | _ => None
                }
                Some(fromMatrix(id, job, m, mp))
              | None => None
              }
            | Some(String(s)) if isExpression(s) =>
              findings->add(
                SevInfo,
                id,
                "M014",
                "strategy is an expression, so the matrix is only known at run time.",
              )
              Some({id, job, status: Dynamic, maxParallel: None})
            | _ => None
            }
          | _ => None
          }
        }
      )
    | _ => [bareMatrix(wf)]
    }
  | _ => [bareMatrix(wf)]
  }
}

// ---------- names and runners ----------
let matrixRefRe = RegExp.fromString("^\\$\\{\\{\\s*matrix\\.([A-Za-z0-9_-]+)\\s*\\}\\}$")
let matrixAnyRe = RegExp.fromString("\\$\\{\\{\\s*matrix\\.([A-Za-z0-9_-]+)\\s*\\}\\}", ~flags="g")

let renderName = (r: jobResult, c: combo): string => {
  let tpl = switch Dict.get(r.job, "name") {
  | Some(String(s)) => Some(s)
  | _ => None
  }
  switch tpl {
  | Some(t) =>
    t->String.replaceRegExpBy1Unsafe(matrixAnyRe, (~match as _, ~group1 as key, ~offset as _, ~input as _) =>
      switch comboGet(c, key) {
      | Some(v) => show(v)
      | None => ""
      }
    )
  | None =>
    if Array.length(c.pairs) == 0 {
      r.id
    } else {
      r.id ++ " (" ++ c.pairs->Array.map(((_, v)) => show(v))->Array.join(", ") ++ ")"
    }
  }
}

let runnerOf = (r: jobResult, c: combo): string =>
  switch Dict.get(r.job, "runs-on") {
  | Some(String(s)) =>
    switch RegExp.exec(matrixRefRe, s) {
    | Some(m) =>
      switch RegExp.Result.matches(m)->Array.get(0) {
      | Some(Some(key)) =>
        switch comboGet(c, key) {
        | Some(v) => show(v)
        | None => "unknown"
        }
      | _ => "unknown"
      }
    | None => isExpression(s) ? "expression" : s
    }
  | Some(other) => JSON.stringify(other)
  | None => "unknown"
  }

let runnerFamily = (label: string) => {
  let l = String.toLowerCase(label)
  if String.includes(l, "windows") {
    "windows"
  } else if String.includes(l, "macos") || String.includes(l, "osx") {
    "macos"
  } else if String.includes(l, "ubuntu") || String.includes(l, "linux") {
    "linux"
  } else {
    "other"
  }
}

// ---------- text output ----------
let printFindings = (findings: array<finding>, jobId: string) =>
  findings
  ->Array.filter(f => f.jobId == jobId)
  ->Array.forEach(f => Console.log(`  ${f.code} ${severityName(f.severity)}: ${f.message}`))

let weightOf = (opts: options, family: string) =>
  switch opts.weights->Array.find(((k, _)) => k == family) {
  | Some((_, w)) => w
  | None => 1.0
  }

let runnerSummary = (r: jobResult, exp: expansion): array<(string, int)> => {
  let counts = Dict.make()
  exp.combos->Array.forEach(c => {
    let fam = runnerFamily(runnerOf(r, c))
    Dict.set(counts, fam, Dict.get(counts, fam)->Option.getOr(0) + 1)
  })
  Dict.toArray(counts)->Array.toSorted(((a, _), (b, _)) => String.compare(a, b))
}

let printJob = (findings, opts: options, r: jobResult) =>
  switch r.status {
  | Dynamic =>
    Console.log(`job ${r.id}: dynamic matrix, not expanded`)
    printFindings(findings, r.id)
  | Failed =>
    Console.log(`job ${r.id}: matrix could not be expanded`)
    printFindings(findings, r.id)
  | Expanded(exp) =>
    let n = Array.length(exp.combos)
    Console.log(
      `job ${r.id}: ${plural(n, "job", "jobs")} (cartesian ${Int.toString(exp.cartesian)}, excluded ${Int.toString(exp.excluded)}, include merged into ${Int.toString(exp.merged)}, include created ${Int.toString(exp.created)})`,
    )
    let hasRunner = Dict.get(r.job, "runs-on") != None
    let summary = hasRunner ? runnerSummary(r, exp) : []
    if Array.length(summary) > 0 {
      Console.log(
        "  runners: " ++
        summary->Array.map(((fam, k)) => fam ++ " " ++ Int.toString(k))->Array.join(", "),
      )
    }
    switch opts.minutes {
    | Some(m) =>
      let total =
        summary->Array.reduce(0.0, (acc, (fam, k)) => acc +. m *. weightOf(opts, fam) *. Int.toFloat(k))
      Console.log(`  weighted minutes: ${Float.toString(total)} (at ${Float.toString(m)} per job)`)
    | None => ()
    }
    switch r.maxParallel {
    | Some(mp) if mp > Int.toFloat(n) =>
      findings->add(
        SevInfo,
        r.id,
        "M016",
        `max-parallel is ${Float.toString(mp)} but the matrix only has ${Int.toString(n)} jobs.`,
      )
    | _ => ()
    }
    printFindings(findings, r.id)
    if opts.listAll {
      exp.combos->Array.forEachWithIndex((c, i) => {
        let tag = switch c.origin {
        | FromBase => ""
        | FromInclude(k) => `  [include ${Int.toString(k)}]`
        }
        Console.log(
          `  ${Int.toString(i + 1)->String.padStart(3, " ")}. ${renderName(r, c)}${hasRunner ? "  runs-on: " ++ runnerOf(r, c) : ""}${tag}`,
        )
      })
    }
  }

// ---------- JSON output ----------
let jstr = s => JSON.String(s)
let jnum = n => JSON.Number(Int.toFloat(n))
let jobj = (pairs: array<(string, JSON.t)>) => JSON.Object(Dict.fromArray(pairs))

let findingJson = (f: finding) =>
  jobj([
    ("job", jstr(f.jobId)),
    ("code", jstr(f.code)),
    ("severity", jstr(severityName(f.severity))),
    ("message", jstr(f.message)),
  ])

let jobJson = (findings, r: jobResult) => {
  let fs = findings->Array.filter(f => f.jobId == r.id)->Array.map(findingJson)
  switch r.status {
  | Expanded(exp) =>
    jobj([
      ("id", jstr(r.id)),
      ("status", jstr("expanded")),
      ("jobs", jnum(Array.length(exp.combos))),
      ("cartesian", jnum(exp.cartesian)),
      ("excluded", jnum(exp.excluded)),
      ("includeMerged", jnum(exp.merged)),
      ("includeCreated", jnum(exp.created)),
      (
        "combinations",
        JSON.Array(
          exp.combos->Array.map(c =>
            jobj([
              ("name", jstr(renderName(r, c))),
              ("runsOn", jstr(runnerOf(r, c))),
              ("values", jobj(c.pairs)),
            ])
          ),
        ),
      ),
      ("findings", JSON.Array(fs)),
    ])
  | Dynamic => jobj([("id", jstr(r.id)), ("status", jstr("dynamic")), ("findings", JSON.Array(fs))])
  | Failed => jobj([("id", jstr(r.id)), ("status", jstr("failed")), ("findings", JSON.Array(fs))])
  }
}

// ---------- exit status ----------
let verdict = (findings: array<finding>, strict: bool) => {
  let errors = findings->Array.some(f => f.severity == SevError)
  let warnings = findings->Array.some(f => f.severity == SevWarning)
  errors || (strict && warnings)
}

// ---------- diff ----------
let namesOf = (r: jobResult): array<string> =>
  switch r.status {
  | Expanded(exp) => exp.combos->Array.map(c => renderName(r, c))
  | _ => []
  }

let diffWorkflows = (before: array<jobResult>, after: array<jobResult>, opts: options): bool => {
  let byId = (rs: array<jobResult>) => Dict.fromArray(rs->Array.map(r => (r.id, r)))
  let b = byId(before)
  let a = byId(after)
  let ids =
    Array.concat(Dict.keysToArray(b), Dict.keysToArray(a))
    ->Set.fromArray
    ->Set.values
    ->Array.fromIterator
    ->Array.toSorted(String.compare)
  let removedAny = ref(false)
  let out: array<JSON.t> = []
  ids->Array.forEach(id => {
    let nb = switch Dict.get(b, id) {
    | Some(r) => namesOf(r)
    | None => []
    }
    let na = switch Dict.get(a, id) {
    | Some(r) => namesOf(r)
    | None => []
    }
    let removed = nb->Array.filter(x => !(na->Array.includes(x)))
    let added = na->Array.filter(x => !(nb->Array.includes(x)))
    if Array.length(removed) > 0 {
      removedAny := true
    }
    if opts.asJson {
      out->Array.push(
        jobj([
          ("job", jstr(id)),
          ("before", jnum(Array.length(nb))),
          ("after", jnum(Array.length(na))),
          ("removed", JSON.Array(removed->Array.map(jstr))),
          ("added", JSON.Array(added->Array.map(jstr))),
        ]),
      )
    } else if Array.length(removed) == 0 && Array.length(added) == 0 {
      Console.log(`job ${id}: unchanged (${plural(Array.length(na), "job", "jobs")})`)
    } else {
      Console.log(
        `job ${id}: ${Int.toString(Array.length(nb))} -> ${Int.toString(Array.length(na))} jobs`,
      )
      removed->Array.forEach(x => Console.log(`  - ${x}`))
      added->Array.forEach(x => Console.log(`  + ${x}`))
      if Array.length(removed) > 0 {
        Console.log("  check names above disappear from the PR. A branch rule that requires one of them will wait forever.")
      }
    }
  })
  if opts.asJson {
    Console.log(JSON.stringify(JSON.Array(out), ~space=2))
  }
  removedAny.contents
}

// ---------- CLI ----------
let usage = `Usage:
  node ActionsMatrixExpander.bs.js expand <workflow.json> [--job ID] [--list] [--json] [--strict]
                                   [--max-jobs N] [--minutes M] [--weight windows=2,macos=10]
  node ActionsMatrixExpander.bs.js diff <before.json> <after.json> [--job ID] [--json] [--fail-on-removed]

Input is the workflow converted to JSON, for example: yq -o=json . .github/workflows/ci.yml
A file holding only a matrix map is accepted too.
Exit codes: 0 clean, 1 findings at failing severity, 2 bad usage or unreadable input.`

exception Usage(string)

let parseWeights = (s: string): array<(string, float)> =>
  s
  ->String.split(",")
  ->Array.filter(p => p != "")
  ->Array.map(p =>
    switch p->String.split("=") {
    | [k, v] =>
      let w = Float.fromString(v)
      switch w {
      | Some(x) if x >= 0.0 => (String.toLowerCase(String.trim(k)), x)
      | _ => throw(Usage(`bad weight "${p}"`))
      }
    | _ => throw(Usage(`bad weight "${p}", expected name=number`))
    }
  )

let parseArgs = (args: array<string>): (array<string>, options) => {
  let positional = []
  let opts = ref({
    maxJobs: githubMatrixLimit,
    jobFilter: None,
    asJson: false,
    listAll: false,
    strict: false,
    failOnRemoved: false,
    minutes: None,
    weights: [],
  })
  let i = ref(0)
  let n = Array.length(args)
  let value = flag => {
    i := i.contents + 1
    switch args->Array.get(i.contents) {
    | Some(v) => v
    | None => throw(Usage(`${flag} needs a value`))
    }
  }
  while i.contents < n {
    let a = args->Array.getUnsafe(i.contents)
    switch a {
    | "--json" => opts := {...opts.contents, asJson: true}
    | "--list" => opts := {...opts.contents, listAll: true}
    | "--strict" => opts := {...opts.contents, strict: true}
    | "--fail-on-removed" => opts := {...opts.contents, failOnRemoved: true}
    | "--job" => opts := {...opts.contents, jobFilter: Some(value(a))}
    | "--max-jobs" =>
      switch Int.fromString(value(a)) {
      | Some(k) if k > 0 => opts := {...opts.contents, maxJobs: k}
      | _ => throw(Usage("--max-jobs needs a positive integer"))
      }
    | "--minutes" =>
      switch Float.fromString(value(a)) {
      | Some(m) if m >= 0.0 => opts := {...opts.contents, minutes: Some(m)}
      | _ => throw(Usage("--minutes needs a non negative number"))
      }
    | "--weight" => opts := {...opts.contents, weights: parseWeights(value(a))}
    | flag if String.startsWith(flag, "--") => throw(Usage(`unknown flag ${flag}`))
    | file => positional->Array.push(file)
    }
    i := i.contents + 1
  }
  (positional, opts.contents)
}

let loadJson = (path: string): JSON.t =>
  try {
    JSON.parseOrThrow(readFileSync(path, "utf8"))
  } catch {
  | _ => throw(Usage(`cannot read ${path} as JSON. Convert YAML first, for example: yq -o=json . file.yml`))
  }

let main = () => {
  let args = argv->Array.slice(~start=2)
  try {
    let (positional, opts) = parseArgs(args)
    switch positional {
    | ["expand", file] =>
      let findings = []
      let results = expandWorkflow(findings, loadJson(file), opts)
      if Array.length(results) == 0 {
        Console.log("no strategy.matrix found")
      } else if opts.asJson {
        Console.log(
          JSON.stringify(JSON.Array(results->Array.map(r => jobJson(findings, r))), ~space=2),
        )
      } else {
        results->Array.forEach(r => printJob(findings, opts, r))
        let count = (s: severity) => findings->Array.filter(f => f.severity == s)->Array.length
        Console.log(
          `${plural(count(SevError), "error", "errors")}, ${plural(count(SevWarning), "warning", "warnings")}, ${plural(count(SevInfo), "note", "notes")}`,
        )
      }
      setExitCode(verdict(findings, opts.strict) ? 1 : 0)
    | ["diff", beforeFile, afterFile] =>
      let findings = []
      let before = expandWorkflow(findings, loadJson(beforeFile), opts)
      let after = expandWorkflow(findings, loadJson(afterFile), opts)
      let removed = diffWorkflows(before, after, opts)
      setExitCode(opts.failOnRemoved && removed ? 1 : 0)
    | _ => throw(Usage("expected: expand <file> or diff <before> <after>"))
    }
  } catch {
  | Usage(msg) =>
    Console.error(`error: ${msg}\n\n${usage}`)
    setExitCode(2)
  }
}

main()
