#!/usr/bin/env tclsh
# SloBurnRateRuleGenerator.tcl
# Turns a small SLO spec into Prometheus recording rules and multi window,
# multi burn rate alerts, and refuses specs whose alerts could never fire.
# Needs Tcl 8.6 and nothing else.

package require Tcl 8.6

namespace eval ::slo {
    variable errors {}
    variable warnings {}

    # Seconds per unit, largest first so formatDuration can peel greedily.
    variable units {w 604800 d 86400 h 3600 m 60 s 1}
    # Output skips weeks so 30d stays 30d instead of 4w2d.
    variable outUnits {d 86400 h 3600 m 60 s 1}

    variable allowedSloKeys {objective period errors total labels min_rate tiers runbook description budget_record}
    variable allowedTierKeys {fraction long short severity for}
    variable reservedLabels {alertname slo severity tier burn_rate}
    variable windowToken %WINDOW%

    # Google SRE workbook defaults: 2% of budget in 1h, 5% in 6h, 10% in 1d, 10% in 3d.
    variable defaultTiers {
        page-fast   {fraction 0.02 long 1h severity page   for 2m}
        page-slow   {fraction 0.05 long 6h severity page   for 15m}
        ticket-fast {fraction 0.10 long 1d severity ticket for 1h}
        ticket-slow {fraction 0.10 long 3d severity ticket for 3h}
    }
    # A short window below this many seconds draws a flapping warning.
    variable minShortSeconds 120
    # More distinct recorded windows than this per SLO draws a warning.
    variable maxRecordedWindows 12
}

# ---------------------------------------------------------------- diagnostics

proc ::slo::err {ctx msg} {
    variable errors
    lappend errors "$ctx: $msg"
}

proc ::slo::warn {ctx msg} {
    variable warnings
    lappend warnings "$ctx: $msg"
}

# ------------------------------------------------------------------- numbers

proc ::slo::isNumber {text} {
    # Strict decimal. Tcl's own "string is double" also accepts Inf and NaN.
    return [regexp {^[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$} $text]
}

proc ::slo::fmtNum {value} {
    # Ten significant digits hides binary noise such as 0.014400000000000001.
    return [format %.10g $value]
}

proc ::slo::parseDuration {text} {
    variable units
    set text [string trim $text]
    if {![regexp {^([0-9]+[smhdw])+$} $text]} {
        return -1
    }
    set total 0
    foreach {all n u} [regexp -all -inline {([0-9]+)([smhdw])} $text] {
        set total [expr {$total + $n * [dict get $units $u]}]
    }
    return $total
}

proc ::slo::formatDuration {seconds} {
    variable outUnits
    if {$seconds <= 0} {
        return 0s
    }
    set out ""
    foreach {u size} $outUnits {
        if {$seconds >= $size} {
            set n [expr {$seconds / $size}]
            append out $n $u
            set seconds [expr {$seconds % $size}]
        }
    }
    return $out
}

# Human readable, for the report only. Prometheus durations go through formatDuration.
proc ::slo::humanDuration {seconds} {
    if {$seconds < 90} {
        return "[format %.0f $seconds] s"
    }
    if {$seconds < 5400} {
        return "[format %.1f [expr {$seconds / 60.0}]] min"
    }
    if {$seconds < 172800} {
        return "[format %.1f [expr {$seconds / 3600.0}]] h"
    }
    return "[format %.1f [expr {$seconds / 86400.0}]] d"
}

# ------------------------------------------------------------- query checks

# Catches the typo that otherwise ships: an unclosed bracket or quote inside a
# query that Prometheus only rejects after the rule file is already deployed.
proc ::slo::checkBalanced {query} {
    set stack {}
    set quote ""
    set len [string length $query]
    for {set i 0} {$i < $len} {incr i} {
        set c [string index $query $i]
        if {$quote ne ""} {
            if {$c eq "\\" && $quote ne "`"} {
                incr i
            } elseif {$c eq $quote} {
                set quote ""
            }
            continue
        }
        switch -- $c {
            "\"" - "'" - "`" { set quote $c }
            "(" - "\[" - "\{" { lappend stack $c }
            ")" - "\]" - "\}" {
                if {![llength $stack]} {
                    return "unmatched '$c' at offset $i"
                }
                set open [lindex $stack end]
                set stack [lrange $stack 0 end-1]
                set want [string map [list ( ) \[ \] \{ \}] $open]
                if {$want ne $c} {
                    return "'$open' closed by '$c' at offset $i"
                }
            }
        }
    }
    if {$quote ne ""} {
        return "unterminated string literal"
    }
    if {[llength $stack]} {
        return "unclosed '[lindex $stack end]'"
    }
    return ""
}

proc ::slo::collapse {text} {
    return [string trim [regsub -all {\s+} $text " "]]
}

proc ::slo::yamlQuote {text} {
    # Always a double quoted scalar, so colons, braces and leading specials are safe.
    set text [string map [list "\\" "\\\\" "\"" "\\\"" "\n" "\\n" "\t" "\\t" "\r" ""] $text]
    return "\"$text\""
}

# --------------------------------------------------------------- validation

proc ::slo::resolveTiers {ctx raw period budget} {
    variable allowedTierKeys
    variable minShortSeconds
    set tiers {}
    set seen {}
    if {[catch {dict size $raw}]} {
        err $ctx "tiers must be a dict of tier name to settings"
        return {}
    }
    dict for {name t} $raw {
        set tctx "$ctx tier '$name'"
        if {![regexp {^[a-z][a-z0-9_-]*$} $name]} {
            err $tctx "name must match ^\[a-z\]\[a-z0-9_-\]*\$"
            continue
        }
        if {[catch {dict size $t}]} {
            err $tctx "settings must be a dict"
            continue
        }
        set bad 0
        dict for {k v} $t {
            if {$k ni $allowedTierKeys} {
                err $tctx "unknown key '$k' (allowed: $allowedTierKeys)"
                set bad 1
            }
        }
        foreach req {fraction long severity} {
            if {![dict exists $t $req]} {
                err $tctx "missing '$req'"
                set bad 1
            }
        }
        if {$bad} continue

        set fraction [dict get $t fraction]
        if {![isNumber $fraction] || $fraction <= 0 || $fraction > 1} {
            err $tctx "fraction must be a number above 0 and at most 1, got '$fraction'"
            continue
        }
        set long [parseDuration [dict get $t long]]
        if {$long < 0} {
            err $tctx "long window '[dict get $t long]' is not a duration like 1h or 90m"
            continue
        }
        if {$long > $period} {
            err $tctx "long window [formatDuration $long] is longer than the SLO period [formatDuration $period]"
            continue
        }
        if {[dict exists $t short]} {
            set short [parseDuration [dict get $t short]]
            if {$short < 0} {
                err $tctx "short window '[dict get $t short]' is not a duration"
                continue
            }
        } else {
            set short [expr {int(round($long / 12.0))}]
        }
        if {$short >= $long} {
            err $tctx "short window [formatDuration $short] must be smaller than long window [formatDuration $long]"
            continue
        }
        if {$short < 1} {
            err $tctx "short window rounds to zero seconds"
            continue
        }
        if {$short < $minShortSeconds} {
            warn $tctx "short window [formatDuration $short] is under 2m and will flap on a 15s or 30s scrape interval"
        }
        set sev [dict get $t severity]
        if {![regexp {^[a-zA-Z][a-zA-Z0-9_]*$} $sev]} {
            err $tctx "severity '$sev' is not a safe label value"
            continue
        }
        set for 0s
        if {[dict exists $t for]} {
            set f [parseDuration [dict get $t for]]
            if {$f < 0} {
                err $tctx "for '[dict get $t for]' is not a duration"
                continue
            }
            set for [formatDuration $f]
        }

        set burn [expr {$fraction * double($period) / $long}]
        set threshold [expr {$burn * $budget}]
        if {$threshold >= 1.0} {
            err $tctx "threshold [fmtNum $threshold] is at least 1.0, an error ratio can never reach it. Burn rate [fmtNum $burn] is above the maximum possible [fmtNum [expr {1.0 / $budget}]] for this objective. Lower the fraction, lengthen the long window or raise the objective."
            continue
        }
        if {$burn < 1.0} {
            warn $tctx "burn rate [fmtNum $burn] is below 1, so the alert fires on a pace that would not exhaust the budget inside [formatDuration $period]. Shrink the period gap or raise the fraction."
        }
        if {[lsearch -exact $seen [list $long $short $fraction]] >= 0} {
            warn $tctx "duplicates another tier's windows and fraction"
        }
        lappend seen [list $long $short $fraction]
        lappend tiers [dict create name $name fraction $fraction long $long short $short \
            severity $sev for $for burn $burn threshold $threshold]
    }
    return $tiers
}

proc ::slo::resolveSlo {name raw globalTiers} {
    variable allowedSloKeys
    variable reservedLabels
    variable windowToken
    variable defaultTiers
    set ctx "slo '$name'"
    if {![regexp {^[a-zA-Z][a-zA-Z0-9_-]*$} $name]} {
        err $ctx "name must start with a letter and use letters, digits, '_' or '-'"
        return {}
    }
    if {[catch {dict size $raw}]} {
        err $ctx "body must be a dict"
        return {}
    }
    set start [llength $::slo::errors]
    dict for {k v} $raw {
        if {$k ni $allowedSloKeys} {
            err $ctx "unknown key '$k' (allowed: $allowedSloKeys)"
        }
    }
    foreach req {objective period errors total} {
        if {![dict exists $raw $req]} {
            err $ctx "missing '$req'"
        }
    }
    if {[llength $::slo::errors] > $start} {
        return {}
    }

    set objective [dict get $raw objective]
    if {![isNumber $objective]} {
        err $ctx "objective '$objective' is not a number"
        return {}
    }
    if {$objective <= 1.0} {
        err $ctx "objective $objective looks like a ratio. Give a percentage such as 99.9"
        return {}
    }
    if {$objective >= 100.0} {
        err $ctx "objective must be below 100, a 100% SLO has no error budget"
        return {}
    }
    if {$objective < 90.0} {
        warn $ctx "objective $objective is low. Burn rate alerts on a budget this large rarely tell you anything useful"
    }
    set budget [expr {(100.0 - $objective) / 100.0}]

    set period [parseDuration [dict get $raw period]]
    if {$period < 3600} {
        err $ctx "period '[dict get $raw period]' must be a duration of at least 1h"
        return {}
    }

    set queries {}
    foreach key {errors total} {
        set q [collapse [dict get $raw $key]]
        if {$q eq ""} {
            err $ctx "'$key' query is empty"
            continue
        }
        if {[string first $windowToken $q] < 0} {
            err $ctx "'$key' query has no $windowToken placeholder, every window would read the same data"
            continue
        }
        set bal [checkBalanced $q]
        if {$bal ne ""} {
            err $ctx "'$key' query is malformed: $bal"
            continue
        }
        dict set queries $key $q
    }
    if {[dict size $queries] != 2} {
        return {}
    }

    set labels {}
    if {[dict exists $raw labels]} {
        set rawLabels [dict get $raw labels]
        if {[catch {dict size $rawLabels}]} {
            err $ctx "labels must be a dict"
            return {}
        }
        dict for {k v} $rawLabels {
            if {![regexp {^[a-zA-Z_][a-zA-Z0-9_]*$} $k] || [string match __* $k]} {
                err $ctx "label name '$k' is not a valid Prometheus label name"
            } elseif {$k in $reservedLabels} {
                err $ctx "label '$k' is reserved by the generator"
            } else {
                dict set labels $k $v
            }
        }
    }

    set minRate 0
    if {[dict exists $raw min_rate]} {
        set minRate [dict get $raw min_rate]
        if {![isNumber $minRate]} {
            err $ctx "min_rate '$minRate' is not a number"
            return {}
        }
    }

    set budgetRecord 0
    if {[dict exists $raw budget_record]} {
        if {![string is boolean -strict [dict get $raw budget_record]]} {
            err $ctx "budget_record must be a boolean"
            return {}
        }
        set budgetRecord [string is true [dict get $raw budget_record]]
    }

    if {[dict exists $raw tiers]} {
        set tierSource [dict get $raw tiers]
    } elseif {$globalTiers ne ""} {
        set tierSource $globalTiers
    } else {
        set tierSource $defaultTiers
    }
    set tiers [resolveTiers $ctx $tierSource $period $budget]
    if {![llength $tiers]} {
        err $ctx "no usable tiers, nothing would alert"
    }
    if {[llength $::slo::errors] > $start} {
        return {}
    }

    # The long window of one tier may be the short window of another, so dedupe.
    set windows {}
    foreach t $tiers {
        lappend windows [dict get $t long] [dict get $t short]
    }
    if {$budgetRecord} {
        lappend windows $period
        if {$period > 1296000} {
            warn $ctx "budget_record reads a [formatDuration $period] window, Prometheus retention must be at least that long (default is 15d)"
        }
    }
    set windows [lsort -integer -unique $windows]
    variable maxRecordedWindows
    if {[llength $windows] > $maxRecordedWindows} {
        warn $ctx "[llength $windows] distinct windows will be recorded per SLO, consider fewer tiers"
    }

    return [dict create name $name objective $objective budget $budget period $period \
        errors [dict get $queries errors] total [dict get $queries total] \
        labels $labels minRate $minRate budgetRecord $budgetRecord tiers $tiers windows $windows \
        runbook [expr {[dict exists $raw runbook] ? [dict get $raw runbook] : ""}] \
        description [expr {[dict exists $raw description] ? [dict get $raw description] : ""}]]
}

proc ::slo::loadSpec {path} {
    if {[catch {
        set fh [open $path r]
        fconfigure $fh -encoding utf-8
        set text [read $fh]
        close $fh
    } msg]} {
        err spec "cannot read '$path': $msg"
        return {}
    }
    # The spec is read as a Tcl list and never evaluated, so a hostile file
    # cannot run code. A brace imbalance surfaces here as a plain error.
    if {[catch {dict size $text} msg]} {
        err spec "'$path' is not a valid dict: $msg"
        return {}
    }
    foreach k [dict keys $text] {
        if {$k ni {slos tiers}} {
            err spec "unknown top level key '$k' (allowed: slos tiers)"
        }
    }
    if {![dict exists $text slos]} {
        err spec "top level key 'slos' is missing"
        return {}
    }
    if {[catch {dict size [dict get $text slos]}]} {
        err spec "'slos' must be a dict of name to body"
        return {}
    }
    set global ""
    if {[dict exists $text tiers]} {
        set global [dict get $text tiers]
    }
    set resolved {}
    dict for {name body} [dict get $text slos] {
        set r [resolveSlo $name $body $global]
        if {$r ne ""} {
            lappend resolved $r
        }
    }
    if {![llength [dict get $text slos]]} {
        err spec "'slos' is empty"
    }
    return $resolved
}

# ------------------------------------------------------------------ renderers

proc ::slo::labelBlock {indent pairs} {
    set out ""
    foreach {k v} $pairs {
        append out "$indent[format %s $k]: [yamlQuote $v]\n"
    }
    return $out
}

proc ::slo::selector {slo} {
    return "\{slo=[yamlQuoteRaw [dict get $slo name]]\}"
}

# Inside a PromQL selector the value uses PromQL double quotes, not YAML ones.
proc ::slo::yamlQuoteRaw {text} {
    return "\"[string map [list "\\" "\\\\" "\"" "\\\""] $text]\""
}

proc ::slo::renderRules {slos source} {
    variable windowToken
    set out "# Generated by SloBurnRateRuleGenerator.tcl from [file tail $source]. Do not edit by hand.\n"
    append out "# Regenerate after any change to the spec. The output is deterministic.\n"
    append out "groups:\n"
    foreach slo $slos {
        set name [dict get $slo name]
        set sel [selector $slo]
        set extra [dict get $slo labels]

        append out "  - name: [yamlQuote "slo:$name:recording"]\n"
        append out "    rules:\n"
        foreach w [dict get $slo windows] {
            set wd [formatDuration $w]
            set err [string map [list $windowToken $wd] [dict get $slo errors]]
            set tot [string map [list $windowToken $wd] [dict get $slo total]]
            append out "      - record: [yamlQuote slo:sli_error:ratio_rate$wd]\n"
            append out "        expr: [yamlQuote "($err) / ($tot)"]\n"
            append out "        labels:\n"
            append out "          slo: [yamlQuote $name]\n"
            append out [labelBlock "          " $extra]
        }
        if {[dict get $slo budgetRecord]} {
            set pd [formatDuration [dict get $slo period]]
            append out "      - record: [yamlQuote slo:error_budget_remaining:ratio]\n"
            append out "        expr: [yamlQuote "1 - (slo:sli_error:ratio_rate$pd$sel / [fmtNum [dict get $slo budget]])"]\n"
            append out "        labels:\n"
            append out "          slo: [yamlQuote $name]\n"
            append out [labelBlock "          " $extra]
        }

        append out "  - name: [yamlQuote "slo:$name:alerts"]\n"
        append out "    rules:\n"
        foreach t [dict get $slo tiers] {
            set thr [fmtNum [dict get $t threshold]]
            set lw [formatDuration [dict get $t long]]
            set sw [formatDuration [dict get $t short]]
            set expr "slo:sli_error:ratio_rate$lw$sel > $thr\nand slo:sli_error:ratio_rate$sw$sel > $thr"
            if {[dict get $slo minRate] > 0} {
                # Without this guard a single failed request at 3am pages somebody.
                set tot [string map [list $windowToken $lw] [dict get $slo total]]
                append expr "\nand ($tot) > [fmtNum [dict get $slo minRate]]"
            }
            set summary "SLO '$name' is burning error budget at [fmtNum [dict get $t burn]]x ([dict get $t name])"
            set desc "Error ratio is above $thr over both $lw and $sw. At this pace the [formatDuration [dict get $slo period]] budget for objective [fmtNum [dict get $slo objective]]% is gone in [humanDuration [expr {[dict get $slo period] / [dict get $t burn]}]]."
            if {[dict get $slo description] ne ""} {
                append desc " [dict get $slo description]"
            }
            append out "      - alert: ErrorBudgetBurn\n"
            append out "        expr: [yamlQuote [string map [list "\n" " "] $expr]]\n"
            if {[dict get $t for] ne "0s"} {
                append out "        for: [dict get $t for]\n"
            }
            append out "        labels:\n"
            append out "          slo: [yamlQuote $name]\n"
            append out "          severity: [yamlQuote [dict get $t severity]]\n"
            append out "          tier: [yamlQuote [dict get $t name]]\n"
            append out "          burn_rate: [yamlQuote [fmtNum [dict get $t burn]]]\n"
            append out [labelBlock "          " $extra]
            append out "        annotations:\n"
            append out "          summary: [yamlQuote $summary]\n"
            append out "          description: [yamlQuote $desc]\n"
            if {[dict get $slo runbook] ne ""} {
                append out "          runbook_url: [yamlQuote [dict get $slo runbook]]\n"
            }
        }
    }
    return $out
}

proc ::slo::renderReport {slos} {
    set out ""
    foreach slo $slos {
        set budget [dict get $slo budget]
        append out "SLO [dict get $slo name]: objective [fmtNum [dict get $slo objective]]% over [formatDuration [dict get $slo period]]\n"
        append out "  error budget: [fmtNum [expr {$budget * 100}]]% of requests, max possible burn rate [fmtNum [expr {1.0 / $budget}]]\n"
        append out "  [format {%-12s %8s %10s %7s %7s %9s %12s %12s} tier burn threshold long short severity {full outage} {budget gone}]\n"
        foreach t [dict get $slo tiers] {
            # Total outage: error ratio 1 fills the long window at rate thr per second of window.
            set detect [expr {[dict get $t threshold] * [dict get $t long]}]
            set gone [expr {[dict get $slo period] / [dict get $t burn]}]
            append out "  [format {%-12s %8s %10s %7s %7s %9s %12s %12s} \
                [dict get $t name] [fmtNum [dict get $t burn]] [fmtNum [dict get $t threshold]] \
                [formatDuration [dict get $t long]] [formatDuration [dict get $t short]] \
                [dict get $t severity] [humanDuration $detect] [humanDuration $gone]]\n"
        }
        append out "  recorded windows: [join [lmap w [dict get $slo windows] {formatDuration $w}] {, }]\n\n"
    }
    return $out
}

# ------------------------------------------------------------------------ CLI

proc ::slo::usage {} {
    return "usage: tclsh SloBurnRateRuleGenerator.tcl <spec.tcl> \[--format rules|report\] \[--out <file>\] \[--check\] \[--strict\]"
}

proc ::slo::writeAtomic {path data} {
    set tmp "$path.tmp.[pid]"
    set fh [open $tmp w]
    fconfigure $fh -encoding utf-8 -translation lf
    puts -nonewline $fh $data
    close $fh
    file rename -force $tmp $path
}

proc ::slo::main {argv} {
    variable errors
    variable warnings
    set spec ""
    set format rules
    set outFile ""
    set check 0
    set strict 0
    set i 0
    while {$i < [llength $argv]} {
        set a [lindex $argv $i]
        switch -- $a {
            --format {
                incr i
                set format [lindex $argv $i]
                if {$format ni {rules report}} {
                    puts stderr "unknown format '$format'\n[usage]"
                    return 64
                }
            }
            --out {
                incr i
                set outFile [lindex $argv $i]
                if {$outFile eq ""} {
                    puts stderr "--out needs a path\n[usage]"
                    return 64
                }
            }
            --check { set check 1 }
            --strict { set strict 1 }
            -h - --help {
                puts [usage]
                return 0
            }
            default {
                if {[string match -* $a] || $spec ne ""} {
                    puts stderr "unexpected argument '$a'\n[usage]"
                    return 64
                }
                set spec $a
            }
        }
        incr i
    }
    if {$spec eq ""} {
        puts stderr [usage]
        return 64
    }

    set slos [loadSpec $spec]
    foreach w $warnings {
        puts stderr "warning: $w"
    }
    if {[llength $errors]} {
        foreach e $errors {
            puts stderr "error: $e"
        }
        puts stderr "[llength $errors] error(s), nothing written"
        return 1
    }
    if {$strict && [llength $warnings]} {
        puts stderr "[llength $warnings] warning(s) and --strict is set, nothing written"
        return 2
    }
    if {$check} {
        puts stderr "ok: [llength $slos] SLO(s) valid"
        return 0
    }

    set body [expr {$format eq "report" ? [renderReport $slos] : [renderRules $slos $spec]}]
    if {$outFile ne ""} {
        if {[catch {writeAtomic $outFile $body} msg]} {
            puts stderr "error: cannot write '$outFile': $msg"
            return 1
        }
    } else {
        puts -nonewline $body
    }
    return 0
}

if {[info exists ::argv0] && [file normalize $::argv0] eq [file normalize [info script]]} {
    exit [::slo::main $::argv]
}
