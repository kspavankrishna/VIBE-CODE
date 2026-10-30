#!/usr/bin/env tclsh
# Run with: tclsh TestSystemdUnitHardeningAuditor.tcl
# Exits 0 when every check passes, 1 otherwise.

package require Tcl 8.6
source [file join [file dirname [file normalize [info script]]] SystemdUnitHardeningAuditor.tcl]

set ::passed 0
set ::failed 0

proc check {name cond} {
    if {[uplevel 1 [list expr $cond]]} {
        incr ::passed
    } else {
        incr ::failed
        puts "FAIL $name"
    }
}

proc near {a b} {return [expr {abs($a - $b) < 0.01}]}

::hd::defineRules
set tmp [file join [expr {[info exists ::env(TMPDIR)] ? $::env(TMPDIR) : "/tmp"}] hdtest[pid]]
file mkdir $tmp

proc writeUnit {name text {dropin {}}} {
    global tmp
    set f [file join $tmp $name]
    set fh [open $f w]
    puts -nonewline $fh $text
    close $fh
    if {$dropin ne ""} {
        file mkdir $f.d
        set fh [open [file join $f.d 50-local.conf] w]
        puts -nonewline $fh $dropin
        close $fh
    }
    return $f
}

proc audit {name text {dropin {}}} {
    set f [writeUnit $name $text $dropin]
    return [::hd::auditUnit $name [::hd::unitFiles $f] {} {}]
}

proc finding {u id} {
    foreach f [dict get $u findings] {
        if {[dict get $f id] eq $id} {return $f}
    }
    return {}
}

proc exposure {u id} {
    set f [finding $u $id]
    if {$f eq ""} {return -1}
    return [dict get $f exposure]
}

proc hasWarn {u pattern} {
    foreach w [dict get $u warnings] {
        if {[string match $pattern $w]} {return 1}
    }
    return 0
}

# continuation, comments and reset semantics
set u [audit A.service "\[Service\]\nExecStart=/bin/x \\\n  --flag\n# note\n; other\nCapabilityBoundingSet=CAP_SYS_ADMIN\nCapabilityBoundingSet=\n"]
check "empty assignment resets caps" {[near [exposure $u CapabilityBoundingSet] 0]}
check "continuation joins lines" {[llength [dict get $u problems]] == 0}

# drop-in order and override
set u [audit B.service "\[Service\]\nNoNewPrivileges=yes\n" "\[Service\]\nNoNewPrivileges=no\n"]
check "drop-in overrides scalar" {[exposure $u NoNewPrivileges] == 1}

# DynamicUser implications
set u [audit C.service "\[Service\]\nDynamicUser=yes\n"]
check "DynamicUser implies PrivateTmp" {[exposure $u PrivateTmp] == 0}
check "DynamicUser implies strict" {[exposure $u ProtectSystem] == 0}
check "DynamicUser not root" {[exposure $u RunsAsRoot] == 0}
check "DynamicUser leaves ProtectHome partial" {[near [exposure $u ProtectHome] 0.4]}
set u [audit C2.service "\[Service\]\nDynamicUser=yes\nPrivateTmp=no\n"]
check "explicit value beats implied" {[exposure $u PrivateTmp] == 1}

# inverted capability sets
set u [audit D.service "\[Service\]\nCapabilityBoundingSet=~CAP_SYS_ADMIN\n"]
check "inverted set keeps most caps" {[exposure $u CapabilityBoundingSet] > 0.5}
set u [audit D2.service "\[Service\]\nCapabilityBoundingSet=CAP_NET_BIND_SERVICE\n"]
check "single harmless cap is small" {[exposure $u CapabilityBoundingSet] < 0.05}
set u [audit D3.service "\[Service\]\nCapabilityBoundingSet=CAP_NET_BIND_SERVICE\nAmbientCapabilities=CAP_NET_BIND_SERVICE CAP_NET_RAW\n"]
check "ambient outside bounding set warns" {[hasWarn $u "*CAP_NET_RAW*never granted*"]}
check "ambient inside bounding set is quiet" {![hasWarn $u "*CAP_NET_BIND_SERVICE but*"]}
set u [audit D4.service "\[Service\]\nCapabilityBoundingSet=CAP_FAKE\n"]
check "unknown capability is reported" {[llength [dict get $u problems]] == 1}

# syscall filter scoring
set u [audit E.service "\[Service\]\nSystemCallFilter=@system-service\nSystemCallFilter=~@privileged @resources\n"]
check "allowlist minus privileged passes" {[exposure $u SystemCallFilter] == 0}
set u [audit E2.service "\[Service\]\nSystemCallFilter=@system-service\n"]
check "bare allowlist is partial" {[near [exposure $u SystemCallFilter] 0.45]}
set u [audit E3.service "\[Service\]\nSystemCallFilter=~@mount\n"]
check "denylist only stays high" {[exposure $u SystemCallFilter] > 0.7}

# address families
set u [audit F.service "\[Service\]\nRestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\n"]
check "three families pass" {[exposure $u RestrictAddressFamilies] == 0}
set u [audit F2.service "\[Service\]\nRestrictAddressFamilies=AF_UNIX AF_PACKET\n"]
check "packet sockets are loud" {[near [exposure $u RestrictAddressFamilies] 0.5]}
set u [audit F3.service "\[Service\]\nRestrictAddressFamilies=none\n"]
check "none means closed" {[exposure $u RestrictAddressFamilies] == 0}
set u [audit F4.service "\[Service\]\nPrivateNetwork=yes\n"]
check "private network skips socket rules" {[exposure $u RestrictAddressFamilies] == 0 && [exposure $u IPAddressDeny] == 0}

# umask direction
set u [audit G.service "\[Service\]\nUMask=0077\n"]
check "0077 passes" {[exposure $u UMask] == 0}
set u [audit G2.service "\[Service\]\nUMask=0022\n"]
check "0022 fails" {[exposure $u UMask] == 1}

# exec prefixes and writable paths
set u [audit H.service "\[Service\]\nExecStart=+/bin/x\nExecStartPre=!/bin/y\n"]
check "plus on ExecStart is full exposure" {[exposure $u PrivilegedExecPrefix] == 1}
set u [audit H2.service "\[Service\]\nExecStart=-/bin/x\n"]
check "dash prefix is harmless" {[exposure $u PrivilegedExecPrefix] == 0}
set u [audit I.service "\[Service\]\nReadWritePaths=-/etc/ssh/ /var/lib/app\nBindPaths=/home:/data\n"]
check "sensitive paths found" {[string match "*/etc/ssh*" [dict get [finding $u SensitiveWritablePaths] detail]] && [string match "*BindPaths=/home*" [dict get [finding $u SensitiveWritablePaths] detail]]}
set u [audit I2.service "\[Service\]\nReadWritePaths=/var/lib/app\n"]
check "dead ReadWritePaths warns" {[hasWarn $u "ReadWritePaths= has no effect*"]}
set u [audit I3.service "\[Service\]\nReadWritePaths=/var/lib/app\nProtectSystem=strict\n"]
check "live ReadWritePaths is quiet" {![hasWarn $u "ReadWritePaths= has no effect*"]}
set u [audit I4.service "\[Service\]\nDynamicUser=yes\nReadWritePaths=/var/lib/app\n"]
check "dynamic user state path warns" {[hasWarn $u "*StateDirectory=*"]}

# environment secrets
set u [audit J.service "\[Service\]\nEnvironment=\"API_TOKEN=abc def\" TOKEN_FILE=/run/x LEVEL=debug\nEnvironment=DB_PASSWORD=\n"]
check "literal secret warns once" {[llength [lsearch -all -glob [dict get $u warnings] "Environment=*"]] == 1}

# JIT hint
set u [audit K.service "\[Service\]\nExecStart=/usr/bin/java -jar app.jar\n"]
check "JIT runtime hint" {[hasWarn $u "*JIT runtime*"]}

# suppression rules
set u [audit L.service "\[Service\]\n# hardening-ignore: PrivateTmp shared with sidecar\n# hardening-ignore: PrivateDevices\n# hardening-ignore: NoSuchRule because\n"]
check "reasoned suppression applies" {[exposure $u PrivateTmp] == -1}
check "reasonless suppression refused" {[exposure $u PrivateDevices] == 1}
check "unknown rule id reported" {[llength [dict get $u problems]] == 2}

# non service units
set f [writeUnit M.service "\[Socket\]\nListenStream=80\n"]
set u [::hd::auditUnit M.service [::hd::unitFiles $f] {} {}]
check "non service skipped" {[dict exists $u skipped]}

# score bounds
set bare [audit N.service "\[Service\]\nExecStart=/bin/x\n"]
check "bare unit is unsafe" {[dict get $bare score] > 8.0}
set root [file dirname [file normalize [info script]]]
set hard [::hd::auditUnit H.service [::hd::unitFiles [file join $root Examples HardenedApp.service]] {} {}]
check "hardened example is tight" {[dict get $hard score] < 1.0 && [dict get $hard band] eq "TIGHT"}
set mixed [::hd::auditUnit M.service [::hd::unitFiles [file join $root Examples MixedApp.service]] {} {}]
check "mixed example merges drop-in" {[exposure $mixed RestrictAddressFamilies] == 0 && [exposure $mixed CapabilityBoundingSet] > 0}

# drop-in emission
set txt [::hd::dropinText $bare safe]
check "safe tier skips risky rules" {[string match "*NoNewPrivileges=yes*" $txt] && ![string match "*DynamicUser=yes*" $txt]}
set txt [::hd::dropinText $bare risky]
check "risky tier includes root fix" {[string match "*DynamicUser=yes*" $txt]}
check "hardened emits nothing" {[::hd::dropinText $hard risky] eq "" || [string match "*ProtectHome*" [::hd::dropinText $hard risky]] == 0}

# json escaping
check "json escapes" {[::hd::jstr "a\"b\\c\nd"] eq "\"a\\\"b\\\\c\\nd\""}

# CLI gate and baseline
set bl [file join $tmp base.tsv]
set rc [::hd::main [list --write-baseline $bl [file join $root Examples]]]
check "baseline written" {$rc == 0 && [file exists $bl]}
set rc [::hd::main [list --baseline $bl --format tsv [file join $root Examples]]]
check "same tree passes baseline" {$rc == 0}
set fh [open $bl w]
puts $fh "HardenedApp.service\t0.00"
puts $fh "MixedApp.service\t1.00"
close $fh
set rc [::hd::main [list --baseline $bl --format tsv [file join $root Examples]]]
check "regression fails baseline" {$rc == 1}
check "max score fails" {[::hd::main [list --max-score 5 --format tsv [file join $root Examples]]] == 1}
check "missing path is usage error" {[::hd::main [list /nonexistent/dir]] == 2}
check "bad option is usage error" {[::hd::main [list --nope x]] == 2}

file delete -force $tmp
puts "$::passed passed, $::failed failed"
exit [expr {$::failed ? 1 : 0}]
