#!/usr/bin/env tclsh
# SystemdUnitHardeningAuditor.tcl
# Audits systemd service units the way the unit really runs: main file plus
# drop-ins, empty assignments that reset lists, implied settings from
# DynamicUser=, inverted capability sets. Scores exposure, flags dead or
# conflicting sandbox settings and gates CI on a budget or a baseline.
# Needs Tcl 8.6 and nothing else.

package require Tcl 8.6

namespace eval ::hd {
    variable cfg {}
    variable sections {}
    variable ignores {}
    variable problems {}
    variable rules {}
    variable tierRank {safe 1 likely 2 risky 3}
    variable knownRuleIds {}

    variable allCaps {
        CAP_CHOWN CAP_DAC_OVERRIDE CAP_DAC_READ_SEARCH CAP_FOWNER CAP_FSETID CAP_KILL
        CAP_SETGID CAP_SETUID CAP_SETPCAP CAP_LINUX_IMMUTABLE CAP_NET_BIND_SERVICE
        CAP_NET_BROADCAST CAP_NET_ADMIN CAP_NET_RAW CAP_IPC_LOCK CAP_IPC_OWNER
        CAP_SYS_MODULE CAP_SYS_RAWIO CAP_SYS_CHROOT CAP_SYS_PTRACE CAP_SYS_PACCT
        CAP_SYS_ADMIN CAP_SYS_BOOT CAP_SYS_NICE CAP_SYS_RESOURCE CAP_SYS_TIME
        CAP_SYS_TTY_CONFIG CAP_MKNOD CAP_LEASE CAP_AUDIT_WRITE CAP_AUDIT_CONTROL
        CAP_SETFCAP CAP_MAC_OVERRIDE CAP_MAC_ADMIN CAP_SYSLOG CAP_WAKE_ALARM
        CAP_BLOCK_SUSPEND CAP_AUDIT_READ CAP_PERFMON CAP_BPF CAP_CHECKPOINT_RESTORE
    }
    # How much of the host a capability hands over. Sums are capped at 1.
    variable capWeight {
        CAP_SYS_ADMIN 0.6 CAP_SYS_MODULE 0.3 CAP_SYS_RAWIO 0.3 CAP_SYS_PTRACE 0.2
        CAP_NET_ADMIN 0.2 CAP_MAC_ADMIN 0.2 CAP_MAC_OVERRIDE 0.2 CAP_SYS_BOOT 0.15
        CAP_DAC_OVERRIDE 0.15 CAP_SETUID 0.15 CAP_BPF 0.15 CAP_NET_RAW 0.1
        CAP_DAC_READ_SEARCH 0.1 CAP_SETGID 0.1 CAP_SETPCAP 0.1 CAP_SYS_CHROOT 0.1
        CAP_SYS_TIME 0.1 CAP_PERFMON 0.1 CAP_MKNOD 0.1 CAP_LINUX_IMMUTABLE 0.1
        CAP_AUDIT_CONTROL 0.1 CAP_SYS_RESOURCE 0.1 CAP_SETFCAP 0.1 CAP_CHOWN 0.05
        CAP_FOWNER 0.05 CAP_KILL 0.05 CAP_SYS_TTY_CONFIG 0.05 CAP_SYS_NICE 0.05
        CAP_SYSLOG 0.05 CAP_CHECKPOINT_RESTORE 0.05 CAP_FSETID 0.03 CAP_SYS_PACCT 0.03
        CAP_IPC_LOCK 0.03 CAP_IPC_OWNER 0.03 CAP_AUDIT_READ 0.03 CAP_AUDIT_WRITE 0.02
        CAP_NET_BIND_SERVICE 0.02 CAP_NET_BROADCAST 0.02 CAP_LEASE 0.02
        CAP_WAKE_ALARM 0.02 CAP_BLOCK_SUSPEND 0.02
    }
    variable allFamilies {
        AF_UNIX AF_INET AF_INET6 AF_NETLINK AF_PACKET AF_BLUETOOTH AF_VSOCK AF_ALG
        AF_KEY AF_XDP AF_CAN AF_TIPC AF_RDS AF_NFC AF_IB AF_MPLS AF_RXRPC AF_ISDN
    }
    variable allNamespaces {cgroup ipc net mnt pid user uts}
    variable nsWeight {user 0.4 mnt 0.25 net 0.15 pid 0.1 ipc 0.05 uts 0.05 cgroup 0.05}

    variable jitRe {^(node(js)?|deno|bun|java|dotnet|mono|php-fpm[0-9.]*|luajit|pypy3?|chrome|chromium|firefox|electron|qemu-.*)$}
    variable secretNameRe {(?i)(PASS(WORD|WD)?|SECRET|TOKEN|API_?KEY|PRIVATE_?KEY|CREDENTIAL)}
    variable deprecated {
        PermissionsStartOnly {use the + prefix on the Exec line instead, or drop it}
        MemoryLimit {renamed to MemoryMax=}
        CPUShares {renamed to CPUWeight=}
    }
}

# ------------------------------------------------------------------ parsing

proc ::hd::problem {msg} {
    variable problems
    if {$msg ni $problems} {lappend problems $msg}
}

proc ::hd::words {s} {
    return [regexp -all -inline {\S+} $s]
}

# Shell style split with single and double quotes, as used by Environment=.
proc ::hd::splitQuoted {s} {
    set out {}
    set cur ""
    set q ""
    set has 0
    foreach ch [split $s ""] {
        if {$q ne ""} {
            if {$ch eq $q} {set q ""} else {append cur $ch}
            continue
        }
        if {$ch eq "\"" || $ch eq "'"} {
            set q $ch
            set has 1
            continue
        }
        if {[string is space $ch]} {
            if {$cur ne "" || $has} {lappend out $cur}
            set cur ""
            set has 0
            continue
        }
        append cur $ch
    }
    if {$cur ne "" || $has} {lappend out $cur}
    return $out
}

proc ::hd::applyStatement {stmt section origin lineno} {
    variable cfg
    if {![regexp {^([A-Za-z0-9_-]+)\s*=\s*(.*)$} $stmt -> key val]} {
        problem "$origin:$lineno: not a Key=Value line, systemd would ignore it"
        return
    }
    if {$section eq ""} {
        problem "$origin:$lineno: $key= appears before any section header"
        return
    }
    set k $section/$key
    # An empty assignment resets everything set so far, drop-ins rely on this.
    if {$val eq ""} {
        dict set cfg $k {}
    } else {
        dict lappend cfg $k $val
    }
}

proc ::hd::parseText {text origin} {
    variable sections
    variable ignores
    set section ""
    set acc ""
    set cont 0
    set lineno 0
    set start 0
    foreach raw [split $text \n] {
        incr lineno
        set line [string trim $raw " \t\r"]
        if {[regexp {^[#;]} $line]} {
            if {[regexp {^#\s*hardening-ignore:\s*(\S+)\s*(.*)$} $line -> rid why]} {
                lappend ignores [list $rid [string trim $why] $origin $lineno]
            }
            continue
        }
        if {$line eq "" && !$cont} continue
        if {!$cont} {set start $lineno}
        if {$line ne "" && [string index $line end] eq "\\"} {
            append acc [string range $line 0 end-1] " "
            set cont 1
            continue
        }
        append acc $line
        set cont 0
        set stmt [string trim $acc]
        set acc ""
        if {$stmt eq ""} continue
        if {[regexp {^\[([^\]]+)\]$} $stmt -> sec]} {
            set section $sec
            dict set sections $sec 1
            continue
        }
        applyStatement $stmt $section $origin $start
    }
    if {$acc ne ""} {
        problem "$origin:$start: line continuation runs off the end of the file"
        applyStatement [string trim $acc] $section $origin $start
    }
}

proc ::hd::readFile {path} {
    set fh [open $path r]
    fconfigure $fh -encoding utf-8
    set data [read $fh]
    close $fh
    return $data
}

# ---------------------------------------------------------------- accessors

proc ::hd::isSet {sec key} {
    variable cfg
    return [dict exists $cfg $sec/$key]
}

proc ::hd::vals {sec key} {
    variable cfg
    if {![dict exists $cfg $sec/$key]} {return {}}
    return [dict get $cfg $sec/$key]
}

proc ::hd::get {sec key} {
    set v [vals $sec $key]
    if {[llength $v] == 0} {return ""}
    return [lindex $v end]
}

# Returns 1, 0 or the default when unset or empty. Bad values are reported.
proc ::hd::bool {sec key default} {
    set v [string tolower [get $sec $key]]
    if {$v eq ""} {return $default}
    if {$v in {1 yes true on y t}} {return 1}
    if {$v in {0 no false off n f}} {return 0}
    problem "$sec/$key=$v is not a boolean, systemd ignores the line"
    return $default
}

proc ::hd::dynamicUser {} {
    return [bool Service DynamicUser 0]
}

# DynamicUser=yes switches on a few settings by itself, an explicit value wins.
proc ::hd::effBool {key} {
    set v [bool Service $key {}]
    if {$v ne ""} {return $v}
    if {[dynamicUser] && $key in {PrivateTmp RemoveIPC RestrictSUIDSGID}} {return 1}
    return 0
}

proc ::hd::effProtect {key implied} {
    set v [string tolower [get Service $key]]
    if {$v eq ""} {
        return [expr {[dynamicUser] ? $implied : "no"}]
    }
    if {$v in {1 yes true on}} {return yes}
    if {$v in {0 no false off}} {return no}
    return $v
}

# Evaluates list directives with the systemd rules: lines add up, a line that
# starts with ~ removes, and the first line decides the starting point.
# Returns the word unset when the directive is absent.
proc ::hd::setEval {key universe normalizer} {
    variable cfg
    if {![dict exists $cfg Service/$key]} {return unset}
    set allowed {}
    set first 1
    foreach line [dict get $cfg Service/$key] {
        set toks [words $line]
        set inv 0
        if {[llength $toks] && [string index [lindex $toks 0] 0] eq "~"} {
            set inv 1
            lset toks 0 [string range [lindex $toks 0] 1 end]
        }
        set norm {}
        foreach t $toks {
            set n [$normalizer $t]
            if {$n ne ""} {lappend norm $n}
        }
        if {$inv} {
            if {$first} {set allowed $universe}
            set keep {}
            foreach x $allowed {
                if {$x ni $norm} {lappend keep $x}
            }
            set allowed $keep
        } else {
            foreach n $norm {
                if {$n ni $allowed} {lappend allowed $n}
            }
        }
        set first 0
    }
    return $allowed
}

proc ::hd::capNorm {t} {
    set u [string toupper $t]
    if {$u eq ""} {return ""}
    if {![string match CAP_* $u]} {set u CAP_$u}
    variable allCaps
    if {$u ni $allCaps} {
        problem "unknown capability $t, systemd would reject the line"
        return ""
    }
    return $u
}

proc ::hd::afNorm {t} {
    set u [string toupper $t]
    if {$u in {NONE ""}} {return ""}
    return $u
}

proc ::hd::nsNorm {t} {
    return [string tolower $t]
}

# -------------------------------------------------------------- rule plumbing

proc ::hd::ok {} {return [list 0 ""]}
proc ::hd::bad {detail} {return [list 1 $detail]}
proc ::hd::part {x detail} {return [list $x $detail]}
proc ::hd::na {detail} {return [list na $detail]}

proc ::hd::defRule {id weight tier title why check fix} {
    variable rules
    variable knownRuleIds
    # Real procs, so return inside a check behaves like a return.
    proc ::hd::check_$id {} $check
    proc ::hd::fix_$id {} $fix
    lappend rules [dict create id $id weight $weight tier $tier title $title why $why]
    lappend knownRuleIds $id
}

proc ::hd::boolRule {id key weight tier title why} {
    defRule $id $weight $tier $title $why \
        [format {if {[effBool %s]} {return [ok]}; return [bad "%s= is not enabled"]} $key $key] \
        [format {list "%s=yes"} $key]
}

proc ::hd::capsExposure {allowed} {
    variable capWeight
    set sum 0.0
    set loud {}
    foreach c $allowed {
        set w 0.02
        if {[dict exists $capWeight $c]} {set w [dict get $capWeight $c]}
        set sum [expr {$sum + $w}]
        if {$w >= 0.1} {lappend loud $c}
    }
    return [list [expr {min(1.0, $sum)}] $loud]
}

# ------------------------------------------------------------------- rules

proc ::hd::defineRules {} {
    defRule RunsAsRoot 3.0 risky "Service runs as root" \
        "Without User= or DynamicUser= every bug in the service is a root bug." \
        {
            if {[dynamicUser]} {return [ok]}
            set u [get Service User]
            if {$u eq "" || $u eq "root" || $u eq "0"} {
                return [bad "no User= and no DynamicUser=, so the service runs as root"]
            }
            return [ok]
        } \
        {list "DynamicUser=yes"}

    boolRule NoNewPrivileges NoNewPrivileges 2.0 safe \
        "NoNewPrivileges is off" \
        "Stops setuid binaries and file capabilities from raising privilege after exec."
    boolRule PrivateTmp PrivateTmp 2.0 likely \
        "Shared /tmp and /var/tmp" \
        "A private tmp stops symlink races and leaks through the shared directories."
    boolRule PrivateDevices PrivateDevices 2.0 likely \
        "Real /dev is visible" \
        "Hides raw disks and most device nodes. Needs a service that does not talk to hardware."
    boolRule ProtectKernelTunables ProtectKernelTunables 1.5 safe \
        "Kernel tunables are writable" \
        "Makes /proc/sys and /sys read only."
    boolRule ProtectKernelModules ProtectKernelModules 2.0 safe \
        "Service can load kernel modules" \
        "Removes module loading, the quickest way from root service to kernel code."
    boolRule ProtectKernelLogs ProtectKernelLogs 1.0 safe \
        "Kernel log buffer is readable" \
        "Blocks /dev/kmsg and syslog(2), which leak addresses and hardware details."
    boolRule ProtectControlGroups ProtectControlGroups 1.5 safe \
        "cgroup tree is writable" \
        "Makes /sys/fs/cgroup read only, closes the release agent escape."
    boolRule ProtectClock ProtectClock 1.0 safe \
        "System clock can be changed" \
        "Denies clock writes and the RTC device."
    boolRule ProtectHostname ProtectHostname 0.5 safe \
        "Hostname can be changed" \
        "Gives the service its own UTS namespace."
    boolRule RestrictSUIDSGID RestrictSUIDSGID 1.5 safe \
        "Service can create setuid or setgid files" \
        "Blocks planting a setuid binary for later."
    boolRule LockPersonality LockPersonality 1.0 safe \
        "Execution domain can be changed" \
        "Blocks personality(2), which old exploits use to switch memory layouts."
    boolRule RestrictRealtime RestrictRealtime 1.0 safe \
        "Realtime scheduling allowed" \
        "A runaway realtime thread can starve the whole host."
    boolRule RemoveIPC RemoveIPC 0.5 safe \
        "SysV IPC objects survive the service" \
        "Cleans shared memory and semaphores when the service stops."
    boolRule MemoryDenyWriteExecute MemoryDenyWriteExecute 2.0 risky \
        "Writable and executable memory allowed" \
        "Kills a whole class of shellcode tricks. Breaks JIT runtimes, so test first."

    defRule ProtectSystem 3.0 likely "Filesystem is writable" \
        "strict makes the whole tree read only except what you list." \
        {
            switch -- [effProtect ProtectSystem strict] {
                strict {return [ok]}
                full {return [part 0.35 "ProtectSystem=full leaves /etc writable"]}
                yes {return [part 0.6 "ProtectSystem=yes leaves /etc and everything outside /usr and /boot writable"]}
                default {return [bad "ProtectSystem= is off"]}
            }
        } \
        {list "# check what the service writes, add StateDirectory= or ReadWritePaths=" "ProtectSystem=strict"}

    defRule ProtectHome 2.0 safe "Home directories are readable" \
        "Hides /home, /root and /run/user from the service." \
        {
            switch -- [effProtect ProtectHome read-only] {
                yes - tmpfs {return [ok]}
                read-only {return [part 0.4 "ProtectHome=read-only still lets the service read every home directory"]}
                default {return [bad "ProtectHome= is off"]}
            }
        } \
        {list "ProtectHome=yes"}

    defRule ProtectProc 1.0 likely "Other processes are visible in /proc" \
        "invisible hides other users processes. Needs kernel 5.8 or newer." \
        {
            switch -- [string tolower [get Service ProtectProc]] {
                invisible - noaccess {return [ok]}
                ptraceable {return [part 0.5 "ProtectProc=ptraceable still shows processes the service could ptrace"]}
                default {return [bad "ProtectProc= is default, all processes are visible"]}
            }
        } \
        {list "ProtectProc=invisible"}

    defRule ProcSubset 0.5 likely "Full /proc is mounted" \
        "pid hides the non process files of /proc." \
        {
            if {[string tolower [get Service ProcSubset]] eq "pid"} {return [ok]}
            return [bad "ProcSubset=pid is not set"]
        } \
        {list "ProcSubset=pid"}

    defRule PrivateUsers 1.0 risky "No user namespace" \
        "Maps the service into a private user namespace. Breaks anything that needs real uids." \
        {
            if {[effBool PrivateUsers]} {return [ok]}
            return [bad "PrivateUsers= is not enabled"]
        } \
        {list "PrivateUsers=yes"}

    defRule CapabilityBoundingSet 3.0 risky "Capability bounding set is open" \
        "An empty set is the goal, then add back only what the service proves it needs." \
        {
            variable allCaps
            set allowed [setEval CapabilityBoundingSet $allCaps capNorm]
            if {$allowed eq "unset"} {
                return [bad "no CapabilityBoundingSet=, all [llength $allCaps] capabilities stay available"]
            }
            lassign [capsExposure $allowed] x loud
            if {$x == 0} {return [ok]}
            if {[llength $loud]} {
                return [part $x "still allows [join $loud {, }]"]
            }
            return [part $x "still allows [llength $allowed] capabilities"]
        } \
        {
            set lines [list "CapabilityBoundingSet="]
            set amb [setEval AmbientCapabilities $allCaps capNorm]
            if {$amb ne "unset" && [llength $amb]} {
                lappend lines "CapabilityBoundingSet=[join $amb { }]"
            }
            return $lines
        }

    defRule SystemCallFilter 3.0 likely "No syscall filter" \
        "A filter turns kernel exploits that need odd syscalls into a clean EPERM." \
        {
            if {![isSet Service SystemCallFilter]} {
                return [bad "no SystemCallFilter=, the full syscall table is reachable"]
            }
            set pos {}
            set neg {}
            foreach line [vals Service SystemCallFilter] {
                set toks [words $line]
                set inv 0
                if {[llength $toks] && [string index [lindex $toks 0] 0] eq "~"} {
                    set inv 1
                    lset toks 0 [string range [lindex $toks 0] 1 end]
                }
                foreach t $toks {
                    regsub {:[A-Za-z0-9]+$} $t {} t
                    if {$inv} {lappend neg $t} else {lappend pos $t}
                }
            }
            set loudGroups {@privileged @mount @module @raw-io @reboot @swap @debug @cpu-emulation @obsolete @clock}
            if {[llength $pos]} {
                set x 0.45
                if {"@privileged" in $neg} {set x [expr {$x - 0.2}]}
                if {"@resources" in $neg} {set x [expr {$x - 0.1}]}
                foreach g $loudGroups {
                    if {$g in $pos} {set x [expr {$x + 0.1}]}
                }
                set what "allowlist"
            } else {
                set x 0.85
                if {"@privileged" in $neg} {set x [expr {$x - 0.15}]}
                if {"@resources" in $neg} {set x [expr {$x - 0.1}]}
                set minor 0.0
                foreach g {@mount @module @raw-io @reboot @swap @debug @cpu-emulation @obsolete @clock} {
                    if {$g in $neg} {set minor [expr {$minor + 0.05}]}
                }
                set x [expr {$x - min(0.25, $minor)}]
                set what "denylist only"
            }
            set x [expr {max(0.05, min(1.0, $x))}]
            if {$x <= 0.25} {return [ok]}
            return [part $x "$what, @privileged or @resources not removed or risky groups allowed"]
        } \
        {list "SystemCallFilter=" "SystemCallFilter=@system-service" "SystemCallFilter=~@privileged @resources"}

    defRule SystemCallArchitectures 1.0 safe "Foreign syscall ABIs reachable" \
        "native blocks the 32 bit compat table on x86_64, a classic filter bypass." \
        {
            if {[string tolower [get Service SystemCallArchitectures]] eq "native"} {return [ok]}
            return [bad "SystemCallArchitectures=native is not set"]
        } \
        {list "SystemCallArchitectures=native"}

    defRule RestrictAddressFamilies 2.0 likely "All socket families allowed" \
        "Most services need unix, inet and inet6 and nothing else." \
        {
            if {[effBool PrivateNetwork]} {return [ok]}
            variable allFamilies
            set allowed [setEval RestrictAddressFamilies $allFamilies afNorm]
            if {$allowed eq "unset"} {
                return [bad "no RestrictAddressFamilies=, raw packet and netlink sockets are open"]
            }
            set x 0.0
            set extra {}
            foreach f $allowed {
                if {$f in {AF_UNIX AF_INET AF_INET6}} continue
                lappend extra $f
                set x [expr {$x + ($f eq "AF_PACKET" ? 0.5 : $f eq "AF_NETLINK" ? 0.2 : 0.1)}]
            }
            set x [expr {min(1.0, $x)}]
            if {$x == 0} {return [ok]}
            return [part $x "also allows [join $extra {, }]"]
        } \
        {list "RestrictAddressFamilies=" "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6"}

    defRule IPAddressDeny 2.5 risky "Network is not firewalled per service" \
        "IPAddressDeny=any plus a short allow list is the cheapest egress control there is." \
        {
            if {[effBool PrivateNetwork]} {return [ok]}
            set toks {}
            foreach line [vals Service IPAddressDeny] {lappend toks {*}[words $line]}
            if {"any" in $toks || ({0.0.0.0/0} in $toks && {::/0} in $toks)} {return [ok]}
            if {[llength $toks]} {return [part 0.5 "IPAddressDeny= is set but does not deny everything"]}
            return [part 0.7 "no IPAddressDeny=, the service can reach any address"]
        } \
        {list "IPAddressDeny=any" "# add IPAddressAllow= for the peers this service really talks to"}

    defRule RestrictNamespaces 2.0 likely "Service can create namespaces" \
        "Namespace creation is the first step of most container escapes." \
        {
            variable allNamespaces
            variable nsWeight
            set raw [string tolower [get Service RestrictNamespaces]]
            if {$raw in {yes true 1 on}} {return [ok]}
            if {$raw in {no false 0 off} || ![isSet Service RestrictNamespaces]} {
                return [bad "RestrictNamespaces= is not set, every namespace type is allowed"]
            }
            set allowed [setEval RestrictNamespaces $allNamespaces nsNorm]
            set x 0.0
            foreach n $allowed {
                if {[dict exists $nsWeight $n]} {set x [expr {$x + [dict get $nsWeight $n]}]}
            }
            set x [expr {min(1.0, $x)}]
            if {$x == 0} {return [ok]}
            return [part $x "allows [join $allowed {, }]"]
        } \
        {list "RestrictNamespaces=yes"}

    defRule DevicePolicy 1.0 likely "Device access policy is auto" \
        "closed allows only the pseudo devices, strict allows nothing but DeviceAllow=." \
        {
            if {[effBool PrivateDevices]} {return [ok]}
            if {[string tolower [get Service DevicePolicy]] in {closed strict}} {return [ok]}
            return [bad "DevicePolicy= is auto, any device node can be opened"]
        } \
        {list "DevicePolicy=closed"}

    defRule UMask 0.5 likely "Files are created world readable" \
        "A umask of 0027 or tighter keeps new files away from other users." \
        {
            set u [get Service UMask]
            if {$u eq ""} {return [bad "no UMask=, the default 0022 makes new files world readable"]}
            if {![regexp {^0?[0-7]{3}$} $u]} {return [ok]}
            scan $u %o v
            if {!($v & 4)} {return [bad "UMask=$u leaves other users read access"]}
            return [ok]
        } \
        {list "UMask=0027"}

    defRule Delegate 1.0 likely "cgroup subtree is delegated" \
        "Delegate= hands cgroup control to the service, which is a lot of trust." \
        {
            if {[bool Service Delegate 0]} {return [bad "Delegate=yes gives the service its own cgroup subtree"]}
            if {[get Service Delegate] ne "" && ![string is boolean -strict [get Service Delegate]]} {
                return [part 0.6 "Delegate= hands controllers [get Service Delegate] to the service"]
            }
            return [ok]
        } \
        {list "Delegate=no"}

    defRule SensitiveWritablePaths 2.0 safe "Sensitive host paths are writable or bind mounted" \
        "ReadWritePaths=/etc and friends undo ProtectSystem= for exactly the paths that matter." \
        {
            set hits {}
            foreach key {ReadWritePaths ReadWriteDirectories BindPaths} {
                foreach line [vals Service $key] {
                    foreach p [words $line] {
                        if {$key eq "BindPaths"} {set p [lindex [split $p :] 0]}
                        set p [string trimleft $p -+!]
                        if {$p ne "/"} {set p [string trimright $p /]}
                        if {[isSensitivePath $p]} {lappend hits "$key=$p"}
                    }
                }
            }
            if {[llength $hits]} {return [bad "writable: [join $hits {, }]"]}
            return [ok]
        } \
        {list "# narrow ReadWritePaths= and BindPaths= to the service's own directory"}

    defRule PrivilegedExecPrefix 1.5 likely "Exec line runs outside the sandbox" \
        "A + or ! prefix runs that command with full privileges and skips the sandbox settings." \
        {
            set worst 0.0
            set hits {}
            foreach key {ExecStart ExecStartPre ExecStartPost ExecReload ExecStop ExecStopPost} {
                foreach line [vals Service $key] {
                    regexp {^([@\-:+!]*)} $line -> prefix
                    if {[string first + $prefix] >= 0} {
                        lappend hits "$key with +"
                        set worst [expr {max($worst, $key eq "ExecStart" ? 1.0 : 0.5)}]
                    } elseif {[string first ! $prefix] >= 0} {
                        lappend hits "$key with !"
                        set worst [expr {max($worst, 0.3)}]
                    }
                }
            }
            if {$worst == 0} {return [ok]}
            return [part $worst [join $hits {, }]]
        } \
        {list "# drop the + prefix, use StateDirectory= or RuntimeDirectory= instead of a root chown"}
}

proc ::hd::isSensitivePath {p} {
    if {$p in {/ /usr /etc /boot /efi /proc /sys /dev /bin /sbin /lib /lib64 /root /var /home /run}} {return 1}
    if {[regexp {^/(usr|boot|proc|sys|dev|bin|sbin|lib|lib64)/} $p]} {return 1}
    if {[regexp {^/etc/(systemd|ssh|sudoers|sudoers\.d|pam\.d|cron[^/]*|ld\.so[^/]*|passwd|shadow)(/|$)} $p]} {return 1}
    return 0
}

# ---------------------------------------------------------- warnings, no score

proc ::hd::collectWarnings {} {
    variable deprecated
    variable jitRe
    variable secretNameRe
    variable allCaps
    set w {}

    foreach {key why} $deprecated {
        if {[isSet Service $key]} {lappend w "$key= is deprecated: $why"}
    }

    set amb [setEval AmbientCapabilities $allCaps capNorm]
    set bound [setEval CapabilityBoundingSet $allCaps capNorm]
    if {$amb ne "unset" && $bound ne "unset"} {
        set dead {}
        foreach c $amb {
            if {$c ni $bound} {lappend dead $c}
        }
        if {[llength $dead]} {
            lappend w "AmbientCapabilities= lists [join $dead {, }] but CapabilityBoundingSet= removes it, so it is never granted"
        }
    }
    if {$bound ne "unset"} {
        if {[effBool PrivateDevices] && ("CAP_SYS_RAWIO" in $bound || "CAP_MKNOD" in $bound)} {
            lappend w "PrivateDevices=yes makes CAP_SYS_RAWIO and CAP_MKNOD useless, drop them from the bounding set"
        }
    }

    set rw {}
    foreach key {ReadWritePaths ReadWriteDirectories} {
        foreach line [vals Service $key] {lappend rw {*}[words $line]}
    }
    if {[llength $rw]
        && [effProtect ProtectSystem strict] eq "no"
        && [effProtect ProtectHome read-only] eq "no"
        && ![isSet Service ReadOnlyPaths] && ![isSet Service InaccessiblePaths]
        && ![isSet Service TemporaryFileSystem]} {
        lappend w "ReadWritePaths= has no effect, nothing is read only (ProtectSystem= and ProtectHome= are off)"
    }
    if {[dynamicUser]} {
        foreach p $rw {
            if {[regexp {^[-+!]*/var/lib/} $p]} {
                lappend w "DynamicUser=yes with ReadWritePaths=$p: the uid changes between restarts, use StateDirectory= so ownership follows it"
            }
        }
    }
    if {[effBool PrivateNetwork] && [isSet Service RestrictAddressFamilies]} {
        lappend w "RestrictAddressFamilies= is redundant with PrivateNetwork=yes"
    }

    foreach line [vals Service Environment] {
        foreach tok [splitQuoted $line] {
            if {![regexp {^([A-Za-z_][A-Za-z0-9_]*)=(.+)$} $tok -> name value]} continue
            if {[regexp {_(FILE|PATH|DIR)$} $name]} continue
            if {[string index $value 0] eq "/"} continue
            if {[regexp $secretNameRe $name]} {
                lappend w "Environment=$name holds a literal value, it shows up in systemctl show and /proc, use LoadCredential= or EnvironmentFile="
            }
        }
    }

    if {![effBool MemoryDenyWriteExecute]} {
        set cmd [lindex [vals Service ExecStart] 0]
        regsub {^[@\-:+!]*} $cmd {} cmd
        set toks [lrange [words $cmd] 0 1]
        foreach t $toks {
            if {[regexp $jitRe [file tail $t]]} {
                lappend w "ExecStart runs [file tail $t], a JIT runtime: MemoryDenyWriteExecute=yes will very likely crash it, leave that rule suppressed"
                break
            }
        }
    }
    return $w
}

# --------------------------------------------------------------- audit core

proc ::hd::bandFor {score} {
    if {$score <= 2.0} {return TIGHT}
    if {$score <= 4.5} {return OK}
    if {$score <= 7.0} {return EXPOSED}
    return UNSAFE
}

proc ::hd::auditUnit {name files globals cliIgnores} {
    variable cfg
    variable sections
    variable ignores
    variable problems
    variable rules
    variable knownRuleIds
    set cfg {}
    set sections {}
    set ignores {}
    set problems {}

    foreach f [concat $globals $files] {
        if {[catch {parseText [readFile $f] $f} err]} {
            return [dict create name $name skipped "cannot read $f: $err"]
        }
    }
    if {![dict exists $sections Service]} {
        return [dict create name $name skipped "no \[Service\] section (socket, timer, target or mount unit)"]
    }

    set suppress {}
    set suppressed {}
    foreach ig $ignores {
        lassign $ig rid why origin lineno
        if {$rid ni $knownRuleIds} {
            problem "$origin:$lineno: hardening-ignore names unknown rule $rid"
            continue
        }
        if {[string length $why] < 3} {
            problem "$origin:$lineno: hardening-ignore for $rid has no reason, suppression refused"
            continue
        }
        dict set suppress $rid $why
    }
    foreach rid $cliIgnores {
        if {$rid ni $knownRuleIds} {
            problem "--ignore names unknown rule $rid"
            continue
        }
        if {![dict exists $suppress $rid]} {dict set suppress $rid "--ignore on the command line"}
    }

    set findings {}
    set sumW 0.0
    set sumE 0.0
    foreach r $rules {
        set id [dict get $r id]
        if {[dict exists $suppress $id]} {
            lappend suppressed [dict create id $id reason [dict get $suppress $id]]
            continue
        }
        if {[catch {check_$id} res]} {
            problem "internal error in rule $id: $res"
            continue
        }
        lassign $res x detail
        if {$x eq "na"} continue
        set w [dict get $r weight]
        set sumW [expr {$sumW + $w}]
        set sumE [expr {$sumE + $w * $x}]
        set fix {}
        if {$x > 0} {
            if {[catch {fix_$id} fix]} {set fix {}}
        }
        lappend findings [dict create id $id title [dict get $r title] why [dict get $r why] \
            weight $w tier [dict get $r tier] exposure $x detail $detail fix $fix]
    }
    set score [expr {$sumW > 0 ? 10.0 * $sumE / $sumW : 0.0}]
    return [dict create name $name score $score band [bandFor $score] findings $findings \
        warnings [collectWarnings] problems $problems suppressed $suppressed files $files]
}

# ---------------------------------------------------------------- discovery

proc ::hd::walk {dir} {
    set found {}
    foreach f [lsort [glob -nocomplain -directory $dir -types {f l} *.service]] {lappend found $f}
    foreach d [lsort [glob -nocomplain -directory $dir -types d *]] {
        lappend found {*}[walk $d]
    }
    return $found
}

# Returns {unitFile ...} and a list of notes about masked units and aliases.
proc ::hd::collect {paths} {
    set units {}
    set notes {}
    foreach p $paths {
        if {[file isdirectory $p]} {
            set cands [walk $p]
        } elseif {[file exists $p]} {
            set cands [list $p]
        } else {
            return -code error "no such file or directory: $p"
        }
        foreach f $cands {
            if {[file type $f] eq "link"} {
                set tgt [file readlink $f]
                if {$tgt eq "/dev/null"} {
                    lappend notes "$f: masked unit, skipped"
                    continue
                }
                if {[string match *.service $tgt]} {
                    lappend notes "$f: alias of $tgt, skipped"
                    continue
                }
            }
            if {$f ni $units} {lappend units $f}
        }
    }
    return [dict create units $units notes $notes]
}

proc ::hd::unitFiles {unitFile} {
    set files [list $unitFile]
    set dd $unitFile.d
    if {[file isdirectory $dd]} {
        foreach f [lsort [glob -nocomplain -directory $dd -types {f l} *.conf]] {lappend files $f}
    }
    return $files
}

# ------------------------------------------------------------------- output

proc ::hd::jstr {s} {
    return \"[string map [list \\ \\\\ \" \\\" \n \\n \r \\r \t \\t] $s]\"
}

proc ::hd::jlist {items} {
    return \[[join $items ,]\]
}

proc ::hd::unitJson {u} {
    if {[dict exists $u skipped]} {
        return "\{\"unit\":[jstr [dict get $u name]],\"skipped\":[jstr [dict get $u skipped]]\}"
    }
    set fs {}
    foreach f [dict get $u findings] {
        if {[dict get $f exposure] == 0} continue
        lappend fs [format {{"id":%s,"title":%s,"tier":%s,"weight":%.2f,"exposure":%.2f,"detail":%s,"fix":%s}} \
            [jstr [dict get $f id]] [jstr [dict get $f title]] [jstr [dict get $f tier]] \
            [dict get $f weight] [dict get $f exposure] [jstr [dict get $f detail]] \
            [jlist [lmap l [dict get $f fix] {jstr $l}]]]
    }
    set sup [lmap s [dict get $u suppressed] {format {{"id":%s,"reason":%s}} [jstr [dict get $s id]] [jstr [dict get $s reason]]}]
    return [format {{"unit":%s,"score":%.2f,"band":%s,"findings":%s,"warnings":%s,"problems":%s,"suppressed":%s}} \
        [jstr [dict get $u name]] [dict get $u score] [jstr [dict get $u band]] [jlist $fs] \
        [jlist [lmap w [dict get $u warnings] {jstr $w}]] \
        [jlist [lmap w [dict get $u problems] {jstr $w}]] [jlist $sup]]
}

proc ::hd::unitText {u verbose} {
    set out {}
    if {[dict exists $u skipped]} {
        return "[dict get $u name]  skipped: [dict get $u skipped]\n"
    }
    append out [format "%s  exposure %.2f  %s  (%d suppressed)\n" [dict get $u name] \
        [dict get $u score] [dict get $u band] [llength [dict get $u suppressed]]]
    set fs [lsort -decreasing -real -command ::hd::byImpact [dict get $u findings]]
    foreach f $fs {
        set x [dict get $f exposure]
        if {$x == 0 && !$verbose} continue
        set tag [expr {$x == 0 ? "pass" : $x >= 1.0 ? "FAIL" : "part"}]
        append out [format "  %-4s %-26s w%.1f x%.2f %-6s %s\n" $tag [dict get $f id] \
            [dict get $f weight] $x [dict get $f tier] \
            [expr {[dict get $f detail] ne "" ? [dict get $f detail] : [dict get $f title]}]]
    }
    foreach w [dict get $u warnings] {append out "  warn $w\n"}
    foreach p [dict get $u problems] {append out "  note $p\n"}
    foreach s [dict get $u suppressed] {
        append out "  skip [dict get $s id]: [dict get $s reason]\n"
    }
    return $out
}

proc ::hd::byImpact {a b} {
    set x [expr {[dict get $a weight] * [dict get $a exposure]}]
    set y [expr {[dict get $b weight] * [dict get $b exposure]}]
    return [expr {$x < $y ? -1 : $x > $y ? 1 : 0}]
}

# Builds the drop-in text for one audited unit, limited to a tier and below.
proc ::hd::dropinText {u maxTier} {
    variable tierRank
    set lim [dict get $tierRank $maxTier]
    set picked {}
    foreach f [dict get $u findings] {
        if {[dict get $f exposure] == 0 || [llength [dict get $f fix]] == 0} continue
        if {[dict get $tierRank [dict get $f tier]] > $lim} continue
        lappend picked $f
    }
    if {[llength $picked] == 0} {return ""}
    set picked [lsort -command ::hd::byTier $picked]
    set out "# Generated by SystemdUnitHardeningAuditor.tcl for [dict get $u name]\n"
    append out "# Review every block. Verify with: systemd-analyze security [dict get $u name]\n"
    append out "# and watch: journalctl -u [dict get $u name] after restarting.\n\n\[Service\]\n"
    foreach f $picked {
        append out "# [dict get $f id] ([dict get $f tier]): [dict get $f title]\n"
        foreach l [dict get $f fix] {append out "$l\n"}
        append out "\n"
    }
    return $out
}

proc ::hd::byTier {a b} {
    variable tierRank
    set x [dict get $tierRank [dict get $a tier]]
    set y [dict get $tierRank [dict get $b tier]]
    if {$x != $y} {return [expr {$x < $y ? -1 : 1}]}
    set wx [dict get $a weight]
    set wy [dict get $b weight]
    return [expr {$wx > $wy ? -1 : $wx < $wy ? 1 : 0}]
}

proc ::hd::readBaseline {path} {
    set b {}
    foreach line [split [readFile $path] \n] {
        set line [string trim $line]
        if {$line eq "" || [string index $line 0] eq "#"} continue
        lassign [split $line \t] unit score
        if {![string is double -strict $score]} {
            return -code error "bad baseline line: $line"
        }
        dict set b $unit $score
    }
    return $b
}

proc ::hd::usage {} {
    return {usage: tclsh SystemdUnitHardeningAuditor.tcl [options] PATH...
  PATH                   a .service file or a directory searched recursively
  --format text|json|tsv output format (default text)
  --verbose              show passing rules too
  --max-score N          fail when any unit scores above N (0 to 10)
  --baseline FILE        fail when a unit scores worse than its baseline row
  --write-baseline FILE  write unit<TAB>score rows and exit 0
  --tolerance N          regression slack against the baseline (default 0.05)
  --ignore RULEID        suppress a rule for every unit (repeatable)
  --global-dropins DIR   apply DIR/*.conf to every unit first (repeatable)
  --emit-dropin DIR      write DIR/<unit>.d/10-hardening.conf for failing rules
  --tier safe|likely|risky  highest breakage tier to emit (default safe)
  --list-rules           print every rule and exit
exit codes: 0 pass, 1 gate failed, 2 usage or input error}
}

proc ::hd::main {argv} {
    variable rules
    defineRules
    set fmt text
    set verbose 0
    set maxScore {}
    set baselineFile {}
    set writeBaseline {}
    set tol 0.05
    set cliIgnores {}
    set globalDirs {}
    set emitDir {}
    set tier safe
    set paths {}
    set i 0
    set n [llength $argv]
    while {$i < $n} {
        set a [lindex $argv $i]
        incr i
        switch -- $a {
            --verbose {set verbose 1}
            --list-rules {
                foreach r $rules {
                    puts [format "%-26s w%.1f %-6s %s" [dict get $r id] [dict get $r weight] [dict get $r tier] [dict get $r title]]
                    puts "    [dict get $r why]"
                }
                return 0
            }
            --help - -h {puts [usage]; return 0}
            --format - --max-score - --baseline - --write-baseline - --tolerance - --ignore -
            --global-dropins - --emit-dropin - --tier {
                if {$i >= $n} {puts stderr "$a needs a value\n[usage]"; return 2}
                set v [lindex $argv $i]
                incr i
                switch -- $a {
                    --format {set fmt $v}
                    --max-score {set maxScore $v}
                    --baseline {set baselineFile $v}
                    --write-baseline {set writeBaseline $v}
                    --tolerance {set tol $v}
                    --ignore {lappend cliIgnores $v}
                    --global-dropins {lappend globalDirs $v}
                    --emit-dropin {set emitDir $v}
                    --tier {set tier $v}
                }
            }
            default {
                if {[string match --* $a]} {puts stderr "unknown option $a\n[usage]"; return 2}
                lappend paths $a
            }
        }
    }
    if {$fmt ni {text json tsv}} {puts stderr "bad --format $fmt"; return 2}
    if {$tier ni {safe likely risky}} {puts stderr "bad --tier $tier"; return 2}
    foreach {opt val} [list --max-score $maxScore --tolerance $tol] {
        if {$val ne "" && ![string is double -strict $val]} {puts stderr "$opt needs a number"; return 2}
    }
    if {[llength $paths] == 0} {puts stderr [usage]; return 2}

    set globals {}
    foreach d $globalDirs {
        if {![file isdirectory $d]} {puts stderr "no such directory: $d"; return 2}
        lappend globals {*}[lsort [glob -nocomplain -directory $d -types {f l} *.conf]]
    }
    if {[catch {collect $paths} col]} {puts stderr $col; return 2}
    set baseline {}
    if {$baselineFile ne ""} {
        if {[catch {readBaseline $baselineFile} baseline]} {
            puts stderr "cannot use baseline: $baseline"
            return 2
        }
    }

    set results {}
    foreach f [dict get $col units] {
        lappend results [auditUnit [file tail $f] [unitFiles $f] $globals $cliIgnores]
    }
    set audited [lmap r $results {expr {[dict exists $r skipped] ? [continue] : $r}}]
    if {[llength $audited] == 0} {
        puts stderr "no service units found"
        return 2
    }

    if {$writeBaseline ne ""} {
        set fh [open $writeBaseline w]
        foreach r [lsort -command ::hd::byName $audited] {
            puts $fh [format "%s\t%.2f" [dict get $r name] [dict get $r score]]
        }
        close $fh
        puts "wrote [llength $audited] rows to $writeBaseline"
        return 0
    }

    set failures {}
    foreach r $audited {
        set nm [dict get $r name]
        set sc [dict get $r score]
        if {$maxScore ne "" && $sc > $maxScore} {
            lappend failures [format "%s scores %.2f, budget is %s" $nm $sc $maxScore]
        }
        if {[dict exists $baseline $nm] && $sc > [dict get $baseline $nm] + $tol} {
            lappend failures [format "%s regressed from %.2f to %.2f" $nm [dict get $baseline $nm] $sc]
        }
    }

    switch -- $fmt {
        json {
            set gate [format {{"failed":%s,"failures":%s}} [expr {[llength $failures] ? "true" : "false"}] \
                [jlist [lmap f $failures {jstr $f}]]]
            puts [format {{"units":%s,"notes":%s,"gate":%s}} [jlist [lmap r $results {unitJson $r}]] \
                [jlist [lmap x [dict get $col notes] {jstr $x}]] $gate]
        }
        tsv {
            foreach r [lsort -command ::hd::byName $audited] {
                puts [format "%s\t%.2f\t%s" [dict get $r name] [dict get $r score] [dict get $r band]]
            }
        }
        default {
            foreach r $results {puts -nonewline [unitText $r $verbose]}
            foreach x [dict get $col notes] {puts "note $x"}
        }
    }
    foreach f $failures {puts stderr "GATE FAIL $f"}

    if {$emitDir ne ""} {
        foreach r $audited {
            set txt [dropinText $r $tier]
            if {$txt eq ""} continue
            set dir [file join $emitDir [dict get $r name].d]
            set target [file join $dir 10-hardening.conf]
            if {[file exists $target]} {
                puts stderr "refusing to overwrite $target"
                return 2
            }
            file mkdir $dir
            set fh [open $target w]
            puts -nonewline $fh $txt
            close $fh
            puts stderr "wrote $target"
        }
    }
    return [expr {[llength $failures] ? 1 : 0}]
}

proc ::hd::byName {a b} {
    return [string compare [dict get $a name] [dict get $b name]]
}

if {[info exists ::argv0] && [info script] ne "" &&
    [file normalize [info script]] eq [file normalize $::argv0]} {
    exit [::hd::main $::argv]
}
