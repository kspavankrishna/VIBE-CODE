#!/usr/bin/env perl
#
# MCP Stdio Conformance Auditor
# Black box test harness that spawns a Model Context Protocol server over
# stdio and drives it through the handshake plus a battery of adversarial
# framing, error handling, concurrency and lifecycle checks. Zero non core
# dependencies: everything used here has shipped with Perl since 5.14.

use strict;
use warnings;
use v5.16;

use Encode        qw(decode encode FB_CROAK);
use Getopt::Long  qw(GetOptionsFromArray);
use IO::Select    ();
use IPC::Open3    qw(open3);
use JSON::PP      ();
use POSIX         qw(:sys_wait_h);
use Symbol        qw(gensym);
use Time::HiRes   qw(time sleep);

our $VERSION = '1.0.0';

use constant {
    JSONRPC_VERSION          => '2.0',
    DEFAULT_TIMEOUT          => 5,
    DEFAULT_STARTUP_TIMEOUT  => 2,
    DEFAULT_SHUTDOWN_TIMEOUT => 5,
    DEFAULT_MAX_PAYLOAD      => 2_000_000,
    HARD_LINE_CAP            => 64 * 1024 * 1024,
    READ_CHUNK               => 65536,
    DEFAULT_PROTOCOL_VERSION => '2025-06-18',
};

# A response arriving late for an id we already timed out on must never be
# read as "no problem" silence for the *next* check, so every send carries
# a fresh, unpredictable id and every read is matched against it explicitly.
my $ID_COUNTER = int(time() * 1000) % 1_000_000;
sub next_id { return ++$ID_COUNTER; }

# ---------------------------------------------------------------------------
# MCA::Session: one child process plus its three pipes and the raw stdout
# ledger we need for cross cutting checks (stdout purity, byte level framing).
# ---------------------------------------------------------------------------
package MCA::Session;
use POSIX qw(:sys_wait_h);
use Encode qw(decode FB_CROAK);
use IPC::Open3 qw(open3);
use Symbol qw(gensym);

sub new {
    my ($class, %args) = @_;
    return bless {
        cmd            => $args{cmd},
        pid            => undef,
        in             => undef,
        out            => undef,
        err            => undef,
        out_sel        => undef,
        out_buf        => '',
        err_buf        => '',
        stderr_log     => '',
        stderr_cap     => 65536,
        all_raw_lines  => [],
        exited         => 0,
        exit_status    => undef,
        started_at     => undef,
    }, $class;
}

sub spawn {
    my ($self) = @_;
    my ($in, $out, $err) = (undef, undef, gensym());
    my $pid = eval { open3($in, $out, $err, @{ $self->{cmd} }) };
    if (!$pid) {
        die "spawn failed: " . ($@ || $!) . "\n";
    }
    binmode($_, ':raw') for ($in, $out, $err);
    $in->autoflush(1);
    $self->{pid}     = $pid;
    $self->{in}      = $in;
    $self->{out}     = $out;
    $self->{err}     = $err;
    $self->{out_sel} = IO::Select->new($out);
    $self->{err_sel} = IO::Select->new($err);
    $self->{started_at} = time();
    return $pid;
}

sub is_alive {
    my ($self) = @_;
    return 0 if $self->{exited};
    my $r = waitpid($self->{pid}, WNOHANG);
    if ($r == $self->{pid} || $r == -1) {
        $self->{exited}      = 1;
        $self->{exit_status} = $?;
        return 0;
    }
    return 1;
}

# Opportunistic, non blocking drain so a chatty child never fills its
# stderr pipe and stalls waiting for a reader nobody will ever provide.
sub drain_stderr {
    my ($self) = @_;
    return unless $self->{err_sel};
    while ($self->{err_sel}->can_read(0)) {
        my $n = sysread($self->{err}, my $chunk, main::READ_CHUNK);
        if (!defined $n) {
            next if $! == &POSIX::EINTR;
            last;
        }
        last if $n == 0;
        if (length($self->{stderr_log}) < $self->{stderr_cap}) {
            $self->{stderr_log} .= $chunk;
        }
    }
}

# Reads one raw NDJSON frame (bytes up to but excluding the terminating
# "\n") within $timeout seconds. Returns a hashref:
#   { ok=>1, raw=>$bytes, ms=>$elapsed }
#   { ok=>0, reason=>'timeout'|'eof', raw=>$partial, ms=>$elapsed }
sub read_line {
    my ($self, $timeout) = @_;
    my $deadline = time() + $timeout;
    my $start    = time();

    while (1) {
        if ((my $nl = index($self->{out_buf}, "\n")) >= 0) {
            my $line = substr($self->{out_buf}, 0, $nl);
            substr($self->{out_buf}, 0, $nl + 1, '');
            push @{ $self->{all_raw_lines} }, $line;
            return { ok => 1, raw => $line, ms => (time() - $start) * 1000 };
        }
        if (length($self->{out_buf}) > main::HARD_LINE_CAP) {
            return { ok => 0, reason => 'hard_cap_exceeded', raw => $self->{out_buf}, ms => (time() - $start) * 1000 };
        }
        $self->drain_stderr;
        my $remaining = $deadline - time();
        if ($remaining <= 0) {
            return { ok => 0, reason => 'timeout', raw => $self->{out_buf}, ms => (time() - $start) * 1000 };
        }
        my @ready = $self->{out_sel}->can_read($remaining > 0.25 ? 0.25 : $remaining);
        next unless @ready;
        my $n = sysread($self->{out}, my $chunk, main::READ_CHUNK);
        if (!defined $n) {
            next if $! == &POSIX::EINTR;
            return { ok => 0, reason => "read_error:$!", raw => $self->{out_buf}, ms => (time() - $start) * 1000 };
        }
        if ($n == 0) {
            return { ok => 0, reason => 'eof', raw => $self->{out_buf}, ms => (time() - $start) * 1000 };
        }
        $self->{out_buf} .= $chunk;
    }
}

# Reads one message and tries to decode it as UTF-8 JSON. If the first line
# does not parse, it keeps stitching on further lines (up to $max_extra) in
# case the server pretty printed the payload across several physical lines,
# which is a real and otherwise silent NDJSON transport violation.
sub read_message {
    my ($self, $timeout, %opt) = @_;
    my $max_extra = $opt{max_extra_lines} // 4;
    my $deadline  = time() + $timeout;
    my @parts;
    my $multiline = 0;

    while (1) {
        my $remaining = $deadline - time();
        $remaining = 0 if $remaining < 0;
        my $r = $self->read_line($remaining);
        if (!$r->{ok}) {
            return { ok => 0, reason => $r->{reason}, raw => join("\n", @parts, $r->{raw}) };
        }
        push @parts, $r->{raw};
        my $joined = join("\n", @parts);

        my $utf8 = eval { decode('UTF-8', $joined, FB_CROAK) };
        if (!defined $utf8) {
            return { ok => 0, reason => 'invalid_utf8', raw => $joined } if @parts > $max_extra;
            $multiline = 1;
            next;
        }
        my $data = eval { JSON::PP->new->decode($utf8) };
        if (!defined $data) {
            return { ok => 0, reason => 'invalid_json', raw => $joined, error => $@ } if @parts > $max_extra;
            $multiline = 1;
            next;
        }
        return { ok => 1, obj => $data, raw => $joined, multiline => $multiline };
    }
}

sub send_raw {
    my ($self, $bytes) = @_;
    local $SIG{PIPE} = 'IGNORE';
    my $off = 0;
    my $len = length($bytes);
    while ($off < $len) {
        my $n = syswrite($self->{in}, $bytes, $len - $off, $off);
        if (!defined $n) {
            return (0, $!) if $! != &POSIX::EINTR;
            next;
        }
        $off += $n;
    }
    return (1, undef);
}

sub send_line { my ($self, $bytes) = @_; return $self->send_raw($bytes . "\n"); }

# Writes the same line as send_line but in small, delayed slices, to prove
# the child reassembles a logical message even when the pipe delivers it
# as several short reads instead of one, which POSIX never guarantees.
sub send_fragmented {
    my ($self, $bytes, %opt) = @_;
    my $chunk = $opt{chunk_size} // 5;
    my $delay = $opt{delay}      // 0.02;
    my $line  = $bytes . "\n";
    local $SIG{PIPE} = 'IGNORE';
    my $len = length($line);
    my $off = 0;
    while ($off < $len) {
        my $n = ($len - $off < $chunk) ? ($len - $off) : $chunk;
        my $wrote = syswrite($self->{in}, $line, $n, $off);
        if (!defined $wrote) {
            return (0, $!) if $! != &POSIX::EINTR;
            next;
        }
        $off += $wrote;
        Time::HiRes::sleep($delay) if $off < $len;
    }
    return (1, undef);
}

sub close_stdin {
    my ($self) = @_;
    close($self->{in}) if $self->{in};
    $self->{in} = undef;
}

sub wait_exit {
    my ($self, $timeout) = @_;
    my $deadline = time() + $timeout;
    while (time() < $deadline) {
        return $self->{exit_status} if !$self->is_alive;
        Time::HiRes::sleep(0.05);
    }
    return undef;
}

sub terminate {
    my ($self, $sig) = @_;
    return unless $self->{pid} && $self->is_alive;
    kill $sig, $self->{pid};
}

sub reap_hard {
    my ($self) = @_;
    return unless $self->{pid};
    return unless $self->is_alive;
    $self->terminate('TERM');
    $self->wait_exit(1);
    return unless $self->is_alive;
    $self->terminate('KILL');
    $self->wait_exit(2);
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
package main;

my %opt = (
    timeout          => DEFAULT_TIMEOUT,
    startup_timeout  => DEFAULT_STARTUP_TIMEOUT,
    shutdown_timeout => DEFAULT_SHUTDOWN_TIMEOUT,
    max_payload      => DEFAULT_MAX_PAYLOAD,
    protocol_version => DEFAULT_PROTOCOL_VERSION,
    strict           => 0,
    json             => 0,
    verbose          => 0,
    only             => undef,
    skip             => undef,
);

sub usage {
    return <<"USAGE";
mcp-stdio-conformance-auditor $VERSION

Usage:
  perl McpStdioConformanceAuditor.pl [options] -- <server-command> [args...]

Options:
  --timeout=SEC            per read timeout (default @{[ DEFAULT_TIMEOUT ]})
  --startup-timeout=SEC     grace window for the silence-before-init check (default @{[ DEFAULT_STARTUP_TIMEOUT ]})
  --shutdown-timeout=SEC    time allowed to exit after stdin closes (default @{[ DEFAULT_SHUTDOWN_TIMEOUT ]})
  --max-payload=BYTES       size of the oversized-message probe (default @{[ DEFAULT_MAX_PAYLOAD ]})
  --protocol-version=STR    protocolVersion sent in initialize (default @{[ DEFAULT_PROTOCOL_VERSION ]})
  --only=ID,ID,...          run only these check ids
  --skip=ID,ID,...          skip these check ids
  --strict                  a WARN finding also fails the build (exit 1)
  --json                    print the machine readable report instead of text
  --verbose                 echo every raw frame sent and received to stderr
  --help                    show this message

Exit codes: 0 all clear, 1 warnings only (with --strict), 2 one or more failed checks.
USAGE
}

my @argv = @ARGV;
my @cmd;
{
    my $sep = -1;
    for my $i (0 .. $#argv) { if ($argv[$i] eq '--') { $sep = $i; last; } }
    if ($sep >= 0) {
        @cmd  = @argv[$sep + 1 .. $#argv];
        @argv = @argv[0 .. $sep - 1];
    }
    GetOptionsFromArray(
        \@argv,
        'timeout=f'          => \$opt{timeout},
        'startup-timeout=f'  => \$opt{startup_timeout},
        'shutdown-timeout=f' => \$opt{shutdown_timeout},
        'max-payload=i'      => \$opt{max_payload},
        'protocol-version=s' => \$opt{protocol_version},
        'only=s'             => \$opt{only},
        'skip=s'             => \$opt{skip},
        'strict'             => \$opt{strict},
        'json'               => \$opt{json},
        'verbose'            => \$opt{verbose},
        'help'               => sub { print usage(); exit 0; },
    ) or do { print STDERR usage(); exit 2; };
    if (!@cmd) {
        print STDERR "error: no server command given after --\n\n";
        print STDERR usage();
        exit 2;
    }
}

my %only_set = $opt{only} ? map { $_ => 1 } split(/,/, $opt{only}) : ();
my %skip_set = $opt{skip} ? map { $_ => 1 } split(/,/, $opt{skip}) : ();

my @findings;

sub record {
    my ($id, $title, $status, $detail) = @_;
    push @findings, { id => $id, title => $title, status => $status, detail => $detail // '' };
    if ($opt{verbose} || $status ne 'PASS') {
        printf STDERR "[%-4s] %-38s %s\n", $status, $id, $detail // '';
    }
    return;
}

sub should_run {
    my ($id) = @_;
    return 0 if %skip_set && $skip_set{$id};
    return 0 if %only_set && !$only_set{$id};
    return 1;
}

sub skip_note {
    my ($id, $title, $reason) = @_;
    record($id, $title, 'SKIP', $reason);
    return;
}

sub jenc { return JSON::PP->new->utf8->canonical->encode($_[0]); }

sub is_jsonrpc_response_shape {
    my ($obj) = @_;
    return 0 unless ref($obj) eq 'HASH';
    return 0 unless ($obj->{jsonrpc} // '') eq JSONRPC_VERSION;
    return 0 unless exists $obj->{id};
    return exists($obj->{result}) || exists($obj->{error});
}

sub vlog {
    return unless $opt{verbose};
    my ($dir, $bytes) = @_;
    print STDERR "$dir $bytes\n";
    return;
}

# ---- spawn -----------------------------------------------------------------

my $sess = MCA::Session->new(cmd => \@cmd);
eval { $sess->spawn; 1 } or do {
    my $err = $@ // 'unknown error';
    $err =~ s/\s+at\s+\S+\s+line\s+\d+\.?\s*$//s;
    record('spawn.failed', 'Server process could not be started', 'FAIL', $err);
    print_report();
    exit 2;
};

my $cleanup_done = 0;
sub cleanup {
    return if $cleanup_done;
    $cleanup_done = 1;
    $sess->reap_hard if $sess;
}
$SIG{INT}  = sub { cleanup(); exit 130; };
$SIG{TERM} = sub { cleanup(); exit 143; };
END { cleanup(); }

# ---- handshake.silence_before_init -----------------------------------------

if (should_run('handshake.silence_before_init')) {
    my $r = $sess->read_line($opt{startup_timeout});
    if ($r->{ok}) {
        record('handshake.silence_before_init',
            'No stdout output before the client sends initialize',
            'FAIL', "child wrote before initialize: " . substr($r->{raw}, 0, 200));
    } elsif ($r->{reason} eq 'eof') {
        record('handshake.silence_before_init', 'No stdout output before the client sends initialize',
            'FAIL', 'child closed stdout before any request was sent');
    } else {
        record('handshake.silence_before_init', 'No stdout output before the client sends initialize',
            'PASS', "quiet for $opt{startup_timeout}s as expected");
    }
} else {
    skip_note('handshake.silence_before_init', 'No stdout output before the client sends initialize', 'skipped by filter');
}

# ---- handshake.initialize_ok ------------------------------------------------

my $init_id = next_id();
my $init_ok = 0;
my $init_raw;
my $init_multiline = 0;

if (should_run('handshake.initialize_ok')) {
    my $req = {
        jsonrpc => JSONRPC_VERSION,
        id      => $init_id,
        method  => 'initialize',
        params  => {
            protocolVersion => $opt{protocol_version},
            capabilities    => {},
            clientInfo      => { name => 'mcp-stdio-conformance-auditor', version => $VERSION },
        },
    };
    my $bytes = jenc($req);
    vlog('>>', $bytes);
    my ($ok, $err) = $sess->send_line($bytes);
    if (!$ok) {
        record('handshake.initialize_ok', 'initialize returns a valid, matching result', 'FAIL', "write failed: $err");
    } else {
        my $resp = $sess->read_message($opt{timeout});
        if (!$resp->{ok}) {
            record('handshake.initialize_ok', 'initialize returns a valid, matching result', 'FAIL',
                "no usable response ($resp->{reason})");
        } else {
            $init_raw       = $resp->{raw};
            $init_multiline = $resp->{multiline} ? 1 : 0;
            vlog('<<', $resp->{raw});
            my $obj = $resp->{obj};
            if (!is_jsonrpc_response_shape($obj)) {
                record('handshake.initialize_ok', 'initialize returns a valid, matching result', 'FAIL',
                    'response is not a well formed JSON-RPC response object');
            } elsif (("$obj->{id}" ne "$init_id")) {
                record('handshake.initialize_ok', 'initialize returns a valid, matching result', 'FAIL',
                    "id mismatch: sent $init_id, got $obj->{id}");
            } elsif ($obj->{error}) {
                record('handshake.initialize_ok', 'initialize returns a valid, matching result', 'FAIL',
                    "server returned an error: " . jenc($obj->{error}));
            } elsif (ref($obj->{result}) ne 'HASH'
                     || !exists $obj->{result}{protocolVersion}
                     || !exists $obj->{result}{capabilities}) {
                record('handshake.initialize_ok', 'initialize returns a valid, matching result', 'FAIL',
                    'result is missing protocolVersion or capabilities');
            } else {
                $init_ok = 1;
                record('handshake.initialize_ok', 'initialize returns a valid, matching result', 'PASS',
                    "protocolVersion=$obj->{result}{protocolVersion}");
            }
        }
    }
} else {
    skip_note('handshake.initialize_ok', 'initialize returns a valid, matching result', 'skipped by filter');
}

# From here on, most checks assume a live, initialized session. If the
# handshake itself failed there is nothing meaningful left to probe, so
# every dependent check is recorded as SKIP with an explicit reason instead
# of producing a wall of confusing follow-on failures.
my $can_continue = $init_ok && $sess->is_alive;

if ($can_continue) {
    # ---- framing checks against the captured initialize frame -------------
    if (should_run('framing.single_line_per_message')) {
        record('framing.single_line_per_message', 'Each JSON-RPC message is exactly one NDJSON line',
            $init_multiline ? 'FAIL' : 'PASS',
            $init_multiline ? 'initialize response was split across multiple physical lines' : 'initialize response parsed from a single physical line');
    } else {
        skip_note('framing.single_line_per_message', 'Each JSON-RPC message is exactly one NDJSON line', 'skipped by filter');
    }

    if (should_run('framing.utf8_validity')) {
        my $ok = eval { decode('UTF-8', $init_raw, FB_CROAK); 1 };
        record('framing.utf8_validity', 'Response bytes are valid UTF-8',
            $ok ? 'PASS' : 'FAIL', $ok ? '' : ($@ // 'decode failed'));
    } else {
        skip_note('framing.utf8_validity', 'Response bytes are valid UTF-8', 'skipped by filter');
    }

    if (should_run('framing.no_bom')) {
        my $has_bom = substr($init_raw, 0, 3) eq "\xEF\xBB\xBF";
        record('framing.no_bom', 'Response frame has no leading UTF-8 BOM',
            $has_bom ? 'FAIL' : 'PASS', $has_bom ? 'frame starts with EF BB BF' : '');
    } else {
        skip_note('framing.no_bom', 'Response frame has no leading UTF-8 BOM', 'skipped by filter');
    }

    # ---- notifications/initialized: must draw no response ------------------
    if (should_run('handshake.initialized_notification')) {
        my $note = { jsonrpc => JSONRPC_VERSION, method => 'notifications/initialized', params => {} };
        my $bytes = jenc($note);
        vlog('>>', $bytes);
        my ($ok, $err) = $sess->send_line($bytes);
        if (!$ok) {
            record('handshake.initialized_notification', 'Server sends nothing back for notifications/initialized',
                'FAIL', "write failed: $err");
        } else {
            my $resp = $sess->read_message(1.0);
            if ($resp->{ok}) {
                record('handshake.initialized_notification', 'Server sends nothing back for notifications/initialized',
                    'FAIL', 'server answered a notification, which has no id to answer');
            } else {
                record('handshake.initialized_notification', 'Server sends nothing back for notifications/initialized',
                    'PASS', '');
            }
        }
    } else {
        skip_note('handshake.initialized_notification', 'Server sends nothing back for notifications/initialized', 'skipped by filter');
    }

    # ---- error handling ------------------------------------------------------
    if (should_run('error_handling.unknown_method_request')) {
        my $id  = next_id();
        my $req = { jsonrpc => JSONRPC_VERSION, id => $id, method => 'totally/not/a/real/method' };
        my $bytes = jenc($req);
        vlog('>>', $bytes);
        my ($ok) = $sess->send_line($bytes);
        my $resp = $ok ? $sess->read_message($opt{timeout}) : { ok => 0, reason => 'write failed' };
        if (!$resp->{ok}) {
            record('error_handling.unknown_method_request', 'Unknown method returns JSON-RPC -32601',
                'FAIL', "no response ($resp->{reason})");
        } elsif (!is_jsonrpc_response_shape($resp->{obj}) || "$resp->{obj}{id}" ne "$id") {
            record('error_handling.unknown_method_request', 'Unknown method returns JSON-RPC -32601',
                'FAIL', 'malformed or mismatched response');
        } elsif (!$resp->{obj}{error} || ($resp->{obj}{error}{code} // 0) != -32601) {
            my $got = $resp->{obj}{error} ? $resp->{obj}{error}{code} : 'no error object';
            record('error_handling.unknown_method_request', 'Unknown method returns JSON-RPC -32601',
                'WARN', "expected code -32601, got $got");
        } else {
            record('error_handling.unknown_method_request', 'Unknown method returns JSON-RPC -32601', 'PASS', '');
        }
    } else {
        skip_note('error_handling.unknown_method_request', 'Unknown method returns JSON-RPC -32601', 'skipped by filter');
    }

    if (should_run('error_handling.unknown_notification_silent')) {
        my $note = { jsonrpc => JSONRPC_VERSION, method => 'totally/not/a/real/notification', params => {} };
        my $bytes = jenc($note);
        vlog('>>', $bytes);
        $sess->send_line($bytes);
        my $resp = $sess->read_message(1.0);
        record('error_handling.unknown_notification_silent', 'Unknown notification draws no response',
            $resp->{ok} ? 'FAIL' : 'PASS', $resp->{ok} ? 'server answered an id-less notification' : '');
    } else {
        skip_note('error_handling.unknown_notification_silent', 'Unknown notification draws no response', 'skipped by filter');
    }

    if (should_run('error_handling.malformed_json_recovers')) {
        $sess->send_raw("{ this is not json, it is bait ]\n");
        my $resp = $sess->read_message($opt{timeout});
        my $parse_error_seen = $resp->{ok} && $resp->{obj}{error} && ($resp->{obj}{error}{code} // 0) == -32700;

        # Whether or not the server bothered to answer the garbage line, the
        # real assertion is that its request loop survived it. A server
        # that wedges its parser on bad input is a much worse bug than one
        # that just stays silent about malformed frames.
        my $probe_id = next_id();
        my $probe    = jenc({ jsonrpc => JSONRPC_VERSION, id => $probe_id, method => 'tools/list' });
        vlog('>>', $probe);
        $sess->send_line($probe);
        my $probe_resp = $sess->read_message($opt{timeout});
        my $recovered = $probe_resp->{ok} && is_jsonrpc_response_shape($probe_resp->{obj})
            && "$probe_resp->{obj}{id}" eq "$probe_id";

        if ($recovered) {
            record('error_handling.malformed_json_recovers', 'Server survives a malformed JSON line and keeps serving requests',
                'PASS', $parse_error_seen ? 'sent -32700 and kept serving' : 'stayed silent on the bad line but kept serving');
        } else {
            record('error_handling.malformed_json_recovers', 'Server survives a malformed JSON line and keeps serving requests',
                'FAIL', 'no valid response to the follow up request; parser likely wedged or the process died');
        }
    } else {
        skip_note('error_handling.malformed_json_recovers', 'Server survives a malformed JSON line and keeps serving requests', 'skipped by filter');
    }

    # ---- concurrency ----------------------------------------------------------
    if (should_run('concurrency.pipelined_requests')) {
        if ($sess->is_alive) {
            my @ids = map { next_id() } (1 .. 3);
            for my $id (@ids) {
                my $bytes = jenc({ jsonrpc => JSONRPC_VERSION, id => $id, method => 'tools/list' });
                vlog('>>', $bytes);
                $sess->send_line($bytes);
            }
            my %seen;
            my $deadline = time() + $opt{timeout} * 2;
            while (keys(%seen) < @ids && time() < $deadline) {
                my $remaining = $deadline - time();
                last if $remaining <= 0;
                my $resp = $sess->read_message($remaining);
                last unless $resp->{ok};
                next unless is_jsonrpc_response_shape($resp->{obj});
                $seen{ $resp->{obj}{id} }++;
            }
            my @missing = grep { !$seen{$_} } @ids;
            my @dupes   = grep { ($seen{$_} // 0) > 1 } @ids;
            if (@missing) {
                record('concurrency.pipelined_requests', 'Three pipelined requests all get a correlated response',
                    'FAIL', 'missing responses for ids: ' . join(',', @missing) . ' (possible read-then-write deadlock)');
            } elsif (@dupes) {
                record('concurrency.pipelined_requests', 'Three pipelined requests all get a correlated response',
                    'WARN', 'duplicate responses for ids: ' . join(',', @dupes));
            } else {
                record('concurrency.pipelined_requests', 'Three pipelined requests all get a correlated response', 'PASS', '');
            }
        } else {
            skip_note('concurrency.pipelined_requests', 'Three pipelined requests all get a correlated response', 'process already exited');
        }
    } else {
        skip_note('concurrency.pipelined_requests', 'Three pipelined requests all get a correlated response', 'skipped by filter');
    }

    # ---- resilience -------------------------------------------------------------
    if (should_run('resilience.oversized_message')) {
        if ($sess->is_alive) {
            my $id  = next_id();
            my $pad = 'A' x $opt{max_payload};
            my $req = jenc({ jsonrpc => JSONRPC_VERSION, id => $id, method => 'tools/call',
                             params => { name => '__auditor_probe__', arguments => { dummy => $pad } } });
            my $start = time();
            my ($ok) = $sess->send_line($req);
            my $resp = $ok ? $sess->read_message($opt{timeout} * 3) : { ok => 0, reason => 'write failed' };
            my $elapsed_ms = int((time() - $start) * 1000);
            if (!$resp->{ok}) {
                record('resilience.oversized_message', "Large single-line request (@{[ int($opt{max_payload}/1000) ]}KB) does not hang the server",
                    'FAIL', "no response after ${elapsed_ms}ms ($resp->{reason})");
            } elsif (!is_jsonrpc_response_shape($resp->{obj}) || "$resp->{obj}{id}" ne "$id") {
                record('resilience.oversized_message', "Large single-line request does not hang the server",
                    'FAIL', 'malformed or mismatched response to the oversized request');
            } else {
                record('resilience.oversized_message', "Large single-line request does not hang the server",
                    'PASS', "answered in ${elapsed_ms}ms");
            }
        } else {
            skip_note('resilience.oversized_message', 'Large single-line request does not hang the server', 'process already exited');
        }
    } else {
        skip_note('resilience.oversized_message', 'Large single-line request does not hang the server', 'skipped by filter');
    }

    if (should_run('resilience.fragmented_write')) {
        if ($sess->is_alive) {
            my $id  = next_id();
            my $req = jenc({ jsonrpc => JSONRPC_VERSION, id => $id, method => 'tools/list' });
            my ($ok) = $sess->send_fragmented($req, chunk_size => 3, delay => 0.01);
            my $resp = $ok ? $sess->read_message($opt{timeout}) : { ok => 0, reason => 'write failed' };
            if ($resp->{ok} && is_jsonrpc_response_shape($resp->{obj}) && "$resp->{obj}{id}" eq "$id") {
                record('resilience.fragmented_write', 'A request delivered in 3-byte writes is still read correctly', 'PASS', '');
            } else {
                record('resilience.fragmented_write', 'A request delivered in 3-byte writes is still read correctly',
                    'FAIL', $resp->{ok} ? 'response id mismatch or malformed' : "no response ($resp->{reason})");
            }
        } else {
            skip_note('resilience.fragmented_write', 'A request delivered in 3-byte writes is still read correctly', 'process already exited');
        }
    } else {
        skip_note('resilience.fragmented_write', 'A request delivered in 3-byte writes is still read correctly', 'skipped by filter');
    }
} else {
    for my $id (qw(
        framing.single_line_per_message framing.utf8_validity framing.no_bom
        handshake.initialized_notification error_handling.unknown_method_request
        error_handling.unknown_notification_silent error_handling.malformed_json_recovers
        concurrency.pipelined_requests resilience.oversized_message resilience.fragmented_write
    )) {
        skip_note($id, $id, 'initialize did not succeed; skipping checks that need a live session');
    }
}

# ---- purity.stdout_is_jsonrpc_only: cross cutting over everything captured --

if (should_run('purity.stdout_is_jsonrpc_only')) {
    my @bad;
    for my $line (@{ $sess->{all_raw_lines} }) {
        next if $line eq '';
        my $obj = eval {
            my $u = decode('UTF-8', $line, FB_CROAK);
            JSON::PP->new->decode($u);
        };
        if (!defined $obj || ref($obj) ne 'HASH' || ($obj->{jsonrpc} // '') ne JSONRPC_VERSION) {
            push @bad, substr($line, 0, 120);
        }
    }
    if (@bad) {
        record('purity.stdout_is_jsonrpc_only', 'Every line on stdout is a JSON-RPC 2.0 message, nothing else',
            'FAIL', scalar(@bad) . ' non-conforming line(s), e.g.: ' . $bad[0]);
    } else {
        record('purity.stdout_is_jsonrpc_only', 'Every line on stdout is a JSON-RPC 2.0 message, nothing else',
            'PASS', scalar(@{ $sess->{all_raw_lines} }) . ' line(s) checked');
    }
} else {
    skip_note('purity.stdout_is_jsonrpc_only', 'Every line on stdout is a JSON-RPC 2.0 message, nothing else', 'skipped by filter');
}

# ---- lifecycle ----------------------------------------------------------------

if (should_run('lifecycle.stdin_eof_clean_exit')) {
    $sess->close_stdin;
    my $status = $sess->wait_exit($opt{shutdown_timeout});
    if (!defined $status) {
        record('lifecycle.stdin_eof_clean_exit', 'Process exits on its own after stdin closes',
            'FAIL', "still running after $opt{shutdown_timeout}s; sending SIGTERM/SIGKILL to reap it");
        $sess->reap_hard;
    } else {
        my $code = ($status >> 8);
        my $sig  = ($status & 127);
        if ($sig) {
            record('lifecycle.stdin_eof_clean_exit', 'Process exits on its own after stdin closes',
                'WARN', "exited via signal $sig rather than a normal return");
        } elsif ($code != 0) {
            record('lifecycle.stdin_eof_clean_exit', 'Process exits on its own after stdin closes',
                'WARN', "exited with non-zero status $code");
        } else {
            record('lifecycle.stdin_eof_clean_exit', 'Process exits on its own after stdin closes', 'PASS', 'exit 0');
        }
    }
} else {
    skip_note('lifecycle.stdin_eof_clean_exit', 'Process exits on its own after stdin closes', 'skipped by filter');
    $sess->reap_hard;
}

cleanup();

# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

sub print_report {
    if ($opt{json}) {
        print jenc({ version => $VERSION, findings => \@findings }), "\n";
        return;
    }
    print "MCP Stdio Conformance Auditor v$VERSION\n";
    print "Target: @cmd\n\n";
    for my $f (@findings) {
        printf "[%-4s] %-42s %s\n", $f->{status}, $f->{id}, $f->{detail};
    }
    my %count;
    $count{$_->{status}}++ for @findings;
    print "\n";
    printf "PASS=%d WARN=%d FAIL=%d SKIP=%d\n",
        $count{PASS} // 0, $count{WARN} // 0, $count{FAIL} // 0, $count{SKIP} // 0;
    return;
}

print_report();

my %count;
$count{$_->{status}}++ for @findings;
if ($count{FAIL}) {
    exit 2;
} elsif ($opt{strict} && $count{WARN}) {
    exit 1;
} else {
    exit 0;
}
