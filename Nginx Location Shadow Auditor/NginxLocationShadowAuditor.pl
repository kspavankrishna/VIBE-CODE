#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long qw(GetOptionsFromArray);
use File::Basename qw(dirname);
use File::Spec ();
use File::Glob qw(bsd_glob);
use Cwd qw(getcwd);
use Digest::SHA qw(sha1_hex);
use JSON::PP ();

our $VERSION = '1.0.0';

use constant {
    EXIT_OK       => 0,
    EXIT_FINDINGS => 1,
    EXIT_ERROR    => 2,
};

use constant MAX_INCLUDE_DEPTH     => 16;
use constant DEFAULT_MAX_PROBES    => 8000;
use constant MAX_SAMPLES_PER_REGEX => 16;
use constant SAMPLER_PRODUCT_CAP   => 24;
use constant MAX_PROBE_LENGTH      => 512;
use constant MAX_TAILS_PER_REGEX   => 6;
use constant FILLER                => 'zq';
use constant DUMP_MARKER           => qr/^# configuration file (\S.*?):[ \t]*\r?$/m;

my %SEVERITY_RANK = (info => 0, low => 1, medium => 2, high => 3);

my %RULES = (
    NLS001 => { name => 'guard-bypass', severity => 'high',
        title => 'A location with access control loses the match to a location that does not carry it' },
    NLS002 => { name => 'script-handler-in-content-prefix', severity => 'medium',
        title => 'A script handler regex takes requests under a prefix that looks like stored content' },
    NLS003 => { name => 'dead-location', severity => 'medium',
        title => 'Every probe that should reach this location is taken by another location' },
    NLS004 => { name => 'duplicate-location', severity => 'high',
        title => 'Two locations at the same level have the same matcher, nginx refuses to load this' },
    NLS005 => { name => 'alias-traversal', severity => 'high',
        title => 'alias ends with a slash but the location prefix does not, the parent directory is reachable' },
    NLS006 => { name => 'inheritance-drop', severity => 'medium',
        title => 'A block redefines an all or nothing directive and silently drops the inherited values' },
    NLS007 => { name => 'access-rule-reset', severity => 'high',
        title => 'allow and deny rules in this block replace the inherited list and leave it open' },
    NLS008 => { name => 'proxy-pass-uri-invalid', severity => 'high',
        title => 'proxy_pass has a URI part in a context where nginx rejects it' },
    NLS009 => { name => 'proxy-pass-slash-mismatch', severity => 'medium',
        title => 'proxy_pass URI ends with a slash but the location prefix does not' },
    NLS010 => { name => 'loose-script-regex', severity => 'medium',
        title => 'A script handler regex matches more file names than its extension suggests' },
    NLS011 => { name => 'if-in-location', severity => 'low',
        title => 'if inside a location holds directives other than return, rewrite, set and break' },
    NLS012 => { name => 'invalid-nesting', severity => 'high',
        title => 'Nested location is not valid inside its parent, nginx refuses to load this' },
    NLS013 => { name => 'undefined-named-location', severity => 'high',
        title => 'A named location is referenced but never defined in this server' },
    NLS014 => { name => 'regex-compile-failure', severity => 'medium',
        title => 'Location regex could not be compiled, it is skipped in every replay' },
    NLS015 => { name => 'include-missing', severity => 'info',
        title => 'An include could not be resolved, the audit covers a partial configuration' },
    NLS016 => { name => 'merge-slashes-off', severity => 'medium',
        title => 'merge_slashes is off while prefix locations carry access control' },
    NLS017 => { name => 'unused-named-location', severity => 'info',
        title => 'A named location is defined but nothing in this server refers to it' },
);

my @GUARD_VALUE_DIRECTIVES = qw(auth_basic auth_request auth_jwt auth_gss auth_ldap);
my @GUARD_LUA_DIRECTIVES   = qw(access_by_lua_block access_by_lua_file access_by_lua);
my %SCRIPT_HANDLERS        = map { $_ => 1 } qw(fastcgi_pass uwsgi_pass scgi_pass);
my %PASS_HANDLERS          = map { $_ => 1 } qw(proxy_pass fastcgi_pass uwsgi_pass scgi_pass grpc_pass memcached_pass);
my %CONTENT_HANDLERS       = map { $_ => 1 } (keys %PASS_HANDLERS,
    qw(content_by_lua_block content_by_lua_file content_by_lua return stub_status empty_gif js_content perl));
my %IF_SAFE_DIRECTIVES     = map { $_ => 1 } qw(return rewrite set break);

my %INHERIT_FAMILIES = (
    add_header        => { ci => 1, critical => qr/^(?:strict-transport-security|content-security-policy|content-security-policy-report-only|x-frame-options|x-content-type-options|referrer-policy|permissions-policy|cross-origin-opener-policy|cross-origin-embedder-policy|cross-origin-resource-policy|cache-control|access-control-allow-origin)$/i,
                           switch => 'add_header_inherit' },
    add_trailer       => { ci => 1, critical => qr/(?!)/, switch => 'add_trailer_inherit' },
    proxy_set_header  => { ci => 1, critical => qr/^(?:host|x-forwarded-for|x-forwarded-proto|x-forwarded-host|x-real-ip|authorization|upgrade|connection|cookie)$/i },
    grpc_set_header   => { ci => 1, critical => qr/^(?:host|x-forwarded-for|x-real-ip|authorization)$/i },
    proxy_hide_header => { ci => 1, critical => qr/^(?:x-powered-by|server|x-accel-.*|set-cookie)$/i },
    fastcgi_param     => { ci => 0, critical => qr/^(?:SCRIPT_FILENAME|REQUEST_METHOD|QUERY_STRING|CONTENT_TYPE|CONTENT_LENGTH|HTTPS|REMOTE_ADDR|HTTP_PROXY)$/ },
    uwsgi_param       => { ci => 0, critical => qr/^(?:REQUEST_METHOD|QUERY_STRING|CONTENT_TYPE|CONTENT_LENGTH|HTTPS|REMOTE_ADDR|HTTP_PROXY)$/ },
    scgi_param        => { ci => 0, critical => qr/^(?:REQUEST_METHOD|QUERY_STRING|CONTENT_TYPE|CONTENT_LENGTH|HTTPS|REMOTE_ADDR|HTTP_PROXY)$/ },
);

my $CONTENT_PREFIX_RE = qr{(?:^|[/_.-])(?:uploads?|uploaded|media|files?|attachments?|avatars?|images?|img|pictures?|photos?|static|assets|public|tmp|temp|cache|storage|usercontent|user[_-]?files|userdata|documents?|docs|downloads?|backups?|exports?|shared?)(?:$|[/_.-])}i;
my $WRITABLE_PREFIX_RE = qr{(?:^|[/_.-])(?:uploads?|uploaded|attachments?|avatars?|tmp|temp|storage|usercontent|user[_-]?files|userdata|shared?)(?:$|[/_.-])}i;

my @CLASS_PREFERENCE = (qw(a z m 0 7 A Z), '-', '_', '.', '/', '~', '+', '%', '@', ':', '=', '!', ',', ' ',
    map { chr } 33 .. 126);

exit main(@ARGV) unless caller;

sub main {
    my @argv = @_;
    my %opt = (
        format         => 'text',
        'fail-on'      => 'high',
        'min-severity' => 'low',
        'max-probes'   => DEFAULT_MAX_PROBES,
        explain        => [],
        ignore         => [],
    );
    my $ok = GetOptionsFromArray(\@argv, \%opt,
        'explain=s@', 'host=s', 'port=i', 'format=s', 'fail-on=s', 'min-severity=s',
        'ignore=s@', 'baseline=s', 'write-baseline=s', 'prefix=s', 'sysroot=s',
        'max-probes=i', 'self-test', 'list-rules', 'version', 'help|h');
    if (!$ok) { print STDERR usage(); return EXIT_ERROR; }
    if ($opt{help})    { print usage(); return EXIT_OK; }
    if ($opt{version}) { print "NginxLocationShadowAuditor $VERSION\n"; return EXIT_OK; }
    if ($opt{'list-rules'}) {
        for my $id (sort keys %RULES) {
            printf "%s  %-7s %-34s %s\n", $id, $RULES{$id}{severity}, $RULES{$id}{name}, $RULES{$id}{title};
        }
        return EXIT_OK;
    }
    return self_test() if $opt{'self-test'};

    for my $key ('fail-on', 'min-severity') {
        my $v = lc $opt{$key};
        unless (exists $SEVERITY_RANK{$v} || ($key eq 'fail-on' && $v eq 'never')) {
            print STDERR "error: --$key must be one of info, low, medium, high"
                . ($key eq 'fail-on' ? ", never\n" : "\n");
            return EXIT_ERROR;
        }
        $opt{$key} = $v;
    }
    unless ($opt{format} =~ /^(?:text|json|sarif)$/) {
        print STDERR "error: --format must be text, json or sarif\n";
        return EXIT_ERROR;
    }
    if ($opt{'max-probes'} < 1) {
        print STDERR "error: --max-probes must be a positive integer\n";
        return EXIT_ERROR;
    }
    if (@argv != 1) { print STDERR usage(); return EXIT_ERROR; }

    my $ctx = eval { load_config($argv[0], \%opt) };
    if (!$ctx) {
        my $err = $@ || 'unknown error';
        $err =~ s/\s+\z//;
        print STDERR "error: $err\n";
        return EXIT_ERROR;
    }
    if (!@{ $ctx->{servers} }) {
        print STDERR "error: no server block and no top level location found in $argv[0]\n";
        return EXIT_ERROR;
    }

    if (@{ $opt{explain} }) {
        my @reports;
        for my $raw (@{ $opt{explain} }) {
            push @reports, explain_request($ctx, $raw, $opt{host}, $opt{port});
        }
        if ($opt{format} eq 'text') {
            print render_explain_text($_) for @reports;
        } else {
            print json_encoder()->encode(\@reports);
        }
        return (grep { $_->{error} } @reports) ? EXIT_ERROR : EXIT_OK;
    }

    audit_server($ctx, $_) for @{ $ctx->{servers} };
    my $rc = eval { finalize_findings($ctx) };
    if (!defined $rc) {
        my $err = $@; $err =~ s/\s+\z//;
        print STDERR "error: $err\n";
        return EXIT_ERROR;
    }

    my @shown = grep { $SEVERITY_RANK{ $_->{severity} } >= $SEVERITY_RANK{ $opt{'min-severity'} } } @{ $ctx->{findings} };
    if    ($opt{format} eq 'json')  { print render_json($ctx, \@shown); }
    elsif ($opt{format} eq 'sarif') { print render_sarif($ctx, \@shown); }
    else                            { print render_text($ctx, \@shown); }

    return EXIT_OK if $opt{'fail-on'} eq 'never';
    my $bar = $SEVERITY_RANK{ $opt{'fail-on'} };
    return (grep { $SEVERITY_RANK{ $_->{severity} } >= $bar } @{ $ctx->{findings} }) ? EXIT_FINDINGS : EXIT_OK;
}

sub usage {
    return <<'USAGE';
Usage: NginxLocationShadowAuditor.pl [options] <nginx.conf | nginx -T dump | ->

Replays the nginx location matching algorithm over synthesized request paths
and reports locations whose access control, handler or headers do not apply
to the requests they look like they cover.

Audit options:
  --format text|json|sarif     output format (default text)
  --fail-on LEVEL              exit 1 when a finding at or above LEVEL exists:
                               info, low, medium, high or never (default high)
  --min-severity LEVEL         hide findings below LEVEL (default low)
  --ignore ID[,ID]             drop rule ids, repeatable
  --baseline FILE              drop findings whose fingerprint is in FILE
  --write-baseline FILE        write current fingerprints to FILE
  --max-probes N               probe paths replayed per server (default 8000)

Explain options:
  --explain URI                trace how one request path is matched, repeatable
  --host NAME                  Host header used to pick the server block
  --port N                     listen port used to pick the server block

Input options:
  --prefix DIR                 directory relative includes resolve against
  --sysroot DIR                directory prepended to absolute include paths

Other:
  --list-rules  --self-test  --version  --help

Inline suppression: put "# nls:ignore NLS006" on the directive line or the
line above it. A bare "# nls:ignore" suppresses every rule for that line.

Exit codes: 0 clean, 1 findings at or above --fail-on, 2 usage or parse error.
USAGE
}

sub json_encoder { return JSON::PP->new->canonical(1)->pretty(1)->utf8(1); }

sub load_config {
    my ($path, $opt) = @_;
    my ($text, $display);
    if ($path eq '-') {
        local $/;
        $text    = <STDIN>;
        $display = '<stdin>';
        die "empty input on stdin\n" unless defined $text && length $text;
    } else {
        $text    = slurp($path);
        $display = $path;
        die "cannot read $path: $!\n" unless defined $text;
    }
    return build_context($text, $display, $opt);
}

sub slurp {
    my ($path) = @_;
    return undef if -d $path;
    open(my $fh, '<', $path) or return undef;
    binmode $fh;
    local $/;
    my $text = <$fh>;
    close $fh;
    return defined $text ? $text : '';
}

sub build_context {
    my ($text, $display, $opt) = @_;
    my $ctx = {
        opt      => $opt,
        findings => [],
        ignores  => {},
        servers  => [],
        includes => 0,
        pending  => [],
    };
    my $st = { ctx => $ctx, stack => [] };

    if ($text =~ DUMP_MARKER) {
        my %vfs;
        my @order;
        my @parts = split DUMP_MARKER, $text;
        shift @parts;
        while (@parts) {
            my $name = shift @parts;
            my $body = shift @parts;
            $body = '' unless defined $body;
            $body =~ s/\A\r?\n//;
            next if exists $vfs{$name};
            $vfs{$name} = $body;
            push @order, $name;
        }
        die "nginx -T dump has no configuration file sections\n" unless @order;
        $st->{vfs}    = \%vfs;
        $st->{prefix} = defined $opt->{prefix} ? $opt->{prefix} : dirname($order[0]);
        $ctx->{mode}  = 'dump';
        $ctx->{main}  = $order[0];
        $ctx->{tree}  = parse_file($st, $order[0], $vfs{ $order[0] }, 0);
    } else {
        my $prefix = $opt->{prefix};
        if (!defined $prefix) {
            $prefix = $display eq '<stdin>' ? getcwd() : dirname(File::Spec->rel2abs($display));
        }
        $st->{prefix} = $prefix;
        $ctx->{mode}  = 'files';
        $ctx->{main}  = $display;
        $ctx->{tree}  = parse_file($st, $display, $text, 0);
    }
    build_servers($ctx);
    return $ctx;
}

sub parse_file {
    my ($st, $file, $text, $depth) = @_;
    die "include depth exceeds " . MAX_INCLUDE_DEPTH . " at $file, includes probably form a cycle\n"
        if $depth > MAX_INCLUDE_DEPTH;
    die "$file: include cycle detected\n" if grep { $_ eq $file } @{ $st->{stack} };
    push @{ $st->{stack} }, $file;
    $text =~ s/\A\x{EF}\x{BB}\x{BF}//;
    my $lx = { text => $text, pos => 0, len => length $text, line => 1, file => $file };
    my $out = parse_block($st, $lx, 1, $depth);
    pop @{ $st->{stack} };
    return $out;
}

sub lex_next {
    my ($st, $lx) = @_;
    my $t = \$lx->{text};
    while ($lx->{pos} < $lx->{len}) {
        my $ch = substr($$t, $lx->{pos}, 1);
        if ($ch eq "\n") { $lx->{line}++; $lx->{pos}++; next; }
        if ($ch eq ' ' || $ch eq "\t" || $ch eq "\r") { $lx->{pos}++; next; }
        if ($ch eq '#') {
            my $end = index($$t, "\n", $lx->{pos});
            $end = $lx->{len} if $end < 0;
            my $comment = substr($$t, $lx->{pos} + 1, $end - $lx->{pos} - 1);
            if ($comment =~ /\bnls:ignore\b[ \t]*([A-Za-z0-9, \t]*)/) {
                my @ids = grep { /^NLS\d{3}$/ } map { uc } split /[,\s]+/, $1;
                @ids = ('*') unless @ids;
                push @{ $st->{ctx}{ignores}{ $lx->{file} }{ $lx->{line} } }, @ids;
            }
            $lx->{pos} = $end;
            next;
        }
        last;
    }
    return undef if $lx->{pos} >= $lx->{len};

    my $line = $lx->{line};
    my $ch   = substr($$t, $lx->{pos}, 1);
    if ($ch eq ';' || $ch eq '{' || $ch eq '}') {
        $lx->{pos}++;
        return [$ch, $ch, $line];
    }

    my $value = '';
    if ($ch eq '"' || $ch eq "'") {
        my $quote = $ch;
        $lx->{pos}++;
        my $closed = 0;
        while ($lx->{pos} < $lx->{len}) {
            my $c = substr($$t, $lx->{pos}, 1);
            if ($c eq '\\' && $lx->{pos} + 1 < $lx->{len}) {
                $value .= unescape_pair(substr($$t, $lx->{pos} + 1, 1));
                $lx->{line}++ if substr($$t, $lx->{pos} + 1, 1) eq "\n";
                $lx->{pos} += 2;
                next;
            }
            if ($c eq $quote) { $closed = 1; $lx->{pos}++; last; }
            $lx->{line}++ if $c eq "\n";
            $value .= $c;
            $lx->{pos}++;
        }
        die "$lx->{file}:$line: unterminated quoted string\n" unless $closed;
        return ['w', $value, $line];
    }

    while ($lx->{pos} < $lx->{len}) {
        my $c = substr($$t, $lx->{pos}, 1);
        if ($c eq '\\' && $lx->{pos} + 1 < $lx->{len}) {
            $value .= unescape_pair(substr($$t, $lx->{pos} + 1, 1));
            $lx->{pos} += 2;
            next;
        }
        if ($c eq '$' && substr($$t, $lx->{pos}, 2) eq '${') {
            my $end = index($$t, '}', $lx->{pos});
            my $nl  = index($$t, "\n", $lx->{pos});
            if ($end > 0 && ($nl < 0 || $end < $nl)) {
                $value .= substr($$t, $lx->{pos}, $end - $lx->{pos} + 1);
                $lx->{pos} = $end + 1;
                next;
            }
        }
        last if $c eq ' ' || $c eq "\t" || $c eq "\r" || $c eq "\n" || $c eq ';' || $c eq '{' || $c eq '}';
        $value .= $c;
        $lx->{pos}++;
    }
    return ['w', $value, $line];
}

sub unescape_pair {
    my ($c) = @_;
    return $c   if $c eq '"' || $c eq "'" || $c eq '\\';
    return "\t" if $c eq 't';
    return "\r" if $c eq 'r';
    return "\n" if $c eq 'n';
    return '\\' . $c;
}

sub lex_skip_raw_block {
    my ($lx, $line) = @_;
    my $t     = \$lx->{text};
    my $depth = 1;
    while ($lx->{pos} < $lx->{len}) {
        my $rest2 = substr($$t, $lx->{pos}, 2);
        my $c     = substr($rest2, 0, 1);
        if ($c eq "\n") { $lx->{line}++; $lx->{pos}++; next; }
        if ($rest2 eq '--') {
            if (substr($$t, $lx->{pos}, 4) =~ /^--\[(=*)\[/) {
                my $close = ']' . $1 . ']';
                my $end = index($$t, $close, $lx->{pos});
                $end = $lx->{len} if $end < 0;
                $lx->{line} += (substr($$t, $lx->{pos}, $end - $lx->{pos}) =~ tr/\n//);
                $lx->{pos} = $end + length $close;
                next;
            }
            my $end = index($$t, "\n", $lx->{pos});
            $lx->{pos} = $end < 0 ? $lx->{len} : $end;
            next;
        }
        if ($c eq '"' || $c eq "'") {
            my $q = $c;
            $lx->{pos}++;
            while ($lx->{pos} < $lx->{len}) {
                my $d = substr($$t, $lx->{pos}, 1);
                if ($d eq '\\') { $lx->{pos} += 2; next; }
                $lx->{line}++ if $d eq "\n";
                $lx->{pos}++;
                last if $d eq $q;
            }
            next;
        }
        if ($c eq '{') { $depth++; }
        if ($c eq '}') {
            $depth--;
            if ($depth == 0) { $lx->{pos}++; return; }
        }
        $lx->{pos}++;
    }
    die "$lx->{file}:$line: unterminated lua block\n";
}

sub parse_block {
    my ($st, $lx, $top, $depth) = @_;
    my @out;
    while (1) {
        my $tok = lex_next($st, $lx);
        if (!defined $tok) {
            die "$lx->{file}:$lx->{line}: unexpected end of file, a block is missing its closing brace\n" unless $top;
            last;
        }
        if ($tok->[0] eq '}') {
            die "$lx->{file}:$tok->[2]: unexpected closing brace\n" if $top;
            last;
        }
        if ($tok->[0] eq ';') { next; }
        die "$lx->{file}:$tok->[2]: unexpected opening brace\n" if $tok->[0] eq '{';

        my $d = { name => $tok->[1], args => [], file => $lx->{file}, line => $tok->[2] };
        while (1) {
            my $arg = lex_next($st, $lx);
            die "$lx->{file}:$d->{line}: directive \"$d->{name}\" is not terminated by a semicolon\n" unless defined $arg;
            if ($arg->[0] eq 'w') { push @{ $d->{args} }, $arg->[1]; next; }
            if ($arg->[0] eq ';') { last; }
            if ($arg->[0] eq '{') {
                if ($d->{name} =~ /_by_lua_block\z/) {
                    lex_skip_raw_block($lx, $d->{line});
                    $d->{block} = [];
                } else {
                    $d->{block} = parse_block($st, $lx, 0, $depth);
                }
                last;
            }
            die "$lx->{file}:$arg->[2]: unexpected closing brace inside directive \"$d->{name}\"\n";
        }
        if ($d->{name} eq 'include' && !$d->{block} && @{ $d->{args} } == 1) {
            push @out, expand_include($st, $d, $depth);
            next;
        }
        push @out, $d;
    }
    return \@out;
}

sub expand_include {
    my ($st, $d, $depth) = @_;
    my $ctx     = $st->{ctx};
    my $pattern = $d->{args}[0];
    my $is_glob = $pattern =~ /[*?\[]/ ? 1 : 0;
    my @out;
    my @paths;

    if ($st->{vfs}) {
        my $full = $pattern =~ m{^/} ? $pattern : "$st->{prefix}/$pattern";
        $full =~ s{/+}{/}g;
        if ($is_glob) {
            my $re = glob_to_regex($full);
            @paths = sort grep { $_ =~ $re } keys %{ $st->{vfs} };
        } elsif (exists $st->{vfs}{$full}) {
            @paths = ($full);
        }
        if (!@paths && !$is_glob) {
            pending_finding($ctx, 'NLS015', $d, "include $pattern is not part of the dump", "include|$pattern");
        }
        for my $p (@paths) {
            $ctx->{includes}++;
            push @out, @{ parse_file($st, $p, $st->{vfs}{$p}, $depth + 1) };
        }
        return @out;
    }

    my $full = File::Spec->file_name_is_absolute($pattern)
        ? (defined $ctx->{opt}{sysroot} ? File::Spec->catfile($ctx->{opt}{sysroot}, $pattern) : $pattern)
        : File::Spec->catfile($st->{prefix}, $pattern);
    if ($is_glob) {
        @paths = sort grep { -f $_ } bsd_glob($full, File::Glob::GLOB_BRACE() | File::Glob::GLOB_QUOTE());
    } else {
        @paths = ($full);
    }
    for my $p (@paths) {
        my $text = slurp($p);
        if (!defined $text) {
            pending_finding($ctx, 'NLS015', $d, "include $pattern could not be read at $p", "include|$pattern");
            next;
        }
        $ctx->{includes}++;
        push @out, @{ parse_file($st, display_path($p), $text, $depth + 1) };
    }
    return @out;
}

sub glob_to_regex {
    my ($glob) = @_;
    my $re = '';
    my $i  = 0;
    while ($i < length $glob) {
        my $c = substr($glob, $i, 1);
        if    ($c eq '*') { $re .= '[^/]*'; }
        elsif ($c eq '?') { $re .= '[^/]'; }
        elsif ($c eq '[') {
            my $end = index($glob, ']', $i + 2);
            if ($end < 0) { $re .= '\\['; }
            else {
                my $body = substr($glob, $i + 1, $end - $i - 1);
                $body =~ s/^!/^/;
                $body =~ s/\\/\\\\/g;
                $re .= "[$body]";
                $i = $end;
            }
        }
        else { $re .= quotemeta $c; }
        $i++;
    }
    return qr/\A$re\z/;
}

sub display_path {
    my ($p) = @_;
    my $cwd = getcwd();
    return $p unless defined $cwd && File::Spec->file_name_is_absolute($p);
    my $rel = File::Spec->abs2rel($p, $cwd);
    return $rel =~ /^\.\./ ? $p : $rel;
}

sub pending_finding {
    my ($ctx, $id, $where, $message, $key) = @_;
    push @{ $ctx->{pending} }, [$id, $where, $message, $key];
}

sub build_servers {
    my ($ctx) = @_;
    my $tree = $ctx->{tree};
    my @http = grep { $_->{name} eq 'http' && $_->{block} } @$tree;
    my $n = 0;
    for my $h (@http) {
        my $http_scope = { kind => 'http', dirs => [grep { $_->{name} ne 'server' } @{ $h->{block} }], up => undef,
                           file => $h->{file}, line => $h->{line} };
        for my $s (grep { $_->{name} eq 'server' && $_->{block} } @{ $h->{block} }) {
            push @{ $ctx->{servers} }, build_server($ctx, $s->{block}, $s, $http_scope, ++$n);
        }
    }
    my $loose_scope = { kind => 'http', dirs => [grep { !$_->{block} } @$tree], up => undef, file => $ctx->{main}, line => 1 };
    for my $s (grep { $_->{name} eq 'server' && $_->{block} } @$tree) {
        push @{ $ctx->{servers} }, build_server($ctx, $s->{block}, $s, $loose_scope, ++$n);
    }
    if (!@{ $ctx->{servers} } && grep { $_->{name} eq 'location' } @$tree) {
        my $fake = { file => $ctx->{main}, line => 1 };
        my $empty = { kind => 'http', dirs => [], up => undef, file => $ctx->{main}, line => 1 };
        push @{ $ctx->{servers} }, build_server($ctx, $tree, $fake, $empty, ++$n);
    }
    for my $p (@{ $ctx->{pending} }) {
        my ($id, $where, $message, $key) = @$p;
        add_finding($ctx, id => $id, where => $where, server => undef, message => $message, key => $key);
    }
    $ctx->{pending} = [];
}

sub build_server {
    my ($ctx, $block, $d, $http_scope, $index) = @_;
    my $srv = {
        kind => 'server', index => $index, file => $d->{file}, line => $d->{line}, up => $http_scope,
        dirs => [], static => [], regex => [], named => [], children => [], all => [],
        names => [], listens => [], default => 0,
    };
    for my $x (@$block) {
        if ($x->{name} eq 'location' && $x->{block}) { build_location($ctx, $srv, $srv, $x); next; }
        push @{ $srv->{dirs} }, $x;
        if ($x->{name} eq 'server_name') { push @{ $srv->{names} }, @{ $x->{args} }; }
        if ($x->{name} eq 'listen' && @{ $x->{args} }) {
            my $addr = $x->{args}[0];
            my $port = 80;
            if    ($addr =~ /^unix:/)   { $port = 0; }
            elsif ($addr =~ /:(\d+)$/)  { $port = $1; }
            elsif ($addr =~ /^(\d+)$/)  { $port = $1; }
            push @{ $srv->{listens} }, $port;
            $srv->{default} = 1 if grep { /^default(?:_server)?$/ } @{ $x->{args} };
        }
    }
    push @{ $srv->{listens} }, 80 unless @{ $srv->{listens} };
    my ($first) = grep { length && $_ ne '_' } @{ $srv->{names} };
    $srv->{label} = defined $first ? $first : '_';
    $srv->{merge_slashes} = 1;
    my $ms = nearest_directive($srv, 'merge_slashes');
    $srv->{merge_slashes} = 0 if $ms && lc($ms->{args}[0] // '') eq 'off';
    return $srv;
}

sub build_location {
    my ($ctx, $srv, $parent, $d) = @_;
    my @a = @{ $d->{args} };
    my ($mod, $pat, $bad);
    if (@a == 2) {
        ($mod, $pat) = @a;
        $bad = "unknown location modifier \"$mod\"" unless $mod =~ /^(?:=|\^~|~|~\*)$/;
    } elsif (@a == 1) {
        if    ($a[0] =~ /^(=|\^~|~\*|~)(.+)\z/s) { ($mod, $pat) = ($1, $2); }
        elsif ($a[0] =~ /^@/)                    { ($mod, $pat) = ('@', $a[0]); }
        else                                     { ($mod, $pat) = ('', $a[0]); }
    } else {
        $bad = 'location takes one or two arguments';
    }
    if ($bad) {
        die "$d->{file}:$d->{line}: $bad\n";
    }
    my $loc = {
        kind => 'location', mod => $mod, pat => $pat, file => $d->{file}, line => $d->{line},
        up => $parent, parent => ($parent->{kind} eq 'location' ? $parent : undef), server => $srv,
        dirs => [], static => [], regex => [], children => [], id => scalar(@{ $srv->{all} }),
        depth => ($parent->{kind} eq 'location' ? $parent->{depth} + 1 : 0),
    };
    $loc->{label} = 'location ' . (length $mod && $mod ne '@' ? "$mod " : '') . $pat;
    push @{ $srv->{all} }, $loc;
    push @{ $parent->{children} }, $loc;

    if ($mod eq '@') {
        push @{ $srv->{named} }, $loc;
    } elsif ($mod eq '~' || $mod eq '~*') {
        my $re = compile_regex($pat, $mod eq '~*');
        if (ref $re) {
            $loc->{re} = $re;
            my ($samples, $tails) = regex_samples($pat, $re);
            $loc->{samples} = $samples;
            $loc->{tails}   = $tails;
        } else {
            $loc->{re_error} = $re;
            $loc->{samples}  = [];
            $loc->{tails}    = [];
        }
        push @{ $parent->{regex} }, $loc;
    } else {
        push @{ $parent->{static} }, $loc;
    }

    for my $x (@{ $d->{block} }) {
        if ($x->{name} eq 'location' && $x->{block}) { build_location($ctx, $srv, $loc, $x); next; }
        push @{ $loc->{dirs} }, $x;
    }
    $loc->{auto_redirect} = ($mod ne '@' && $mod ne '~' && $mod ne '~*' && $pat =~ m{/\z}
        && (grep { $PASS_HANDLERS{ $_->{name} } } @{ $loc->{dirs} })) ? 1 : 0;
    return $loc;
}

sub compile_regex {
    my ($pat, $ci) = @_;
    my $re = eval {
        local $SIG{__WARN__} = sub { };
        $ci ? qr/$pat/i : qr/$pat/;
    };
    return $re if ref $re;
    my $err = $@ || 'unknown regex error';
    $err =~ s/ at \S+ line \d+.*//s;
    $err =~ s/\s+\z//;
    return $err;
}

sub regex_samples {
    my ($pat, $re) = @_;
    my @chars = split //, $pat;
    my $pos   = 0;
    my $raw   = eval { sampler_alternation(\@chars, \$pos) };
    return ([], []) if !$raw || $pos < @chars;
    my (@samples, @tails, %seen, %tseen);
    for my $s (@$raw) {
        next if $s =~ /[\x00-\x1f\x7f]/;
        if (length $s && !$tseen{$s}++ && @tails < MAX_TAILS_PER_REGEX) { push @tails, $s; }
        my $u = $s =~ m{^/} ? $s : '/' . FILLER . $s;
        next if $seen{$u}++;
        next unless probe_is_canonical($u);
        next unless $u =~ $re;
        push @samples, $u;
        last if @samples >= MAX_SAMPLES_PER_REGEX;
    }
    return (\@samples, \@tails);
}

sub sampler_alternation {
    my ($c, $p) = @_;
    my @branches;
    while (1) {
        push @branches, sampler_sequence($c, $p);
        if ($$p < @$c && $c->[$$p] eq '|') { $$p++; next; }
        last;
    }
    my (@out, %seen);
    my $more = 1;
    my $i    = 0;
    while ($more && @out < SAMPLER_PRODUCT_CAP) {
        $more = 0;
        for my $b (@branches) {
            next if $i >= @$b;
            $more = 1;
            push @out, $b->[$i] unless $seen{ $b->[$i] }++;
        }
        $i++;
    }
    return \@out;
}

sub sampler_sequence {
    my ($c, $p) = @_;
    my $acc = [''];
    while ($$p < @$c) {
        my $ch = $c->[$$p];
        last if $ch eq '|' || $ch eq ')';
        my $atom = sampler_atom($c, $p);
        $atom = sampler_quantifier($c, $p, $atom);
        my (@next, %seen);
        OUTER: for my $x (@$acc) {
            for my $y (@$atom) {
                my $s = $x . $y;
                next if $seen{$s}++;
                push @next, $s;
                last OUTER if @next >= SAMPLER_PRODUCT_CAP;
            }
        }
        $acc = \@next;
    }
    return $acc;
}

sub sampler_atom {
    my ($c, $p) = @_;
    my $ch = $c->[$$p++];
    if ($ch eq '(') {
        my $lookaround = 0;
        if ($$p < @$c && $c->[$$p] eq '?') {
            my $hi = $$p + 48 < $#$c ? $$p + 48 : $#$c;
            my $rest = join '', @$c[$$p .. $hi];
            if    ($rest =~ /^\?(?:<[=!]|[=!])/)                         { $lookaround = 1; $$p += length $&; }
            elsif ($rest =~ /^\?(?:P?<[A-Za-z_]\w*>|'[A-Za-z_]\w*')/)    { $$p += length $&; }
            elsif ($rest =~ /^\?[a-zA-Z^-]*:/)                           { $$p += length $&; }
            elsif ($rest =~ /^\?[a-zA-Z^-]+\)/)                          { $$p += length $&; return ['']; }
            elsif ($rest =~ /^\?[>|]/)                                   { $$p += 2; }
            else { die "unsupported group\n"; }
        }
        my $inner = sampler_alternation($c, $p);
        die "unbalanced group\n" unless $$p < @$c && $c->[$$p] eq ')';
        $$p++;
        return $lookaround ? [''] : $inner;
    }
    if ($ch eq '[') {
        my $start = $$p;
        my $i     = $$p;
        $i++ if $i < @$c && $c->[$i] eq '^';
        $i++ if $i < @$c && $c->[$i] eq ']';
        while ($i < @$c && $c->[$i] ne ']') {
            if ($c->[$i] eq '\\') { $i += 2; next; }
            if ($c->[$i] eq '[' && $i + 1 < @$c && $c->[ $i + 1 ] eq ':') {
                my $j = $i + 2;
                $j++ while $j + 1 < @$c && !($c->[$j] eq ':' && $c->[ $j + 1 ] eq ']');
                $i = $j + 2;
                next;
            }
            $i++;
        }
        die "unterminated class\n" if $i >= @$c;
        my $body = join '', @$c[$start .. $i - 1];
        $$p = $i + 1;
        my $cls = eval { local $SIG{__WARN__} = sub { }; qr/[$body]/ } or die "bad class\n";
        for my $cand (@CLASS_PREFERENCE) {
            return [$cand] if $cand =~ $cls;
        }
        die "empty class\n";
    }
    if ($ch eq '\\') {
        die "dangling escape\n" unless $$p < @$c;
        my $e = $c->[$$p++];
        return ['7'] if $e eq 'd';
        return ['a'] if $e eq 'w' || $e eq 'D' || $e eq 'S';
        return ['-'] if $e eq 'W';
        return [' '] if $e eq 's' || $e eq 'h';
        return ['']  if $e =~ /^[bBAZzGKQEntrfv]$/;
        return ['']  if $e =~ /^\d$/;
        if ($e eq 'x') {
            my $hex = '';
            while ($$p < @$c && length($hex) < 2 && $c->[$$p] =~ /^[0-9A-Fa-f]$/) { $hex .= $c->[$$p++]; }
            die "bad hex escape\n" unless length $hex;
            return [ chr hex $hex ];
        }
        return [$e];
    }
    return ['x'] if $ch eq '.';
    return ['']  if $ch eq '^' || $ch eq '$';
    return [$ch];
}

sub sampler_quantifier {
    my ($c, $p, $atom) = @_;
    return $atom unless $$p < @$c;
    my $q = $c->[$$p];
    my $res;
    if ($q eq '*' || $q eq '?') {
        $$p++;
        $res = [@$atom, ''];
    } elsif ($q eq '+') {
        $$p++;
        $res = [@$atom];
    } elsif ($q eq '{') {
        my $hi = $$p + 16 < $#$c ? $$p + 16 : $#$c;
        my $rest = join '', @$c[$$p .. $hi];
        return $atom unless $rest =~ /^\{(\d+)(?:,\d*)?\}/;
        $$p += length $&;
        my $n = $1 > 64 ? 64 : $1;
        $res = $n == 0 ? [@$atom, ''] : [map { $_ x $n } @$atom];
    } else {
        return $atom;
    }
    $$p++ if $$p < @$c && ($c->[$$p] eq '?' || $c->[$$p] eq '+');
    my %seen;
    return [grep { !$seen{$_}++ } @$res];
}

sub probe_is_canonical {
    my ($u) = @_;
    return 0 unless $u =~ m{^/};
    return 0 if length($u) > MAX_PROBE_LENGTH;
    return 0 if $u =~ m{//} || $u =~ m{/\.\.?(?:/|\z)};
    return 0 if $u =~ /[\x00-\x1f\x7f?#]/;
    return 1;
}

sub normalize_uri {
    my ($raw, $merge) = @_;
    my $u = $raw;
    $u =~ s/[?#].*\z//s;
    return (undef, 'request path must start with a slash') unless $u =~ m{^/};
    return (undef, 'malformed percent escape, nginx answers 400') if $u =~ /%(?![0-9A-Fa-f]{2})/;
    $u =~ s/%([0-9A-Fa-f]{2})/chr(hex $1)/ge;
    return (undef, 'null byte in path, nginx answers 400') if $u =~ /\0/;
    $u =~ s{/+}{/}g if $merge;
    my @segs = split m{/}, substr($u, 1), -1;
    my @out;
    for my $i (0 .. $#segs) {
        my $seg  = $segs[$i];
        my $last = $i == $#segs;
        if ($seg eq '.') {
            push @out, '' if $last;
        } elsif ($seg eq '..') {
            return (undef, 'path climbs above the root, nginx answers 400') unless @out;
            pop @out;
            push @out, '' if $last;
        } else {
            push @out, $seg;
        }
    }
    return ('/' . join('/', @out), undef);
}

sub resolve_level {
    my ($lvl, $uri, $r) = @_;
    my ($exact, $same, $best, $redir);
    for my $l (@{ $lvl->{static} }) {
        my $pat = $l->{pat};
        if ($l->{mod} eq '=') {
            $exact = $l if !$exact && $pat eq $uri;
        } elsif (index($uri, $pat) == 0) {
            $same = $l if !$same && length($pat) == length($uri);
            $best = $l if !$best || length($pat) > length($best->{pat});
        }
        $redir = $l if !$redir && $l->{auto_redirect} && $pat eq "$uri/";
    }
    my $where = $lvl->{kind} eq 'server' ? 'at server level' : "inside $lvl->{label}";
    if ($exact) {
        $r->{loc} = $exact;
        push @{ $r->{steps} }, "static scan $where: exact match " . describe($exact) . ", search stops";
        return 'OK';
    }
    if ($redir && !$same) {
        $r->{loc}      = $redir;
        $r->{redirect} = "$uri/";
        push @{ $r->{steps} }, "static scan $where: " . describe($redir)
            . " passes to an upstream and the path only lacks the trailing slash, nginx answers 301";
        return 'DONE';
    }
    my $rc      = 'DECLINED';
    my $noregex = 0;
    if ($best) {
        $r->{loc} = $best;
        push @{ $r->{chain} }, $best;
        $noregex = $best->{mod} eq '^~' ? 1 : 0;
        push @{ $r->{steps} }, "static scan $where: longest prefix is " . describe($best)
            . ($noregex ? ', ^~ turns off the regex scan at this level' : '');
        if (@{ $best->{static} } || @{ $best->{regex} }) {
            $rc = resolve_level($best, $uri, $r);
            return $rc if $rc eq 'OK' || $rc eq 'DONE';
        }
        $rc = 'AGAIN';
    } elsif (@{ $lvl->{static} }) {
        push @{ $r->{steps} }, "static scan $where: no prefix matches";
    }
    if (!$noregex) {
        my $list = $lvl->{regex};
        for my $i (0 .. $#$list) {
            my $l = $list->[$i];
            next unless $l->{re};
            next unless $uri =~ $l->{re};
            push @{ $r->{steps} }, "regex scan $where: " . describe($l) . ' is the first regex that matches'
                . ($i ? " after $i earlier regex location" . ($i == 1 ? '' : 's') . ' did not' : '');
            for my $j ($i + 1 .. $#$list) {
                my $later = $list->[$j];
                next unless $later->{re} && $uri =~ $later->{re};
                push @{ $r->{order_losers} }, $later;
            }
            $r->{loc} = $l;
            push @{ $r->{regex_winners} }, $l;
            resolve_level($l, $uri, $r) if @{ $l->{static} } || @{ $l->{regex} };
            return 'OK';
        }
        push @{ $r->{steps} }, "regex scan $where: none of " . scalar(@$list) . ' regex locations match' if @$list;
    }
    return $rc;
}

sub resolve {
    my ($srv, $uri) = @_;
    my $r = { loc => undef, chain => [], steps => [], order_losers => [], regex_winners => [], redirect => undef };
    resolve_level($srv, $uri, $r);
    return $r;
}

sub describe {
    my ($x) = @_;
    return 'server block' unless $x;
    return "$x->{label} ($x->{file}:$x->{line})";
}

sub is_within {
    my ($outer, $inner) = @_;
    for (my $x = $inner; $x; $x = $x->{parent}) {
        return 1 if $x == $outer;
    }
    return 0;
}

sub nearest_directive {
    my ($scope, @names) = @_;
    my %want = map { $_ => 1 } @names;
    for (my $s = $scope; $s; $s = $s->{up}) {
        for my $d (@{ $s->{dirs} }) {
            return $d if $want{ $d->{name} };
        }
    }
    return undef;
}

sub nearest_family {
    my ($scope, @names) = @_;
    my %want = map { $_ => 1 } @names;
    for (my $s = $scope; $s; $s = $s->{up}) {
        my @own = grep { $want{ $_->{name} } } @{ $s->{dirs} };
        return ($s, \@own) if @own;
    }
    return (undef, []);
}

sub guards_of {
    my ($scope) = @_;
    return $scope->{guards} if $scope->{guards};
    my %g;
    for my $n (@GUARD_VALUE_DIRECTIVES) {
        my $d = nearest_directive($scope, $n);
        next unless $d;
        my $v = join ' ', @{ $d->{args} };
        next if lc $v eq 'off';
        $g{$n} = $v;
    }
    my (undef, $acl) = nearest_family($scope, 'allow', 'deny');
    $g{access} = join '; ', map { "$_->{name} " . join(' ', @{ $_->{args} }) } @$acl if @$acl;
    my $lua = nearest_directive($scope, @GUARD_LUA_DIRECTIVES);
    $g{access_by_lua} = "$lua->{file}:$lua->{line}" if $lua;
    for (my $s = $scope; $s && $s->{kind} eq 'location'; $s = $s->{up}) {
        if (grep { $_->{name} eq 'internal' } @{ $s->{dirs} }) { $g{internal} = 'on'; last; }
    }
    my ($le) = grep { $_->{name} eq 'limit_except' && $_->{block} } @{ $scope->{dirs} };
    if ($le) {
        $g{limit_except} = join(' ', @{ $le->{args} }) . ' { '
            . join('; ', map { "$_->{name} " . join(' ', @{ $_->{args} }) } @{ $le->{block} }) . ' }';
    }
    my ($ret) = grep { $_->{name} eq 'return' && @{ $_->{args} } && $_->{args}[0] =~ /^(?:4\d\d|444)$/ } @{ $scope->{dirs} };
    $g{return} = $ret->{args}[0] if $ret;
    $scope->{guards} = \%g;
    return \%g;
}

sub blocks_everything {
    my ($scope) = @_;
    my $g = guards_of($scope);
    return 1 if exists $g->{return};
    return 1 if exists $g->{internal};
    return 1 if exists $g->{access} && $g->{access} =~ /^deny all(?:;|\z)/;
    return 0;
}

sub handler_of {
    my ($scope) = @_;
    return undef unless $scope;
    for my $d (@{ $scope->{dirs} }) {
        return $d if $CONTENT_HANDLERS{ $d->{name} };
    }
    return undef;
}

sub build_probes {
    my ($srv, $cap) = @_;
    my (@probes, %seen);
    my $add = sub {
        my ($u) = @_;
        return if @probes >= $cap || $seen{$u}++;
        return unless probe_is_canonical($u);
        push @probes, $u;
    };
    my @locs    = grep { $_->{mod} ne '@' } @{ $srv->{all} };
    my @statics = grep { $_->{mod} !~ /~\*?\z/ || $_->{mod} eq '^~' } @locs;
    my @regexes = grep { $_->{re} } @locs;

    $add->('/');
    for my $l (@statics) {
        my $p = $l->{pat};
        $add->($p);
        next if $l->{mod} eq '=';
        (my $bare = $p) =~ s{/\z}{};
        $add->($p . FILLER);
        $add->("$bare/" . FILLER);
        $add->("$bare/" . FILLER . '/' . FILLER . '.txt');
        $add->($bare) if length $bare;
    }
    for my $l (@regexes) { $add->($_) for @{ $l->{samples} }; }

    for my $l (@statics) {
        next if $l->{mod} eq '=';
        my $p = $l->{pat};
        (my $bare = $p) =~ s{/\z}{};
        for my $rx (@regexes) {
            for my $t (@{ $rx->{tails} }) {
                $add->($bare . ($t =~ m{^/} ? $t : '/' . FILLER . $t));
                $add->($p . $t);
                $add->($p . FILLER . $t);
            }
        }
    }
    for my $a (@regexes) {
        for my $s (@{ $a->{samples} }) {
            (my $bare = $s) =~ s{/\z}{};
            for my $b (@regexes) {
                next if $a == $b;
                for my $t (@{ $b->{tails} }) {
                    $add->($s . $t);
                    $add->($s . FILLER . $t);
                    $add->($bare . ($t =~ m{^/} ? $t : '/' . FILLER . $t));
                }
            }
        }
    }
    return \@probes;
}

sub audit_server {
    my ($ctx, $srv) = @_;
    check_structure($ctx, $srv);
    check_replay($ctx, $srv);
    check_alias($ctx, $srv);
    check_proxy_pass($ctx, $srv);
    check_script_regex($ctx, $srv);
    check_inheritance($ctx, $srv);
    check_access_reset($ctx, $srv);
    check_if_blocks($ctx, $srv);
    check_named($ctx, $srv);
    check_merge_slashes($ctx, $srv);
}

sub check_structure {
    my ($ctx, $srv) = @_;
    for my $lvl ($srv, @{ $srv->{all} }) {
        my %seen;
        for my $l (@{ $lvl->{children} }) {
            next if $l->{mod} eq '~' || $l->{mod} eq '~*' || $l->{mod} eq '@';
            my $key = $l->{mod} eq '=' ? "=|$l->{pat}" : "p|$l->{pat}";
            if (my $first = $seen{$key}) {
                add_finding($ctx, id => 'NLS004', where => $l, server => $srv, location => $l,
                    message => "$l->{label} repeats " . describe($first)
                        . ($l->{mod} ne $first->{mod} ? '. A plain prefix and a ^~ prefix with the same path count as the same location' : '')
                        . '. nginx stops with "duplicate location" and the old configuration keeps serving.',
                    key => "dup|$key");
            } else {
                $seen{$key} = $l;
            }
        }
    }
    for my $l (@{ $srv->{all} }) {
        if ($l->{re_error}) {
            add_finding($ctx, id => 'NLS014', where => $l, server => $srv, location => $l,
                message => "The pattern of $l->{label} does not compile ($l->{re_error}). Check it with nginx -t. "
                    . 'The replay treats this location as never matching.',
                key => 're');
        }
        my $p = $l->{parent};
        next unless $p;
        my $why;
        if    ($l->{mod} eq '@') { $why = 'a named location can only sit at the server level'; }
        elsif ($p->{mod} eq '@') { $why = 'a location cannot sit inside a named location'; }
        elsif ($p->{mod} eq '=') { $why = 'a location cannot sit inside an exact match location'; }
        elsif ($l->{mod} !~ /^~/ && index($l->{pat}, $p->{pat}) != 0) {
            $why = $p->{mod} =~ /^~/
                ? 'a prefix or exact location is only accepted inside a regex location when its path starts with the literal regex text'
                : "its path is outside the parent prefix $p->{pat}";
        }
        next unless $why;
        add_finding($ctx, id => 'NLS012', where => $l, server => $srv, location => $l,
            message => "$l->{label} is nested in " . describe($p) . " but $why. nginx refuses to load this configuration.",
            key => 'nest');
    }
}

sub check_replay {
    my ($ctx, $srv) = @_;
    my $probes = build_probes($srv, $ctx->{opt}{'max-probes'});
    $srv->{probe_count} = scalar @$probes;
    my (%claims, %wins, %takers, %pairs);

    my $lose = sub {
        my ($target, $final, $uri, $kind) = @_;
        $claims{ $target->{id} }++;
        my $wid = $final ? $final->{id} : -1;
        my $slot = \$takers{ $target->{id} }{$wid};
        $$slot = $uri if !defined $$slot || probe_cost($uri) < probe_cost($$slot);
        my $pk = "$target->{id}|$wid";
        if (!$pairs{$pk} || probe_cost($uri) < probe_cost($pairs{$pk}{uri})) {
            $pairs{$pk} = { target => $target, winner => $final, uri => $uri, kind => $kind };
        }
    };

    for my $uri (@$probes) {
        my $r     = resolve($srv, $uri);
        my $final = $r->{loc};
        next if $r->{redirect};
        for (my $x = $final; $x; $x = $x->{parent}) {
            $wins{ $x->{id} }++;
            $claims{ $x->{id} }++;
        }
        for my $t (@{ $r->{chain} }) {
            next if $final && is_within($t, $final);
            $lose->($t, $final, $uri, 'regex');
        }
        for my $t (@{ $r->{order_losers} }) {
            next if $final && is_within($t, $final);
            $lose->($t, $final, $uri, 'order');
        }
    }

    my (%bypass, %script, %reported_pair);
    for my $pk (sort keys %pairs) {
        my $p = $pairs{$pk};
        my ($t, $w) = @$p{qw(target winner)};
        next unless $w;
        my $gt = guards_of($t);
        my $gw = guards_of($w);
        next unless %$gt;
        next if blocks_everything($w);
        my @missing = sort grep { !exists $gw->{$_} || $gw->{$_} ne $gt->{$_} } keys %$gt;
        next unless @missing;
        my ($winner_rx) = grep { $_->{mod} =~ /^~/ } reverse ancestry($w);
        my $scoped = 0;
        if ($winner_rx && $winner_rx->{pat} =~ /^\^/) {
            $scoped = $t->{mod} =~ /^~/ ? 1
                : (@{ $winner_rx->{samples} } && !grep { index($_, $t->{pat}) != 0 } @{ $winner_rx->{samples} }) ? 1 : 0;
        }
        push @{ $bypass{ $t->{id} } }, { %$p, missing => \@missing, scoped => $scoped,
            detail => join(', ', map { exists $gw->{$_} ? "$_ differs ($gt->{$_} against $gw->{$_})" : "$_ ($gt->{$_})" } @missing) };
        $reported_pair{$pk} = 1;
    }
    for my $l (@{ $srv->{all} }) {
        my $hits = $bypass{ $l->{id} } or next;
        my $up = $l->{parent};
        if ($up && $bypass{ $up->{id} } && !guards_differ(guards_of($l), guards_of($up)) && !guards_differ(guards_of($up), guards_of($l))) {
            my %covered = map { $_->{winner}{id} => 1 } @{ $bypass{ $up->{id} } };
            next unless grep { !$covered{ $_->{winner}{id} } } @$hits;
        }
        my @hits  = sort { $a->{scoped} <=> $b->{scoped} || $a->{winner}{id} <=> $b->{winner}{id} } @$hits;
        my $first = $hits[0];
        my $how = $first->{kind} eq 'order'
            ? 'Both are regex locations and nginx takes the first one that matches in file order.'
            : 'nginx remembers the longest prefix, then lets the first matching regex override it unless the prefix uses ^~.';
        my $tail = $first->{scoped}
            ? ' The winning pattern is anchored under this path, so it may be a deliberate exception. Confirm it.'
            : ' The winning pattern is not scoped to this path, so the hole is probably accidental.';
        my $also = @hits > 1
            ? ' Same result through: ' . join('; ', map { "$_->{uri} to " . describe($_->{winner}) } @hits[1 .. $#hits]) . '.'
            : '';
        add_finding($ctx, id => 'NLS001', where => $l, server => $srv, location => $l,
            severity => ($first->{scoped} ? 'medium' : 'high'),
            message => "A request for $first->{uri} matches $l->{label} but is served by " . describe($first->{winner})
                . ", which does not apply: $first->{detail}. $how$tail$also",
            key => 'bypass|' . join('|', map { $_->{winner}{label} } @hits),
            evidence => { routes => [map { { example => $_->{uri}, winner => $_->{winner}{label},
                winner_at => "$_->{winner}{file}:$_->{winner}{line}", missing => $_->{missing}, kind => $_->{kind},
                scoped => ($_->{scoped} ? JSON::PP::true : JSON::PP::false) } } @hits] });
    }

    for my $pk (sort keys %pairs) {
        next if $reported_pair{$pk};
        my $p = $pairs{$pk};
        my ($t, $w) = @$p{qw(target winner)};
        next unless $w && $p->{kind} eq 'regex';
        next if $t->{mod} =~ /^~/;
        next unless $t->{pat} =~ $CONTENT_PREFIX_RE;
        next if handler_of($t);
        my $h = handler_of($w);
        next unless $h && $SCRIPT_HANDLERS{ $h->{name} };
        push @{ $script{ $t->{id} } }, { %$p, handler => $h->{name} };
    }
    for my $l (@{ $srv->{all} }) {
        my $hits = $script{ $l->{id} } or next;
        my @hits  = sort { $a->{winner}{id} <=> $b->{winner}{id} } @$hits;
        my $first = $hits[0];
        my $also = @hits > 1
            ? ' Same result through: ' . join('; ', map { "$_->{uri} to " . describe($_->{winner}) } @hits[1 .. $#hits]) . '.'
            : '';
        add_finding($ctx, id => 'NLS002', where => $l, server => $srv, location => $l,
            severity => ($l->{pat} =~ $WRITABLE_PREFIX_RE ? 'high' : 'medium'),
            message => "A request for $first->{uri} sits under $l->{label} but is handed to $first->{handler} by "
                . describe($first->{winner}) . '. If anything can write a file into this path, that file runs as code. '
                . "Use ^~ on the prefix or add a nested regex location that returns 403.$also",
            key => 'script|' . join('|', map { $_->{winner}{label} } @hits),
            evidence => { routes => [map { { example => $_->{uri}, winner => $_->{winner}{label},
                winner_at => "$_->{winner}{file}:$_->{winner}{line}", handler => $_->{handler} } } @hits] });
    }

    my %dead;
    for my $l (@{ $srv->{all} }) {
        next if $l->{mod} eq '@';
        next unless $claims{ $l->{id} };
        next if $wins{ $l->{id} };
        $dead{ $l->{id} } = 1;
    }
    for my $l (@{ $srv->{all} }) {
        next unless $dead{ $l->{id} };
        next if $l->{parent} && $dead{ $l->{parent}{id} };
        my @who;
        for my $wid (sort { $a <=> $b } keys %{ $takers{ $l->{id} } || {} }) {
            my $uri = $takers{ $l->{id} }{$wid};
            my $w   = $wid >= 0 ? $srv->{all}[$wid] : undef;
            push @who, { example => $uri, winner => ($w ? $w->{label} : 'server block'),
                         winner_at => ($w ? "$w->{file}:$w->{line}" : "$srv->{file}:$srv->{line}") };
        }
        my $text = join '; ', map { "$_->{example} goes to $_->{winner} ($_->{winner_at})" } @who[0 .. ($#who > 2 ? 2 : $#who)];
        add_finding($ctx, id => 'NLS003', where => $l, server => $srv, location => $l,
            message => "None of the $claims{ $l->{id} } probe paths that match $l->{label} end up in it. $text. "
                . 'Either the block is dead configuration or its matcher is not doing what it was written for.',
            key => 'dead',
            evidence => { probes => $claims{ $l->{id} }, takers => \@who });
    }
}

sub probe_cost {
    my ($u) = @_;
    return length($u) + ($u =~ m{/\.} ? 1000 : 0);
}

sub ancestry {
    my ($l) = @_;
    my @chain;
    for (my $x = $l; $x; $x = $x->{parent}) { unshift @chain, $x; }
    return @chain;
}

sub check_alias {
    my ($ctx, $srv) = @_;
    for my $l (@{ $srv->{all} }) {
        next unless $l->{mod} eq '' || $l->{mod} eq '^~';
        next if $l->{pat} =~ m{/\z};
        for my $d (grep { $_->{name} eq 'alias' && @{ $_->{args} } } @{ $l->{dirs} }) {
            my $target = $d->{args}[0];
            next unless $target =~ m{/\z};
            (my $parent = $target) =~ s{[^/]+/\z}{};
            add_finding($ctx, id => 'NLS005', where => $d, server => $srv, location => $l,
                message => "$l->{label} has no trailing slash but alias $target does. "
                    . "A request for $l->{pat}../ maps to $target../ and reads from $parent. "
                    . "Write the location as $l->{pat}/ or drop the slash from the alias.",
                key => 'alias',
                evidence => { example => "$l->{pat}../", alias => $target });
        }
    }
}

sub proxy_uri_part {
    my ($value) = @_;
    return undef if $value =~ /\$/;
    return $1 if $value =~ m{^[a-z]+://unix:[^:]+:(.*)\z}i;
    return $1 if $value =~ m{^[a-z]+://[^/]+(.*)\z}i;
    return '';
}

sub check_proxy_pass {
    my ($ctx, $srv) = @_;
    for my $l (@{ $srv->{all} }) {
        for my $d (grep { $_->{name} eq 'proxy_pass' && @{ $_->{args} } } @{ $l->{dirs} }) {
            my $part = proxy_uri_part($d->{args}[0]);
            next unless defined $part && length $part;
            if ($l->{mod} =~ /^~/ || $l->{mod} eq '@') {
                my $what = $l->{mod} eq '@' ? 'a named location' : 'a regex location';
                add_finding($ctx, id => 'NLS008', where => $d, server => $srv, location => $l,
                    message => "proxy_pass $d->{args}[0] carries the URI part \"$part\" inside $what. "
                        . 'nginx cannot tell which part of the path to replace and refuses to load the file. '
                        . 'Drop the URI part or build it from variables.',
                    key => 'ppuri');
                next;
            }
            if ($part =~ m{/\z} && $l->{pat} !~ m{/\z}) {
                (my $stem = $part) =~ s{/\z}{};
                add_finding($ctx, id => 'NLS009', where => $d, server => $srv, location => $l,
                    message => "$l->{label} has no trailing slash but proxy_pass replaces it with \"$part\". "
                        . "$l->{pat}" . FILLER . " reaches the upstream as $part" . FILLER
                        . " and $l->{pat}../x reaches it as $part../x, outside the path you meant to expose. "
                        . "Write the location as $l->{pat}/.",
                    key => 'ppslash',
                    evidence => { example => "$l->{pat}../x", upstream_path => "$part../x" });
            }
        }
        for my $blk (grep { $_->{block} && ($_->{name} eq 'if' || $_->{name} eq 'limit_except') } @{ $l->{dirs} }) {
            for my $d (grep { $_->{name} eq 'proxy_pass' && @{ $_->{args} } } @{ $blk->{block} }) {
                my $part = proxy_uri_part($d->{args}[0]);
                next unless defined $part && length $part;
                add_finding($ctx, id => 'NLS008', where => $d, server => $srv, location => $l,
                    message => "proxy_pass $d->{args}[0] carries the URI part \"$part\" inside $blk->{name}. "
                        . 'nginx refuses a URI part there. Drop it or build it from variables.',
                    key => "ppuri|$blk->{name}");
            }
        }
    }
}

sub check_script_regex {
    my ($ctx, $srv) = @_;
    for my $l (@{ $srv->{all} }) {
        next unless $l->{re};
        my $h = handler_of($l);
        next unless $h && ($SCRIPT_HANDLERS{ $h->{name} } || $h->{name} eq 'proxy_pass');
        my %ext;
        for my $s (@{ $l->{samples} }) { $ext{$1} = 1 if $s =~ /\.([A-Za-z0-9]{1,8})\z/; }
        for my $t (@{ $l->{tails} })   { $ext{$1} = 1 if $t =~ /\.([A-Za-z0-9]{1,8})(?:\/|\z)/; }
        my (@trailing, @dotless);
        for my $e (sort keys %ext) {
            my $a = '/' . FILLER . ".$e." . FILLER . 'x';
            my $b = '/' . FILLER . "x$e";
            push @trailing, $a if $a =~ $l->{re} && $l->{pat} !~ /^\^?\/?\.?\*?\z/;
            push @dotless,  $b if $b =~ $l->{re} && ('/' . FILLER . ".$e") =~ $l->{re} && $l->{pat} =~ /(?<!\\)\.\(?\??:?[A-Za-z0-9|]*\Q$e\E/;
        }
        if (@trailing) {
            add_finding($ctx, id => 'NLS010', where => $l, server => $srv, location => $l,
                message => "$l->{label} feeds $h->{name} and also matches $trailing[0]. "
                    . 'The extension is not anchored, so a file with a harmless final extension still reaches the handler. '
                    . 'End the pattern with $ or with (/|$).',
                key => 'loose|trailing',
                evidence => { example => $trailing[0] });
        }
        if (@dotless) {
            add_finding($ctx, id => 'NLS010', where => $l, server => $srv, location => $l, severity => 'low',
                message => "$l->{label} feeds $h->{name} and also matches $dotless[0]. "
                    . 'The dot before the extension is not escaped, so it stands for any character. Write it as \\. instead.',
                key => 'loose|dot',
                evidence => { example => $dotless[0] });
        }
    }
}

sub scopes_of_server {
    my ($srv) = @_;
    my @scopes;
    my $visit;
    $visit = sub {
        my ($scope, $owner) = @_;
        push @scopes, [$scope, $owner];
        for my $d (@{ $scope->{dirs} }) {
            next unless $d->{block} && $d->{name} eq 'if';
            my $sub = { kind => 'if', dirs => $d->{block}, up => $scope, file => $d->{file}, line => $d->{line},
                        label => 'if ' . join(' ', @{ $d->{args} }) };
            $visit->($sub, $owner);
        }
    };
    $visit->($srv, undef);
    $visit->($_, $_) for @{ $srv->{all} };
    undef $visit;
    return @scopes;
}

sub check_inheritance {
    my ($ctx, $srv) = @_;
    for my $pair (scopes_of_server($srv)) {
        my ($scope, $owner) = @$pair;
        for my $fam (sort keys %INHERIT_FAMILIES) {
            my $spec = $INHERIT_FAMILIES{$fam};
            my @own = grep { $_->{name} eq $fam && @{ $_->{args} } } @{ $scope->{dirs} };
            next unless @own;
            next unless $scope->{up};
            if ($spec->{switch}) {
                my $sw = nearest_directive($scope, $spec->{switch});
                next if $sw && lc($sw->{args}[0] // '') =~ /^(?:merge|off)$/;
            }
            my ($from, $inherited) = nearest_family($scope->{up}, $fam);
            next unless $from && @$inherited;
            my $norm = $spec->{ci} ? sub { lc $_[0] } : sub { $_[0] };
            my %have = map { $norm->($_->{args}[0]) => 1 } @own;
            my (@dropped, %dseen);
            for my $d (@$inherited) {
                next unless @{ $d->{args} };
                my $n = $norm->($d->{args}[0]);
                next if $have{$n} || $dseen{$n}++;
                push @dropped, $d->{args}[0];
            }
            next unless @dropped;
            my @critical = grep { $_ =~ $spec->{critical} } @dropped;
            my $place = $scope->{kind} eq 'if' ? "$scope->{label} in " . ($owner ? $owner->{label} : 'the server block')
                      : $scope->{kind} eq 'server' ? 'The server block'
                      :                              $scope->{label};
            my $src = $from->{kind} eq 'http' ? 'the http block'
                    : $from->{kind} eq 'server' ? 'the server block'
                    : $from->{kind} eq 'if' ? 'the enclosing if'
                    :                         $from->{label};
            add_finding($ctx, id => 'NLS006', where => $own[0], server => $srv, location => $owner,
                severity => (@critical ? 'medium' : 'low'),
                message => "$place sets $fam, so nothing from $src is inherited. Dropped here: "
                    . join(', ', @dropped) . ". nginx only inherits $fam when the block defines none of its own. "
                    . 'Repeat the missing lines in this block'
                    . ($spec->{switch} ? " or set $spec->{switch} merge on a release that has it." : '.'),
                key => "inherit|$fam|" . ($scope->{kind} eq 'if' ? $scope->{label} : $scope->{kind}),
                evidence => { family => $fam, dropped => \@dropped, inherited_from => "$from->{file}:$from->{line}" });
        }
    }
}

sub check_access_reset {
    my ($ctx, $srv) = @_;
    for my $pair (scopes_of_server($srv)) {
        my ($scope, $owner) = @$pair;
        my @own = grep { $_->{name} eq 'allow' || $_->{name} eq 'deny' } @{ $scope->{dirs} };
        next unless @own;
        my $closes = grep { $_->{name} eq 'deny' && lc($_->{args}[0] // '') eq 'all' } @own;
        next if $closes;
        my @allows = grep { $_->{name} eq 'allow' } @own;
        my $allows_all = grep { lc($_->{args}[0] // '') eq 'all' } @allows;
        my $place = $scope->{kind} eq 'server' ? 'The server block' : $scope->{kind} eq 'if' ? $scope->{label} : $scope->{label};
        my ($from, $inherited) = $scope->{up} ? nearest_family($scope->{up}, 'allow', 'deny') : (undef, []);
        my $parent_closed = grep { $_->{name} eq 'deny' && lc($_->{args}[0] // '') eq 'all' } @$inherited;
        my $satisfy = nearest_directive($scope, 'satisfy');
        next if $satisfy && lc($satisfy->{args}[0] // '') eq 'any' && !$parent_closed;
        if ($parent_closed && !$allows_all) {
            my $src = $from->{kind} eq 'location' ? $from->{label} : "the $from->{kind} block";
            add_finding($ctx, id => 'NLS007', where => $own[0], server => $srv, location => $owner,
                message => "$place lists its own allow and deny rules without a closing deny all. "
                    . "That replaces the whole list inherited from $src ($from->{file}:$from->{line}), including its deny all, "
                    . 'so every address is let in here. End the list with deny all.',
                key => 'aclreset',
                evidence => { rules => [map { "$_->{name} " . join(' ', @{ $_->{args} }) } @own] });
        } elsif (@allows && !$allows_all && !$parent_closed) {
            add_finding($ctx, id => 'NLS007', where => $own[0], server => $srv, location => $owner, severity => 'medium',
                message => "$place has allow rules but no deny all after them. "
                    . 'An address that matches no rule is allowed, so the list restricts nothing. End it with deny all.',
                key => 'aclopen',
                evidence => { rules => [map { "$_->{name} " . join(' ', @{ $_->{args} }) } @own] });
        }
    }
}

sub check_if_blocks {
    my ($ctx, $srv) = @_;
    for my $l (@{ $srv->{all} }) {
        for my $blk (grep { $_->{name} eq 'if' && $_->{block} } @{ $l->{dirs} }) {
            my %bad;
            $bad{ $_->{name} } = 1 for grep { !$IF_SAFE_DIRECTIVES{ $_->{name} } } @{ $blk->{block} };
            next unless %bad;
            add_finding($ctx, id => 'NLS011', where => $blk, server => $srv, location => $l,
                message => 'if ' . join(' ', @{ $blk->{args} }) . " in $l->{label} holds " . join(', ', sort keys %bad)
                    . '. An if inside a location is its own hidden location, so directives other than return, rewrite, set and '
                    . 'break behave in surprising ways and can drop the settings of the surrounding block. Move the logic to map or to a separate location.',
                key => 'if|' . join(' ', @{ $blk->{args} }));
        }
    }
}

sub collect_named_refs {
    my ($dirs, $refs) = @_;
    for my $d (@$dirs) {
        if (($d->{name} eq 'try_files' || $d->{name} eq 'error_page' || $d->{name} eq 'post_action') && @{ $d->{args} }) {
            my $last = $d->{args}[-1];
            push @$refs, [$last, $d] if $last =~ /^@[\w.-]+\z/;
        }
        collect_named_refs($d->{block}, $refs) if $d->{block};
    }
}

sub check_named {
    my ($ctx, $srv) = @_;
    my @refs;
    collect_named_refs($srv->{dirs}, \@refs);
    collect_named_refs($srv->{up}{dirs}, \@refs) if $srv->{up};
    my @server_refs = @refs;
    my %owner;
    for my $l (@{ $srv->{all} }) {
        my @local;
        collect_named_refs($l->{dirs}, \@local);
        $owner{ 0 + $_->[1] } = $l for @local;
        push @refs, @local;
    }
    my %defined = map { $_->{pat} => $_ } @{ $srv->{named} };
    my %used;
    for my $ref (@refs) {
        my ($name, $d) = @$ref;
        $used{$name} = 1;
        next if $defined{$name};
        next if $d->{file} ne $srv->{file} && !$owner{ 0 + $d } && !grep { $_->[1] == $d } @server_refs;
        my $l = $owner{ 0 + $d };
        add_finding($ctx, id => 'NLS013', where => $d, server => $srv, location => $l,
            message => "$d->{name} points at $name but this server defines no such named location. "
                . 'The request ends in a 500 the first time that fallback is taken.',
            key => "named|$name");
    }
    for my $n (@{ $srv->{named} }) {
        next if $used{ $n->{pat} };
        add_finding($ctx, id => 'NLS017', where => $n, server => $srv, location => $n,
            message => "$n->{label} is never referenced by try_files, error_page or post_action in this server. "
                . 'It can still be reached from Lua or njs code, which this tool does not read.',
            key => 'unused');
    }
}

sub check_merge_slashes {
    my ($ctx, $srv) = @_;
    return if $srv->{merge_slashes};
    my @guarded = grep { ($_->{mod} eq '' || $_->{mod} eq '^~' || $_->{mod} eq '=') && length($_->{pat}) > 1
        && %{ guards_of($_) } && guards_differ(guards_of($_), guards_of($srv)) } @{ $srv->{all} };
    return unless @guarded;
    my $d = nearest_directive($srv, 'merge_slashes');
    my $l = $guarded[0];
    add_finding($ctx, id => 'NLS016', where => $d, server => $srv, location => undef,
        message => 'merge_slashes is off, so /' . $l->{pat} . " does not match $l->{label} and skips its access control. "
            . 'Most upstreams and file systems collapse the double slash and serve the same resource. '
            . scalar(@guarded) . ' guarded prefix location' . (@guarded == 1 ? ' is' : 's are') . ' exposed this way.',
        key => 'merge',
        evidence => { example => "/$l->{pat}", locations => [map { $_->{label} } @guarded] });
}

sub guards_differ {
    my ($a, $b) = @_;
    for my $k (keys %$a) {
        return 1 if !exists $b->{$k} || $b->{$k} ne $a->{$k};
    }
    return 0;
}

sub add_finding {
    my ($ctx, %f) = @_;
    my $rule  = $RULES{ $f{id} } or die "unknown rule $f{id}\n";
    my $where = $f{where} || {};
    my $srv   = $f{server};
    my $loc   = $f{location};
    my $finding = {
        id       => $f{id},
        rule     => $rule->{name},
        severity => $f{severity} || $rule->{severity},
        title    => $rule->{title},
        file     => defined $where->{file} ? $where->{file} : $ctx->{main},
        line     => $where->{line} || 1,
        server   => $srv ? $srv->{label} : undef,
        location => $loc ? $loc->{label} : undef,
        message  => $f{message},
        evidence => $f{evidence} || {},
    };
    my @path = $loc ? map { $_->{label} } ancestry($loc) : ();
    $finding->{fingerprint} = substr(sha1_hex(join("\x1f", $f{id}, ($srv ? $srv->{label} : ''), @path, $f{key} // '')), 0, 20);
    push @{ $ctx->{findings} }, $finding;
}

sub finalize_findings {
    my ($ctx) = @_;
    my $opt = $ctx->{opt};
    my %skip = map { uc($_) => 1 } map { split /[,\s]+/ } @{ $opt->{ignore} };
    for my $id (keys %skip) {
        die "--ignore names an unknown rule: $id\n" unless $RULES{$id};
    }
    my %base;
    if (defined $opt->{baseline}) {
        my $text = slurp($opt->{baseline});
        die "cannot read baseline $opt->{baseline}: $!\n" unless defined $text;
        my $data = eval { JSON::PP->new->utf8(1)->decode($text) };
        die "baseline $opt->{baseline} is not valid JSON\n" unless ref $data eq 'HASH' && ref $data->{fingerprints} eq 'ARRAY';
        %base = map { $_ => 1 } @{ $data->{fingerprints} };
    }
    my (@kept, %seen);
    my %stats = (suppressed_inline => 0, suppressed_rule => 0, suppressed_baseline => 0);
    for my $f (@{ $ctx->{findings} }) {
        next if $seen{ $f->{fingerprint} }++;
        if ($skip{ $f->{id} }) { $stats{suppressed_rule}++; next; }
        my $ig  = $ctx->{ignores}{ $f->{file} } || {};
        my @ids = (@{ $ig->{ $f->{line} } || [] }, @{ $ig->{ $f->{line} - 1 } || [] });
        if (grep { $_ eq '*' || $_ eq $f->{id} } @ids) { $stats{suppressed_inline}++; next; }
        if ($base{ $f->{fingerprint} }) { $stats{suppressed_baseline}++; next; }
        push @kept, $f;
    }
    @kept = sort {
        $SEVERITY_RANK{ $b->{severity} } <=> $SEVERITY_RANK{ $a->{severity} }
            || $a->{file} cmp $b->{file} || $a->{line} <=> $b->{line} || $a->{id} cmp $b->{id}
            || $a->{fingerprint} cmp $b->{fingerprint}
    } @kept;
    $ctx->{findings} = \@kept;
    $ctx->{stats}    = \%stats;

    if (defined $opt->{'write-baseline'}) {
        my $out = json_encoder()->encode({
            version      => 1,
            tool         => 'NginxLocationShadowAuditor',
            fingerprints => [sort map { $_->{fingerprint} } @kept],
        });
        open(my $fh, '>', $opt->{'write-baseline'}) or die "cannot write baseline $opt->{'write-baseline'}: $!\n";
        print {$fh} $out;
        close($fh) or die "cannot write baseline $opt->{'write-baseline'}: $!\n";
    }
    return 1;
}

sub summary_of {
    my ($ctx, $shown) = @_;
    my %count = map { $_ => 0 } keys %SEVERITY_RANK;
    $count{ $_->{severity} }++ for @{ $ctx->{findings} };
    my $locations = 0;
    my $probes    = 0;
    for my $s (@{ $ctx->{servers} }) {
        $locations += @{ $s->{all} };
        $probes    += $s->{probe_count} || 0;
    }
    return {
        servers   => scalar @{ $ctx->{servers} },
        locations => $locations,
        probes    => $probes,
        includes  => $ctx->{includes},
        counts    => \%count,
        shown     => scalar @$shown,
        %{ $ctx->{stats} || {} },
    };
}

sub render_text {
    my ($ctx, $shown) = @_;
    my $s   = summary_of($ctx, $shown);
    my $out = '';
    for my $f (@$shown) {
        $out .= sprintf "%-6s %s %s  %s:%d\n", uc $f->{severity}, $f->{id}, $f->{rule}, $f->{file}, $f->{line};
        my @ctxline;
        push @ctxline, "server $f->{server}" if defined $f->{server};
        push @ctxline, $f->{location}        if defined $f->{location};
        $out .= '       ' . join(', ', @ctxline) . "\n" if @ctxline;
        $out .= wrap_text($f->{message}, '       ', 96);
        $out .= "       fingerprint $f->{fingerprint}\n\n";
    }
    $out .= sprintf "%d server%s, %d location%s, %d probe path%s replayed, %d include%s read\n",
        $s->{servers}, plural($s->{servers}), $s->{locations}, plural($s->{locations}),
        $s->{probes}, plural($s->{probes}), $s->{includes}, plural($s->{includes});
    $out .= sprintf "findings: %d high, %d medium, %d low, %d info",
        @{ $s->{counts} }{qw(high medium low info)};
    my $hidden = @{ $ctx->{findings} } - @$shown;
    $out .= ", $hidden hidden by --min-severity" if $hidden;
    my $supp = $s->{suppressed_inline} + $s->{suppressed_rule} + $s->{suppressed_baseline};
    $out .= sprintf ", %d suppressed (%d inline, %d by rule, %d by baseline)",
        $supp, @$s{qw(suppressed_inline suppressed_rule suppressed_baseline)} if $supp;
    $out .= "\n";
    return $out;
}

sub plural { return $_[0] == 1 ? '' : 's'; }

sub wrap_text {
    my ($text, $indent, $width) = @_;
    my $out  = '';
    my $line = '';
    for my $word (split / +/, $text) {
        if (length $line && length($line) + 1 + length($word) > $width) {
            $out .= "$indent$line\n";
            $line = $word;
        } else {
            $line = length $line ? "$line $word" : $word;
        }
    }
    $out .= "$indent$line\n" if length $line;
    return $out;
}

sub render_json {
    my ($ctx, $shown) = @_;
    return json_encoder()->encode({
        tool     => 'NginxLocationShadowAuditor',
        version  => $VERSION,
        input    => $ctx->{main},
        summary  => summary_of($ctx, $shown),
        findings => $shown,
    });
}

sub render_sarif {
    my ($ctx, $shown) = @_;
    my %level = (high => 'error', medium => 'warning', low => 'note', info => 'note');
    my @rules = map {
        { id => $_, name => $RULES{$_}{name},
          shortDescription     => { text => $RULES{$_}{title} },
          defaultConfiguration => { level => $level{ $RULES{$_}{severity} } } }
    } sort keys %RULES;
    my @results = map {
        my $f = $_;
        { ruleId  => $f->{id},
          level   => $level{ $f->{severity} },
          message => { text => $f->{message} },
          locations => [ { physicalLocation => {
              artifactLocation => { uri => $f->{file} },
              region           => { startLine => $f->{line} + 0 } } } ],
          partialFingerprints => { nlsFingerprint => $f->{fingerprint} },
          properties          => { severity => $f->{severity}, server => $f->{server}, location => $f->{location} } }
    } @$shown;
    return json_encoder()->encode({
        '$schema' => 'https://json.schemastore.org/sarif-2.1.0.json',
        version   => '2.1.0',
        runs      => [ {
            tool    => { driver => { name => 'NginxLocationShadowAuditor', version => $VERSION, rules => \@rules } },
            results => \@results,
        } ],
    });
}

sub pick_server {
    my ($ctx, $host, $port) = @_;
    my @pool = @{ $ctx->{servers} };
    if (defined $port) {
        my @on = grep { my $s = $_; grep { $_ == $port } @{ $s->{listens} } } @pool;
        return (undef, "no server block listens on port $port") unless @on;
        @pool = @on;
    }
    my ($default) = grep { $_->{default} } @pool;
    $default ||= $pool[0];
    return ($default, 'no --host given, using the default server') unless defined $host && length $host;
    $host = lc $host;
    $host =~ s/:\d+\z//;
    $host =~ s/\.\z//;

    for my $s (@pool) {
        for my $n (@{ $s->{names} }) {
            my $name = lc $n;
            return ($s, "exact server_name $n") if $name eq $host;
            return ($s, "server_name $n covers the bare domain") if $name =~ /^\.(.+)/ && $host eq $1;
        }
    }
    my ($best, $why, $len) = (undef, undef, -1);
    for my $s (@pool) {
        for my $n (@{ $s->{names} }) {
            my $name = lc $n;
            my $suffix = $name =~ /^\*(\..+)\z/ ? $1 : $name =~ /^(\..+)\z/ ? $1 : undef;
            next unless defined $suffix;
            next unless length($host) > length($suffix) && substr($host, -length $suffix) eq $suffix;
            ($best, $why, $len) = ($s, "leading wildcard server_name $n", length $suffix) if length($suffix) > $len;
        }
    }
    return ($best, $why) if $best;
    for my $s (@pool) {
        for my $n (@{ $s->{names} }) {
            my $name = lc $n;
            next unless $name =~ /^(.+\.)\*\z/;
            my $prefix = $1;
            next unless index($host, $prefix) == 0 && length($host) > length($prefix);
            ($best, $why, $len) = ($s, "trailing wildcard server_name $n", length $prefix) if length($prefix) > $len;
        }
    }
    return ($best, $why) if $best;
    for my $s (@pool) {
        for my $n (@{ $s->{names} }) {
            next unless $n =~ /^~(\*?)(.+)\z/s;
            my $re = compile_regex($2, 1);
            next unless ref $re;
            return ($s, "regex server_name $n") if $host =~ $re;
        }
    }
    return ($default, "no server_name matches $host, using the default server");
}

sub explain_request {
    my ($ctx, $raw, $host, $port) = @_;
    my $rep = { request => $raw };
    my ($srv, $why) = pick_server($ctx, $host, $port);
    if (!$srv) { $rep->{error} = $why; return $rep; }
    $rep->{server}        = "$srv->{label} ($srv->{file}:$srv->{line})";
    $rep->{server_reason} = $why;
    my ($uri, $err) = normalize_uri($raw, $srv->{merge_slashes});
    if (!defined $uri) { $rep->{error} = $err; return $rep; }
    $rep->{uri} = $uri;

    my @notes;
    my @rewrites = grep { $_->{name} eq 'rewrite' || $_->{name} eq 'return' || ($_->{name} eq 'if' && $_->{block}) } @{ $srv->{dirs} };
    push @notes, 'the server block has ' . join(', ', map { "$_->{name} at line $_->{line}" } @rewrites)
        . ' which runs before location matching and can change or end the request' if @rewrites;

    my $r   = resolve($srv, $uri);
    my $loc = $r->{loc};
    $rep->{steps}  = $r->{steps};
    $rep->{result} = $loc ? describe($loc) : 'no location matches, the server block handles the request';
    $rep->{redirect} = $r->{redirect} if $r->{redirect};
    my $scope = $loc || $srv;
    my $h = handler_of($loc);
    $rep->{handler} = $h ? "$h->{name} " . join(' ', @{ $h->{args} }) : 'static file or index (no content handler in the block)';
    $rep->{guards}  = guards_of($scope);
    for my $fam (qw(add_header proxy_set_header)) {
        my ($from, $list) = nearest_family($scope, $fam);
        next unless @$list;
        $rep->{$fam} = [map { join ' ', @{ $_->{args} } } @$list];
        $rep->{"${fam}_from"} = $from->{kind} eq 'location' ? describe($from) : "$from->{kind} block";
    }
    my $gw = guards_of($scope);
    for my $t (@{ $r->{chain} }, @{ $r->{order_losers} }) {
        next if $loc && is_within($t, $loc);
        my $gt = guards_of($t);
        my @missing = sort grep { !exists $gw->{$_} || $gw->{$_} ne $gt->{$_} } keys %$gt;
        push @notes, describe($t) . ' also matches this path but lost'
            . (@missing ? '. It carries ' . join(', ', @missing) . ' which the winner does not apply' : '');
    }
    push @notes, 'the winner is internal, an outside request gets 404' if $gw->{internal};
    $rep->{notes} = \@notes;
    return $rep;
}

sub render_explain_text {
    my ($rep) = @_;
    my $out = "request  $rep->{request}\n";
    if ($rep->{error} && !$rep->{server}) { return $out . "error    $rep->{error}\n\n"; }
    $out .= "server   $rep->{server}, $rep->{server_reason}\n";
    if ($rep->{error}) { return $out . "error    $rep->{error}\n\n"; }
    $out .= "path     $rep->{uri}" . ($rep->{uri} ne $rep->{request} ? '  (after decoding and normalizing)' : '') . "\n";
    my $n = 0;
    $out .= sprintf("  %d. %s\n", ++$n, $_) for @{ $rep->{steps} };
    $out .= "result   $rep->{result}\n";
    $out .= "redirect 301 to $rep->{redirect}\n" if $rep->{redirect};
    $out .= "handler  $rep->{handler}\n";
    my $g = $rep->{guards};
    $out .= 'guards   ' . (%$g ? join('; ', map { "$_: $g->{$_}" } sort keys %$g) : 'none') . "\n";
    for my $fam (qw(add_header proxy_set_header)) {
        next unless $rep->{$fam};
        $out .= sprintf "%-8s from %s: %s\n", ($fam eq 'add_header' ? 'headers' : 'upstream'), $rep->{"${fam}_from"},
            join(' | ', @{ $rep->{$fam} });
    }
    $out .= "note     $_\n" for @{ $rep->{notes} };
    return $out . "\n";
}

sub self_test {
    my $conf = <<'CONF';
http {
    add_header X-Frame-Options DENY;
    add_header Strict-Transport-Security "max-age=63072000";
    server {
        listen 80 default_server;
        server_name example.test *.example.test;
        root /srv/www;

        location / { try_files $uri $uri/ @app; }
        location = /health { return 200; }
        location /admin/ {
            auth_basic "admin";
            auth_basic_user_file /etc/nginx/htpasswd;
            location /admin/reports/ { add_header Cache-Control no-store; }
        }
        location /uploads/ { }
        location ^~ /downloads/ { }
        location /static { alias /srv/static/; }
        location /api { proxy_pass http://backend/; }
        location /svc/ { proxy_pass http://backend; }
        location /internal/ {
            allow 10.0.0.0/8;
            deny all;
            location /internal/metrics { allow 192.168.0.0/16; }
        }
        location ~ \.php$ { fastcgi_pass unix:/run/php.sock; }
        location ~ ^/legacy/ { return 410; }
        location ~ ^/legacy/reports/ { auth_basic "old"; }
        location ~* \.(?:jpg|png)$ { expires 30d; }
        location ~ \.cgi { fastcgi_pass 127.0.0.1:9000; }
        location ~ ^/proxy/(.*)$ { proxy_pass http://backend/x/; }
        location /dup/ { }
        location ^~ /dup/ { }
        location /quiet/ {
            # nls:ignore NLS006
            add_header X-Quiet 1;
        }
        location /flag/ {
            if ($arg_x) { add_header X-Flag 1; }
        }
        location @app { proxy_pass http://backend; }
        location @orphan { return 204; }
        error_page 502 @missing;
    }
    server {
        listen 8080;
        server_name api.example.test;
        location / { return 204; }
    }
}
CONF
    my %opt = ('max-probes' => DEFAULT_MAX_PROBES, ignore => [], 'fail-on' => 'high', 'min-severity' => 'info');
    my $ctx = eval { build_context($conf, 'selftest.conf', \%opt) };
    if (!$ctx) { print "self-test: parse failed: $@"; return EXIT_ERROR; }
    my $srv = $ctx->{servers}[0];
    my ($fail, $pass) = (0, 0);
    my $check = sub {
        my ($okay, $name) = @_;
        if ($okay) { $pass++; } else { $fail++; print "FAIL $name\n"; }
    };
    my $route = sub {
        my ($raw) = @_;
        my ($uri, $err) = normalize_uri($raw, 1);
        return "error: $err" unless defined $uri;
        my $r = resolve($srv, $uri);
        return "301 $r->{redirect}" if $r->{redirect};
        return $r->{loc} ? $r->{loc}{label} : 'server';
    };
    my @routes = (
        ['/',                         'location /'],
        ['/health',                   'location = /health'],
        ['/healthz',                  'location /'],
        ['/admin/',                   'location /admin/'],
        ['/admin/x.php',              'location ~ \.php$'],
        ['/admin/reports/q',          'location /admin/reports/'],
        ['/admin/reports/q.php',      'location ~ \.php$'],
        ['/downloads/x.php',          'location ^~ /downloads/'],
        ['/uploads/a.PNG',            'location ~* \.(?:jpg|png)$'],
        ['/uploads/%2e%2e/admin/',    'location /admin/'],
        ['/a//b/../admin/./x',        'location /'],
        ['/legacy/reports/2020',      'location ~ ^/legacy/'],
        ['/svc',                      '301 /svc/'],
        ['/svc/x',                    'location /svc/'],
        ['/staticfile',               'location /static'],
        ['/../etc/passwd',            'error: path climbs above the root, nginx answers 400'],
        ['/x%zz',                     'error: malformed percent escape, nginx answers 400'],
        ['/internal/metrics?x=1',     'location /internal/metrics'],
    );
    for my $case (@routes) {
        my $got = $route->($case->[0]);
        $check->($got eq $case->[1], "route $case->[0]: expected $case->[1], got $got");
    }
    my ($picked) = pick_server($ctx, 'api.example.test', undef);
    $check->($picked == $ctx->{servers}[1], 'exact server_name selects the second server');
    ($picked) = pick_server($ctx, 'www.example.test', undef);
    $check->($picked == $srv, 'wildcard server_name selects the first server');
    ($picked) = pick_server($ctx, 'other.invalid', 8080);
    $check->($picked == $ctx->{servers}[1], 'port filter falls back to the only server on that port');

    my ($samples) = regex_samples('^/(?<kind>img|doc)/\d{2,4}/[a-f0-9]+\.(?:jpe?g|png)$', qr{^/(?<kind>img|doc)/\d{2,4}/[a-f0-9]+\.(?:jpe?g|png)$});
    $check->(@$samples >= 4, 'regex sampler yields verified samples for a structured pattern');
    $check->(!(grep { $_ !~ qr{^/(?:img|doc)/\d{2,4}/[a-f0-9]+\.(?:jpe?g|png)$} } @$samples), 'every regex sample matches its pattern');

    audit_server($ctx, $_) for @{ $ctx->{servers} };
    finalize_findings($ctx);
    my %got;
    for my $f (@{ $ctx->{findings} }) {
        $got{ $f->{id} . ' ' . ($f->{location} // '-') } = $f;
    }
    my @expected = (
        ['NLS001 location /admin/',                'high'],
        ['NLS002 location /uploads/',              'high'],
        ['NLS003 location ~ ^/legacy/reports/',    'medium'],
        ['NLS004 location ^~ /dup/',               'high'],
        ['NLS005 location /static',                'high'],
        ['NLS006 location /admin/reports/',        'medium'],
        ['NLS006 location /flag/',                 'medium'],
        ['NLS007 location /internal/metrics',      'high'],
        ['NLS008 location ~ ^/proxy/(.*)$',        'high'],
        ['NLS009 location /api',                   'medium'],
        ['NLS010 location ~ \.cgi',                'medium'],
        ['NLS011 location /flag/',                 'low'],
        ['NLS013 -',                               'high'],
        ['NLS017 location @orphan',                'info'],
    );
    for my $e (@expected) {
        my ($key, $sev) = @$e;
        $check->($got{$key} && $got{$key}{severity} eq $sev,
            "finding $key at $sev" . ($got{$key} ? ", got $got{$key}{severity}" : ', not reported'));
    }
    my $legacy = $got{'NLS001 location ~ ^/legacy/reports/'};
    $check->($legacy && !(grep { $_->{winner} eq 'location ~ ^/legacy/' } @{ $legacy->{evidence}{routes} }),
        'a winner that returns 410 is not counted as a bypass');
    $check->(!$got{'NLS001 location /admin/reports/'}, 'a nested location with the same guards is covered by its parent finding');
    $check->(!$got{'NLS006 location /quiet/'}, 'inline nls:ignore suppresses the finding on the next line');
    $check->(!$got{'NLS001 location ^~ /downloads/'} && !$got{'NLS002 location ^~ /downloads/'}, '^~ prefix is not reported as shadowed');
    $check->($ctx->{stats}{suppressed_inline} == 1, 'one finding counted as suppressed inline');

    my $bad = eval { build_context("server { location / { root /x; }\n", 'broken.conf', \%opt); 1 };
    $check->(!$bad && $@ =~ /missing its closing brace/, 'unbalanced braces are a parse error');
    my $dump = "# configuration file /etc/nginx/nginx.conf:\nhttp { include conf.d/*.conf; }\n"
        . "# configuration file /etc/nginx/conf.d/a.conf:\nserver { location /a { return 200; } }\n";
    my $dctx = eval { build_context($dump, '<stdin>', \%opt) };
    $check->($dctx && @{ $dctx->{servers} } == 1 && $dctx->{servers}[0]{all}[0]{file} eq '/etc/nginx/conf.d/a.conf',
        'nginx -T dump resolves includes from its own sections');

    printf "self-test: %d passed, %d failed\n", $pass, $fail;
    return $fail ? EXIT_ERROR : EXIT_OK;
}
