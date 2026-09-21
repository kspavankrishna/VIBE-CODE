#!/usr/bin/env raku
use v6.d;

# AI Crawler Policy Auditor
# Simulates real robots.txt matching semantics (RFC 9309 + the Google/Bing
# implementation quirks that differ from the RFC) against a registry of
# known AI training and browsing crawlers, and flags the mistakes that are
# invisible by eye: wildcard rules that collateral-damage search engines,
# bot-specific groups that silently override a catch-all block, and
# ambiguous rules that strict-RFC crawlers will resolve differently from
# Google-style ones.

enum Verdict <Allow Deny>;

#|( One Allow/Disallow line inside a group. )
class Rule {
    has Str $.type is required;   # 'allow' or 'disallow'
    has Str $.pattern is required;
    has Int $.line is required;
}

#|( One User-agent group: the agents it names and the rules under it. )
class Group {
    has Str  @.agents;
    has Rule @.rules;
    has Int  $.crawl-delay is rw;
    has Int  $.first-line is required;
    has Int  $.order is required;
}

#|( The outcome of deciding one bot/path pair against a parsed policy. )
class Decision {
    has Verdict $.verdict is required;
    has Rule    $.rule;
    has Group   $.group;
    has Bool    $.tied = False;
}

class Finding {
    has Str $.severity is required; # INFO LOW MEDIUM HIGH
    has Str $.code is required;
    has Str $.message is required;
}

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

grammar RobotsLine {
    token TOP {
        ^ \h* <directive>? \h* <comment>? \h* $
    }
    token directive     { <key=.ident> \h* ':' \h* <value> }
    token ident         { <[A..Za..z0..9\-]>+ }
    token value         { <-[\#]>* }
    token comment       { '#' .* }
}

class RobotsLineActions {
    method TOP($/) {
        if $<directive> {
            make ($<directive><key>.Str.lc, $<directive><value>.Str.trim);
        } else {
            make Nil;
        }
    }
}

#| Strip a UTF-8 BOM and normalize line endings so CRLF/CR-only files parse
#| the same as LF files.
sub clean-text(Str $raw --> Str) {
    my $text = $raw;
    $text .= subst(/^ "\x[FEFF]" /, '');
    $text .= subst(/ \r\n /, "\n", :g);
    $text .= subst(/ \r /,   "\n", :g);
    return $text;
}

#| Parse robots.txt source into an ordered list of Group objects. Unknown
#| directives (Sitemap, Host, Clean-param, ...) and malformed lines are
#| skipped rather than raised: a bad line is a lint problem, not a crash.
sub parse-robots(Str $raw) {
    my Group @groups;
    my Group $current;
    my Bool  $awaiting-agents = True; # true right after a group boundary
    my $order = 0;
    my $actions = RobotsLineActions.new;

    for clean-text($raw).split("\n").kv -> $idx, $raw-line {
        my $lineno = $idx + 1;
        my $parsed = RobotsLine.parse($raw-line, :$actions);
        next unless $parsed && $parsed.made;
        my ($key, $value) = $parsed.made;
        next unless $key.defined;

        if $key eq 'user-agent' {
            if $current && !$awaiting-agents {
                # A new User-agent line after directives started a fresh
                # group, per the standard group-boundary rule.
                @groups.push: $current;
                $current = Nil;
            }
            unless $current {
                $current = Group.new(first-line => $lineno, order => $order++);
            }
            $current.agents.push($value) if $value;
            $awaiting-agents = True;
        }
        elsif $key eq 'disallow' || $key eq 'allow' {
            next unless $current; # directive before any User-agent: ignore
            $current.rules.push: Rule.new(type => $key, pattern => $value, line => $lineno);
            $awaiting-agents = False;
        }
        elsif $key eq 'crawl-delay' {
            next unless $current;
            $current.crawl-delay = $value.Int if $value ~~ /^ \d+ $/;
            $awaiting-agents = False;
        }
        else {
            # sitemap, host, clean-param, noindex, etc: informational only,
            # not part of matching, so we simply don't attach it to a group.
            $awaiting-agents = False if $current;
        }
    }
    @groups.push: $current if $current;
    return @groups;
}

# ---------------------------------------------------------------------------
# Path matching (RFC 9309 section 2.2.2 + the '*' / '$' wildcard extension
# every major crawler actually implements even though the base RFC does not
# require it)
# ---------------------------------------------------------------------------

#| Decode percent-encoded octets that map to RFC 3986 "unreserved" characters
#| so that e.g. "%2Ecfg" and ".cfg" compare equal, while leaving reserved
#| octets (like %2F for '/') encoded so they can't smuggle a path separator
#| or a wildcard character past the matcher.
sub normalize-percent(Str $s --> Str) {
    return $s.subst(:g, / '%' (<[0..9A..Fa..f]> ** 2) /, -> $/ {
        my $byte = :16($0.Str);
        my $chr  = $byte.chr;
        $chr ~~ /^ <[A..Za..z0..9\-._~]> $/ ?? $chr !! "%{$0.Str.uc}";
    });
}

sub escape-regex-literal(Str $s --> Str) {
    return $s.subst(:g, / <-[A..Za..z0..9]> /, -> $/ { '\\' ~ $/ });
}

#| Turn a robots.txt path pattern into Raku regex source text. '*' becomes
#| ".*"; a trailing '$' anchors the end; everything else matches literally
#| (byte for byte, after percent-normalization).
sub pattern-regex-source(Str $pattern --> Str) {
    my $anchored-end = $pattern.ends-with('$');
    my $core = $anchored-end ?? $pattern.substr(0, *-1) !! $pattern;
    my @segments = $core.split('*', :all);
    my $body = @segments.map({ escape-regex-literal($_) }).join('.*');
    return '^' ~ $body ~ ($anchored-end ?? '$' !! '');
}

my %matcher-cache;
sub pattern-matches(Str $pattern, Str $path --> Bool) {
    my $norm-pattern = normalize-percent($pattern);
    my $norm-path    = normalize-percent($path);
    my $src = %matcher-cache{$norm-pattern} //= pattern-regex-source($norm-pattern);
    return so $norm-path ~~ / <$src> /;
}

# ---------------------------------------------------------------------------
# Group selection and the allow/deny decision
# ---------------------------------------------------------------------------

#| Pick the group(s) that apply to a bot token. In 'merge' mode (the
#| behavior Google's own parser implements) every group that names the
#| exact token is combined; in 'strict' mode only the first one counts,
#| which is how a literal reading of RFC 9309 groups works and is what
#| some smaller crawlers actually do. Falls back to the '*' groups when no
#| exact group exists.
sub select-groups(@groups, Str $token, Str $mode) {
    my $tok-lc = $token.lc;
    my Group @exact = @groups.grep({ .agents.first({ .lc eq $tok-lc }).defined });
    my Group @pool = @exact ?? @exact !! @groups.grep({ .agents.first({ $_ eq '*' }).defined });
    return [] unless @pool;
    return $mode eq 'strict' ?? [@pool[0]] !! @pool;
}

#| Shared ranking step: given the group(s) already chosen as applicable,
#| find the longest matching rule for $path and resolve ties (Allow wins).
sub decision-from-groups(@applicable, Str $path --> Decision) {
    return Decision.new(verdict => Allow) unless @applicable;

    my Rule @candidates = @applicable.map(*.rules).flat.grep({ pattern-matches(.pattern, $path) });
    return Decision.new(verdict => Allow, group => @applicable[0]) unless @candidates;

    my @ranked = @candidates.sort(-> $a, $b {
        my $len-cmp = $b.pattern.encode.bytes <=> $a.pattern.encode.bytes;
        $len-cmp != 0
            ?? $len-cmp
            !! ($a.type eq 'allow' ?? 0 !! 1) <=> ($b.type eq 'allow' ?? 0 !! 1);
    });
    my $best = @ranked[0];
    my $max-len = $best.pattern.encode.bytes;
    my $tied = @ranked.grep({ .pattern.encode.bytes == $max-len }).map(*.type).unique.elems > 1;

    return Decision.new(
        verdict => ($best.type eq 'allow' ?? Allow !! Deny),
        rule    => $best,
        group   => @applicable[0],
        tied    => $tied,
    );
}

sub decide(@groups, Str $token, Str $path, Str $mode = 'merge' --> Decision) {
    return decision-from-groups(select-groups(@groups, $token, $mode), $path);
}

#| The decision a bot with no group of its own, and no name matching any
#| declared token, would get from the plain 'User-agent: *' group(s) alone.
#| Used to test collateral damage without needing a fake bot token.
sub wildcard-decision(@groups, Str $path, Str $mode = 'merge' --> Decision) {
    my Group @wild = @groups.grep({ .agents.first({ $_ eq '*' }).defined });
    @wild = [@wild[0]] if @wild && $mode eq 'strict';
    return decision-from-groups(@wild, $path);
}

# ---------------------------------------------------------------------------
# Known AI crawler registry
#
# This list reflects publicly documented crawler product tokens as of this
# writing. New AI crawlers show up often; treat this as a starting point
# and pass --bots=your-file.txt (one token per line) to extend or replace
# it, rather than trusting any fixed list to stay complete forever.
# ---------------------------------------------------------------------------

my @default-ai-bots = <
    GPTBot ChatGPT-User OAI-SearchBot
    ClaudeBot Claude-Web anthropic-ai
    Google-Extended Applebot-Extended
    CCBot Bytespider PerplexityBot
    Amazonbot Diffbot cohere-ai
    Meta-ExternalAgent FacebookBot
    Timpibot ImagesiftBot
>;

my @reference-search-bots = <Googlebot Bingbot>;

my @default-paths = </ /robots.txt>;

# ---------------------------------------------------------------------------
# Audit heuristics
# ---------------------------------------------------------------------------

my %severity-rank = INFO => 0, LOW => 1, MEDIUM => 2, HIGH => 3;

sub audit(@groups, @bots, @search-bots, @paths) {
    my Finding @findings;

    for @search-bots -> $bot {
        my @own = @groups.grep({ .agents.first({ .lc eq $bot.lc }).defined });
        next if @own; # bot has its own explicit group; wildcard can't touch it
        my $d = decide(@groups, $bot, '/', 'merge');
        if $d.verdict === Deny {
            @findings.push: Finding.new(
                severity => 'HIGH',
                code     => 'search-engine-collateral',
                message  => "$bot has no group of its own and is blocked by the wildcard rule "
                          ~ "(User-agent: * , line {$d.group.first-line}, Disallow: {$d.rule.pattern} at line {$d.rule.line}). "
                          ~ "If the intent was to block AI training bots only, give $bot its own "
                          ~ "'User-agent: $bot' group with 'Allow: /'.",
            );
        }
    }

    my $wildcard-blocks-root = wildcard-decision(@groups, '/', 'merge').verdict === Deny;
    for @bots -> $bot {
        my @own = @groups.grep({ .agents.first({ .lc eq $bot.lc }).defined });
        next unless @own;
        next unless $wildcard-blocks-root;
        my $own-decision = decide(@groups, $bot, '/', 'merge');
        if $own-decision.verdict === Allow {
            @findings.push: Finding.new(
                severity => 'HIGH',
                code     => 'bot-group-overrides-wildcard-block',
                message  => "$bot has its own group (starting line {@own[0].first-line}) that currently "
                          ~ "allows '/', even though the wildcard 'User-agent: *' group blocks everything else. "
                          ~ "A bot's own group always wins over the wildcard, so $bot is not actually blocked "
                          ~ "no matter how strict the wildcard rule looks.",
            );
        }
    }

    for @groups -> $g {
        for $g.rules.grep({ .type eq 'disallow' && .pattern eq '' }) -> $r {
            @findings.push: Finding.new(
                severity => 'LOW',
                code     => 'empty-disallow-is-allow-all',
                message  => "Line {$r.line}: 'Disallow:' with no path means allow everything under this "
                          ~ "group, not block everything. If the goal was to block this group's agents "
                          ~ "entirely, use 'Disallow: /' instead.",
            );
        }
    }

    my %seen-token-lines;
    for @groups -> $g {
        for $g.agents -> $a {
            next if $a eq '*';
            (%seen-token-lines{$a.lc} //= []).push($g.first-line);
        }
    }
    for %seen-token-lines.kv -> $tok, @lines {
        if @lines.elems > 1 {
            @findings.push: Finding.new(
                severity => 'INFO',
                code     => 'duplicate-group-fragile',
                message  => "'$tok' is named in more than one group (lines {@lines.join(', ')}). "
                          ~ "Google-style crawlers merge all of them; a crawler that follows RFC 9309 "
                          ~ "literally may only honor the first one at line {@lines[0]}. Consolidate into "
                          ~ "a single group for that token to remove the ambiguity.",
            );
        }
    }

    # Bots that resolve to the same group(s) get identical verdicts, so
    # bucket by outcome per path and name every affected bot in one finding
    # instead of repeating the same sentence once per bot.
    for @paths -> $path {
        my %divergence-bucket;
        my %tie-bucket;
        for |@bots, |@search-bots -> $bot {
            my $merged = decide(@groups, $bot, $path, 'merge');
            my $strict = decide(@groups, $bot, $path, 'strict');
            if $merged.verdict !== $strict.verdict {
                my $key = "{$merged.verdict}|{$strict.verdict}";
                %divergence-bucket{$key} //= { merged => $merged, strict => $strict, bots => [] };
                %divergence-bucket{$key}<bots>.push($bot);
            }
            if $merged.tied {
                my $key = $merged.rule.line;
                %tie-bucket{$key} //= { rule => $merged.rule, bots => [] };
                %tie-bucket{$key}<bots>.push($bot);
            }
        }
        for %divergence-bucket.values -> $b {
            @findings.push: Finding.new(
                severity => 'MEDIUM',
                code     => 'merge-vs-strict-divergence',
                message  => "Requesting '$path': {$b<bots>.join(', ')} "
                          ~ "{$b<bots>.elems == 1 ?? 'gets' !! 'get'} {$b<merged>.verdict} under Google-style "
                          ~ "merged-group evaluation but {$b<strict>.verdict} under a strict single-group "
                          ~ "RFC 9309 reading. Different crawlers will treat this file differently.",
            );
        }
        for %tie-bucket.values -> $b {
            @findings.push: Finding.new(
                severity => 'INFO',
                code     => 'tie-break-ambiguous',
                message  => "Requesting '$path': {$b<bots>.join(', ')} "
                          ~ "{$b<bots>.elems == 1 ?? 'hits' !! 'hit'} a tie between the longest matching Allow "
                          ~ "and Disallow rules (line {$b<rule>.line} wins because Allow beats Disallow on a "
                          ~ "tie). Lengthen the intended rule if this tie is accidental.",
            );
        }
    }

    return @findings.sort({ -%severity-rank{.severity} }).Array;
}

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

sub read-source(Str $path --> Str) {
    return $*IN.slurp if $path eq '-';
    die "robots.txt not found: $path" unless $path.IO.e;
    return $path.IO.slurp;
}

sub load-token-list(Str $path) {
    die "token list file not found: $path" unless $path.IO.e;
    return $path.IO.lines.map(*.trim).grep(*.chars).Array;
}

multi sub MAIN('check', Str $robots-file, Str $bot, Str $path, Str :$mode = 'merge') {
    CATCH { default { note "Error: {.message}"; exit 2; } }
    die "unknown --mode '$mode', expected 'merge' or 'strict'" unless $mode eq 'merge' | 'strict';
    my @groups = parse-robots(read-source($robots-file));
    my $d = decide(@groups, $bot, $path, $mode);
    say "verdict:  {$d.verdict}";
    say "mode:     $mode";
    if $d.rule {
        say "rule:     {$d.rule.type} '{$d.rule.pattern}' (line {$d.rule.line})";
        say "group:    starts line {$d.group.first-line}, agents [{$d.group.agents.join(', ')}]";
        say "tie:      yes, Allow wins the byte-length tie" if $d.tied;
    } else {
        say "rule:     none matched" ~ ($d.group ?? " in the applicable group (starts line {$d.group.first-line})" !! " — no group applies to '$bot', default allow");
    }
    exit ($d.verdict === Deny ?? 1 !! 0);
}

multi sub MAIN(
    'audit', Str $robots-file,
    Str  :$bots,
    Str  :$paths,
    Str  :$format   = 'text',
    Str  :$fail-on  = 'MEDIUM',
) {
    CATCH { default { note "Error: {.message}"; exit 2; } }
    die "unknown --format '$format', expected 'text' or 'json'" unless $format eq 'text' | 'json';
    die "unknown --fail-on '$fail-on', expected one of {%severity-rank.keys.sort.join(', ')}"
        unless %severity-rank{$fail-on.uc}:exists;
    my @groups = parse-robots(read-source($robots-file));
    my @bot-list   = $bots  ?? load-token-list($bots)  !! @default-ai-bots;
    my @path-list  = $paths ?? load-token-list($paths) !! @default-paths;

    my @findings = audit(@groups, @bot-list, @reference-search-bots, @path-list);

    if $format eq 'json' {
        my @rows = @findings.map({ '{"severity":"' ~ .severity ~ '","code":"' ~ .code
            ~ '","message":' ~ to-json-string(.message) ~ '}' });
        say '[' ~ @rows.join(',') ~ ']';
    } else {
        say "AI Crawler Policy Audit: $robots-file";
        say "Groups parsed: {@groups.elems}  |  Bots checked: {@bot-list.elems + @reference-search-bots.elems}  |  Paths checked: {@path-list.elems}";
        say '';
        if @findings {
            for @findings -> $f {
                say "[{$f.severity}] {$f.code}";
                say "  {$f.message}";
                say '';
            }
        } else {
            say "No findings at or above any tracked severity.";
        }
    }

    my $threshold = %severity-rank{$fail-on.uc} // 2;
    my $worst = @findings.map({ %severity-rank{.severity} }).max // -1;
    exit ($worst >= $threshold ?? 1 !! 0);
}

sub to-json-string(Str $s --> Str) {
    return '"' ~ $s.subst('\\', '\\\\', :g).subst('"', '\\"', :g).subst("\n", '\\n', :g) ~ '"';
}

multi sub MAIN() {
    say "Usage (named options come before the subcommand, Raku's own convention):";
    say "  raku AiCrawlerPolicyAuditor.raku [--mode=merge|strict] check <robots.txt> <bot-token> <path>";
    say "  raku AiCrawlerPolicyAuditor.raku [--bots=file] [--paths=file] [--format=text|json] [--fail-on=INFO|LOW|MEDIUM|HIGH] audit <robots.txt>";
    exit 2;
}
