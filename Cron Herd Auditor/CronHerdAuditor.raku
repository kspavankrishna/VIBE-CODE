#!/usr/bin/env raku
use v6.d;

# Cron Herd Auditor
# Reads crontab files and Kubernetes CronJob manifests, expands every schedule
# over a fixed horizon and reports where jobs pile up on the same minute, which
# schedules do not mean what they look like, and which fire inside a DST window.
# It can also propose a stagger plan for jobs pinned to minute zero.

constant @DAYS-IN-MONTH = 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31;
constant %SEVERITY-RANK = INFO => 0, LOW => 1, MEDIUM => 2, HIGH => 3;
constant @UTC-ZONES = <UTC Etc/UTC GMT Etc/GMT Z Zulu Etc/Zulu>;
constant @DST-HOURS = 1, 2, 3;

my constant %ALIASES =
    '@hourly'   => '0 * * * *',
    '@daily'    => '0 0 * * *',
    '@midnight' => '0 0 * * *',
    '@weekly'   => '0 0 * * 0',
    '@monthly'  => '0 0 1 * *',
    '@yearly'   => '0 0 1 1 *',
    '@annually' => '0 0 1 1 *';

my constant %SPEC =
    minute => %( lo => 0, hi => 59 ),
    hour   => %( lo => 0, hi => 23 ),
    dom    => %( lo => 1, hi => 31 ),
    month  => %( lo => 1, hi => 12, names => %(
        JAN => 1, FEB => 2, MAR => 3, APR => 4, MAY => 5, JUN => 6,
        JUL => 7, AUG => 8, SEP => 9, OCT => 10, NOV => 11, DEC => 12 ) ),
    dow    => %( lo => 0, hi => 7, names => %(
        SUN => 0, MON => 1, TUE => 2, WED => 3, THU => 4, FRI => 5, SAT => 6 ) );

constant @FIELD-ORDER = <minute hour dom month dow>;

class Finding {
    has Str $.severity is required;
    has Str $.code     is required;
    has Str $.message  is required;
}

class Job {
    has Int   $.id       is required;
    has Str   $.name     is required;
    has Str   $.source   is required;
    has Str   $.schedule is required;
    has Str   $.tz       is required;
    has Str   $.kind     is required;   # 'crontab' or 'k8s'
    has       %.fields;
    has Bool  $.dom-star = False;
    has Bool  $.dow-star = False;
    has Int   @.fires;
    has Finding @.findings;

    method flag(Str $severity, Str $code, Str $message) {
        @!findings.push: Finding.new(:$severity, :$code, :$message);
    }
}

# ---------------------------------------------------------------------------
# Field parsing
# ---------------------------------------------------------------------------

sub quartz-token(Str $item --> Bool) {
    so $item ~~ / '?' | '#' | ^ \d* <[LW]>+ $ /;
}

sub resolve-value(Str $tok, Str $kind --> Int) {
    my %spec = %SPEC{$kind};
    return +$tok if $tok ~~ /^ \d+ $/;
    my %names = %spec<names> // %();
    return %names{$tok.uc} if %names{$tok.uc}:exists;
    die "unknown value '$tok' in $kind field";
}

#| Expand one cron field into the set of values it selects.
sub parse-field(Str $text, Str $kind --> Hash) {
    my ($lo, $hi) = %SPEC{$kind}<lo hi>;
    my @notes;
    my %vals;
    die "empty $kind field" unless $text.chars;

    for $text.split(',', :skip-empty) -> $item {
        die "Quartz syntax '$item' in $kind field is not valid in crontab or Kubernetes"
            if quartz-token($item);
        my $m = $item ~~ / ^ (<-[/]>+) [ '/' (\d+) ]? $ /
            or die "cannot read '$item' in $kind field";
        my $base = ~$m[0];
        my $step = $m[1].defined ?? +$m[1] !! Nil;
        die "step of zero in $kind field" if $step.defined && $step == 0;

        my ($from, $to);
        if $base eq '*' {
            ($from, $to) = $lo, $hi;
        }
        elsif $base ~~ / ^ (<-[-]>+) '-' (<-[-]>+) $ / {
            $from = resolve-value(~$/[0], $kind);
            $to   = resolve-value(~$/[1], $kind);
            die "range $base wraps around, split it into two ranges in the $kind field"
                if $from > $to;
        }
        else {
            $from = resolve-value($base, $kind);
            $to   = $step.defined ?? $hi !! $from;
            @notes.push("'$base/$step' has no range: it is read as $base-$hi/$step, which some daemons reject")
                if $step.defined;
        }
        die "value out of range $lo-$hi in $kind field: $item" if $from < $lo || $to > $hi || $from > $hi;

        my $inc = $step // 1;
        my $v = $from;
        while $v <= $to { %vals{$v} = True; $v += $inc }
    }

    if $kind eq 'dow' {
        %vals{0} = True if %vals{7}:delete;
    }

    my $nonuniform = False;
    if $kind eq 'minute' | 'hour' && $text ~~ / ^ '*/' (\d+) $ / {
        my $s = +$/[0];
        if $s > $hi {
            @notes.push("*/$s only ever selects $lo in the $kind field");
        }
        elsif ($hi + 1) % $s {
            $nonuniform = True;
            @notes.push("*/$s does not divide {$hi + 1}: the gap resets every {$kind eq 'minute' ?? 'hour' !! 'day'}, so runs are not evenly spaced");
        }
    }

    %( set => %vals.keys.map(+*).sort.Array, star => $text.starts-with('*'),
       notes => @notes, nonuniform => $nonuniform );
}

# ---------------------------------------------------------------------------
# Input readers
# ---------------------------------------------------------------------------

sub short(Str $s, Int $n = 36 --> Str) {
    my $t = $s.trim;
    $t.chars > $n ?? $t.substr(0, $n - 1) ~ '…' !! $t;
}

#| Yield %( name source schedule tz kind ) records from a crontab body.
sub read-crontab(Str $path, Str $body, Bool $system) {
    my @out;
    for $body.lines.kv -> $i, $raw {
        my $line = $raw.trim;
        next if !$line || $line.starts-with('#');
        next if $line ~~ / ^ <[A..Za..z_]> \w* \s* '=' /;   # VAR=value
        my @tok = $line.words;
        my ($sched, @rest);
        if @tok[0].starts-with('@') {
            my $alias = @tok[0].lc;
            if $alias eq '@reboot' {
                next;
            }
            $sched = %ALIASES{$alias} // die-line($path, $i + 1, "unknown alias {@tok[0]}");
            @rest = @tok[1..*];
        }
        else {
            if @tok < 6 {
                @out.push: %( error => "expected 5 schedule fields and a command", source => "$path:{$i + 1}" );
                next;
            }
            $sched = @tok[0..4].join(' ');
            @rest = @tok[5..*];
        }
        @rest.shift if $system && @rest;               # /etc/crontab user column
        @out.push: %( name => short(@rest.join(' ') || '(no command)'),
                      source => "$path:{$i + 1}", schedule => $sched,
                      tz => 'host local', kind => 'crontab' );
    }
    @out;
}

sub die-line(Str $path, Int $n, Str $msg) { die "$path:$n: $msg" }

sub unquote(Str $s --> Str) {
    my $t = $s.subst(/ \s+ '#' .* $ /, '').trim;
    $t ~~ / ^ <['"]> (.*) <['"]> $ / ?? ~$/[0] !! $t;
}

#| Read every kind: CronJob document out of a (multi document) YAML manifest.
sub read-k8s(Str $path, Str $body) {
    my @out;
    my $offset = 0;
    for $body.split(/ ^^ '---' \s* $$ /) -> $doc {
        my $start-line = $offset;
        $offset += $doc.lines.elems + 1;
        next unless $doc ~~ / ^^ 'kind:' \s* CronJob \s* $$ /;
        my ($name, $sched, $tz, $line-no);
        for $doc.lines.kv -> $i, $l {
            if !$name && $l ~~ / ^ \s+ 'name:' \s* (\S+) / { $name = unquote(~$/[0]) }
            elsif $l ~~ / ^ \s+ 'schedule:' \s* (.+) $ / { $sched = unquote(~$/[0]); $line-no = $start-line + $i + 1 }
            elsif $l ~~ / ^ \s+ 'timeZone:' \s* (.+) $ / { $tz = unquote(~$/[0]) }
        }
        unless $sched.defined {
            @out.push: %( error => "CronJob without spec.schedule", source => "$path:{$start-line + 1}" );
            next;
        }
        @out.push: %( name => $name // '(unnamed)', source => "$path:$line-no",
                      schedule => $sched, tz => $tz // 'cluster default', kind => 'k8s' );
    }
    @out;
}

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

sub build-job(Int $id, %rec --> Job) {
    my $sched = %rec<schedule>;
    my $expanded = %ALIASES{$sched.lc} // $sched;
    my @f = $expanded.words;
    die "expected 5 fields, found {+@f}: $sched" if @f != 5;
    die "'@every' style intervals cannot be expanded here" if $sched.starts-with('@every');

    my %parsed;
    my @notes;
    for @FIELD-ORDER Z @f -> ($kind, $text) {
        my %p = parse-field($text, $kind);
        %parsed{$kind} = %p;
        @notes.append: %p<notes>.map({ "$kind: $_" });
    }

    my $job = Job.new(:$id, name => %rec<name>, source => %rec<source>, schedule => $sched,
                      tz => %rec<tz>, kind => %rec<kind>, fields => %parsed,
                      dom-star => %parsed<dom><star>, dow-star => %parsed<dow><star>);
    for @notes -> $n {
        $job.flag($n.contains('divide') || $n.contains('only ever') ?? 'MEDIUM' !! 'LOW',
                  $n.contains('has no range') ?? 'AMBIGUOUS_STEP' !! 'UNEVEN_STEP', $n);
    }
    $job;
}

sub static-checks(Job $job) {
    my %f = $job.fields;
    my @doms   = %f<dom><set>.list;
    my @months = %f<month><set>.list;

    if !$job.dom-star && !$job.dow-star {
        $job.flag('MEDIUM', 'DOM_DOW_OR',
            'day of month and day of week are both restricted: classic cron fires when EITHER matches, not both. '
            ~ 'A schedule like "0 3 13 * 5" runs on every 13th and on every Friday.');
    }

    # Only the AND case can make a date impossible.
    if $job.dom-star || $job.dow-star {
        my @pairs = (@months X @doms).grep(-> ($m, $d) { $d <= @DAYS-IN-MONTH[$m - 1] });
        if !@pairs {
            $job.flag('HIGH', 'NEVER_FIRES', 'no selected month has any of the selected days, so this job never runs');
        }
        elsif !@pairs.grep({ .[0] != 2 || .[1] != 29 }) {
            $job.flag('LOW', 'LEAP_ONLY', 'only fires on 29 February, which means once every four years or less');
        }
        elsif %f<dom><set>.max > 28 && !$job.dom-star {
            my @short = @months.grep({ %f<dom><set>.max > @DAYS-IN-MONTH[$_ - 1] });
            $job.flag('LOW', 'SKIPS_MONTHS',
                "day {%f<dom><set>.max} does not exist in month(s) {@short.join(',')}: those months are skipped silently");
        }
    }

    my $tz = $job.tz;
    my $hours = %f<hour><set>;
    if $hours.elems < 24 && $hours.grep({ $_ ∈ @DST-HOURS }) {
        my $is-utc = $tz ∈ @UTC-ZONES || ($job.kind eq 'k8s' && $tz eq 'cluster default');
        unless $is-utc {
            $job.flag('MEDIUM', 'DST_WINDOW',
                "fires between 01:00 and 03:59 in {$tz}: on a spring forward night the run can be skipped and on a fall back night it can run twice. Pin to UTC or move it out of the window.");
        }
    }
}

sub expand-fires(Job $job, Date $start, Int $days) {
    my %f = $job.fields;
    my %months = %f<month><set>.map({ $_ => True });
    my %doms   = %f<dom><set>.map({ $_ => True });
    my %dows   = %f<dow><set>.map({ $_ => True });
    my @hours  = %f<hour><set>.list;
    my @mins   = %f<minute><set>.list;
    my $and    = $job.dom-star || $job.dow-star;

    my @fires;
    for ^$days -> $d {
        my $date = $start.later(:day($d));
        next unless %months{$date.month};
        my $dom-ok = so %doms{$date.day};
        my $dow-ok = so %dows{$date.day-of-week % 7};
        next unless $and ?? ($dom-ok && $dow-ok) !! ($dom-ok || $dow-ok);
        for @hours -> $h {
            @fires.push($d * 1440 + $h * 60 + $_) for @mins;
        }
    }
    $job.fires = @fires;
}

sub min-gap(@fires --> Int) {
    return 0 if @fires < 2;
    (1 ..^ @fires).map({ @fires[$_] - @fires[$_ - 1] }).min;
}

sub when-text(Date $start, Int $minute --> Str) {
    my $dt = DateTime.new(date => $start, hour => 0, minute => 0, second => 0).later(:minutes($minute));
    sprintf '%04d-%02d-%02d %02d:%02d', $dt.year, $dt.month, $dt.day, $dt.hour, $dt.minute;
}

#| Pile up analysis and stagger planning for one time zone group.
sub analyse-group(Str $tz, @jobs, Date $start, Int $herd-limit, Int $max-shift --> Hash) {
    my %load;                      # minute offset => count
    my %members;                   # minute offset => [job ids]
    for @jobs -> $j {
        for $j.fires -> $f { %load{$f}++; (%members{$f} //= []).push($j.id) }
    }
    my $peak = %load.values.max // 0;

    my @herds = %load.grep(*.value >= $herd-limit).sort({ $^b.value <=> $^a.value || $^a.key <=> $^b.key });

    # Jobs pinned to the exact top of the hour.
    my @top = @jobs.grep({ .fields<minute><set> eqv [0] && .fires });
    my $top-share = @jobs ?? @top / @jobs !! 0;

    # Greedy stagger: move minute-zero jobs by the smallest shift that lowers their own worst minute.
    my @plan;
    my %work = %load;
    if @top >= $herd-limit {
        for @top.sort(*.name) -> $j {
            %work{$_}-- for $j.fires;
            my ($best-shift, $best-score) = 0, Inf;
            for 0 .. min($max-shift, 59) -> $s {
                my ($worst, $sum) = 0, 0;
                for $j.fires -> $f {
                    my $c = (%work{$f + $s} // 0) + 1;
                    $worst = $c if $c > $worst;
                    $sum += $c * $c;
                }
                my $score = $worst * 1_000_000 + $sum;
                if $score < $best-score { ($best-shift, $best-score) = $s, $score }
            }
            %work{$_ + $best-shift}++ for $j.fires;
            @plan.push: %( job => $j, shift => $best-shift ) if $best-shift;
        }
    }
    my $peak-after = %work.values.max // 0;

    %( tz => $tz, peak => $peak, herds => @herds, members => %members, top => @top.elems,
       top-share => $top-share, plan => @plan, peak-after => $peak-after );
}

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

sub json-str(Str $s --> Str) {
    my $e = $s.trans(['\\', '"', "\n", "\t", "\r"] => ['\\\\', '\\"', '\\n', '\\t', '\\r']);
    $e = $e.subst(/ <[\x00..\x1F]> /, { sprintf('\\u%04x', .Str.ord) }, :g);
    '"' ~ $e ~ '"';
}

sub json-of($v --> Str) {
    given $v {
        when Bool    { $v ?? 'true' !! 'false' }
        when Numeric { ~$v }
        when Str     { json-str($v) }
        when Positional { '[' ~ $v.map(&json-of).join(',') ~ ']' }
        when Associative { '{' ~ $v.sort(*.key).map({ json-str(.key) ~ ':' ~ json-of(.value) }).join(',') ~ '}' }
        default      { 'null' }
    }
}

sub pad(Str $s, Int $w --> Str) { $s.chars >= $w ?? $s !! $s ~ ' ' x ($w - $s.chars) }

sub USAGE {
    $*ERR.say: q:to/TXT/;
    Usage: CronHerdAuditor.raku [options] <file>...

      <file>             crontab, /etc/crontab (with --system) or Kubernetes manifest. Use - for stdin.
      --days=N           days to expand, default 7 (use 8 or more to see a weekly job twice)
      --from=YYYY-MM-DD  first day of the window, UTC, default today
      --herd=N           jobs on one minute that count as a herd, default 5
      --max-runtime=M    flag jobs whose smallest gap between runs is under M minutes
      --max-shift=M      largest minute offset the stagger plan may use, default 20
      --system           crontab files carry a user column after the schedule
      --json             machine readable output
      --fail-on=LEVEL    INFO, LOW, MEDIUM or HIGH, exit 1 at or above it, default HIGH
    Exit codes: 0 clean, 1 findings at or above --fail-on, 2 usage or I/O error.
    TXT
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

sub MAIN(*@paths, Int :$days = 7, Str :$from, Int :$herd = 5, Int :$max-runtime = 0,
         Int :$max-shift = 20, Bool :$system = False, Bool :$json = False, Str :$fail-on = 'HIGH') {
    unless @paths && (%SEVERITY-RANK{$fail-on.uc}:exists) && $days > 0 && $days <= 400 && $herd >= 2 {
        USAGE();
        exit 2;
    }
    my $threshold = %SEVERITY-RANK{$fail-on.uc};
    my Date $start = do { $from ?? (try Date.new($from)) // do { note "bad --from date: $from"; exit 2 }
                               !! DateTime.now(:timezone(0)).Date };

    my @records;
    for @paths -> $p {
        my $body = try { $p eq '-' ?? $*IN.slurp !! $p.IO.slurp };
        unless $body.defined {
            note "cannot read $p: {$!.message}";
            exit 2;
        }
        my @recs = ($p.ends-with('.yaml') || $p.ends-with('.yml') || $body ~~ / ^^ 'kind:' \s* CronJob /)
            ?? read-k8s($p, $body) !! read-crontab($p, $body, $system);
        @records.append: @recs;
    }

    my @jobs;
    my @parse-errors;
    for @records.kv -> $i, %rec {
        if %rec<error> { @parse-errors.push: %( source => %rec<source>, message => %rec<error> ); next }
        my $job = try build-job($i, %rec);
        if $job.defined {
            @jobs.push($job);
        }
        else {
            @parse-errors.push: %( source => %rec<source>, message => $!.message.trim );
        }
    }

    for @jobs -> $j {
        static-checks($j);
        expand-fires($j, $start, $days);
        my $gap = min-gap($j.fires);
        $j.flag('INFO', 'NO_RUN_IN_WINDOW', "does not fire in the {$days} day window starting $start")
            unless $j.fires || $j.findings.grep(*.code eq 'NEVER_FIRES');
        $j.flag('MEDIUM', 'OVERLAP_RISK',
                "smallest gap between runs is $gap min, under --max-runtime=$max-runtime: set concurrencyPolicy Forbid or a lock")
            if $max-runtime && $gap && $gap < $max-runtime;
    }

    my %groups;
    %groups{.tz}.push($_) for @jobs;
    my @group-reports = %groups.sort(*.key).map({ analyse-group(.key, .value, $start, $herd, $max-shift) });

    my @global;
    for @group-reports -> %g {
        if %g<herds> {
            my ($key, $n) = %g<herds>[0].kv;
            @global.push: Finding.new(severity => ($n >= 2 * $herd ?? 'HIGH' !! 'MEDIUM'), code => 'HERD',
                message => "[{%g<tz>}] {+%g<herds>} minute(s) have $herd or more jobs. Worst: $n jobs at {when-text($start, +$key)}");
        }
        if %g<top> >= $herd && %g<top-share> >= 0.4 {
            @global.push: Finding.new(severity => 'MEDIUM', code => 'TOP_OF_HOUR',
                message => sprintf('[%s] %d of %d jobs are pinned to minute 0 (%d%%). Spread them.',
                                   %g<tz>, %g<top>, %groups{%g<tz>}.elems, 100 * %g<top-share>));
        }
    }

    my @all-findings = @global.Slip, @jobs.map(*.findings.Slip).Slip;
    my $worst = (@all-findings.map({ %SEVERITY-RANK{.severity} }).max // -1);
    $worst = %SEVERITY-RANK<HIGH> if @parse-errors;

    if $json {
        my @out-jobs = @jobs.map(-> $j { %(
            name => $j.name, source => $j.source, schedule => $j.schedule, tz => $j.tz,
            runs-in-window => $j.fires.elems, min-gap-minutes => min-gap($j.fires),
            findings => $j.findings.map({ %( severity => .severity, code => .code, message => .message ) }).Array ) });
        my @out-groups = @group-reports.map(-> %g { %(
            tz => %g<tz>, peak => %g<peak>, peak-after-stagger => %g<peak-after>,
            herd-minutes => %g<herds>.map({ %( at => when-text($start, +.key), jobs => .value,
                names => %g<members>{.key}.map({ @jobs.first(*.id == $_).name }).Array ) }).Array,
            stagger-plan => %g<plan>.map({ %( job => .<job>.name, source => .<job>.source,
                shift-minutes => .<shift> ) }).Array ) });
        say json-of(%(
            window => %( start => ~$start, days => $days ), jobs-analysed => +@jobs,
            parse-errors => @parse-errors, global-findings => @global.map({ %( severity => .severity, code => .code, message => .message ) }).Array,
            jobs => @out-jobs, groups => @out-groups ));
    }
    else {
        say "Cron Herd Auditor: {+@jobs} job(s), window $start + $days day(s), herd limit $herd";
        say '';
        for @parse-errors { say "PARSE   {.<source>}: {.<message>}" }
        for @global { say pad(.severity, 8) ~ .message }
        for @jobs -> $j {
            for $j.findings {
                say pad(.severity, 8) ~ "{$j.source} [{.code}] {$j.name}  ({$j.schedule}): {.message}";
            }
        }
        for @group-reports -> %g {
            say '';
            say "Time zone group: {%g<tz>}  peak simultaneous jobs: {%g<peak>}";
            for %g<herds>.head(10) {
                my @names = %g<members>{.key}.map({ @jobs.first(*.id == $_).name });
                say "  {when-text($start, +.key)}  x{.value}  {@names.head(6).join(' | ')}{@names > 6 ?? ' ...' !! ''}";
            }
            if %g<plan> {
                say "  Stagger plan (peak {%g<peak>} to {%g<peak-after>}), edit the minute field:";
                for %g<plan>.list -> %p {
                    my @f = %p<job>.schedule.words;
                    my $new = %p<shift> ~ @f[1..4].map({ " $_" }).join;
                    say "    {pad(%p<job>.source, 28)} {%p<job>.schedule}  ->  {@f[0] eq '0' ?? $new !! '?'}";
                }
            }
        }
        say '';
        say @all-findings || @parse-errors ?? "{+@all-findings} finding(s), {+@parse-errors} parse error(s)" !! 'No findings.';
    }

    exit($worst >= $threshold ?? 1 !! 0);
}
