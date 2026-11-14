#!/usr/bin/env raku
use v6.d;

# PromQL Hazard Linter
# Parses PromQL from Prometheus rule files, PrometheusRule manifests or plain
# query lists, type checks it and reports expressions that are valid but will
# return nothing, flap or lie. See README.md for the full rule list.

my %*SUB-MAIN-OPTS = :named-anywhere;

constant VERSION    = '1.0.0';
constant @SEVERITIES = <INFO LOW MEDIUM HIGH ERROR>;
constant %SEV-RANK   = %( @SEVERITIES.antipairs );

constant %UNIT = ms => 0.001, s => 1, m => 60, h => 3600, d => 86400, w => 604800, y => 31536000;

constant %PREC = 'or' => 1, 'and' => 2, 'unless' => 2,
    '==' => 3, '!=' => 3, '<=' => 3, '<' => 3, '>=' => 3, '>' => 3,
    '+' => 4, '-' => 4, '*' => 5, '/' => 5, '%' => 5, 'atan2' => 5, '^' => 6;
constant %COMPARE = ('==', '!=', '<=', '<', '>=', '>').map(* => True);
constant %SET-OP  = <and or unless>.map(* => True);
constant %FLIP    = '<' => '>', '>' => '<', '<=' => '>=', '>=' => '<=', '==' => '==', '!=' => '!=';

constant %AGGS = <sum min max avg group stddev stdvar count count_values bottomk topk quantile limitk limit_ratio>.map(* => True);
constant %PARAM-AGGS = topk => 'scalar', bottomk => 'scalar', quantile => 'scalar',
    limitk => 'scalar', limit_ratio => 'scalar', count_values => 'string';
constant %KEEPS-SERIES = <topk bottomk limitk limit_ratio>.map(* => True);

constant @RANGE-FUNCS = <rate irate increase delta idelta deriv changes resets
    avg_over_time min_over_time max_over_time sum_over_time count_over_time
    stddev_over_time stdvar_over_time last_over_time first_over_time present_over_time
    absent_over_time mad_over_time ts_of_max_over_time ts_of_min_over_time ts_of_last_over_time>;
constant @VECTOR-FUNCS = <abs ceil floor exp ln log2 log10 sqrt sgn absent sort sort_desc timestamp
    histogram_avg histogram_count histogram_sum histogram_stddev histogram_stdvar
    acos acosh asin asinh atan atanh cos cosh sin sinh tan tanh deg rad>;
constant @DATE-FUNCS = <day_of_month day_of_week day_of_year days_in_month hour minute month year>;

my %FUNCS;
%FUNCS{$_} = ('vector', ('matrix',), 0) for @RANGE-FUNCS;
%FUNCS{$_} = ('vector', ('vector',), 0) for @VECTOR-FUNCS;
%FUNCS{$_} = ('vector', ('vector',), 1) for @DATE-FUNCS;
%FUNCS<quantile_over_time>           = ('vector', <scalar matrix>, 0);
%FUNCS<predict_linear>               = ('vector', <matrix scalar>, 0);
%FUNCS<double_exponential_smoothing> = ('vector', <matrix scalar scalar>, 0);
%FUNCS<holt_winters>                 = ('vector', <matrix scalar scalar>, 0);
%FUNCS<round>                        = ('vector', <vector scalar>, 1);
%FUNCS<clamp>                        = ('vector', <vector scalar scalar>, 0);
%FUNCS<clamp_max>                    = ('vector', <vector scalar>, 0);
%FUNCS<clamp_min>                    = ('vector', <vector scalar>, 0);
%FUNCS<histogram_quantile>           = ('vector', <scalar vector>, 0);
%FUNCS<histogram_fraction>           = ('vector', <scalar scalar vector>, 0);
%FUNCS<label_replace>                = ('vector', <vector string string string string>, 0);
%FUNCS<label_join>                   = ('vector', <vector string string>, -1);
%FUNCS<sort_by_label>                = ('vector', ('vector',), -1);
%FUNCS<sort_by_label_desc>           = ('vector', ('vector',), -1);
%FUNCS<info>                         = ('vector', <vector vector>, 1);
%FUNCS<vector>                       = ('vector', ('scalar',), 0);
%FUNCS<scalar>                       = ('scalar', ('vector',), 0);
%FUNCS<time>                         = ('scalar', (), 0);
%FUNCS<pi>                           = ('scalar', (), 0);

constant %NEEDS-TWO-SAMPLES = <rate irate increase delta idelta deriv predict_linear
    double_exponential_smoothing holt_winters>.map(* => True);
constant %COUNTER-FUNCS = <rate irate increase resets>.map(* => True);
constant %GAUGE-ONLY-FUNCS = <delta idelta deriv avg_over_time sum_over_time stddev_over_time
    stdvar_over_time quantile_over_time mad_over_time>.map(* => True);
constant %FRACTIONAL = <rate irate increase deriv avg_over_time quantile_over_time histogram_quantile
    predict_linear>.map(* => True);
constant %DEPRECATED = holt_winters => 'double_exponential_smoothing';

constant @GAUGE-SUFFIXES = <_ratio _percent _percentage _celsius _info _current _usage _free
    _available _in_use _timestamp_seconds _timestamp _size _size_bytes _used _used_bytes
    _free_bytes _available_bytes _limit _limit_bytes _capacity _temperature _utilization>;

class X::PromQL is Exception {
    has Int $.pos;
    has Str $.reason;
    method message { $!reason }
}

sub dur-seconds(Str $s) {
    return Nil unless $s ~~ /^ [ (\d+) (ms|s|m|h|d|w|y) ]+ $/;
    my $total = 0;
    for $0.list Z $1.list -> ($n, $u) { $total += $n.Int * %UNIT{~$u} }
    $total
}

sub fmt-dur($seconds) {
    return '0s' if $seconds == 0;
    return ($seconds * 1000).round ~ 'ms' if $seconds < 1;
    my $rest = $seconds.Int;
    my $out  = '';
    for (d => 86400, h => 3600, m => 60, s => 1) -> $p {
        my $q = $rest div $p.value;
        next unless $q;
        $out  ~= $q ~ $p.key;
        $rest -= $q * $p.value;
    }
    $out
}

class Token {
    has Str $.kind;
    has Str $.text;
    has Int $.pos;
    has Int $.end;
    has $.value;
}

sub unescape(Str $body) {
    $body.subst(/ '\\' (.) /, {
        given ~$0 {
            when 'n' { "\n" }
            when 't' { "\t" }
            when '"' | "'" | '\\' { $_ }
            default { '\\' ~ $_ }
        }
    }, :g)
}

sub tokenize(Str $src, @comments) {
    my @t;
    my Int $p     = 0;
    my Int $n     = $src.chars;
    my Int $depth = 0;
    my sub emit(Str $kind, Int $len, $value = Nil) {
        @t.push: Token.new(:$kind, text => $src.substr($p, $len), pos => $p, end => $p + $len, :$value);
        $p += $len;
    }
    while $p < $n {
        my $c = $src.substr($p, 1);
        if $c ~~ /\s/ { $p++; next }
        if $c eq '#' {
            @comments.push: $p;
            $p = $src.index("\n", $p) // $n;
            next;
        }
        if $c eq ':' && $depth > 0 { emit('colon', 1); next }
        if $src ~~ m:p($p)/ 0 <[xX]> (<[0..9a..fA..F]>+) / {
            emit('num', $/.chars, :16(~$0)); next;
        }
        if $src ~~ m:p($p)/ [ \d+ [ms|s|m|h|d|w|y] ]+ <!before <[A..Za..z0..9_]>> / {
            my $len = $/.chars;
            emit('dur', $len, dur-seconds(~$/)); next;
        }
        if $src ~~ m:p($p)/ [ \d <[0..9_]>* [ '.' \d* ]? | '.' \d+ ] [ <[eE]> <[+\-]>? \d+ ]? / {
            my $len = $/.chars;
            my $txt = (~$/).subst('_', '', :g).subst(/ '.' <?before <[eE]> | $ > /, '.0');
            emit('num', $len, $txt.Num); next;
        }
        if $src ~~ m:p($p)/ <[a..zA..Z_:]> <[a..zA..Z0..9_:]>* / {
            emit('ident', $/.chars); next;
        }
        if $src ~~ m:p($p)/ '"' ( [ '\\' . | <-["\\\n]> ]* ) '"' / {
            emit('str', $/.chars, unescape(~$0)); next;
        }
        if $src ~~ m:p($p)/ "'" ( [ '\\' . | <-['\\\n]> ]* ) "'" / {
            emit('str', $/.chars, unescape(~$0)); next;
        }
        if $src ~~ m:p($p)/ '`' ( <-[`]>* ) '`' / {
            emit('str', $/.chars, ~$0); next;
        }
        if $c eq '"' | "'" | '`' {
            X::PromQL.new(pos => $p, reason => 'unterminated string literal').throw;
        }
        my $two = $src.substr($p, 2);
        if $two eq any('==', '!=', '<=', '>=', '=~', '!~') { emit('op', 2); next }
        if $c eq any('+', '-', '*', '/', '%', '^', '<', '>', '=', '@') { emit('op', 1); next }
        if $two eq '{{' {
            X::PromQL.new(pos => $p, reason => "unexpected '\{\{' (template placeholder, render the file before linting)").throw;
        }
        given $c {
            when '(' { emit('lparen', 1); next }
            when ')' { emit('rparen', 1); next }
            when '{' { emit('lbrace', 1); next }
            when '}' { emit('rbrace', 1); next }
            when ',' { emit('comma', 1); next }
            when '[' { $depth++; emit('lbrack', 1); next }
            when ']' { $depth-- if $depth > 0; emit('rbrack', 1); next }
        }
        my $hint = $c eq '$' ?? ' (dashboard variable, substitute it before linting)'
                !! '';
        X::PromQL.new(pos => $p, reason => "unexpected character '$c'$hint").throw;
    }
    @t.push: Token.new(kind => 'eof', text => '', pos => $n, end => $n);
    @t
}

role Node {
    has Int $.from is rw = 0;
    has Int $.to   is rw = 0;
}
class NumLit does Node { has $.value }
class StrLit does Node { has Str $.value }
class Matcher { has Str $.label; has Str $.op; has Str $.value }
class Selector does Node {
    has Str $.name = '';
    has @.matchers;
    has $.range  is rw;
    has $.offset is rw;
    has $.at     is rw;
}
class Subquery does Node {
    has $.expr;
    has $.range;
    has $.step;
    has $.offset is rw;
    has $.at     is rw;
}
class Call does Node { has Str $.func; has @.args }
class Agg does Node {
    has Str $.op;
    has $.param;
    has $.expr;
    has Str $.mode = '';
    has @.labels;
}
class Binary does Node {
    has Str $.op;
    has $.lhs;
    has $.rhs;
    has Bool $.bool = False;
    has Str $.match = '';
    has @.match-labels;
    has Str $.group = '';
    has @.group-labels;
}
class Unary does Node { has Str $.op; has $.expr }
class Paren does Node { has $.expr }

class Parser {
    has Str $.src;
    has @.comments;
    has @!tok;
    has Int $!i = 0;

    submethod TWEAK { @!tok = tokenize($!src, @!comments) }

    method last-token-pos { @!tok.elems > 1 ?? @!tok[*-2].pos !! -1 }

    method !peek(Int $ahead = 0) { @!tok[min($!i + $ahead, @!tok.end)] }
    method !advance { my $t = @!tok[$!i]; $!i++ if $!i < @!tok.end; $t }
    method !fail(Str $reason, $tok = self!peek) {
        X::PromQL.new(pos => $tok.pos, :$reason).throw;
    }
    method !describe($t) { $t.kind eq 'eof' ?? 'end of expression' !! "'{$t.text}'" }
    method !expect(Str $kind, Str $what) {
        my $t = self!peek;
        self!fail("expected $what but found {self!describe($t)}") unless $t.kind eq $kind;
        self!advance
    }
    method !is-kw($t, *@words) { $t.kind eq 'ident' && $t.text.lc eq any(@words) }

    method parse {
        self!fail('empty expression') if self!peek.kind eq 'eof';
        my $e = self!expr(1);
        my $t = self!peek;
        self!fail("unexpected {self!describe($t)} after a complete expression") unless $t.kind eq 'eof';
        $e
    }

    method !binop($t) {
        return $t.text if $t.kind eq 'op' && (%PREC{$t.text}:exists);
        return $t.text.lc if $t.kind eq 'ident' && $t.text.lc eq any(<and or unless atan2>);
        Nil
    }

    method !expr(Int $min) {
        my $lhs = self!unary;
        loop {
            my $op = self!binop(self!peek);
            last unless $op.defined;
            my $prec = %PREC{$op};
            last if $prec < $min;
            my $optok = self!advance;
            my %mod = self!modifiers($op, $optok);
            my $rhs = self!expr($op eq '^' ?? $prec !! $prec + 1);
            $lhs = Binary.new(:$op, :$lhs, :$rhs, bool => so %mod<bool>,
                match => %mod<match> // '', match-labels => (%mod<match-labels> // []).list,
                group => %mod<group> // '', group-labels => (%mod<group-labels> // []).list,
                from => $lhs.from, to => $rhs.to);
        }
        $lhs
    }

    method !modifiers(Str $op, $optok) {
        my %m;
        if self!is-kw(self!peek, 'bool') {
            self!fail("bool modifier is only valid on comparison operators, not '$op'") unless %COMPARE{$op};
            self!advance;
            %m<bool> = True;
        }
        if self!is-kw(self!peek, 'on', 'ignoring') && self!peek(1).kind eq 'lparen' {
            %m<match> = self!advance.text.lc;
            %m<match-labels> = self!label-list;
            if self!is-kw(self!peek, 'group_left', 'group_right') {
                my $g = self!advance;
                self!fail("$op is a set operator and cannot use {$g.text}", $g) if %SET-OP{$op};
                %m<group> = $g.text.lc.subst('group_', '');
                %m<group-labels> = self!peek.kind eq 'lparen' ?? self!label-list !! [];
            }
        }
        elsif self!is-kw(self!peek, 'group_left', 'group_right') {
            my $g = self!advance;
            self!fail("$op is a set operator and cannot use {$g.text}", $g) if %SET-OP{$op};
            %m<group> = $g.text.lc.subst('group_', '');
            %m<group-labels> = self!peek.kind eq 'lparen' && self!looks-like-label-list
                ?? self!label-list !! [];
        }
        %m
    }

    method !looks-like-label-list {
        my $k = 1;
        loop {
            my $t = self!peek($k);
            return True if $t.kind eq 'rparen';
            return False unless $t.kind eq 'ident' | 'str';
            $k++;
            my $sep = self!peek($k);
            return True if $sep.kind eq 'rparen';
            return False unless $sep.kind eq 'comma';
            $k++;
        }
    }

    method !label-list {
        self!expect('lparen', "'('");
        my @labels;
        until self!peek.kind eq 'rparen' {
            my $t = self!peek;
            self!fail("expected a label name but found {self!describe($t)}") unless $t.kind eq 'ident' | 'str';
            self!advance;
            @labels.push: $t.kind eq 'str' ?? $t.value !! $t.text;
            last unless self!peek.kind eq 'comma';
            self!advance;
        }
        self!expect('rparen', "')'");
        @labels
    }

    method !unary {
        my $t = self!peek;
        if $t.kind eq 'op' && $t.text eq '+' | '-' {
            self!advance;
            my $e = self!expr(6);
            return Unary.new(op => $t.text, expr => $e, from => $t.pos, to => $e.to);
        }
        self!postfix(self!primary)
    }

    method !duration(Str $what) {
        my $t = self!peek;
        self!fail("expected $what but found {self!describe($t)}") unless $t.kind eq 'dur' | 'num';
        self!advance;
        self!fail("$what must be positive", $t) unless $t.value > 0;
        $t.value
    }

    method !postfix($node is copy) {
        loop {
            my $t = self!peek;
            if $t.kind eq 'lbrack' {
                self!advance;
                my $range = self!duration('a range duration');
                if self!peek.kind eq 'colon' {
                    self!advance;
                    my $step = self!peek.kind eq 'rbrack' ?? Nil !! self!duration('a subquery step');
                    my $end = self!expect('rbrack', "']'");
                    self!fail('a subquery needs an instant vector, this is already a range', $t)
                        if $node ~~ Subquery || ($node ~~ Selector && $node.range.defined);
                    $node = Subquery.new(expr => $node, :$range, :$step, from => $node.from, to => $end.end);
                }
                else {
                    my $end = self!expect('rbrack', "']'");
                    self!fail('a range like [5m] is only valid directly on a selector, use a subquery [5m:] for other expressions', $t)
                        unless $node ~~ Selector && !$node.range.defined;
                    self!fail('the range must come before offset and @', $t)
                        if $node.offset.defined || $node.at.defined;
                    $node.range = $range;
                    $node.to    = $end.end;
                }
            }
            elsif self!is-kw($t, 'offset') {
                self!advance;
                my $sign = 1;
                if self!peek.kind eq 'op' && self!peek.text eq '-' | '+' {
                    $sign = -1 if self!advance.text eq '-';
                }
                my $d = self!peek;
                self!fail("expected an offset duration but found {self!describe($d)}") unless $d.kind eq 'dur' | 'num';
                self!advance;
                self!fail('offset is only valid on a selector or a subquery', $t) unless $node ~~ Selector | Subquery;
                self!fail('offset given twice', $t) if $node.offset.defined;
                $node.offset = $sign * $d.value;
                $node.to     = $d.end;
            }
            elsif $t.kind eq 'op' && $t.text eq '@' {
                self!advance;
                my $v = self!peek;
                my $at;
                if $v.kind eq 'num' { self!advance; $at = $v.value }
                elsif $v.kind eq 'op' && $v.text eq '-' && self!peek(1).kind eq 'num' {
                    self!advance; $v = self!advance; $at = -$v.value;
                }
                elsif self!is-kw($v, 'start', 'end') {
                    self!advance;
                    self!expect('lparen', "'('");
                    $v  = self!expect('rparen', "')'");
                    $at = 'boundary';
                }
                else { self!fail("expected a timestamp, start() or end() after @ but found {self!describe($v)}") }
                self!fail('@ is only valid on a selector or a subquery', $t) unless $node ~~ Selector | Subquery;
                self!fail('@ given twice', $t) if $node.at.defined;
                $node.at = $at;
                $node.to = $v.end;
            }
            else { last }
        }
        $node
    }

    method !primary {
        my $t = self!peek;
        given $t.kind {
            when 'num' | 'dur' { self!advance; return NumLit.new(value => $t.value, from => $t.pos, to => $t.end) }
            when 'str' { self!advance; return StrLit.new(value => $t.value, from => $t.pos, to => $t.end) }
            when 'lparen' {
                self!advance;
                my $e   = self!expr(1);
                my $end = self!expect('rparen', "')'");
                return Paren.new(expr => $e, from => $t.pos, to => $end.end);
            }
            when 'lbrace' { return self!selector('', $t.pos, $t.pos) }
            when 'ident' {
                my $lc   = $t.text.lc;
                my $next = self!peek(1);
                if %AGGS{$lc} && ($next.kind eq 'lparen' || self!is-kw($next, 'by', 'without')) {
                    return self!aggregation;
                }
                if $next.kind eq 'lparen' {
                    self!fail("unknown function '{$t.text}'") unless %FUNCS{$t.text}:exists;
                    return self!call;
                }
                self!advance;
                if $lc eq 'inf' | 'nan' {
                    return NumLit.new(value => ($lc eq 'inf' ?? Inf !! NaN), from => $t.pos, to => $t.end);
                }
                return self!selector($t.text, $t.pos, $t.end);
            }
        }
        self!fail("unexpected {self!describe($t)}");
    }

    method !selector(Str $name is copy, Int $from, Int $to is copy) {
        my @matchers;
        if self!peek.kind eq 'lbrace' {
            self!advance;
            until self!peek.kind eq 'rbrace' {
                my $l = self!peek;
                self!fail("expected a label matcher but found {self!describe($l)}") unless $l.kind eq 'ident' | 'str';
                self!advance;
                if $l.kind eq 'str' && self!peek.kind eq 'comma' | 'rbrace' {
                    self!fail('metric name given twice', $l) if $name ne '';
                    $name = $l.value;
                }
                else {
                    my $op = self!peek;
                    self!fail("expected =, !=, =~ or !~ but found {self!describe($op)}")
                        unless $op.kind eq 'op' && $op.text eq any('=', '!=', '=~', '!~');
                    self!advance;
                    my $v = self!expect('str', 'a quoted label value');
                    @matchers.push: Matcher.new(
                        label => ($l.kind eq 'str' ?? $l.value !! $l.text), op => $op.text, value => $v.value);
                }
                last unless self!peek.kind eq 'comma';
                self!advance;
            }
            $to = self!expect('rbrace', "'}'").end;
        }
        Selector.new(:$name, :@matchers, :$from, :$to)
    }

    method !grouping {
        my $mode = self!advance.text.lc;
        ($mode, |self!label-list)
    }

    method !aggregation {
        my $t  = self!advance;
        my $op = $t.text.lc;
        my ($mode, @labels) = ('',);
        ($mode, @labels) = self!grouping if self!is-kw(self!peek, 'by', 'without');
        self!expect('lparen', "'('");
        my @args;
        until self!peek.kind eq 'rparen' {
            @args.push: self!expr(1);
            last unless self!peek.kind eq 'comma';
            self!advance;
        }
        my $end = self!expect('rparen', "')'").end;
        if self!is-kw(self!peek, 'by', 'without') {
            self!fail('grouping given both before and after the aggregation') if $mode ne '';
            ($mode, @labels) = self!grouping;
            $end = @!tok[$!i - 1].end;
        }
        my $want = %PARAM-AGGS{$op}:exists ?? 2 !! 1;
        self!fail("$op takes $want argument{$want == 1 ?? '' !! 's'} but got {+@args}", $t) unless @args == $want;
        Agg.new(:$op, param => ($want == 2 ?? @args[0] !! Nil), expr => @args[*-1],
                :$mode, :@labels, from => $t.pos, to => $end)
    }

    method !call {
        my $t = self!advance;
        self!expect('lparen', "'('");
        my @args;
        until self!peek.kind eq 'rparen' {
            @args.push: self!expr(1);
            last unless self!peek.kind eq 'comma';
            self!advance;
        }
        my $end = self!expect('rparen', "')'").end;
        Call.new(func => $t.text, :@args, from => $t.pos, to => $end)
    }
}

multi sub type-of(NumLit $)    { 'scalar' }
multi sub type-of(StrLit $)    { 'string' }
multi sub type-of(Selector $n) { $n.range.defined ?? 'matrix' !! 'vector' }
multi sub type-of(Subquery $)  { 'matrix' }
multi sub type-of(Call $n)     { %FUNCS{$n.func}[0] }
multi sub type-of(Agg $)       { 'vector' }
multi sub type-of(Unary $n)    { type-of($n.expr) }
multi sub type-of(Paren $n)    { type-of($n.expr) }
multi sub type-of(Binary $n) {
    type-of($n.lhs) eq 'scalar' && type-of($n.rhs) eq 'scalar' ?? 'scalar' !! 'vector'
}

multi sub children(Node $)     { () }
multi sub children(Subquery $n) { ($n.expr,) }
multi sub children(Call $n)    { $n.args.List }
multi sub children(Agg $n)     { $n.param.defined ?? ($n.param, $n.expr) !! ($n.expr,) }
multi sub children(Binary $n)  { ($n.lhs, $n.rhs) }
multi sub children(Unary $n)   { ($n.expr,) }
multi sub children(Paren $n)   { ($n.expr,) }

sub subtree($n) { gather { take $n; .take for children($n).map({ |subtree($_) }) } }

sub strip($n is copy) { $n = $n.expr while $n ~~ Paren; $n }

sub const-value($n) {
    given strip($n) {
        when NumLit { return .value }
        when Unary {
            my $v = const-value(.expr);
            return $v.defined ?? (.op eq '-' ?? -$v !! $v) !! Nil;
        }
    }
    Nil
}

sub has-bare-selector($n) {
    given $n {
        when Selector { return !.range.defined }
        default { return so children($n).first({ has-bare-selector($_) }) }
    }
}

class LabelInfo {
    has $.exact;
    has Set $.absent = set();
    method known { $!exact.defined }
    method lacks(Str $l) { ($!exact.defined && $l !(elem) $!exact) || $l (elem) $!absent }
    method plus(@labels) {
        LabelInfo.new(exact => ($!exact.defined ?? $!exact (|) set(@labels) !! Any),
                      absent => $!absent (-) set(@labels))
    }
    method minus(@labels) {
        LabelInfo.new(exact => ($!exact.defined ?? $!exact (-) set(@labels) !! Any),
                      absent => $!absent (|) set(@labels))
    }
}

sub fmt-set($s) { '(' ~ $s.keys.sort.join(', ') ~ ')' }

multi sub labels-of(NumLit $)    { Nil }
multi sub labels-of(StrLit $)    { Nil }
multi sub labels-of(Selector $)  { LabelInfo.new }
multi sub labels-of(Subquery $n) { labels-of($n.expr) }
multi sub labels-of(Paren $n)    { labels-of($n.expr) }
multi sub labels-of(Unary $n)    { labels-of($n.expr) }
multi sub labels-of(Agg $n) {
    my $inner = labels-of($n.expr) // LabelInfo.new;
    return $inner if %KEEPS-SERIES{$n.op};
    my @extra = $n.op eq 'count_values' && $n.param ~~ StrLit ?? ($n.param.value,) !! ();
    given $n.mode {
        when 'by'      { return LabelInfo.new(exact => set(|$n.labels, |@extra)) }
        when 'without' { return $inner.minus($n.labels.List).plus(@extra) }
        default        { return LabelInfo.new(exact => set(@extra)) }
    }
}
multi sub labels-of(Call $n) {
    my $f = $n.func;
    return Nil if type-of($n) ne 'vector';
    return LabelInfo.new(exact => set()) if $f eq 'vector';
    if $f eq 'absent' | 'absent_over_time' {
        my $arg = strip($n.args[0] // return LabelInfo.new(exact => set()));
        return LabelInfo.new(exact => set()) unless $arg ~~ Selector;
        return LabelInfo.new(exact => set($arg.matchers.grep({ .op eq '=' && .value ne '' }).map(*.label)));
    }
    return LabelInfo.new if $f eq 'info';
    my $vec = $n.args.first({ type-of($_) eq 'vector' | 'matrix' });
    my $inner = $vec.defined ?? (labels-of($vec) // LabelInfo.new) !! LabelInfo.new(exact => set());
    given $f {
        when 'histogram_quantile' | 'histogram_fraction' { return $inner.minus(('le',)) }
        when 'label_replace' | 'label_join' {
            my $dst = $n.args[1];
            return $dst ~~ StrLit ?? $inner.plus(($dst.value,)) !! LabelInfo.new;
        }
    }
    $inner
}
multi sub labels-of(Binary $n) {
    my $l = labels-of($n.lhs);
    my $r = labels-of($n.rhs);
    return $r unless $l.defined;
    return $l unless $r.defined;
    if $n.op eq 'and' | 'unless' { return $l }
    if $n.op eq 'or' {
        return $l if $l.known && $r.known && $l.exact eqv $r.exact;
        return LabelInfo.new(absent => $l.absent (&) $r.absent);
    }
    given $n.group {
        when 'left'  { return $l.plus($n.group-labels.List) }
        when 'right' { return $r.plus($n.group-labels.List) }
    }
    given $n.match {
        when 'on'       { return LabelInfo.new(exact => set($n.match-labels)) }
        when 'ignoring' { return $l.minus($n.match-labels.List) }
    }
    $l
}

sub regex-matches-empty(Str $v) {
    so $v eq '' || $v ~~ /^ '('? '.*' ')'? $/ || $v ~~ /^ '|' | '|' $ | '||' / || $v ~~ /^ . <[*?]> $/
}

sub matcher-accepts-missing(Matcher $m) {
    given $m.op {
        when '='  { return $m.value eq '' }
        when '!=' { return $m.value ne '' }
        when '=~' { return regex-matches-empty($m.value) }
        when '!~' { return !regex-matches-empty($m.value) }
    }
}

sub looks-like-counter(Str $name) { so $name ~~ / [ '_total' | '_bucket' ] $ / }
sub looks-like-gauge(Str $name)   { $name eq 'up' || so @GAUGE-SUFFIXES.first({ $name.ends-with($_) }) }

class Analyzer {
    has Str $.src;
    has $.scrape;
    has $.eval;
    has Str $.kind = 'expr';
    has @.found;
    has %!seen;

    method in-rule { $!kind ne 'expr' }

    method flag($node, Str $code, Str $severity, Str $message) {
        my $key = "$code {$node.from} {$node.to}";
        return if %!seen{$key}++;
        @!found.push: %( :$code, :$severity, :$message, from => $node.from, to => $node.to );
    }

    method type-error($node, Str $message) { self.flag($node, 'TYPE_ERROR', 'ERROR', $message) }

    multi method visit(NumLit $) { }
    multi method visit(StrLit $) { }
    multi method visit(Paren $n) { self.visit($n.expr) }
    multi method visit(Unary $n) {
        self.visit($n.expr);
        my $t = type-of($n.expr);
        self.type-error($n, "unary {$n.op} needs a scalar or an instant vector, got a $t") if $t eq 'matrix' | 'string';
    }

    multi method visit(Selector $n) {
        my @m = $n.matchers;
        if $n.name eq '' && !@m.first({ !matcher-accepts-missing($_) }) {
            self.flag($n, 'SELECTOR_MATCHES_EMPTY', 'ERROR',
                'Every matcher here also matches a missing label, so this selects every series in the database. Prometheus rejects it. Add a metric name or one matcher that cannot match the empty string.');
        }
        my %equal;
        for @m.grep(*.op eq '=') -> $m {
            if (%equal{$m.label}:exists) && %equal{$m.label} ne $m.value {
                self.flag($n, 'SELECTOR_NEVER_MATCHES', 'HIGH',
                    "Label {$m.label} is required to equal both \"{%equal{$m.label}}\" and \"{$m.value}\". No series can satisfy that, so the selector is always empty.");
            }
            %equal{$m.label} = $m.value;
        }
        for @m.grep(*.op eq '=~' | '!~') -> $m {
            my $v = $m.value;
            if $v eq '.*' || $v eq '(.*)' {
                self.flag($n, 'REGEX_MATCH_ALL', 'LOW',
                    $m.op eq '=~'
                        ?? "{$m.label}=~\"$v\" matches every series including ones without the label, so it filters nothing. Use {$m.label}=~\".+\" to require the label or drop the matcher."
                        !! "{$m.label}!~\"$v\" can never match, so the selector is always empty.");
            }
            elsif $v ne '' && $v !~~ / <[\.\*\+\?\(\)\[\]\{\}\|\\\^\$]> / {
                self.flag($n, 'REGEX_WITHOUT_PATTERN', 'LOW',
                    "{$m.label}{$m.op}\"$v\" has no regex syntax in it. PromQL regexes are fully anchored, so this only matches the exact value \"$v\", not values that contain it. Use {$m.op eq '=~' ?? '=' !! '!='} or write \".*$v.*\" if you meant a substring match.");
            }
            elsif $v.starts-with('^') || $v ~~ / <!after '\\'> '$' $ / {
                self.flag($n, 'REGEX_REDUNDANT_ANCHOR', 'INFO',
                    "{$m.label}{$m.op}\"$v\" carries ^ or \$ anchors. PromQL already anchors both ends of every regex, so they add nothing.");
            }
            elsif $m.op eq '=~' && $v ne '' && $v ~~ /^ '|' | '|' $ | '||' / {
                self.flag($n, 'REGEX_EMPTY_ALTERNATIVE', 'MEDIUM',
                    "{$m.label}=~\"$v\" has an empty alternative, so it also matches every series that does not have the {$m.label} label at all.");
            }
        }
    }

    multi method visit(Subquery $n) {
        self.visit($n.expr);
        my $t = type-of($n.expr);
        self.type-error($n, "a subquery needs an instant vector inside, got a $t") unless $t eq 'vector';
        if $n.step.defined && $n.step > $n.range {
            self.flag($n, 'SUBQUERY_STEP_EXCEEDS_RANGE', 'HIGH',
                "The subquery step {fmt-dur($n.step)} is longer than its range {fmt-dur($n.range)}, so it yields at most one point and anything computed over it is empty or constant.");
        }
        my $step = $n.step // $!eval;
        for subtree($n.expr).grep({ $_ ~~ Selector && .range.defined && .range < $step }) -> $s {
            self.flag($n, 'SUBQUERY_UNDERSAMPLES', 'MEDIUM',
                "The subquery evaluates every {fmt-dur($step)} but the inner window is only {fmt-dur($s.range)}. Samples between steps are never looked at, so spikes can disappear. Make the inner range at least as long as the step.");
        }
    }

    multi method visit(Call $n) {
        self.visit($_) for $n.args;
        my $f = $n.func;
        my ($ret, $types, $opt) = %FUNCS{$f};
        my @types = $types.list;
        my $min = @types.elems - ($opt > 0 ?? $opt !! 0);
        my $max = $opt < 0 ?? Inf !! @types.elems;
        if $n.args.elems < $min || $n.args.elems > $max {
            self.type-error($n, "$f expects {$min == $max ?? $min !! $max == Inf ?? "at least $min" !! "$min to $max"} argument{$max == 1 ?? '' !! 's'} but got {$n.args.elems}");
            return;
        }
        for $n.args.kv -> $i, $arg {
            my $want = $i < @types.elems ?? @types[$i] !! 'string';
            my $got  = type-of($arg);
            if $got ne $want {
                my %nice = scalar => 'a scalar', vector => 'an instant vector', matrix => 'a range vector like metric[5m]', string => 'a string';
                self.type-error($arg, "argument {$i + 1} of $f must be %nice{$want} but this is %nice{$got}");
                return;
            }
        }
        if %DEPRECATED{$f} -> $new {
            self.flag($n, 'DEPRECATED_FUNCTION', 'LOW', "$f was renamed to $new in Prometheus 3.0 and the old name no longer parses there.");
        }
        if $f eq 'irate' && self.in-rule {
            self.flag($n, 'IRATE_IN_RULE', $!kind eq 'alert' ?? 'MEDIUM' !! 'LOW',
                $!kind eq 'alert'
                    ?? 'irate only looks at the last two samples in the window, so one noisy scrape flips the alert on and off and a for clause rarely holds. Use rate over a window of at least four scrape intervals.'
                    !! 'irate only looks at the last two samples, so the recorded value depends on which instant the rule happened to run. Use rate for recorded series.');
        }
        my $matrix = $n.args.first({ type-of($_) eq 'matrix' });
        self.check-range-arg($n, strip($matrix)) if $matrix.defined;
        self.check-histogram($n) if $f eq 'histogram_quantile';
        if $f eq 'quantile_over_time' { self.check-phi($n.args[0], $f) }
    }

    method check-phi($arg, Str $what) {
        my $phi = const-value($arg) // return;
        return if $phi === NaN;
        if $phi < 0 || $phi > 1 {
            self.flag($arg, 'QUANTILE_OUT_OF_RANGE', 'HIGH',
                "$what takes a quantile between 0 and 1. $phi returns {$phi > 1 ?? '+Inf' !! '-Inf'} for every series. For the {$phi > 1 && $phi <= 100 ?? $phi !! 99}th percentile write {$phi > 1 && $phi <= 100 ?? $phi / 100 !! 0.99}.");
        }
    }

    method check-range-arg(Call $n, $arg) {
        my $f = $n.func;
        given $arg {
            when Selector {
                my $r = .range;
                if %NEEDS-TWO-SAMPLES{$f} {
                    if $r < 2 * $!scrape {
                        self.flag($n, 'RANGE_TOO_SHORT', 'HIGH',
                            "$f needs two samples inside the window. A {fmt-dur($r)} range at a {fmt-dur($!scrape)} scrape interval usually holds one or none, so this returns nothing most of the time. Use at least {fmt-dur(4 * $!scrape)}.");
                    }
                    elsif $r < 4 * $!scrape {
                        self.flag($n, 'RANGE_FRAGILE', 'MEDIUM',
                            "A {fmt-dur($r)} range is under four scrape intervals of {fmt-dur($!scrape)}. One failed or late scrape leaves fewer than two samples and the series drops out. Use at least {fmt-dur(4 * $!scrape)}.");
                    }
                }
                elsif $r < $!scrape {
                    self.flag($n, 'RANGE_TOO_SHORT', 'MEDIUM',
                        "A {fmt-dur($r)} range is shorter than the {fmt-dur($!scrape)} scrape interval, so the window is often empty and $f returns nothing for those evaluations.");
                }
                if self.in-rule && $r < $!eval {
                    self.flag($n, 'RANGE_SHORTER_THAN_EVAL', 'MEDIUM',
                        "The rule runs every {fmt-dur($!eval)} but only looks back {fmt-dur($r)}. Whatever happens in the gap between two evaluations is never seen.");
                }
                my $name = .name;
                if $name ne '' && !$name.contains(':') {
                    if %COUNTER-FUNCS{$f} && looks-like-gauge($name) {
                        self.flag($n, 'COUNTER_FUNCTION_ON_GAUGE', 'MEDIUM',
                            "$f treats every decrease as a counter reset. $name looks like a gauge, so each time it goes down the result jumps by its full value. Use delta or deriv for gauges.");
                    }
                    elsif %GAUGE-ONLY-FUNCS{$f} && looks-like-counter($name) {
                        self.flag($n, 'GAUGE_FUNCTION_ON_COUNTER', 'MEDIUM',
                            "$f does not handle counter resets and $name looks like a counter. The raw value only tells you how long the process has been up. Take rate or increase first.");
                    }
                }
            }
            when Subquery {
                my $step = .step // $!eval;
                if %NEEDS-TWO-SAMPLES{$f} && .range < 2 * $step && !(.step.defined && .step > .range) {
                    self.flag($n, 'RANGE_TOO_SHORT', 'HIGH',
                        "$f needs two points but a {fmt-dur(.range)} subquery at a {fmt-dur($step)} step produces at most one.");
                }
                if %COUNTER-FUNCS{$f} {
                    my $inner = strip(.expr);
                    if $inner ~~ Selector { }
                    elsif has-bare-selector($inner) {
                        self.flag($n, 'RATE_OF_AGGREGATE', 'HIGH',
                            "$f is applied after the series were combined. When one underlying counter resets, the combined value drops a little and $f reads that as a reset of the whole total, which produces a huge false spike. Apply $f to each raw series first and aggregate the result.");
                    }
                    else {
                        self.flag($n, 'RATE_OF_GAUGE', 'MEDIUM',
                            "$f is applied to a value that is already a rate or another derived gauge. Every dip is treated as a counter reset. Use deriv or delta, or an _over_time function.");
                    }
                }
            }
        }
    }

    method check-histogram(Call $n) {
        self.check-phi($n.args[0], 'histogram_quantile');
        my $v = $n.args[1];
        my @buckets = subtree($v).grep({ $_ ~~ Selector && .name.ends-with('_bucket') });
        return unless @buckets;
        my $info = labels-of($v);
        if $info.defined && $info.lacks('le') {
            self.flag($n, 'HISTOGRAM_MISSING_LE', 'HIGH',
                'The le label is aggregated away before histogram_quantile sees the buckets. Without le it cannot tell the buckets apart and returns NaN or nothing. Keep le in the grouping, for example sum by (le, job).');
        }
        unless subtree($v).first({ $_ ~~ Call && %COUNTER-FUNCS{.func} }) {
            self.flag($n, 'HISTOGRAM_WITHOUT_RATE', 'MEDIUM',
                'histogram_quantile is fed raw bucket counters. That is the quantile over everything since each process started, not over a recent window, and it barely moves. Wrap the buckets in rate first.');
        }
    }

    multi method visit(Agg $n) {
        self.visit($n.param) if $n.param.defined;
        self.visit($n.expr);
        my $t = type-of($n.expr);
        self.type-error($n.expr, "{$n.op} aggregates an instant vector, got a $t") unless $t eq 'vector';
        if $n.param.defined {
            my $want = %PARAM-AGGS{$n.op};
            my $got  = type-of($n.param);
            self.type-error($n.param, "the first argument of {$n.op} must be a $want, got a $got") unless $got eq $want;
            self.check-phi($n.param, 'quantile') if $n.op eq 'quantile';
        }
        if $n.op eq 'avg' | 'sum' {
            my $inner = strip($n.expr);
            my $quantile-label = $inner ~~ Selector && $inner.matchers.first({ .label eq 'quantile' && .op eq '=' });
            if ($inner ~~ Call && $inner.func eq 'histogram_quantile') || $quantile-label {
                self.flag($n, 'AGGREGATE_OF_QUANTILES', 'MEDIUM',
                    "{$n.op} over per series quantiles is not the quantile of the combined data. A busy instance and an idle one count the same. Aggregate the buckets by le first and take the quantile once.");
            }
        }
    }

    multi method visit(Binary $n) {
        self.visit($n.lhs);
        self.visit($n.rhs);
        my ($lt, $rt) = type-of($n.lhs), type-of($n.rhs);
        for ($n.lhs, $lt), ($n.rhs, $rt) -> ($side, $t) {
            if $t eq 'matrix' | 'string' {
                self.type-error($side, "operator {$n.op} needs scalars or instant vectors, this side is a {$t eq 'matrix' ?? 'range vector' !! 'string'}");
                return;
            }
        }
        my $cmp = so %COMPARE{$n.op};
        if %SET-OP{$n.op} && ($lt eq 'scalar' || $rt eq 'scalar') {
            self.type-error($n, "{$n.op} only works between two instant vectors");
            return;
        }
        if $cmp && $lt eq 'scalar' && $rt eq 'scalar' && !$n.bool {
            self.type-error($n, 'a comparison between two scalars must use the bool modifier');
            return;
        }
        if $lt eq 'vector' && $rt eq 'vector' {
            self.check-matching($n);
        }
        elsif $n.match ne '' || $n.group ne '' {
            self.type-error($n, 'on, ignoring, group_left and group_right only apply between two instant vectors');
        }
        if $cmp && $lt ne $rt {
            my ($vec, $num, $op) = $lt eq 'vector' ?? ($n.lhs, $n.rhs, $n.op) !! ($n.rhs, $n.lhs, %FLIP{$n.op});
            self.check-threshold($n, strip($vec), const-value($num), $op);
        }
    }

    method check-matching(Binary $n) {
        my $l = labels-of($n.lhs) // return;
        my $r = labels-of($n.rhs) // return;
        return if $n.op eq 'or';
        my $effect = $n.op eq 'unless' ?? 'unless never removes anything'
                  !! 'the result is always empty';
        if $n.match eq 'on' {
            my @on = $n.match-labels;
            for ('left', $l, $r), ('right', $r, $l) -> ($side, $mine, $other) {
                my @gone = @on.grep({ $mine.lacks($_) && !$other.lacks($_) });
                if @gone {
                    self.flag($n, 'ON_LABEL_MISSING', 'HIGH',
                        "on({@on.join(', ')}) matches on {@gone.join(', ')} but the $side side has already dropped {@gone == 1 ?? 'that label' !! 'those labels'}. The two sides can never pair up, so $effect.");
                }
            }
            unless %SET-OP{$n.op} {
                my $on = set(@on);
                my @sides = $n.group eq 'left'  ?? (('right', $r),)
                         !! $n.group eq 'right' ?? (('left', $l),)
                         !! (('left', $l), ('right', $r));
                for @sides -> ($side, $info) {
                    next unless $info.known;
                    my $extra = $info.exact (-) $on;
                    next unless $extra;
                    self.flag($n, 'MANY_TO_ONE_NEEDS_GROUP', 'MEDIUM',
                        $n.group eq ''
                            ?? "The $side side still carries {fmt-set($extra)} beyond the on() labels. As soon as two of its series share the same on() values the query fails with a many to one matching error. Add group_{$side} or aggregate those labels away."
                            !! "group_{$n.group} needs the $side side to be unique per on() value, but it still carries {fmt-set($extra)}. Two series with the same on() values make the query fail with a many to many matching error.");
                }
            }
        }
        elsif $l.known && $r.known {
            my $ign = set($n.match eq 'ignoring' ?? $n.match-labels.List !! ());
            my $a = $l.exact (-) $ign;
            my $b = $r.exact (-) $ign;
            unless $a eqv $b {
                self.flag($n, 'LABEL_SET_MISMATCH', 'HIGH',
                    "The left side has labels {fmt-set($a)} and the right side has {fmt-set($b)}. {$n.op} pairs series only when all labels are equal, so nothing ever pairs up and $effect. Group both sides by the same labels or add on(...).");
            }
        }
    }

    method check-threshold(Binary $n, $vec, $value, Str $op) {
        if $value.defined && $value === NaN {
            self.flag($n, 'NAN_COMPARISON', 'HIGH',
                'NaN is not equal to anything, itself included, so a comparison against NaN is never true. To find NaN values compare the expression with itself: x != x.');
            return;
        }
        my $counts = ($vec ~~ Agg && $vec.op eq 'count') || ($vec ~~ Call && $vec.func eq 'count_over_time');
        if $counts && $value.defined
            && (($op eq '==' && $value == 0) || ($op eq '<' && $value <= 1) || ($op eq '<=' && $value < 1)) {
            my $what = $vec ~~ Agg ?? 'count' !! 'count_over_time';
            self.flag($n, 'COUNT_NEVER_ZERO', 'HIGH',
                "$what over nothing returns no series at all, not 0, so this condition can never be true. Use absent(...) or append: or vector(0).");
        }
        if $op ne '==' && $op ne '!=' {
            my $inner = $vec;
            $inner = strip($inner.expr) while $inner ~~ Agg && $inner.op eq any(<sum max min avg>);
            if $inner ~~ Selector && !$inner.range.defined && $inner.name.ends-with('_total') {
                self.flag($n, 'RAW_COUNTER_THRESHOLD', 'MEDIUM',
                    "{$inner.name} is a counter and is compared as a raw value. It only ever grows until the process restarts, so the condition says how long the process has been up, not how it behaves now. Compare rate or increase over a window.");
            }
        }
        if $op eq '==' | '!=' and $value.defined and $value != 0 {
            my $inner = $vec;
            $inner = strip($inner.expr) while $inner ~~ Agg && $inner.op eq any(<sum max min avg>);
            if $inner ~~ Call && %FRACTIONAL{$inner.func} {
                self.flag($n, 'FRACTIONAL_EQUALITY', 'MEDIUM',
                    "{$inner.func} returns extrapolated fractional values such as 0.9997 or 1.0345, so {$op} $value {$op eq '==' ?? 'is almost never true' !! 'is almost always true'}. Use a range such as > {$value - 0.5} or round() the value first.");
            }
        }
    }
}

class Rule {
    has Str $.file;
    has Str $.group = '';
    has Int $.group-id = 0;
    has Str $.kind is rw = 'expr';
    has Str $.name is rw = '';
    has Int $.line = 0;
    has $.expr is rw;
    has Int $.expr-line is rw = 0;
    has Bool $.block is rw = False;
    has Bool $.folded is rw = False;
    has $.for is rw;
    has Bool $.has-for is rw = False;
    has $.interval is rw;
    has %.static-labels;
    has Str $.templates is rw = '';
    has %.ignored;
}

sub note-directives(Str $line, %into) {
    if $line ~~ / 'promql-lint:' \s* 'ignore' \s+ (<[A..Z_*,\ ]>+) / {
        %into{$_} = True for (~$0).split(/ <[,\s]>+ /, :skip-empty);
    }
}

sub decode-scalar(Str $first, @cont) {
    if $first ~~ /^ (<[|>]>) <[+\-0..9]>* \s* [ '#' .* ]? $/ {
        my $folded = ~$0 eq '>';
        my @lines = @cont;
        @lines.pop while @lines && @lines[*-1].trim eq '';
        my $indent = @lines.grep(*.trim ne '').map({ .match(/^ \s*/).chars }).min;
        $indent = 0 if $indent ~~ Inf;
        my $text = @lines.map({ .chars >= $indent ?? .substr($indent) !! '' }).join("\n");
        return %( :$text, :block, :$folded );
    }
    my @parts = ($first, |@cont.map(*.trim)).grep(* ne '');
    my $joined = @parts.join(' ');
    if $joined.starts-with('"') {
        if $joined ~~ /^ '"' ( [ '\\' . | <-["\\]> ]* ) '"' / {
            my $body = ~$0;
            my %esc = n => "\n", t => "\t";
            return %( text => $body.subst(/ '\\' (.) /, { %esc{~$0} // ~$0 }, :g), :!block, :!folded );
        }
        return %( text => $joined.substr(1), :!block, :!folded );
    }
    if $joined.starts-with("'") {
        if $joined ~~ /^ "'" ( [ "''" | <-[']> ]* ) "'" / {
            return %( text => (~$0).subst("''", "'", :g), :!block, :!folded );
        }
        return %( text => $joined.substr(1), :!block, :!folded );
    }
    my $plain = @parts.map({ .subst(/ \s+ '#' .* $/, '') }).join(' ').trim;
    %( text => $plain, :!block, :!folded )
}

constant %RULE-KEYS = <alert record expr for keep_firing_for labels annotations>.map(* => True);

sub read-rule-file(Str $file, @lines) {
    my @rules;
    my %group-interval;
    my Int $group-id = 0;
    my Str $group = '';
    my $group-key-indent = -1;
    my $rule;
    my $rule-key-indent = -1;
    my %pending;
    my ($key, $key-first, $key-line, @key-cont);

    my sub finish-key {
        return unless $key.defined && $rule.defined;
        given $key {
            when 'expr' {
                my %d = decode-scalar($key-first, @key-cont);
                $rule.expr      = %d<text>;
                $rule.block     = %d<block>;
                $rule.folded    = %d<folded>;
                $rule.expr-line = %d<block> ?? $key-line + 1 !! $key-line;
            }
            when 'alert' | 'record' {
                $rule.kind = $key;
                $rule.name = decode-scalar($key-first, @key-cont)<text>;
            }
            when 'for' {
                $rule.has-for = True;
                $rule.for = dur-seconds(decode-scalar($key-first, @key-cont)<text>);
            }
            when 'labels' | 'annotations' {
                my $raw = ($key-first, |@key-cont).join("\n");
                $rule.templates ~= "\n" ~ $raw;
                if $key eq 'labels' {
                    for @key-cont { $rule.static-labels{~$0} = True if $_ ~~ /^ \s* '"'? (<[\w]>+) '"'? \s* ':' / }
                    for $key-first.match(/ (<[\w]>+) \s* ':' /, :g) { $rule.static-labels{~.[0]} = True }
                }
            }
        }
        $key = Nil;
        @key-cont = ();
    }
    my sub finish-rule {
        finish-key();
        @rules.push: $rule if $rule.defined && $rule.expr.defined;
        $rule = Nil;
        $rule-key-indent = -1;
    }
    my sub start-key(Str $k, Str $value, Int $line-no) {
        finish-key();
        $key = $k; $key-first = $value.trim; $key-line = $line-no; @key-cont = ();
    }

    for @lines.kv -> $idx, $line {
        my $line-no = $idx + 1;
        my $blank   = $line.trim eq '';
        my $indent  = $line.match(/^ \s*/).chars;
        my $comment = $line.trim.starts-with('#');

        if $key.defined && ($blank || $indent > $rule-key-indent) {
            @key-cont.push: $line;
            note-directives($line, $rule.ignored);
            next;
        }
        if $blank { next }
        if $comment {
            note-directives($line, ($rule.defined && $indent >= $rule-key-indent) ?? $rule.ignored !! %pending);
            next;
        }
        if $line ~~ /^ (\s*) '-' (\s+) (<[\w]>+) \s* ':' [ \s+ (.*) | $ ]/ {
            my $k = ~$2;
            my $key-indent = $0.chars + 1 + $1.chars;
            my $value = ($3 // '').Str;
            if %RULE-KEYS{$k} {
                finish-rule();
                $rule = Rule.new(:$file, :$group, :$group-id, line => $line-no, ignored => %pending);
                %pending = ();
                $rule-key-indent = $key-indent;
                start-key($k, $value, $line-no);
                note-directives($line, $rule.ignored);
                next;
            }
            finish-rule();
            %pending = ();
            if $k eq 'name' {
                $group = decode-scalar($value, [])<text>;
                $group-id++;
                $group-key-indent = $key-indent;
            }
            next;
        }
        if $line ~~ /^ (\s*) (<[\w]>+) \s* ':' [ \s+ (.*) | $ ]/ {
            my $k = ~$1;
            my $value = ($2 // '').Str;
            if $rule.defined && $indent == $rule-key-indent {
                start-key($k, $value, $line-no);
                note-directives($line, $rule.ignored);
                next;
            }
            finish-rule() if $rule.defined && $indent < $rule-key-indent;
            if $k eq 'interval' && $indent == $group-key-indent {
                %group-interval{$group-id} = dur-seconds(decode-scalar($value, [])<text>);
            }
            %pending = () unless $k eq 'rules';
            next;
        }
        finish-rule() if $rule.defined && $indent < $rule-key-indent;
    }
    finish-rule();
    for @rules { .interval = %group-interval{.group-id} if %group-interval{.group-id}.defined }
    @rules
}

sub read-expression-list(Str $file, @lines) {
    my @rules;
    my %pending;
    for @lines.kv -> $idx, $line {
        my $text = $line.trim;
        if $text eq '' { %pending = (); next }
        if $text.starts-with('#') { note-directives($text, %pending); next }
        my %ignored = %pending;
        note-directives($text, %ignored);
        @rules.push: Rule.new(:$file, name => "line {$idx + 1}", line => $idx + 1,
                              expr => $text, expr-line => $idx + 1, :%ignored);
        %pending = ();
    }
    @rules
}

sub read-source(Str $file, Str $content) {
    my @lines = $content.lines;
    my $is-rules = so @lines.first({ $_ ~~ /^ \s* [ '-' \s+ ]? [ 'expr' | 'groups' | 'rules' ] \s* ':' / });
    $is-rules ?? read-rule-file($file, @lines) !! read-expression-list($file, @lines)
}

sub snippet(Str $src, Int $from, Int $to) {
    my $s = $src.substr($from, max(0, $to - $from)).subst(/\s+/, ' ', :g).trim;
    $s.chars > 140 ?? $s.substr(0, 137) ~ '...' !! $s
}

sub template-labels(Str $text) {
    my %refs;
    %refs{~.[0]} = True for $text.match(/ '$labels.' (<[a..zA..Z_]> <[\w]>*) /, :g);
    %refs{~.[0]} = True for $text.match(/ '.Labels.' (<[a..zA..Z_]> <[\w]>*) /, :g);
    %refs{~.[0]} = True for $text.match(/ 'index' \s+ '$labels' \s+ '\\'? '"' (<-["\\]>+) /, :g);
    %refs.keys.sort
}

sub lint-rule(Rule $rule, :$scrape, :$eval, :%disabled) {
    my $src = $rule.expr;
    my $an  = Analyzer.new(:$src, :$scrape, eval => ($rule.interval // $eval), kind => $rule.kind);
    my $parser;
    my $ast;
    {
        $parser = Parser.new(:$src);
        $ast    = $parser.parse;
        CATCH {
            when X::PromQL {
                my $pos = min(.pos, $src.chars);
                $an.found.push: %( code => 'PARSE_ERROR', severity => 'ERROR', message => .reason,
                                   from => $pos, to => min($src.chars, $pos + 40), :col );
            }
        }
    }
    if $ast.defined {
        $an.visit($ast);
        my $t = type-of($ast);
        if $rule.kind ne 'expr' && $t eq 'matrix' | 'string' {
            $an.type-error($ast, "a rule expression must be an instant vector or a scalar, this is a {$t eq 'matrix' ?? 'range vector' !! 'string'}");
        }
        if $rule.folded && $parser.comments.first({ $_ < $parser.last-token-pos }) {
            $an.flag($ast, 'FOLDED_COMMENT', 'HIGH',
                'This expression sits in a folded YAML scalar (expr: >) and has a # comment before its last line. Folding joins the lines with spaces, so the comment swallows the rest of the query. Use a literal block (expr: |).');
        }
        if $rule.kind eq 'alert' {
            my $top = strip($ast);
            if $top ~~ Binary && $top.bool {
                $an.flag($top, 'BOOL_IN_ALERT', 'HIGH',
                    'With the bool modifier the comparison returns 0 or 1 for every series instead of filtering. An alert fires for every series its expression returns, so this one fires all the time. Remove bool.');
            }
            if !$rule.has-for || ($rule.for.defined && $rule.for == 0) {
                $an.flag($ast, 'ALERT_WITHOUT_FOR', 'LOW',
                    'The alert has no for clause, so a single evaluation over the threshold pages someone. Add a for duration unless one bad sample really is an incident.');
            }
            my $info = labels-of($ast);
            if $info.defined {
                for template-labels($rule.templates) -> $label {
                    next if $label eq 'alertname' || $rule.static-labels{$label};
                    next unless $info.lacks($label);
                    $an.flag($ast, 'TEMPLATE_LABEL_MISSING', 'HIGH',
                        "The annotations or labels use \$labels.$label but the expression drops $label before the alert is created, so it always renders as an empty string. Keep $label in the grouping or remove it from the template.");
                }
            }
        }
    }
    my @out;
    for $an.found -> %f {
        next if %disabled{%f<code>} || $rule.ignored{%f<code>} || $rule.ignored<*>;
        my $before = $src.substr(0, %f<from>);
        my $row    = $before.comb("\n").elems;
        my $col    = %f<from> - (($before.rindex("\n") // -1) + 1) + 1;
        @out.push: %(
            file     => $rule.file,
            line     => $rule.block ?? $rule.expr-line + $row !! ($rule.expr-line || $rule.line),
            column   => $col,
            group    => $rule.group,
            kind     => $rule.kind,
            rule     => $rule.name,
            code     => %f<code>,
            severity => %f<severity>,
            message  => %f<message>,
            snippet  => snippet($src, %f<from>, %f<to>),
        );
    }
    @out
}

sub json($v) {
    given $v {
        when Bool        { return $v ?? 'true' !! 'false' }
        when Int         { return ~$v }
        when Numeric     { return $v === NaN || $v.abs == Inf ?? 'null' !! ~$v }
        when Str {
            my $s = $v.trans(['\\', '"', "\n", "\r", "\t"] => ['\\\\', '\\"', '\\n', '\\r', '\\t']);
            $s .= subst(/ <[\x[0]..\x[1f]]> /, { sprintf('\\u%04x', $/.Str.ord) }, :g);
            return '"' ~ $s ~ '"';
        }
        when Associative { return '{' ~ $v.sort(*.key).map({ json(.key) ~ ':' ~ json(.value) }).join(',') ~ '}' }
        when Positional  { return '[' ~ $v.map({ json($_) }).join(',') ~ ']' }
        default          { return 'null' }
    }
}

sub render-text(@findings, %stats) {
    my @out;
    for @findings -> %f {
        my $where = %f<kind> eq 'expr' ?? '' !! " ({%f<kind>} {%f<rule>})";
        @out.push: "{%f<file>}:{%f<line>} [{%f<severity>}] {%f<code>}$where";
        @out.push: "    {%f<message>}";
        @out.push: "    > {%f<snippet>}" if %f<snippet> ne '';
        @out.push: '';
    }
    my %count;
    %count{.<severity>}++ for @findings;
    my $by = @SEVERITIES.reverse.grep({ %count{$_} }).map({ "%count{$_} $_" }).join(', ');
    @out.push: "Checked {%stats<expressions>} expression{%stats<expressions> == 1 ?? '' !! 's'} in {%stats<files>} file{%stats<files> == 1 ?? '' !! 's'}: "
        ~ (@findings ?? "{+@findings} finding{@findings == 1 ?? '' !! 's'} ($by)" !! 'no findings');
    @out.join("\n")
}

sub render-github(@findings) {
    my sub esc(Str $s) { $s.subst('%', '%25', :g).subst("\r", '%0D', :g).subst("\n", '%0A', :g) }
    my sub prop(Str $s) { esc($s).subst(':', '%3A', :g).subst(',', '%2C', :g) }
    @findings.map(-> %f {
        my $level = %SEV-RANK{%f<severity>} >= %SEV-RANK<HIGH> ?? 'error'
                 !! %f<severity> eq 'MEDIUM' ?? 'warning' !! 'notice';
        "::$level file={prop(%f<file>)},line={%f<line>},title={prop(%f<code>)}::{esc(%f<message>)}"
    }).join("\n")
}

sub sort-findings(@findings) {
    @findings.sort({ $^a<file> cmp $^b<file> || $^a<line> <=> $^b<line>
        || %SEV-RANK{$^b<severity>} <=> %SEV-RANK{$^a<severity>} || $^a<code> cmp $^b<code> })
}

constant @SELF-TESTS =
    ('rate(http_requests_total[5m])', 'expr', ()),
    ('sum by (job) (rate(http_requests_total[5m])) / sum by (job) (rate(http_requests_total[5m] offset 1h))', 'expr', ()),
    ('histogram_quantile(0.99, sum by (le, job) (rate(http_request_duration_seconds_bucket[5m])))', 'expr', ()),
    ('-1 ^ 2 + foo{a="b", c!~"d.+"} % 3 >= bool 2', 'expr', ()),
    ('max_over_time(rate(x_total[5m])[1h:1m] @ end()) > 0 unless on (job) up == 0', 'expr', ()),
    ('topk(5, sum without (instance) (rate(x_total[5m]))) and on () vector(1) == 1', 'expr', ()),
    ('label_replace(up, "host", "$1", "instance", "(.*):.*") * on (host) group_left (team) owner_info', 'expr', ()),
    ('{"http.server.duration", job="api"} > 1e3 or absent(up{job="api"})', 'expr', ()),
    ('rate(http_requests_total[1m])', 'expr', ('RANGE_TOO_SHORT',)),
    ('rate(http_requests_total[3m])', 'expr', ('RANGE_FRAGILE',)),
    ('rate(sum(http_requests_total)[5m:])', 'expr', ('RATE_OF_AGGREGATE',)),
    ('rate(node_memory_free_bytes[5m])', 'expr', ('COUNTER_FUNCTION_ON_GAUGE',)),
    ('avg_over_time(http_requests_total[5m])', 'expr', ('GAUGE_FUNCTION_ON_COUNTER',)),
    ('histogram_quantile(0.99, sum by (job) (rate(lat_bucket[5m])))', 'expr', ('HISTOGRAM_MISSING_LE',)),
    ('histogram_quantile(0.99, sum(rate(lat_bucket[5m])))', 'expr', ('HISTOGRAM_MISSING_LE',)),
    ('histogram_quantile(0.99, sum without (le) (rate(lat_bucket[5m])))', 'expr', ('HISTOGRAM_MISSING_LE',)),
    ('histogram_quantile(99, sum by (le) (lat_bucket))', 'expr', ('HISTOGRAM_WITHOUT_RATE', 'QUANTILE_OUT_OF_RANGE')),
    ('avg(histogram_quantile(0.9, sum by (le, pod) (rate(lat_bucket[5m]))))', 'expr', ('AGGREGATE_OF_QUANTILES',)),
    ('count(up{job="api"}) == 0', 'expr', ('COUNT_NEVER_ZERO',)),
    ('1 > count(up{job="api"})', 'expr', ('COUNT_NEVER_ZERO',)),
    ('sum(errors_total) > 100', 'expr', ('RAW_COUNTER_THRESHOLD',)),
    ('increase(restarts_total[10m]) == 1', 'expr', ('FRACTIONAL_EQUALITY',)),
    ('up == NaN', 'expr', ('NAN_COMPARISON',)),
    ('sum by (job) (rate(a_total[5m])) / sum by (job, instance) (rate(b_total[5m]))', 'expr', ('LABEL_SET_MISMATCH',)),
    ('sum by (job) (rate(a_total[5m])) / sum(rate(b_total[5m]))', 'expr', ('LABEL_SET_MISMATCH',)),
    ('sum by (job) (rate(a_total[5m])) / ignoring (job) sum(rate(b_total[5m]))', 'expr', ()),
    ('sum by (pod) (rate(a_total[5m])) / on (node) sum by (node) (rate(b_total[5m]))', 'expr', ('MANY_TO_ONE_NEEDS_GROUP', 'ON_LABEL_MISSING')),
    ('sum by (job, pod) (rate(a_total[5m])) / on (job) sum by (job) (rate(b_total[5m]))', 'expr', ('MANY_TO_ONE_NEEDS_GROUP',)),
    ('sum by (job, pod) (rate(a_total[5m])) / on (job) group_left sum by (job) (rate(b_total[5m]))', 'expr', ()),
    ('up{job=~"api"}', 'expr', ('REGEX_WITHOUT_PATTERN',)),
    ('up{job=~"^api.*$"}', 'expr', ('REGEX_REDUNDANT_ANCHOR',)),
    ('up{job=~"api|"}', 'expr', ('REGEX_EMPTY_ALTERNATIVE',)),
    ('{job=~".*"}', 'expr', ('REGEX_MATCH_ALL', 'SELECTOR_MATCHES_EMPTY')),
    ('up{job="a", job="b"}', 'expr', ('SELECTOR_NEVER_MATCHES',)),
    ('max_over_time(rate(x_total[5m])[10m:15m])', 'expr', ('SUBQUERY_STEP_EXCEEDS_RANGE', 'SUBQUERY_UNDERSAMPLES')),
    ('max_over_time(rate(x_total[4m])[1h:10m])', 'expr', ('SUBQUERY_UNDERSAMPLES',)),
    ('holt_winters(x[10m], 0.5, 0.5)', 'expr', ('DEPRECATED_FUNCTION',)),
    ('rate(http_requests_total)', 'expr', ('TYPE_ERROR',)),
    ('1 > 2', 'expr', ('TYPE_ERROR',)),
    ('up and 1', 'expr', ('TYPE_ERROR',)),
    ('sum(rate(x_total[5m]))[5m]', 'expr', ('PARSE_ERROR',)),
    ('rate(x_total[$__rate_interval])', 'expr', ('PARSE_ERROR',)),
    ('sum by (job (up)', 'expr', ('PARSE_ERROR',)),
    ('irate(x_total[5m]) > 5', 'alert', ('ALERT_WITHOUT_FOR', 'IRATE_IN_RULE')),
    ('up == bool 0', 'alert', ('ALERT_WITHOUT_FOR', 'BOOL_IN_ALERT')),
    ('rate(x_total[30s])', 'record', ('RANGE_SHORTER_THAN_EVAL', 'RANGE_TOO_SHORT'));

constant $SELF-TEST-RULES = q:to/END/;
    groups:
      - name: demo
        interval: 2m
        rules:
          # promql-lint: ignore ALERT_WITHOUT_FOR
          - alert: HighErrors
            expr: >
              sum by (job) (rate(errors_total[10m]))  # per job
                > 5
            labels:
              severity: page
            annotations:
              summary: "{{ $labels.instance }} of {{ $labels.job }} is failing ({{ $labels.severity }})"
          - record: job:errors:rate1m
            expr: sum by (job) (rate(errors_total[1m]))
          - alert: Quiet
            expr: |
              sum without (pod) (
                rate(errors_total[10m])
              ) > 5
            for: 10m
            annotations:
              description: 'pod {{ $labels.pod }}'
    END

sub self-test {
    my $failed = 0;
    my sub check(Str $label, @got, @want) {
        my $ok = @got.sort.join(' ') eq @want.sort.join(' ');
        $failed++ unless $ok;
        say ($ok ?? 'ok   ' !! 'FAIL ') ~ $label ~ ($ok ?? '' !! "\n       want: {@want.sort.join(' ') || '(none)'}\n       got:  {@got.sort.join(' ') || '(none)'}");
    }
    for @SELF-TESTS -> ($expr, $kind, $want) {
        my $rule = Rule.new(file => 'self-test', :$kind, name => 't', :$expr, has-for => False);
        my @got = lint-rule($rule, scrape => 60, eval => 60, disabled => {}).map(*<code>).unique;
        check($expr, @got, $want.list);
    }
    my @rules = read-source('self-test.yml', $SELF-TEST-RULES);
    check('rule file: three rules read', (+@rules,), (3,));
    my %by-rule;
    for @rules -> $r {
        %by-rule{$r.name} = lint-rule($r, scrape => 60, eval => 60, disabled => {}).map({ .<code> ~ '@' ~ .<line> });
    }
    check('rule file: folded comment and missing label', %by-rule<HighErrors>.list,
          ('FOLDED_COMMENT@8', 'TEMPLATE_LABEL_MISSING@8'));
    check('rule file: group interval applies', %by-rule<job:errors:rate1m>.list,
          ('RANGE_SHORTER_THAN_EVAL@15', 'RANGE_TOO_SHORT@15'));
    check('rule file: without() label in template, block line numbers', %by-rule<Quiet>.list,
          ('TEMPLATE_LABEL_MISSING@18',));
    say $failed ?? "$failed self test{$failed == 1 ?? '' !! 's'} failed" !! 'all self tests passed';
    $failed ?? 1 !! 0
}

sub usage-error(Str $message) {
    note "PromQLHazardLinter: $message";
    exit 2;
}

sub MAIN(
    *@files,
    Str  :$expr,
    Str  :$scrape-interval = '1m',
    Str  :$eval-interval   = '1m',
    Str  :$format          = 'text',
    Str  :$fail-on         = 'HIGH',
    Str  :$min-severity    = 'LOW',
    Str  :$disable         = '',
    Bool :$self-test,
    Bool :$version,
) {
    if $version   { say "PromQLHazardLinter {VERSION}"; exit 0 }
    if $self-test { exit self-test() }

    my $scrape = dur-seconds($scrape-interval) // usage-error("--scrape-interval '$scrape-interval' is not a duration like 15s or 1m");
    my $eval   = dur-seconds($eval-interval)   // usage-error("--eval-interval '$eval-interval' is not a duration like 30s or 1m");
    usage-error('--scrape-interval and --eval-interval must be greater than zero') unless $scrape > 0 && $eval > 0;
    my $fail-rank = $fail-on.uc eq 'NEVER' ?? Inf
        !! (%SEV-RANK{$fail-on.uc} // usage-error("--fail-on must be one of {@SEVERITIES.join(', ')} or NEVER"));
    my $min-rank = %SEV-RANK{$min-severity.uc} // usage-error("--min-severity must be one of {@SEVERITIES.join(', ')}");
    usage-error("--format must be text, json or github") unless $format eq any(<text json github>);
    my %disabled = $disable.split(/ <[,\s]>+ /, :skip-empty).map(*.uc => True);
    usage-error('give at least one file, - for standard input, or --expr') unless @files || $expr.defined;

    my @rules;
    my $file-count = 0;
    if $expr.defined {
        @rules.push: Rule.new(file => '<expr>', name => 'expr', line => 1, :$expr, expr-line => 1);
        $file-count++;
    }
    for @files -> $f {
        my $content;
        if $f eq '-' { $content = $*IN.slurp }
        else {
            usage-error("cannot read '$f': no such file") unless $f.IO.f;
            $content = try $f.IO.slurp;
            usage-error("cannot read '$f': {$!.message}") without $content;
        }
        $file-count++;
        @rules.append: read-source($f eq '-' ?? '<stdin>' !! $f.Str, $content);
    }

    my @all;
    @all.append: lint-rule($_, :$scrape, :$eval, :%disabled) for @rules;
    my @shown  = sort-findings(@all.grep({ %SEV-RANK{.<severity>} >= $min-rank }));
    my %stats  = files => $file-count, expressions => +@rules;
    my $failed = so @all.first({ %SEV-RANK{.<severity>} >= $fail-rank });

    given $format {
        when 'json' {
            say json(%( version => VERSION, files => $file-count, expressions => +@rules,
                        scrape_interval_seconds => $scrape, eval_interval_seconds => $eval,
                        failed => $failed, findings => @shown ));
        }
        when 'github' {
            my $text = render-github(@shown);
            say $text if $text ne '';
        }
        default { say render-text(@shown, %stats) }
    }
    exit $failed ?? 1 !! 0;
}
