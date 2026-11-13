# Nginx Location Shadow Auditor

An nginx `location /admin/` block with `auth_basic` does not protect `/admin/report.php` if a `location ~ \.php$` block exists anywhere in the same server. This Perl command line tool replays the real nginx location matching algorithm over synthesized request paths and reports every location whose access control, handler or headers do not apply to the requests it looks like it covers.

**Language:** Perl | **Lines:** 2029 | **Added:** 2026-11-13

## What this solves

nginx does not pick a location the way the config file reads. It scans exact and prefix locations first and remembers the longest prefix. Then, unless that prefix was written with `^~`, it walks the regex locations in file order and the first regex that matches takes the request away from the prefix. Nested locations add another layer: the regex locations of an outer level can still override a prefix location that was chosen inside a nested block.

Most people write nginx config as if the most specific looking block wins. That mental model is wrong in a few specific ways. Each of them turns into a production incident or a security hole:

- `location /admin/ { auth_basic ...; }` plus `location ~ \.php$ { fastcgi_pass ...; }`. Every PHP file under `/admin/` is served with no password.
- `location /internal/ { allow 10.0.0.0/8; deny all; }` plus a static asset regex. `/internal/dump.css` is public.
- `location /uploads/ { }` plus a PHP regex. A user uploads `shell.php` and it runs.
- A regex location placed after a broader regex. It never receives a single request and nobody notices for a year.
- A child location adds one `add_header` line and silently loses HSTS, CSP and every other header set in the `http` block.
- A child location adds `allow 192.168.0.0/16;` and by doing so throws away the parent `deny all`.
- `location /static { alias /srv/static/; }` lets `/static../` read the parent directory.

Linters that pattern match on single directives cannot see the first four, because the bug is not in any one block. It only exists in how two blocks interact under the matching algorithm. So this tool does not pattern match. It builds the same decision procedure nginx uses, generates request paths that sit on the boundaries between locations, runs every path through the procedure and compares where the request was meant to land with where it actually lands.

## Why I built it

I got bitten by the `/admin/` plus `\.php$` case on a config I had reviewed twice. `nginx -t` passed, the page asked for a password in the browser and everything looked fine. The hole was only visible if you requested a PHP file directly. Reading the config again did not help, because the config reads correctly. The bug is in the order of evaluation and you have to actually run the algorithm to see it.

The existing options did not cover this. `nginx -t` only checks that the file loads. gixy catches alias traversal and a few header problems but does not model location selection at all. Online testers take one location and one path at a time. I wanted something that takes the whole config, including every include, works out the dangerous paths by itself and fails a CI job when a guarded location has a hole in it. I also wanted an `--explain` mode that answers the question I kept asking by hand: which block serves this URL and why.

Since the whole value depends on the matcher being right, I tested it against a real nginx 1.24 binary. A harness generated random location trees, loaded each one into nginx, sent the probe paths over a socket and compared the block nginx picked with the block the tool predicted. That found several bugs in my first version, including the fact that nginx accepts duplicate regex locations and that a `^~` location with `proxy_pass` still triggers the trailing slash redirect. After the fixes, about ten thousand requests across 150 random configs matched with zero differences. The verdict on whether a config loads at all matched `nginx -t` on all 750 generated configs.

## When to use it

- In CI on the repository that holds your nginx config, so a pull request that adds a regex location cannot open a hole in an existing protected prefix.
- Against a live box with `nginx -T | NginxLocationShadowAuditor.pl -` to audit what is actually loaded, includes and all.
- Before a migration from Apache or between nginx hosts, when rules get reordered.
- During an incident, with `--explain`, to find out which block is serving a path and which headers and access rules it ends up with.
- As a review aid for ingress snippets and templated config where the final location order is not obvious from the template.

It is not a replacement for `nginx -t`. Run both. It also does not read Lua or njs logic, so anything decided in code is outside its view.

## How it works

**Parsing.** `lex_next` is a tokenizer that follows the nginx rules: a `#` only starts a comment at the start of a token, `${var}` does not end a word, quoted strings keep regex escapes such as `\.` intact and only `\"`, `\'`, `\\`, `\t`, `\r` and `\n` are unescaped. `parse_block` builds the directive tree and `lex_skip_raw_block` steps over `*_by_lua_block` bodies without trying to read Lua as nginx config. `expand_include` resolves includes with glob support, relative to the directory of the main file or to `--prefix`. It stops at `MAX_INCLUDE_DEPTH` or on a cycle. If the input contains `# configuration file ...:` markers it is treated as an `nginx -T` dump and includes are resolved from the sections of the dump itself, so you can audit a config from a server without copying the files.

**Model.** `build_servers` finds every `server` block, inside `http` or at the top level of a fragment. `build_location` reads the modifier (`=`, `^~`, `~`, `~*`, none or a named `@location`), compiles regex patterns with `compile_regex` and sorts each location into the static or regex list of its parent level.

**The matcher.** `resolve_level` is a direct port of the nginx lookup. At one level it finds an exact match, a prefix whose name equals the path, the longest prefix and a possible trailing slash redirect. An exact match ends the search. Otherwise it records the longest prefix, descends into its nested locations. On the way back up it checks the regex locations of each level in file order unless the prefix at that level used `^~`. `normalize_uri` does what nginx does to the request line before matching: strips the query string, decodes percent escapes, merges slashes when `merge_slashes` is on and resolves `.` and `..` segments, returning an error where nginx would answer 400.

**Probe synthesis.** `regex_samples` walks each location regex with a small recursive sampler (`sampler_alternation`, `sampler_sequence`, `sampler_atom`, `sampler_quantifier`) and produces strings that match it. Every sample is checked against the compiled regex and dropped if it does not match, so the sampler can be approximate without producing false evidence. `build_probes` then crosses these: every prefix path, every regex sample, every prefix joined with every regex tail and every regex sample joined with every other regex tail, up to `DEFAULT_MAX_PROBES` per server.

**Replay.** `check_replay` runs each probe through `resolve` and records two things: the location that won plus the locations that matched but lost. A prefix loses when a regex overrides it. A regex loses when an earlier regex at the same level matches first. For every loser, `guards_of` computes its effective access control (`auth_basic`, `auth_request`, `auth_jwt`, `allow` and `deny` lists, `internal`, `limit_except`, `access_by_lua` and unconditional 4xx `return`) with nginx inheritance applied. Then it compares that with the winner. If the loser has a guard the winner does not, that is **NLS001**. A winner that itself blocks everything is not counted. A location that never wins a single probe it matches is **NLS003**.

**Static checks.** The rest are cheaper rules that share the same model:

| Rule | Name | What it catches |
|---|---|---|
| NLS001 | guard-bypass | Guarded location loses the match to a location without the guard |
| NLS002 | script-handler-in-content-prefix | `fastcgi_pass`, `uwsgi_pass` or `scgi_pass` regex takes paths under an uploads or static style prefix |
| NLS003 | dead-location | No probe that matches the location ever reaches it |
| NLS004 | duplicate-location | Same exact or prefix matcher twice at one level, nginx will not load |
| NLS005 | alias-traversal | `alias` ends with a slash, the location does not |
| NLS006 | inheritance-drop | `add_header`, `proxy_set_header`, `fastcgi_param` and similar redefined in a child, parent values lost |
| NLS007 | access-rule-reset | Child `allow` or `deny` list replaces a parent list that ended in `deny all` |
| NLS008 | proxy-pass-uri-invalid | `proxy_pass` with a URI part in a regex location, named location, `if` or `limit_except` |
| NLS009 | proxy-pass-slash-mismatch | `proxy_pass` URI ends with a slash, the location prefix does not |
| NLS010 | loose-script-regex | Script handler regex with an unanchored extension or an unescaped dot |
| NLS011 | if-in-location | `if` in a location holding more than `return`, `rewrite`, `set` and `break` |
| NLS012 | invalid-nesting | Nested location nginx rejects |
| NLS013 | undefined-named-location | `try_files` or `error_page` points at a named location that does not exist |
| NLS014 | regex-compile-failure | Location regex that does not compile |
| NLS015 | include-missing | Include that could not be read, so the audit is partial |
| NLS016 | merge-slashes-off | `merge_slashes off` with guarded prefix locations |
| NLS017 | unused-named-location | Named location nothing refers to |

**Output and gating.** `add_finding` gives each finding a fingerprint built from the rule, the server name, the location path and a rule specific key, never from line numbers, so a baseline survives unrelated edits. `finalize_findings` applies `--ignore`, inline `# nls:ignore` comments and the baseline file, then sorts. `render_text`, `render_json` and `render_sarif` write the report. `explain_request` and `pick_server` power the `--explain` mode, including `server_name` selection by exact name, leading wildcard, trailing wildcard and regex.

## Usage

Perl 5.10 or later with core modules only. Nothing to install.

Audit a config tree and fail the job on high severity findings:

```bash
perl NginxLocationShadowAuditor.pl /etc/nginx/nginx.conf
```

Audit what a running server has loaded:

```bash
nginx -T 2>/dev/null | perl NginxLocationShadowAuditor.pl -
```

Ask which block serves a path and what it ends up with:

```bash
perl NginxLocationShadowAuditor.pl nginx.conf --host blog.example.test \
  --explain '/wp-admin/admin-ajax.php' --explain '/uploads/..%2fadmin/'
```

```text
request  /wp-admin/admin-ajax.php
server   blog.example.test (conf.d/wp.conf:1), exact server_name blog.example.test
path     /wp-admin/admin-ajax.php
  1. static scan at server level: longest prefix is location /wp-admin/ (conf.d/wp.conf:13)
  2. regex scan at server level: location ~ [^/]\.php(/|$) (conf.d/wp.conf:17) is the first regex that matches after 1 earlier regex location did not
result   location ~ [^/]\.php(/|$) (conf.d/wp.conf:17)
handler  fastcgi_pass unix:/run/php/php-fpm.sock
guards   none
note     location /wp-admin/ (conf.d/wp.conf:13) also matches this path but lost. It carries access which the winner does not apply
```

CI with SARIF for code scanning, a stricter gate and an accepted baseline:

```bash
perl NginxLocationShadowAuditor.pl conf/nginx.conf --write-baseline nls-baseline.json
perl NginxLocationShadowAuditor.pl conf/nginx.conf --baseline nls-baseline.json \
  --fail-on medium --format sarif > nginx-audit.sarif
```

Audit a checked out copy of `/etc/nginx` whose includes use absolute paths:

```bash
perl NginxLocationShadowAuditor.pl repo/etc/nginx/nginx.conf --sysroot repo
```

All options:

| Option | Meaning |
|---|---|
| `--format text\|json\|sarif` | Report format, text by default |
| `--fail-on LEVEL` | Exit 1 at or above `info`, `low`, `medium`, `high`. `never` always exits 0. Default `high` |
| `--min-severity LEVEL` | Hide findings below this level. Default `low` |
| `--ignore ID[,ID]` | Drop rules by id, repeatable |
| `--baseline FILE` | Drop findings whose fingerprint is listed in the file |
| `--write-baseline FILE` | Write the fingerprints of the current findings |
| `--max-probes N` | Probe paths replayed per server, default 8000 |
| `--explain URI` | Trace one request path, repeatable |
| `--host NAME`, `--port N` | Pick the server block for `--explain` |
| `--prefix DIR` | Directory that relative includes resolve against |
| `--sysroot DIR` | Directory prepended to absolute include paths |
| `--list-rules`, `--self-test`, `--version`, `--help` | Housekeeping |

Suppress one finding in the config itself by putting the comment on the directive line or the line above it:

```nginx
location /assets/ {
    # nls:ignore NLS006
    add_header Cache-Control "public, max-age=31536000";
}
```

Exit codes: `0` clean, `1` findings at or above `--fail-on`, `2` usage error, unreadable input or a config that does not parse.

## Notes

- The matcher was checked against nginx 1.24 on Linux. Prefix matching is case sensitive, as it is on Linux. On macOS and Windows nginx compares prefixes without case and the tool does not model that.
- Location regexes are compiled by Perl, not PCRE. The two agree on everything that normally shows up in a location pattern. A pattern that Perl rejects is reported as NLS014 and skipped, never guessed at.
- NLS001 and NLS003 are found by probing, not by proof. A reported hole is always real because the example path was replayed through the matcher. A clean result means none of the synthesized paths found a hole, which is strong evidence but not a guarantee for exotic regexes. Raise `--max-probes` on very large configs.
- NLS001 is reported at medium when the winning regex is anchored under the guarded path, since that is how people write deliberate exceptions. An unanchored winner such as `\.php$` is reported at high.
- A nested location with the same guards as its parent is folded into the parent finding to keep the report short.
- Server level `rewrite`, `return` and `if` run before location matching. The audit does not simulate them. `--explain` prints a note when the chosen server has any.
- Internal redirects from `try_files`, `index` and `error_page` are not followed. The access phase has already run by the time those fire, so they do not create the kind of bypass this tool looks for.
- `add_header_inherit` and `add_trailer_inherit` are respected: when either is set to `merge` or `off` in scope, NLS006 stays quiet for that family.
- `satisfy any` is not modelled beyond keeping NLS007 from firing on a bare allow list. If you combine it with `auth_basic`, read the NLS001 output with that in mind.
- The `--self-test` flag runs 44 built in assertions covering routing, normalization, server selection, the regex sampler, every major rule, inline suppression and dump parsing. Run it once after copying the file to a new machine.
- Fingerprints do not contain line numbers. They change when the server name, the location path or the set of winning locations changes, which is exactly when a baseline entry should be looked at again.
