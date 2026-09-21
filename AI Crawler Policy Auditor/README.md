# AI Crawler Policy Auditor

Most sites that tried to block GPTBot, ClaudeBot or CCBot in their robots.txt did it by copying a snippet from a blog post, and robots.txt has enough sharp edges that the snippet is often wrong in a way nobody notices. This tool reads a robots.txt file and replays the real matching rules that crawlers use, so you see which bots are actually blocked instead of which ones look blocked in the file.

**Language:** Raku | **Lines:** 452 | **Added:** 2026-09-21

## What this solves

robots.txt looks like a simple format. Two directives, a few lines, done. In practice the matching rules have three separate traps that are each individually well documented and still get missed constantly when someone hand writes a policy under deadline.

The first trap is the wildcard collateral hit. A site owner adds `User-agent: *` with `Disallow: /` meaning "block every AI bot," and it also blocks Googlebot and Bingbot, because those two have no group of their own and fall into the same wildcard bucket as everything else. The fix is a dedicated `User-agent: Googlebot` group with `Allow: /`, but you only find out you need it by reading the spec closely or by watching your search ranking fall off a cliff.

The second trap runs the other way. A site owner adds a `User-agent: GPTBot` group thinking it blocks GPTBot, but the group is empty or only has a narrow `Disallow: /private/`, and meanwhile a `User-agent: *` group further down blocks everything else with `Disallow: /`. GPTBot's own group always wins over the wildcard group, full stop, so GPTBot walks straight through the site while every other visitor without a dedicated group gets turned away. The person who wrote this file believes GPTBot is blocked because they can see a Disallow rule with `/` in the file somewhere. It just does not apply to GPTBot.

The third trap is quieter and it is the one search engines and AI companies genuinely disagree about. RFC 9309 defines a robots.txt group as a block of User-agent lines followed by rules, but it does not clearly settle what happens when the same product token, say ClaudeBot, appears in two separate groups further down the file. Google's own robots.txt parser merges every group naming that token into one combined rule set. A stricter reading of the RFC says only the first group counts, and some smaller crawlers do exactly that. So the same file, the same bot, the same path, can get a different verdict depending on whose parser is reading it. Nobody sees this divergence unless they simulate both interpretations side by side, which is exactly what this tool does.

## Why I built it

Every AI crawler blocking guide floating around gives you a snippet to paste and never tells you to check it against your own site's existing rules. I wanted something that reads the actual file you are about to ship, applies the real precedence rules instead of a naive text match, and tells you in plain language which specific line is going to surprise you. A grep for "Disallow" tells you nothing about which group wins. This tool tells you the verdict, the line that produced it, and whether that verdict would change under a different but equally reasonable interpretation of the same file.

Raku ended up being a genuinely good fit rather than an arbitrary choice. Its grammar engine parses each robots.txt line into a clean key and value without a hand rolled string splitter falling over on stray colons or trailing comments. Its multi dispatch turns the two subcommands, `check` and `audit`, into two separate `MAIN` candidates instead of one function with a mode flag threaded through it. And its regex engine can build a matcher from a runtime string, which is exactly what turning a `*` and `$` wildcard pattern into an anchored match needs.

## When to use it

Run it before you commit any robots.txt change that touches AI crawler rules, the same way you would run a linter before a deploy. It also earns its place as a CI gate: point it at the robots.txt your build is about to publish, set `--fail-on=HIGH` if you only care about the collateral damage and override cases, and let it block the pipeline the same way a broken test would. It is equally useful after the fact, when someone forwards you a site's robots.txt and asks "does this actually block GPTBot," because eyeballing group precedence by hand is exactly the kind of task people get wrong under time pressure.

## How it works

Parsing goes through a small grammar, `RobotsLine`, that matches one line at a time against `<directive>` or `<comment>`, with `RobotsLineActions` turning a match into a lowercase key and trimmed value. `clean-text` strips a stray UTF-8 byte order mark and normalizes CRLF and lone CR line endings before the line loop runs, because real robots.txt files come from all kinds of editors and web servers. `parse-robots` walks the cleaned lines and builds a list of `Group` objects, each holding its `agents`, its `rules` and the line number the group started on. A `User-agent` line after the group has already collected `Disallow` or `Allow` rules starts a fresh group, matching the boundary rule every real parser follows. Unknown directives such as `Sitemap` or `Host` are read and discarded, since they play no part in path matching and choking on them would make the tool useless on the messy files people actually publish.

Matching is where the real work happens. `pattern-regex-source` turns a robots.txt pattern into Raku regex source text: it splits on `*`, escapes every literal segment through `escape-regex-literal` so a dot or a plus sign in a path cannot accidentally act as regex syntax, joins the segments back with `.*`, and anchors the end with `$` only when the original pattern asked for it. `pattern-matches` normalizes percent encoded characters on both the pattern and the path through `normalize-percent` before testing, decoding only the RFC 3986 unreserved characters so a reserved character like an encoded slash can never sneak past the matcher disguised as something else. The compiled regex source is cached in `%matcher-cache` so a large audit run does not rebuild the same pattern over and over.

Deciding a verdict is a two step process. `select-groups` picks which groups apply to a bot token: an exact case insensitive match on the token if one exists, otherwise the `*` groups, and either every matching group in `merge` mode or only the first one in `strict` mode. `decision-from-groups` then finds every rule in those groups whose pattern matches the path, ranks them by byte length with `Allow` breaking ties in its favor per the specification, and returns a `Decision` carrying the `Verdict` enum value, the winning `Rule`, the `Group` it came from and whether the ranking was actually a tie. `decide` wires those two together for a named bot, and `wildcard-decision` runs the same ranking against only the `*` groups, which is how the audit checks what an unlisted bot would experience without inventing a fake token.

The `audit` function runs six checks over a bot list and a path list: `search-engine-collateral` for reference search bots caught by the wildcard, `bot-group-overrides-wildcard-block` for AI bots whose own group quietly allows what the wildcard tries to deny, `empty-disallow-is-allow-all` for a bare `Disallow:` line that people misread as a full block, `duplicate-group-fragile` for a token repeated across separate groups, and `merge-vs-strict-divergence` plus `tie-break-ambiguous`, which run every bot against every path in both matching modes and group identical outcomes into one finding instead of repeating the same sentence per bot. Findings carry a severity from `%severity-rank`, from `INFO` up to `HIGH`, and the built in registry in `@default-ai-bots` plus the two entries in `@reference-search-bots` cover the crawlers documented publicly as of this writing. Extend or replace that list with `--bots` any time a new one shows up, because this space moves fast and no fixed list stays complete for long.

## Usage

Raku's own argument parser expects named options before the subcommand, so that is the order the CLI needs:

```
raku AiCrawlerPolicyAuditor.raku check robots.txt GPTBot /private/
raku AiCrawlerPolicyAuditor.raku [--mode=strict] check robots.txt ClaudeBot /docs

raku AiCrawlerPolicyAuditor.raku audit robots.txt
raku AiCrawlerPolicyAuditor.raku [--bots=my-bots.txt] [--paths=my-paths.txt] [--format=json] [--fail-on=HIGH] audit robots.txt
curl https://example.com/robots.txt | raku AiCrawlerPolicyAuditor.raku audit -
```

`check` prints the verdict for one bot and one path along with the exact rule and line that produced it, and exits 1 on Deny so it composes with shell scripts. `audit` runs the full set of findings against the built in registry, or against your own `--bots` and `--paths` files, one token per line. `--format=json` gives machine readable output for a CI step; `--fail-on` sets the severity that turns a clean exit into a failing one, from `INFO` through `HIGH`, defaulting to `MEDIUM`.

## Notes

The byte length tie break used for ranking rules follows the specification's own wording, longest rule wins, measured in encoded bytes rather than characters, so a pattern using non ASCII text ranks the way a real crawler's implementation would rank it. This tool never fetches a URL itself, on purpose: hand it a file, or pipe one in through `curl` and a dash, and it stays a pure text processing step with no network surface of its own. Path matching is case sensitive, which matches the specification and every crawler implementation, so `/Private/` and `/private/` are different paths even though the crawler's own product token comparison is case insensitive. The crawler registry is a snapshot, not a promise. New training and browsing bots appear often enough that trusting a hardcoded list forever would be a mistake, which is exactly why `--bots` exists as an escape hatch rather than a rarely used option.
