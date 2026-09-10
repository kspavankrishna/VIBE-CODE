# Structured JSON Repair

An LLM returns almost valid JSON: wrapped in a markdown fence, with a bare key, a `True` instead of `true` and a trailing comma. `json.loads` throws, your request fails and you pay for a retry that may break the same way. This is a single file Python class that repairs those payloads deterministically before parsing.

**Language:** Python | **Lines:** 152 | **Added:** 2026-04-10

## What this solves

The failure is boring and it is everywhere. You ask a model for structured output, you get back something that looks like JSON to a human and is rejected by a parser. The usual suspects: a ```` ```json ```` fence with a sentence of commentary above it, keys written bare like `user:` instead of `"user":`, Python literals `True` `False` `None` leaking in because the model was trained on a lot of Python, a `// comment` explaining a field, a trailing comma before a closing brace, or smart quotes because the text passed through something that autocorrected them.

Without a repair step in front of your parser, each of these raises `json.JSONDecodeError` and the whole call is dead. In a chat product the user sees a spinner then an error. In a batch job a row silently lands in a dead letter queue. In an agent loop the tool call fails, the agent retries, and you pay full input and output tokens again for a response that may fail on a different cosmetic problem. At scale that retry tax is real money and real latency, and the on call engineer who gets paged finds a payload that is obviously correct data, just malformed syntax.

The other cost is trust. Teams start writing defensive prompt text begging the model to emit only JSON, then a `try/except` that falls back to a regex, then a second model call to fix the first model call. That is three fragile layers doing the job of one deterministic one. Repair belongs between the model output and your schema validation step, not inside the prompt.

This file does the repair and nothing else. It hands you a Python object or it raises. It does not call a model, it does not touch the network and it has no dependencies beyond the standard library.

## Why I built it

Retrying the model is expensive and still not reliable. The same prompt that produced a bare key once will produce it again, and a "please output valid JSON only" instruction reduces the rate without driving it to zero. What I wanted was a small deterministic layer that recovers the known breakage patterns and refuses anything it cannot honestly fix, so a real structural error still surfaces as an error instead of being papered over.

Existing options are either too heavy or too clever. Pulling a whole tolerant parser dependency into a service for this is overkill, and the aggressive repair libraries will happily invent closing brackets and hand you a truncated object that passes validation and corrupts downstream data. The design here is the opposite: repair in small ordered passes, try the least invasive candidate first and give up loudly.

## When to use it

- Parsing model output in a tool calling or function calling loop where a failed parse costs you a full retry
- A batch pipeline over thousands of generated records where a one percent parse failure rate is thousands of dropped rows
- Reading JSON that a human pasted into a config box or a ticket, complete with smart quotes from a word processor
- Consuming JSON5 flavoured config or fixtures with comments and trailing commas, without adding a parser dependency
- Normalising scraped or logged payloads to canonical, key sorted JSON before diffing or hashing them
- Any place you are currently wrapping `json.loads` in a bare `except` and hoping

## How it works

Everything hangs off one class, `StructuredJsonRepair`, with two public class methods. `loads(text)` returns the parsed Python object. `dumps(text, indent=2)` parses then re encodes with `json.dumps` using `ensure_ascii=False` and `sort_keys=True`, so you get canonical, readable output with Unicode intact.

The first stage is candidate extraction. `_extract_candidate` strips whitespace and a leading BOM, then tries `FENCE_RE`, a regex for a ```` ``` ```` or ```` ```json ```` block, and takes the first fenced body if one exists. If there is no fence it finds the earliest `{` or `[` and walks forward with a bracket stack, tracking whether it is inside a string and whether the current character is escaped. It returns the slice ending at the character that empties the stack. That is what lets it pull a clean object out of `here you go: {...} hope that helps`, and why a second object later in the text is ignored rather than concatenated into garbage. If the brackets never balance it returns everything from the first bracket onward and lets the parse attempts fail naturally.

The second stage is a fixed ladder of repair passes, each a small pure function. `_normalize_quotes` maps curly quotes to ASCII. `_strip_comments` removes `/* ... */` with a DOTALL regex, then walks each line character by character with the same string and escape tracking to cut a `//` comment only when it is genuinely outside a string. `_fix_python_literals` rewrites `True`, `False` and `None` on word boundaries. `_quote_keys` applies `KEY_RE`, which matches a bare identifier that follows `{`, `[` or `,` and precedes a colon, and it loops to a fixed point because adjacent matches overlap and one `re.sub` pass will not catch them all. `_fix_single_quotes` handles single quoted keys with `SQ_KEY_RE` and single quoted values after a colon with `SQ_VALUE_RE`, re encoding each value through `json.dumps` so embedded escapes come out correct. `_remove_trailing_commas` runs `TRAILING_COMMA_RE` to a fixed point for the same overlap reason.

The third stage is the search. `loads` builds a candidate list ordered from least invasive to most: quote normalised only, then comment stripped, then for each of those the literal fix, the key quoting, the single quote fix and the trailing comma removal applied cumulatively. Ten candidates, deduplicated through a `seen` set, tried in order against `json.loads`. First one that parses wins. This ordering matters. Payloads that are already valid JSON hit the very first candidate and are returned byte identical in meaning, so a string value containing the word `None` is never mangled by the literal rewriter. If every candidate fails, `loads` raises `ValueError` carrying the first 280 characters of the extracted candidate so the log line shows you the actual bad input.

## Usage

````python
from StructuredJsonRepair import StructuredJsonRepair

raw = """here you go:
```json
{
  user: 'Pavan',
  active: True,
  city: 'Bangalore',   // from profile
  score: 91,
}
```
hope that helps"""

data = StructuredJsonRepair.loads(raw)
# {'user': 'Pavan', 'active': True, 'city': 'Bangalore', 'score': 91}

print(StructuredJsonRepair.dumps(raw, indent=2))
# canonical, key sorted JSON text

try:
    StructuredJsonRepair.loads('{"a": 1')      # truncated, not repairable
except ValueError as exc:
    print(exc)   # Could not repair JSON payload: {"a": 1
````

Run the file directly to see the bundled demo:

```bash
python3 StructuredJsonRepair.py
```

## Notes

- Standard library only: `json`, `re` and `typing`. No install step, drop the file in.
- Single quoted strings are only repaired in two positions: as a key, and as a value directly after a colon. Single quoted elements inside an array such as `{"tags": ['a', 'b']}` are not covered and will fail. The `__main__` demo in the file contains exactly that pattern and currently raises `ValueError`. The class methods themselves work as described above.
- It does not repair truncated output. Nothing adds a missing `}` or `]`, by design, so a cut off response fails loudly instead of validating as a partial object.
- The repair passes are regex based and operate on the whole text, so on a payload that needs them a `None` or a trailing comma inside a string value can be rewritten. Ordering protects the common case because valid JSON parses on the first candidate, but be aware of it for adversarial content.
- Failure raises `ValueError`, not `json.JSONDecodeError`. Catch accordingly.
- `dumps` sorts keys, so it does not preserve original key order. Use `loads` plus your own `json.dumps` if order matters.
- Only the first fenced block is considered, and when there is no fence only the first balanced bracket group is returned. Multiple JSON objects in one response need to be split before calling this.
- This is a parser front end, not a validator. Run your schema check, Pydantic model or type coercion on the result as the next step.
