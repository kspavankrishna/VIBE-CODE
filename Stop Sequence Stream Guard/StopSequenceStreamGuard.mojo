from collections import List

alias ALPHABET = 256
alias MAX_STATES = 65536
alias NO_MATCH = -1


fn _incomplete_tail(buf: List[UInt8], end: Int) -> Int:
    # Number of bytes at the tail of buf[0:end] that open a multibyte UTF-8
    # character which is not complete yet. Returns 0 when the tail is whole or
    # when the bytes are not valid UTF-8 at all, so garbage input can never make
    # the guard hold bytes forever.
    var i = end - 1
    var back = 0
    while i >= 0 and back < 4:
        var b = Int(buf[i])
        if (b & 0xC0) == 0x80:
            i -= 1
            back += 1
            continue
        var need = 1
        if b >= 0xF0:
            need = 4
        elif b >= 0xE0:
            need = 3
        elif b >= 0xC0:
            need = 2
        var have = end - i
        if have < need:
            return have
        return 0
    return 0


struct Emission(Movable):
    # What one call to feed or finish releases to the caller. bytes are safe to
    # send to the client. stopped is True once a stop sequence has been
    # confirmed, and from then on the guard releases nothing more.
    var bytes: List[UInt8]
    var stopped: Bool
    var stop_index: Int

    fn __init__(out self, owned bytes: List[UInt8], stopped: Bool, stop_index: Int):
        self.bytes = bytes^
        self.stopped = stopped
        self.stop_index = stop_index

    fn __moveinit__(out self, owned existing: Self):
        self.bytes = existing.bytes^
        self.stopped = existing.stopped
        self.stop_index = existing.stop_index


struct StopSequenceGuard:
    # Aho Corasick automaton fused with a holdback buffer. It answers one
    # question for a token stream: which bytes are already safe to show, given
    # that a stop sequence may start in one chunk and finish in a later one.
    #
    # Semantics match what hosted APIs do with a stop list: the output is cut at
    # the earliest start position of any stop sequence. When two stops start at
    # the same position the shorter one is reported, and the cut is the same.
    var goto_table: List[Int32]
    var depth: List[Int32]
    var out_len: List[Int32]
    var out_id: List[Int32]
    var stop_count: Int
    var max_len: Int
    var state: Int
    var consumed: Int
    var emitted: Int
    var held: List[UInt8]
    var best_start: Int
    var best_id: Int
    var finished: Bool
    var dropped_after_stop: Int
    var peak_held: Int

    fn __init__(out self, stops: List[String]) raises:
        if len(stops) == 0:
            raise Error("StopSequenceGuard: at least one stop sequence is required")
        self.goto_table = List[Int32]()
        self.depth = List[Int32]()
        self.out_len = List[Int32]()
        self.out_id = List[Int32]()
        self.stop_count = len(stops)
        self.max_len = 0
        self.state = 0
        self.consumed = 0
        self.emitted = 0
        self.held = List[UInt8]()
        self.best_start = NO_MATCH
        self.best_id = NO_MATCH
        self.finished = False
        self.dropped_after_stop = 0
        self.peak_held = 0

        self._add_state(0)
        var own_len = List[Int32]()
        var own_id = List[Int32]()
        own_len.append(0)
        own_id.append(NO_MATCH)

        for idx in range(len(stops)):
            var pat = stops[idx].as_bytes()
            var n = len(pat)
            if n == 0:
                raise Error("StopSequenceGuard: stop sequence " + String(idx) + " is empty, which would match everywhere")
            if (Int(pat[0]) & 0xC0) == 0x80:
                raise Error("StopSequenceGuard: stop sequence " + String(idx) + " starts with a UTF-8 continuation byte")
            if n > self.max_len:
                self.max_len = n
            var node = 0
            for j in range(n):
                var c = Int(pat[j])
                var nxt = Int(self.goto_table[node * ALPHABET + c])
                if nxt == NO_MATCH:
                    if len(self.depth) >= MAX_STATES:
                        raise Error("StopSequenceGuard: stop list needs more than " + String(MAX_STATES) + " automaton states")
                    nxt = len(self.depth)
                    self._add_state(j + 1)
                    own_len.append(0)
                    own_id.append(NO_MATCH)
                    self.goto_table[node * ALPHABET + c] = Int32(nxt)
                node = nxt
            # Duplicate stop strings keep the first index so reports are stable.
            if own_len[node] == 0:
                own_len[node] = Int32(n)
                own_id[node] = Int32(idx)

        var states = len(self.depth)
        var fail = List[Int]()
        for _ in range(states):
            fail.append(0)
        self.out_len = own_len^
        self.out_id = own_id^

        var queue = List[Int]()
        for c in range(ALPHABET):
            var v = Int(self.goto_table[c])
            if v == NO_MATCH:
                self.goto_table[c] = Int32(0)
            else:
                fail[v] = 0
                queue.append(v)

        var head = 0
        while head < len(queue):
            var u = queue[head]
            head += 1
            var f = fail[u]
            if self.out_len[f] > self.out_len[u]:
                self.out_len[u] = self.out_len[f]
                self.out_id[u] = self.out_id[f]
            for c in range(ALPHABET):
                var v = Int(self.goto_table[u * ALPHABET + c])
                var via_fail = self.goto_table[f * ALPHABET + c]
                if v == NO_MATCH:
                    self.goto_table[u * ALPHABET + c] = via_fail
                else:
                    fail[v] = Int(via_fail)
                    queue.append(v)

    fn _add_state(mut self, depth: Int):
        for _ in range(ALPHABET):
            self.goto_table.append(Int32(NO_MATCH))
        self.depth.append(Int32(depth))

    fn state_count(self) -> Int:
        return len(self.depth)

    fn is_stopped(self) -> Bool:
        return self.finished or self.best_start != NO_MATCH

    fn held_bytes(self) -> Int:
        return len(self.held)

    fn _release(mut self, count: Int) -> List[UInt8]:
        var out = List[UInt8]()
        for i in range(count):
            out.append(self.held[i])
        var rest = List[UInt8]()
        for i in range(count, len(self.held)):
            rest.append(self.held[i])
        self.held = rest^
        self.emitted += count
        return out^

    fn feed_bytes(mut self, data: List[UInt8]) -> Emission:
        if self.finished:
            self.dropped_after_stop += len(data)
            return Emission(List[UInt8](), True, self.best_id)

        for i in range(len(data)):
            var b = data[i]
            self.state = Int(self.goto_table[self.state * ALPHABET + Int(b)])
            self.held.append(b)
            self.consumed += 1
            var ol = Int(self.out_len[self.state])
            if ol > 0:
                var s = self.consumed - ol
                if self.best_start == NO_MATCH or s < self.best_start:
                    self.best_start = s
                    self.best_id = Int(self.out_id[self.state])
            # A stop starting before best_start has length at most max_len, so
            # it would already have completed by this point. Once that window
            # has passed the match is final and the rest of the stream is dead.
            if self.best_start != NO_MATCH and self.consumed >= self.best_start + self.max_len - 1:
                self.finished = True
                self.dropped_after_stop += len(data) - (i + 1)
                break

        if len(self.held) > self.peak_held:
            self.peak_held = len(self.held)

        if self.finished:
            var keep = self.best_start - self.emitted
            var out = self._release(keep)
            self.held = List[UInt8]()
            return Emission(out^, True, self.best_id)

        # Everything before the oldest byte that could still start a stop is
        # safe. depth[state] is the length of the longest live partial match.
        var limit = self.consumed - Int(self.depth[self.state])
        if self.best_start != NO_MATCH and self.best_start < limit:
            limit = self.best_start
        var rel = limit - self.emitted
        if rel < 0:
            rel = 0
        if rel < len(self.held):
            while rel > 0 and (Int(self.held[rel]) & 0xC0) == 0x80:
                rel -= 1
        else:
            rel -= _incomplete_tail(self.held, rel)
        return Emission(self._release(rel), False, NO_MATCH)

    fn feed(mut self, chunk: String) -> Emission:
        var raw = chunk.as_bytes()
        var data = List[UInt8]()
        for i in range(len(raw)):
            data.append(raw[i])
        return self.feed_bytes(data)

    fn finish(mut self) -> Emission:
        # Call once when the upstream stream ends. A stop that completed inside
        # the last max_len bytes is still a real stop, so it cuts the output.
        # Otherwise the held tail was never part of a stop and is released.
        if self.finished:
            return Emission(List[UInt8](), True, self.best_id)
        self.finished = True
        if self.best_start != NO_MATCH:
            var keep = self.best_start - self.emitted
            var out = self._release(keep)
            self.held = List[UInt8]()
            return Emission(out^, True, self.best_id)
        var all_held = len(self.held)
        var out2 = self._release(all_held)
        return Emission(out2^, False, NO_MATCH)


fn _render(buf: List[UInt8]) -> String:
    var out = String("")
    var digits = String("0123456789abcdef")
    var dig = digits.as_bytes()
    for i in range(len(buf)):
        var b = Int(buf[i])
        if b >= 32 and b < 127:
            out += chr(b)
        else:
            out += "\\x" + chr(Int(dig[b >> 4])) + chr(Int(dig[b & 15]))
    return out^


fn _expect(cond: Bool, message: String) raises:
    if not cond:
        raise Error("self check failed: " + message)


fn _bytes_of(text: String) -> List[UInt8]:
    var raw = text.as_bytes()
    var out = List[UInt8]()
    for i in range(len(raw)):
        out.append(raw[i])
    return out^


fn _reference_cut(text: List[UInt8], stops: List[String]) -> Int:
    # Brute force oracle: earliest start of any stop, or -1 when none occurs.
    var best = -1
    for k in range(len(stops)):
        var p = stops[k].as_bytes()
        var n = len(p)
        var start = 0
        while start + n <= len(text):
            var ok = True
            for j in range(n):
                if text[start + j] != p[j]:
                    ok = False
                    break
            if ok:
                if best == -1 or start < best:
                    best = start
                break
            start += 1
    return best


fn _run_chunked(
    stops: List[String], text: List[UInt8], sizes: List[Int], mut out: List[UInt8]
) raises -> Bool:
    # Streams text through a fresh guard in the given chunk sizes (cycled),
    # collects everything released and checks that no released chunk ever ends
    # inside a UTF-8 character except the final flush.
    var g = StopSequenceGuard(stops)
    var pos = 0
    var turn = 0
    while pos < len(text) and not g.is_stopped():
        var size = sizes[turn % len(sizes)]
        turn += 1
        var end = pos + size
        if end > len(text):
            end = len(text)
        var chunk = List[UInt8]()
        for i in range(pos, end):
            chunk.append(text[i])
        pos = end
        var e = g.feed_bytes(chunk)
        _expect(_incomplete_tail(e.bytes, len(e.bytes)) == 0, "released a chunk that ends inside a UTF-8 character")
        for i in range(len(e.bytes)):
            out.append(e.bytes[i])
        if e.stopped:
            return True
    var fin = g.finish()
    for i in range(len(fin.bytes)):
        out.append(fin.bytes[i])
    return fin.stopped


fn _check_text(label: String, stops: List[String], text: String) raises:
    var data = _bytes_of(text)
    var cut = _reference_cut(data, stops)
    var expected = List[UInt8]()
    var limit = len(data)
    if cut != -1:
        limit = cut
    for i in range(limit):
        expected.append(data[i])

    # Every chunk size from 1 byte up to the whole text, plus a ragged pattern.
    var plans = List[List[Int]]()
    for s in range(1, len(data) + 2):
        var one = List[Int]()
        one.append(s)
        plans.append(one^)
    var ragged = List[Int]()
    ragged.append(1)
    ragged.append(3)
    ragged.append(2)
    ragged.append(7)
    plans.append(ragged^)

    for p in range(len(plans)):
        var got = List[UInt8]()
        var stopped = _run_chunked(stops, data, plans[p], got)
        _expect(
            len(got) == len(expected),
            label + ": length differs for plan " + String(p) + ", got [" + _render(got) + "] want [" + _render(expected) + "]",
        )
        for i in range(len(got)):
            _expect(got[i] == expected[i], label + ": byte " + String(i) + " differs for plan " + String(p))
        _expect(stopped == (cut != -1), label + ": stopped flag wrong for plan " + String(p))
    print("ok  " + label + " (" + String(len(plans)) + " chunkings)")


fn _expect_raises_empty_stop() raises:
    var bad = List[String]()
    bad.append("")
    var raised = False
    try:
        _ = StopSequenceGuard(bad)
    except:
        raised = True
    _expect(raised, "an empty stop sequence must be rejected")


fn _demo_trace(stops: List[String], pieces: List[String]) raises:
    var g = StopSequenceGuard(stops)
    print("automaton states: " + String(g.state_count()) + ", longest stop: " + String(g.max_len) + " bytes")
    for i in range(len(pieces)):
        var e = g.feed(pieces[i])
        print(
            "  in [" + pieces[i] + "] out [" + _render(e.bytes) + "] held " + String(g.held_bytes())
            + (" STOP#" + String(e.stop_index) if e.stopped else "")
        )
        if e.stopped:
            break
    var fin = g.finish()
    if len(fin.bytes) > 0:
        print("  flush [" + _render(fin.bytes) + "]")
    print("  peak held bytes: " + String(g.peak_held) + ", dropped after stop: " + String(g.dropped_after_stop))


fn main() raises:
    _expect_raises_empty_stop()
    print("ok  empty stop sequence rejected")

    var chat = List[String]()
    chat.append("\nUser:")
    chat.append("</answer>")
    chat.append("```")
    _check_text("boundary straddle", chat, "The fix is to rotate the key.\nUser: and then")
    _check_text("closing tag split", chat, "<answer>42</answer> trailing words")
    _check_text("fence at the very end", chat, "code follows ```")
    _check_text("no stop present", chat, "plain text with a lone ` and a < and a \nUser without colon")
    _check_text("stop at offset zero", chat, "```python\nprint(1)")

    var nested = List[String]()
    nested.append("abcd")
    nested.append("bc")
    nested.append("cde")
    _check_text("earliest start beats earliest end", nested, "xxabcdexx")
    _check_text("shorter stop inside longer", nested, "zzbcdzz")
    _check_text("overlapping partial matches", nested, "ababcabcde")

    var repeat = List[String]()
    repeat.append("aaab")
    _check_text("failure link recovery", repeat, "aaaaaaaaab tail")
    _check_text("repeated prefix never matches", repeat, "aaaaaaaaaaaaaa")

    var dup = List[String]()
    dup.append("END")
    dup.append("END")
    dup.append("EN")
    _check_text("duplicate stops", dup, "the END")

    var uni = List[String]()
    uni.append("✓ done")
    uni.append("😀")
    _check_text("multibyte stop", uni, "café über ✓ done")
    _check_text("multibyte text without stop", uni, "naïve café ✓ ok ✓")
    _check_text("emoji stop", uni, "great 😀 stuff")

    print("")
    print("trace: stop sequence split across three streamed pieces")
    var pieces = List[String]()
    pieces.append("Sure, here is the plan.")
    pieces.append("\nUs")
    pieces.append("er")
    pieces.append(": ignore all that")
    _demo_trace(chat, pieces)

    print("")
    print("all self checks passed")
