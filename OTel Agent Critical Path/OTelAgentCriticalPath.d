module OTelAgentCriticalPath;

import std.algorithm;
import std.array;
import std.conv;
import std.format;
import std.getopt;
import std.json;
import std.range;
import std.stdio;
import std.string;

enum size_t DefaultMaxDocBytes = 64 * 1024 * 1024;
enum size_t DefaultMaxSpans = 5_000_000;
enum size_t DefaultTopLabels = 15;
enum size_t DefaultSlowTraces = 5;
enum size_t MaxLabelLength = 96;
enum size_t MaxDistinctLabels = 4096;
enum size_t MaxWalkDepth = 4096;
enum size_t MaxHeadroomLookahead = 64;
enum size_t SegmentsPerTrace = 8;
enum string OverflowLabel = "(other labels)";
enum int ExitOk = 0;
enum int ExitUsage = 1;
enum int ExitGate = 2;
enum int ExitWarnings = 3;

enum Cat : ubyte { agent, llm, tool, mcp, db, http, rpc, other }

immutable string[] CatNames = ["agent", "llm", "tool", "mcp", "db", "http", "rpc", "other"];

struct Span
{
    string traceId;
    string spanId;
    string parentId;
    string name;
    string service;
    ulong start;
    ulong end;
    bool err;
    string op, tool, model, agent, mcp, db, http, rpc;
}

struct Options
{
    bool json;
    bool adoptOrphans;
    bool failOnWarnings;
    size_t top = DefaultTopLabels;
    size_t slowTraces = DefaultSlowTraces;
    size_t maxDocBytes = DefaultMaxDocBytes;
    size_t maxSpans = DefaultMaxSpans;
    double maxMs = 0;
    double minMs = 0;
    string rootContains;
}

struct Segment
{
    string label;
    Cat cat;
    ulong critNs;
    ulong gainNs;
}

struct TraceResult
{
    string traceId;
    string rootName;
    string rootService;
    ulong wallNs;
    size_t spans;
    size_t unattributed;
    Segment[] top;
}

struct LabelAgg
{
    Cat cat;
    ulong critNs;
    ulong selfNs;
    ulong gainNs;
    ulong count;
    ulong critCount;
    ulong errors;
}

/* Splits a byte stream into top level JSON objects and arrays. It tracks string
   and escape state so braces inside strings never end a document early, which
   means JSONL, concatenated exports and pretty printed files all work. */
struct DocSplitter
{
    size_t maxBytes = DefaultMaxDocBytes;
    Appender!(char[]) buf;
    int depth;
    bool inStr, esc, overflow, inGarbage;
    ulong oversize, garbage;

    void feed(const(ubyte)[] chunk, scope void delegate(const(char)[]) sink)
    {
        foreach (b; chunk)
        {
            immutable char c = cast(char) b;
            if (depth == 0)
            {
                if (c == '{' || c == '[')
                {
                    depth = 1;
                    inStr = false;
                    esc = false;
                    overflow = false;
                    inGarbage = false;
                    buf.clear();
                    buf.put(c);
                }
                else if (c == ' ' || c == '\t' || c == '\r' || c == '\n')
                    inGarbage = false;
                else if (!inGarbage)
                {
                    ++garbage;
                    inGarbage = true;
                }
                continue;
            }
            if (!overflow)
            {
                if (buf.data.length >= maxBytes)
                {
                    overflow = true;
                    ++oversize;
                    buf.clear();
                }
                else
                    buf.put(c);
            }
            if (inStr)
            {
                if (esc)
                    esc = false;
                else if (c == '\\')
                    esc = true;
                else if (c == '"')
                    inStr = false;
                continue;
            }
            if (c == '"')
                inStr = true;
            else if (c == '{' || c == '[')
                ++depth;
            else if (c == '}' || c == ']')
            {
                if (--depth == 0)
                {
                    if (!overflow)
                        sink(buf.data);
                    buf.clear();
                    overflow = false;
                }
            }
        }
    }

    bool midDocument() const
    {
        return depth != 0;
    }
}

const(JSONValue)* member(ref const JSONValue v, string key)
{
    if (v.type != JSONType.object)
        return null;
    return key in v.object;
}

string ownedString(const(JSONValue)* p)
{
    if (p is null || p.type != JSONType.string)
        return "";
    return p.str.idup;
}

string normalizeId(const(JSONValue)* p)
{
    string s = ownedString(p);
    if (s.length == 0 || s.length > 64)
        return "";
    if (s.all!(ch => ch == '0'))
        return "";
    return s.toLower();
}

bool toNano(const(JSONValue)* p, out ulong r)
{
    if (p is null)
        return false;
    switch (p.type)
    {
    case JSONType.string:
        auto s = p.str;
        if (s.length == 0 || s.length > 20)
            return false;
        foreach (ch; s)
            if (ch < '0' || ch > '9')
                return false;
        try
            r = s.to!ulong;
        catch (ConvException)
            return false;
        return true;
    case JSONType.integer:
        if (p.integer < 0)
            return false;
        r = cast(ulong) p.integer;
        return true;
    case JSONType.uinteger:
        r = p.uinteger;
        return true;
    case JSONType.float_:
        immutable double d = p.floating;
        if (!(d >= 0) || d > 1.8e19)
            return false;
        r = cast(ulong) d;
        return true;
    default:
        return false;
    }
}

string stringAttr(ref const JSONValue attrs, string wanted)
{
    if (attrs.type != JSONType.array)
        return "";
    foreach (ref a; attrs.array)
    {
        if (ownedString(member(a, "key")) != wanted)
            continue;
        auto v = member(a, "value");
        if (v is null)
            return "";
        return ownedString(member(*v, "stringValue"));
    }
    return "";
}

bool parseSpan(ref const JSONValue j, string service, out Span s)
{
    if (j.type != JSONType.object)
        return false;
    s.traceId = normalizeId(member(j, "traceId"));
    s.spanId = normalizeId(member(j, "spanId"));
    if (s.traceId.length == 0 || s.spanId.length == 0)
        return false;
    s.parentId = normalizeId(member(j, "parentSpanId"));
    if (!toNano(member(j, "startTimeUnixNano"), s.start))
        return false;
    if (!toNano(member(j, "endTimeUnixNano"), s.end))
        return false;
    if (s.end < s.start)
        return false;
    s.name = ownedString(member(j, "name"));
    if (s.name.length == 0)
        s.name = "(unnamed)";
    s.service = service;
    if (auto st = member(j, "status"))
    {
        auto code = member(*st, "code");
        if (code !is null)
        {
            if (code.type == JSONType.integer)
                s.err = code.integer == 2;
            else if (code.type == JSONType.string)
                s.err = code.str == "STATUS_CODE_ERROR" || code.str == "2";
        }
    }
    if (auto at = member(j, "attributes"))
    {
        if (at.type == JSONType.array)
        {
            foreach (ref a; at.array)
            {
                immutable key = ownedString(member(a, "key"));
                auto v = member(a, "value");
                if (v is null)
                    continue;
                immutable val = ownedString(member(*v, "stringValue"));
                if (val.length == 0)
                    continue;
                switch (key)
                {
                case "gen_ai.operation.name":
                    s.op = val;
                    break;
                case "llm.request.type":
                    if (s.op.length == 0)
                        s.op = val;
                    break;
                case "gen_ai.tool.name":
                    s.tool = val;
                    break;
                case "gen_ai.request.model":
                    s.model = val;
                    break;
                case "gen_ai.agent.name":
                    s.agent = val;
                    break;
                case "mcp.method.name":
                    s.mcp = val;
                    break;
                case "db.system":
                case "db.system.name":
                    s.db = val;
                    break;
                case "http.request.method":
                case "http.method":
                    s.http = val;
                    break;
                case "rpc.system":
                    s.rpc = val;
                    break;
                default:
                    break;
                }
            }
        }
    }
    return true;
}

string clipLabel(string label)
{
    if (label.length <= MaxLabelLength)
        return label;
    size_t cut = MaxLabelLength;
    while (cut > 0 && (label[cut] & 0xC0) == 0x80)
        --cut;
    return label[0 .. cut];
}

Cat classify(ref const Span s, out string label)
{
    immutable op = s.op;
    Cat cat;
    string text;
    if (op.among("chat", "text_completion", "completion", "generate_content", "embeddings"))
    {
        cat = Cat.llm;
        text = "llm:" ~ (s.model.length ? s.model : s.name);
    }
    else if (op == "execute_tool" || (op.length == 0 && s.tool.length))
    {
        cat = Cat.tool;
        text = "tool:" ~ (s.tool.length ? s.tool : s.name);
    }
    else if (op.among("invoke_agent", "create_agent") || (op.length == 0 && s.agent.length))
    {
        cat = Cat.agent;
        text = "agent:" ~ (s.agent.length ? s.agent : s.name);
    }
    else if (s.mcp.length)
    {
        cat = Cat.mcp;
        text = "mcp:" ~ s.name;
    }
    else if (s.db.length)
    {
        cat = Cat.db;
        text = "db:" ~ s.db ~ " " ~ s.name;
    }
    else if (s.http.length)
    {
        cat = Cat.http;
        text = "http:" ~ s.name;
    }
    else if (s.rpc.length)
    {
        cat = Cat.rpc;
        text = "rpc:" ~ s.name;
    }
    else
    {
        cat = Cat.other;
        text = s.name;
    }
    label = clipLabel(text);
    return cat;
}

final class Store
{
    Span[][string] traces;
    ulong spanCount, invalidSpans, docs, badDocs, nonOtlpDocs;
    ulong oversizeDocs, garbageRuns, truncatedDocs;
    bool capHit;
    size_t maxSpans = DefaultMaxSpans;

    void addDoc(const(char)[] doc)
    {
        JSONValue root;
        try
            root = parseJSON(doc);
        catch (JSONException)
        {
            ++badDocs;
            return;
        }
        ++docs;
        if (root.type == JSONType.array)
        {
            foreach (ref e; root.array)
                addExport(e);
        }
        else
            addExport(root);
    }

    private void addExport(ref const JSONValue e)
    {
        auto rs = member(e, "resourceSpans");
        if (rs is null || rs.type != JSONType.array)
        {
            ++nonOtlpDocs;
            return;
        }
        foreach (ref r; rs.array)
        {
            string service;
            if (auto res = member(r, "resource"))
                if (auto at = member(*res, "attributes"))
                    service = stringAttr(*at, "service.name");
            foreach (scopeKey; ["scopeSpans", "instrumentationLibrarySpans"])
            {
                auto ss = member(r, scopeKey);
                if (ss is null || ss.type != JSONType.array)
                    continue;
                foreach (ref sc; ss.array)
                {
                    auto spans = member(sc, "spans");
                    if (spans is null || spans.type != JSONType.array)
                        continue;
                    foreach (ref sj; spans.array)
                    {
                        if (spanCount >= maxSpans)
                        {
                            capHit = true;
                            return;
                        }
                        Span s;
                        if (parseSpan(sj, service, s))
                        {
                            traces[s.traceId] ~= s;
                            ++spanCount;
                        }
                        else
                            ++invalidSpans;
                    }
                }
            }
        }
    }

    void ingest(File f, size_t maxDocBytes)
    {
        DocSplitter sp;
        sp.maxBytes = maxDocBytes;
        foreach (chunk; f.byChunk(1 << 16))
        {
            sp.feed(chunk, (const(char)[] d) { addDoc(d); });
            if (capHit)
                break;
        }
        if (sp.midDocument() && !capHit)
            ++truncatedDocs;
        oversizeDocs += sp.oversize;
        garbageRuns += sp.garbage;
    }
}

final class Agg
{
    LabelAgg[string] labels;
    ulong[8] catCrit, catSelf;
    ulong wallTotal, selfTotal;
    ulong[] walls;
    TraceResult[] results;
    ulong tracesSeen, tracesFiltered, orphanSpans, cycleSpans, skewedSpans, dupSpans;
    ulong extraRoots, accountingErrors, deepTruncations;
}

struct TraceCtx
{
    Span[] sp;
    ulong[] es, ee, crit, hr, selfNs;
    size_t[][] kidsEnd;
    size_t deepTruncations;

    void walk(size_t n, ulong lo, ulong hi, size_t depth)
    {
        if (hi <= lo)
            return;
        if (depth > MaxWalkDepth || kidsEnd[n].length == 0)
        {
            if (depth > MaxWalkDepth)
                ++deepTruncations;
            crit[n] += hi - lo;
            return;
        }
        auto ks = kidsEnd[n];
        ulong cursor = hi;
        foreach (i, c; ks)
        {
            if (cursor <= lo)
                break;
            if (es[c] >= cursor)
                continue;
            immutable ce = min(ee[c], cursor);
            if (ce <= lo)
                break;
            immutable cl = max(es[c], lo);
            if (ce <= cl)
                continue;
            if (cursor > ce)
                crit[n] += cursor - ce;
            ulong next = lo;
            size_t looked = 0;
            foreach (d; ks[i + 1 .. $])
            {
                if (++looked > MaxHeadroomLookahead)
                {
                    next = ce;
                    break;
                }
                if (es[d] >= ce)
                    continue;
                immutable de = min(ee[d], ce);
                if (de <= max(es[d], lo))
                    continue;
                next = de;
                break;
            }
            immutable floor_ = max(next, cl);
            hr[c] = ce > floor_ ? ce - floor_ : 0;
            walk(c, cl, ce, depth + 1);
            cursor = cl;
        }
        if (cursor > lo)
            crit[n] += cursor - lo;
    }
}

bool analyzeTrace(string traceId, Span[] input, const ref Options o, Agg agg)
{
    ++agg.tracesSeen;
    Span[] sp;
    size_t[string] idxOf;
    foreach (ref s; input)
    {
        if (s.spanId in idxOf)
        {
            ++agg.dupSpans;
            continue;
        }
        idxOf[s.spanId] = sp.length;
        sp ~= s;
    }
    immutable n = sp.length;
    enum long TrueRoot = -1;
    enum long Orphan = -2;
    auto par = new long[n];
    foreach (i, ref s; sp)
    {
        if (s.parentId.length == 0)
            par[i] = TrueRoot;
        else if (auto p = s.parentId in idxOf)
            par[i] = cast(long)*p;
        else
            par[i] = Orphan;
    }

    long primary = -1;
    size_t trueRoots = 0;
    foreach (i; 0 .. n)
        if (par[i] == TrueRoot)
            ++trueRoots;
    foreach (i; 0 .. n)
    {
        immutable wantTrue = trueRoots > 0;
        if (par[i] != (wantTrue ? TrueRoot : Orphan))
            continue;
        if (primary < 0)
            primary = cast(long) i;
        else
        {
            immutable a = sp[i].end - sp[i].start;
            immutable b = sp[primary].end - sp[primary].start;
            if (a > b || (a == b && sp[i].start < sp[primary].start))
                primary = cast(long) i;
        }
    }
    if (primary < 0)
    {
        agg.cycleSpans += n;
        return false;
    }
    if (trueRoots > 1)
        agg.extraRoots += trueRoots - 1;
    if (o.adoptOrphans)
        foreach (i; 0 .. n)
            if (par[i] == Orphan)
                par[i] = primary;

    immutable wall = sp[primary].end - sp[primary].start;
    if (o.rootContains.length && !sp[primary].name.canFind(o.rootContains))
    {
        ++agg.tracesFiltered;
        return false;
    }
    if (o.minMs > 0 && wall < cast(ulong)(o.minMs * 1e6))
    {
        ++agg.tracesFiltered;
        return false;
    }

    auto kids = new size_t[][](n);
    foreach (i; 0 .. n)
        if (par[i] >= 0)
            kids[cast(size_t) par[i]] ~= i;

    TraceCtx cx;
    cx.sp = sp;
    cx.es = new ulong[n];
    cx.ee = new ulong[n];
    cx.crit = new ulong[n];
    cx.hr = new ulong[n];
    cx.selfNs = new ulong[n];
    cx.kidsEnd = new size_t[][](n);
    auto reached = new bool[n];
    cx.es[primary] = sp[primary].start;
    cx.ee[primary] = sp[primary].end;
    cx.hr[primary] = wall;

    size_t[] stack = [cast(size_t) primary];
    size_t reachedCount = 0;
    size_t skewed = 0;
    while (stack.length)
    {
        immutable cur = stack[$ - 1];
        stack.length = stack.length - 1;
        reached[cur] = true;
        ++reachedCount;
        foreach (c; kids[cur])
        {
            immutable s0 = sp[c].start;
            immutable e0 = sp[c].end;
            immutable a = min(max(s0, cx.es[cur]), cx.ee[cur]);
            immutable b = min(max(e0, cx.es[cur]), cx.ee[cur]);
            if (a != s0 || b != e0)
                ++skewed;
            cx.es[c] = a;
            cx.ee[c] = b;
            stack ~= c;
        }
    }
    agg.skewedSpans += skewed;

    auto state = new ubyte[n];
    size_t unattributed = 0;
    foreach (i; 0 .. n)
    {
        if (reached[i])
            continue;
        ++unattributed;
        size_t[] path;
        size_t cur = i;
        ubyte result = 2;
        while (true)
        {
            if (state[cur] == 2 || state[cur] == 3)
            {
                result = state[cur];
                break;
            }
            if (state[cur] == 1)
            {
                result = 3;
                break;
            }
            state[cur] = 1;
            path ~= cur;
            immutable p = par[cur];
            if (p < 0)
            {
                result = 2;
                break;
            }
            cur = cast(size_t) p;
        }
        foreach (q; path)
            state[q] = result;
        if (result == 3)
            ++agg.cycleSpans;
        else
            ++agg.orphanSpans;
    }

    foreach (i; 0 .. n)
    {
        if (!reached[i])
            continue;
        auto byStart = kids[i].dup;
        byStart.sort!((a, b) => cx.es[a] < cx.es[b] || (cx.es[a] == cx.es[b] && a < b));
        ulong cur = cx.es[i];
        ulong covered = 0;
        foreach (c; byStart)
        {
            if (cx.ee[c] <= cur)
                continue;
            immutable from = max(cx.es[c], cur);
            covered += cx.ee[c] - from;
            cur = cx.ee[c];
        }
        immutable total = cx.ee[i] - cx.es[i];
        cx.selfNs[i] = total > covered ? total - covered : 0;
        auto byEnd = kids[i].dup;
        byEnd.sort!((a, b) => cx.ee[a] > cx.ee[b] || (cx.ee[a] == cx.ee[b]
                && (cx.es[a] > cx.es[b] || (cx.es[a] == cx.es[b] && a < b))));
        cx.kidsEnd[i] = byEnd;
    }

    cx.walk(cast(size_t) primary, cx.es[primary], cx.ee[primary], 0);
    agg.deepTruncations += cx.deepTruncations;

    ulong critSum = 0;
    foreach (i; 0 .. n)
        if (reached[i])
            critSum += cx.crit[i];
    if (critSum != wall)
        ++agg.accountingErrors;

    ulong[string] perLabel;
    Cat[string] labelCat;
    foreach (i; 0 .. n)
    {
        if (!reached[i])
            continue;
        string label;
        Cat cat = classify(sp[i], label);
        if (label !in agg.labels && agg.labels.length >= MaxDistinctLabels)
        {
            label = OverflowLabel;
            cat = Cat.other;
        }
        auto la = label in agg.labels;
        if (la is null)
        {
            agg.labels[label] = LabelAgg(cat);
            la = label in agg.labels;
        }
        immutable gain = min(cx.crit[i], cx.hr[i]);
        la.critNs += cx.crit[i];
        la.selfNs += cx.selfNs[i];
        la.gainNs += gain;
        ++la.count;
        if (cx.crit[i] > 0)
            ++la.critCount;
        if (sp[i].err)
            ++la.errors;
        agg.catCrit[cat] += cx.crit[i];
        agg.catSelf[cat] += cx.selfNs[i];
        agg.selfTotal += cx.selfNs[i];
        perLabel[label] += cx.crit[i];
        labelCat[label] = cat;
    }
    agg.wallTotal += wall;
    agg.walls ~= wall;

    TraceResult tr;
    tr.traceId = traceId;
    tr.rootName = sp[primary].name;
    tr.rootService = sp[primary].service;
    tr.wallNs = wall;
    tr.spans = n;
    tr.unattributed = unattributed;
    auto ranked = perLabel.byKeyValue.array;
    ranked.sort!((a, b) => a.value > b.value || (a.value == b.value && a.key < b.key));
    foreach (kv; ranked.take(SegmentsPerTrace))
    {
        if (kv.value == 0)
            break;
        ulong gain = 0;
        foreach (i; 0 .. n)
            if (reached[i] && cx.crit[i] > 0)
            {
                string l;
                classify(sp[i], l);
                if (l == kv.key)
                    gain += min(cx.crit[i], cx.hr[i]);
            }
        tr.top ~= Segment(kv.key, labelCat[kv.key], kv.value, gain);
    }
    agg.results ~= tr;
    return true;
}

ulong percentile(const ulong[] sorted, double p)
{
    if (sorted.length == 0)
        return 0;
    size_t rank = cast(size_t)(p / 100.0 * sorted.length);
    if (p / 100.0 * sorted.length > rank)
        ++rank;
    if (rank == 0)
        rank = 1;
    return sorted[min(rank, sorted.length) - 1];
}

string ms(ulong ns)
{
    return format("%.2f", ns / 1.0e6);
}

string pct(ulong part, ulong whole)
{
    return whole == 0 ? "0.0" : format("%.1f", 100.0 * part / whole);
}

string[] warnings(Store st, Agg agg)
{
    string[] w;
    void add(ulong n, string text)
    {
        if (n)
            w ~= format("%s %s", n, text);
    }

    add(st.badDocs, "documents were not valid JSON and were skipped");
    add(st.nonOtlpDocs, "JSON documents had no resourceSpans and were skipped");
    add(st.oversizeDocs, "documents exceeded --max-doc-mb and were skipped");
    add(st.garbageRuns, "runs of non JSON text between documents were skipped");
    add(st.truncatedDocs, "input ended in the middle of a JSON document");
    add(st.invalidSpans, "spans had bad ids, bad timestamps or end before start and were skipped");
    add(agg.dupSpans, "duplicate span ids were dropped (first copy kept)");
    add(agg.orphanSpans, "spans sit under a missing parent and are not on any critical path (try --adopt-orphans)");
    add(agg.cycleSpans, "spans form a parent cycle or have no root and were ignored");
    add(agg.extraRoots, "extra root spans were found in traces and ignored");
    add(agg.skewedSpans, "spans fell outside their parent window and were clamped (clock skew or async work)");
    add(agg.deepTruncations, "spans were nested deeper than the walk limit");
    add(agg.accountingErrors, "traces failed the critical path accounting check, please report this");
    if (st.capHit)
        w ~= format("span cap of %s reached, later input was not read (raise --max-spans)", st.maxSpans);
    return w;
}

LabelAgg[string] dummyLabels;

auto rankedLabels(Agg agg)
{
    auto items = agg.labels.byKeyValue.array;
    items.sort!((a, b) => a.value.critNs > b.value.critNs
            || (a.value.critNs == b.value.critNs && a.key < b.key));
    return items;
}

string pad(string s, size_t w)
{
    return s.length >= w ? s : s ~ ' '.repeat(w - s.length).array.to!string;
}

string rpad(string s, size_t w)
{
    return s.length >= w ? s : ' '.repeat(w - s.length).array.to!string ~ s;
}

void printText(Store st, Agg agg, const ref Options o, string[] gateFails)
{
    auto sorted = agg.walls.dup.sort().release;
    writefln("Traces analysed: %s  spans read: %s  total wall: %s ms",
            agg.results.length, st.spanCount, ms(agg.wallTotal));
    if (agg.tracesFiltered)
        writefln("Traces filtered out by --root or --min-ms: %s", agg.tracesFiltered);
    if (agg.results.length == 0)
    {
        writeln("No traces to report.");
    }
    else
    {
        writefln("Trace wall ms: p50 %s  p95 %s  p99 %s  max %s",
                ms(percentile(sorted, 50)), ms(percentile(sorted, 95)),
                ms(percentile(sorted, 99)), ms(sorted[$ - 1]));
        immutable par = agg.wallTotal ? agg.selfTotal / cast(double) agg.wallTotal : 0.0;
        writefln("Parallelism: %.2f (exclusive work divided by wall clock, above 1.0 means overlap)", par);

        writeln("\nWall clock by category (critical path)");
        writeln(pad("category", 10), rpad("crit ms", 14), rpad("% wall", 9), rpad("self ms", 14));
        foreach (c; 0 .. CatNames.length)
        {
            if (agg.catCrit[c] == 0 && agg.catSelf[c] == 0)
                continue;
            writeln(pad(CatNames[c], 10), rpad(ms(agg.catCrit[c]), 14),
                    rpad(pct(agg.catCrit[c], agg.wallTotal), 9), rpad(ms(agg.catSelf[c]), 14));
        }

        writefln("\nTop %s labels by critical path time", o.top);
        writeln(pad("label", 52), rpad("crit ms", 12), rpad("% wall", 8), rpad("gain ms", 12),
                rpad("self ms", 12), rpad("calls", 8), rpad("err", 5));
        foreach (kv; rankedLabels(agg).take(o.top))
        {
            auto v = kv.value;
            if (v.critNs == 0)
                break;
            writeln(pad(kv.key.length > 50 ? kv.key[0 .. 50] : kv.key, 52), rpad(ms(v.critNs), 12),
                    rpad(pct(v.critNs, agg.wallTotal), 8), rpad(ms(v.gainNs), 12),
                    rpad(ms(v.selfNs), 12), rpad(v.count.to!string, 8), rpad(v.errors.to!string, 5));
        }
        writeln("gain ms is the most the label could save if it alone got faster, before a parallel sibling takes over.");

        auto slow = agg.results.dup;
        slow.sort!((a, b) => a.wallNs > b.wallNs || (a.wallNs == b.wallNs && a.traceId < b.traceId));
        writefln("\nSlowest %s traces", min(o.slowTraces, slow.length));
        foreach (tr; slow.take(o.slowTraces))
        {
            writefln("%s  %s ms  %s (%s)  %s spans", tr.traceId, ms(tr.wallNs), tr.rootName,
                    tr.rootService.length ? tr.rootService : "unknown service", tr.spans);
            foreach (sg; tr.top)
                writefln("    %s%s ms  %s%%  gain %s ms  [%s]", pad(sg.label.length > 46 ? sg.label[0 .. 46] : sg.label, 48),
                        ms(sg.critNs), pct(sg.critNs, tr.wallNs), ms(sg.gainNs), CatNames[sg.cat]);
        }
    }
    auto w = warnings(st, agg);
    if (w.length)
    {
        writeln("\nWarnings");
        foreach (line; w)
            writeln("  ", line);
    }
    foreach (g; gateFails)
        writeln("GATE FAIL: ", g);
}

JSONValue buildJson(Store st, Agg agg, const ref Options o, string[] gateFails)
{
    auto sorted = agg.walls.dup.sort().release;
    JSONValue[string] root;
    root["version"] = JSONValue(1);
    root["traces"] = JSONValue(cast(ulong) agg.results.length);
    root["spansRead"] = JSONValue(st.spanCount);
    root["wallNs"] = JSONValue(agg.wallTotal);
    root["exclusiveNs"] = JSONValue(agg.selfTotal);
    root["parallelism"] = JSONValue(agg.wallTotal ? agg.selfTotal / cast(double) agg.wallTotal : 0.0);
    JSONValue[string] pcs;
    pcs["p50"] = JSONValue(percentile(sorted, 50));
    pcs["p95"] = JSONValue(percentile(sorted, 95));
    pcs["p99"] = JSONValue(percentile(sorted, 99));
    pcs["max"] = JSONValue(sorted.length ? sorted[$ - 1] : 0UL);
    root["traceWallNs"] = JSONValue(pcs);

    JSONValue[] cats;
    foreach (c; 0 .. CatNames.length)
    {
        JSONValue[string] row;
        row["category"] = JSONValue(CatNames[c]);
        row["criticalNs"] = JSONValue(agg.catCrit[c]);
        row["exclusiveNs"] = JSONValue(agg.catSelf[c]);
        cats ~= JSONValue(row);
    }
    root["categories"] = JSONValue(cats);

    JSONValue[] labels;
    foreach (kv; rankedLabels(agg).take(o.top))
    {
        JSONValue[string] row;
        row["label"] = JSONValue(kv.key);
        row["category"] = JSONValue(CatNames[kv.value.cat]);
        row["criticalNs"] = JSONValue(kv.value.critNs);
        row["gainNs"] = JSONValue(kv.value.gainNs);
        row["exclusiveNs"] = JSONValue(kv.value.selfNs);
        row["calls"] = JSONValue(kv.value.count);
        row["criticalCalls"] = JSONValue(kv.value.critCount);
        row["errors"] = JSONValue(kv.value.errors);
        labels ~= JSONValue(row);
    }
    root["labels"] = JSONValue(labels);

    auto slow = agg.results.dup;
    slow.sort!((a, b) => a.wallNs > b.wallNs || (a.wallNs == b.wallNs && a.traceId < b.traceId));
    JSONValue[] slowJson;
    foreach (tr; slow.take(o.slowTraces))
    {
        JSONValue[string] row;
        row["traceId"] = JSONValue(tr.traceId);
        row["root"] = JSONValue(tr.rootName);
        row["service"] = JSONValue(tr.rootService);
        row["wallNs"] = JSONValue(tr.wallNs);
        row["spans"] = JSONValue(cast(ulong) tr.spans);
        row["unattributedSpans"] = JSONValue(cast(ulong) tr.unattributed);
        JSONValue[] segs;
        foreach (sg; tr.top)
        {
            JSONValue[string] sr;
            sr["label"] = JSONValue(sg.label);
            sr["category"] = JSONValue(CatNames[sg.cat]);
            sr["criticalNs"] = JSONValue(sg.critNs);
            sr["gainNs"] = JSONValue(sg.gainNs);
            segs ~= JSONValue(sr);
        }
        row["criticalPath"] = JSONValue(segs);
        slowJson ~= JSONValue(row);
    }
    root["slowestTraces"] = JSONValue(slowJson);

    JSONValue[] warn;
    foreach (line; warnings(st, agg))
        warn ~= JSONValue(line);
    root["warnings"] = JSONValue(warn);
    JSONValue[] gates;
    foreach (g; gateFails)
        gates ~= JSONValue(g);
    root["gateFailures"] = JSONValue(gates);
    return JSONValue(root);
}

int run(string[] args)
{
    Options o;
    double maxDocMb = DefaultMaxDocBytes / (1024.0 * 1024.0);
    size_t maxSpans = DefaultMaxSpans;
    bool help;
    try
    {
        auto parsed = getopt(args,
                "json", "Print a JSON report instead of text", &o.json,
                "top", "Labels to list (default 15)", &o.top,
                "traces", "Slowest traces to list (default 5)", &o.slowTraces,
                "root", "Only traces whose root span name contains this text", &o.rootContains,
                "min-ms", "Ignore traces shorter than this", &o.minMs,
                "max-ms", "Exit 2 if any trace is slower than this", &o.maxMs,
                "adopt-orphans", "Attach spans with a missing parent to the root", &o.adoptOrphans,
                "fail-on-warnings", "Exit 3 if the input had any warning", &o.failOnWarnings,
                "max-doc-mb", "Largest single JSON document to parse (default 64)", &maxDocMb,
                "max-spans", "Stop reading after this many spans (default 5000000)", &maxSpans);
        help = parsed.helpWanted;
        if (help)
        {
            defaultGetoptPrinter("Usage: OTelAgentCriticalPath [options] [file ...]  (no file or - reads stdin)",
                    parsed.options);
            return ExitOk;
        }
    }
    catch (Exception e)
    {
        stderr.writeln("error: ", e.msg);
        return ExitUsage;
    }
    if (maxDocMb <= 0 || maxSpans == 0)
    {
        stderr.writeln("error: --max-doc-mb and --max-spans must be positive");
        return ExitUsage;
    }
    o.maxDocBytes = cast(size_t)(maxDocMb * 1024 * 1024);
    o.maxSpans = maxSpans;

    auto st = new Store;
    st.maxSpans = o.maxSpans;
    auto files = args.length > 1 ? args[1 .. $] : ["-"];
    foreach (path; files)
    {
        try
        {
            if (path == "-")
                st.ingest(stdin, o.maxDocBytes);
            else
                st.ingest(File(path, "rb"), o.maxDocBytes);
        }
        catch (Exception e)
        {
            stderr.writeln("error: cannot read ", path, ": ", e.msg);
            return ExitUsage;
        }
        if (st.capHit)
            break;
    }

    auto agg = new Agg;
    foreach (id; st.traces.keys.sort())
        analyzeTrace(id, st.traces[id], o, agg);

    string[] gateFails;
    if (o.maxMs > 0)
    {
        immutable limit = cast(ulong)(o.maxMs * 1e6);
        foreach (ref tr; agg.results)
            if (tr.wallNs > limit)
                gateFails ~= format("trace %s took %s ms, limit is %s ms", tr.traceId, ms(tr.wallNs), ms(limit));
    }

    if (o.json)
        writeln(buildJson(st, agg, o, gateFails).toPrettyString());
    else
        printText(st, agg, o, gateFails);

    if (gateFails.length)
        return ExitGate;
    if (o.failOnWarnings && warnings(st, agg).length)
        return ExitWarnings;
    return ExitOk;
}

version (unittest)
{
    Span mk(string id, string parent, ulong s, ulong e, string name = "x")
    {
        Span sp;
        sp.traceId = "t1";
        sp.spanId = id;
        sp.parentId = parent;
        sp.start = s;
        sp.end = e;
        sp.name = name;
        return sp;
    }

    ulong critOf(Agg a, string label)
    {
        auto p = label in a.labels;
        return p ? p.critNs : 0;
    }

    unittest // splitter ignores braces inside strings and handles pretty plus JSONL
    {
        DocSplitter sp;
        string[] got;
        auto text = "{\"a\":\"}{\"}\n{\"b\":\n[1,2]}\nnoise [3]";
        sp.feed(cast(const(ubyte)[]) text, (const(char)[] d) { got ~= d.idup; });
        assert(got == ["{\"a\":\"}{\"}", "{\"b\":\n[1,2]}", "[3]"]);
        assert(sp.garbage == 1);
        assert(!sp.midDocument);
    }

    unittest // sequential children split the root exactly
    {
        Options o;
        auto a = new Agg;
        auto spans = [mk("a", "", 0, 100, "root"), mk("b", "a", 10, 40, "b"), mk("c", "a", 50, 90, "c")];
        assert(analyzeTrace("t1", spans, o, a));
        assert(a.accountingErrors == 0);
        assert(critOf(a, "root") == 30);
        assert(critOf(a, "b") == 30);
        assert(critOf(a, "c") == 40);
    }

    unittest // parallel children: only the slower one is critical and headroom is the lead over the other
    {
        Options o;
        auto a = new Agg;
        auto spans = [mk("a", "", 0, 100, "root"), mk("b", "a", 0, 90, "slow"), mk("c", "a", 0, 60, "fast")];
        assert(analyzeTrace("t1", spans, o, a));
        assert(critOf(a, "slow") == 90);
        assert(critOf(a, "fast") == 0);
        assert(a.labels["slow"].gainNs == 30);
        assert(a.catSelf[Cat.other] == 160);
    }

    unittest // clock skew is clamped and counted, accounting stays exact
    {
        Options o;
        auto a = new Agg;
        auto spans = [mk("a", "", 100, 200, "root"), mk("b", "a", 50, 250, "wide")];
        assert(analyzeTrace("t1", spans, o, a));
        assert(a.skewedSpans == 1);
        assert(a.accountingErrors == 0);
        assert(critOf(a, "wide") == 100);
    }

    unittest // orphans, duplicates and cycles never crash and are reported
    {
        Options o;
        auto a = new Agg;
        auto spans = [mk("a", "", 0, 100, "root"), mk("a", "", 0, 5, "dupe"), mk("o", "gone", 10, 20, "orphan"),
            mk("x", "y", 0, 10, "cx"), mk("y", "x", 0, 10, "cy")];
        assert(analyzeTrace("t1", spans, o, a));
        assert(a.dupSpans == 1);
        assert(a.orphanSpans == 1);
        assert(a.cycleSpans == 2);
        assert(a.accountingErrors == 0);
        auto b = new Agg;
        o.adoptOrphans = true;
        assert(analyzeTrace("t1", spans, o, b));
        assert(critOf(b, "orphan") == 10);
    }

    unittest // gen_ai attributes drive categories and labels
    {
        Span s = mk("a", "", 0, 1, "chat claude");
        s.op = "chat";
        s.model = "some-model";
        string label;
        assert(classify(s, label) == Cat.llm && label == "llm:some-model");
        Span t = mk("b", "", 0, 1, "run");
        t.op = "execute_tool";
        t.tool = "search";
        assert(classify(t, label) == Cat.tool && label == "tool:search");
    }

    void main()
    {
    }
}
else
{
    int main(string[] args)
    {
        return run(args);
    }
}
