module FlameGraphRegressionAttributor;

import std.algorithm;
import std.array;
import std.ascii : isDigit, isAlphaNum, isHexDigit, isAlpha;
import std.conv;
import std.format;
import std.getopt;
import std.json;
import std.math;
import std.mathspecial : betaIncomplete, erfc, normalDistributionInverse;
import std.range : iota, repeat;
import std.stdio;
import std.string;

enum string SchemaId = "flamegraph-regression-attribution/1";
enum string ToolName = "FlameGraphRegressionAttributor";
enum size_t DefaultMaxNodes = 4_000_000;
enum size_t MaxStackDepth = 4096;
enum size_t DefaultTop = 15;
enum size_t PathTailFrames = 5;
enum size_t MaxFrameDisplay = 72;
enum size_t MaxGateReasons = 8;
enum size_t MinWelchReplicates = 3;
enum size_t MinFramesForDispersion = 5;
enum double DefaultMinDeltaPts = 0.5;
enum double DefaultAlpha = 0.05;
enum double DefaultDesignEffect = 2.0;
enum double MaxDesignEffect = 1000.0;
enum double MinExpectedForDispersion = 20.0;
enum double DefaultMinSamples = 2000.0;
enum double SymbolOverlapWarn = 0.5;
enum double UnknownShareWarn = 0.10;
enum double ShiftRatio = 0.5;
enum double PowerZ = 0.8416212335729143;
enum string UnknownFrame = "[unknown]";
enum string RootLabel = "(all)";
enum uint NoFrame = uint.max;
enum ubyte FlagExclude = 1;
enum ubyte FlagOnly = 2;
enum int ExitOk = 0;
enum int ExitUsage = 1;
enum int ExitGate = 2;
enum int ExitWarnings = 3;

enum Mode { share, perOp, absolute }
enum WeightKind { samples, values }
enum Kind { self, subtree, remainder }

immutable string[] ModeNames = ["share", "per-op", "absolute"];
immutable string[] KindNames = ["self", "subtree", "rest"];

class InputError : Exception
{
    this(string msg) { super(msg); }
}

class UsageError : Exception
{
    this(string msg) { super(msg); }
}

struct Options
{
    string[] baseFiles;
    string[] headFiles;
    double[] baseOps;
    double[] headOps;
    Mode mode = Mode.share;
    WeightKind weights = WeightKind.samples;
    double minDeltaPts = DefaultMinDeltaPts;
    double alpha = DefaultAlpha;
    double designEffect = 0;
    double minSamples = DefaultMinSamples;
    double maxRegressionPts = double.nan;
    double maxTotalRegressionPct = double.nan;
    size_t top = DefaultTop;
    size_t maxNodes = DefaultMaxNodes;
    bool json;
    bool strict;
    bool fullPaths;
    bool collapseRecursion;
    bool collapseGenerics;
    bool stripAnnotations;
    bool mergeThreads;
    bool keepLines;
    bool keepAddresses;
    string[] skipFrames;
    string[] excludeStacks;
    string[] onlyStacks;
}

struct LoadStats
{
    string file;
    size_t lines;
    size_t stacks;
    size_t malformed;
    size_t firstBadLine;
    size_t truncated;
    double keptWeight = 0;
    double excludedWeight = 0;
}

struct Cmp
{
    double base = 0;
    double head = 0;
    double delta = 0;
    double p = double.nan;
}

struct Culprit
{
    uint node;
    Kind kind;
    Cmp c;
    double q = double.nan;
    bool shift;
}

struct FrameRow
{
    uint frame;
    Cmp self;
    Cmp incl;
    double qSelf = double.nan;
    double qIncl = double.nan;
    size_t paths;
    uint topCaller = NoFrame;
    double topCallerShare = 0;
}

struct Report
{
    Mode mode;
    WeightKind weights;
    string testName;
    double designEffect = 1;
    string designEffectSource;
    double alpha;
    double minDeltaPts;
    size_t baseProfiles;
    size_t headProfiles;
    double baseWeight = 0;
    double headWeight = 0;
    double baseTotal = 0;
    double headTotal = 0;
    Cmp total;
    double mdeSmall = double.nan;
    double mdeLarge = double.nan;
    size_t nodes;
    size_t frames;
    Culprit[] culprits;
    string[][] culpritPaths;
    double residual = 0;
    FrameRow[] selfRows;
    FrameRow[] inclRows;
    string[] frameNames;
    string[] warnings;
    string[] gateReasons;
    LoadStats[] loads;
}

bool wildcardMatch(const(char)[] text, const(char)[] pattern) pure nothrow @nogc @safe
{
    size_t t = 0, p = 0;
    size_t starP = size_t.max, starT = 0;
    while (t < text.length)
    {
        if (p < pattern.length && (pattern[p] == '?' || pattern[p] == text[t]))
        {
            t++;
            p++;
        }
        else if (p < pattern.length && pattern[p] == '*')
        {
            starP = p++;
            starT = t;
        }
        else if (starP != size_t.max)
        {
            p = starP + 1;
            t = ++starT;
        }
        else
        {
            return false;
        }
    }
    while (p < pattern.length && pattern[p] == '*')
        p++;
    return p == pattern.length;
}

bool matchesAny(const(char)[] text, const(string)[] patterns) pure nothrow @nogc @safe
{
    foreach (pat; patterns)
        if (wildcardMatch(text, pat))
            return true;
    return false;
}

bool allHex(const(char)[] s) pure nothrow @nogc @safe
{
    if (s.length == 0)
        return false;
    foreach (ch; s)
        if (!isHexDigit(ch))
            return false;
    return true;
}

bool isIdentChar(char ch) pure nothrow @nogc @safe
{
    return isAlphaNum(ch) || ch == '_';
}

char[] stripOffset(char[] b) pure @safe
{
    auto idx = b.lastIndexOf("+0x");
    if (idx > 0 && allHex(b[idx + 3 .. $]))
        return b[0 .. idx];
    return b;
}

bool isBareAddress(const(char)[] b) pure @safe
{
    if (b == "??" || b == "[unknown]" || b == "<unknown>" || b == "unknown" || b == "[unresolved]")
        return true;
    if (b.length > 2 && b[0] == '0' && (b[1] == 'x' || b[1] == 'X'))
        return allHex(b[2 .. $]);
    if (b.length >= 8 && allHex(b))
        return b.any!(ch => isDigit(ch));
    return false;
}

char[] stripRustHash(char[] b) pure @safe
{
    if (b.length > 19 && b[$ - 19 .. $ - 16] == "::h" && allHex(b[$ - 16 .. $]))
        return b[0 .. $ - 19];
    return b;
}

char[] maskHex(char[] b) pure @safe
{
    if (b.indexOf("0x") < 0)
        return b;
    char[] o;
    o.reserve(b.length);
    size_t i = 0;
    while (i < b.length)
    {
        if (b[i] == '0' && i + 1 < b.length && b[i + 1] == 'x' && (i == 0 || !isIdentChar(b[i - 1])))
        {
            size_t j = i + 2;
            while (j < b.length && isHexDigit(b[j]))
                j++;
            if (j - (i + 2) >= 4)
            {
                o ~= "0x?";
                i = j;
                continue;
            }
        }
        o ~= b[i++];
    }
    return o;
}

char[] maskDigitsAfter(char[] b, string marker) pure @safe
{
    if (b.indexOf(marker) < 0)
        return b;
    char[] o;
    o.reserve(b.length);
    size_t i = 0;
    while (i < b.length)
    {
        if (i + marker.length <= b.length && b[i .. i + marker.length] == marker)
        {
            o ~= marker;
            i += marker.length;
            size_t j = i;
            while (j < b.length && isDigit(b[j]))
                j++;
            if (j > i)
            {
                o ~= 'N';
                i = j;
            }
            continue;
        }
        o ~= b[i++];
    }
    return o;
}

char[] stripLineNumbers(char[] b) pure @safe
{
    for (;;)
    {
        bool paren = b.length > 0 && b[$ - 1] == ')';
        size_t e = paren ? b.length - 1 : b.length;
        size_t s = e;
        while (s > 0 && isDigit(b[s - 1]))
            s--;
        if (s == e || s < 2 || b[s - 1] != ':' || b[s - 2] == ':')
            return b;
        b = paren ? b[0 .. s - 1] ~ ")" : b[0 .. s - 1];
    }
}

char[] stripAnnotation(char[] b) pure @safe
{
    if (b.length > 4 && b[$ - 4 .. $ - 2] == "_[" && b[$ - 1] == ']' && isAlpha(b[$ - 2]))
        return b[0 .. $ - 4];
    return b;
}

char[] collapseGenericArgs(char[] b) pure @safe
{
    if (b.indexOf('<') < 0)
        return b;
    char[] o;
    o.reserve(b.length);
    size_t i = 0;
    while (i < b.length)
    {
        if (b[i] == '<' && o.length > 0 && isIdentChar(o[$ - 1]) && !o.endsWith("operator"))
        {
            size_t depth = 0;
            size_t j = i;
            bool closed = false;
            for (; j < b.length; j++)
            {
                if (b[j] == '<')
                    depth++;
                else if (b[j] == '>' && !(j > 0 && b[j - 1] == '-'))
                {
                    depth--;
                    if (depth == 0)
                    {
                        closed = true;
                        break;
                    }
                }
            }
            if (closed)
            {
                o ~= "<>";
                i = j + 1;
                continue;
            }
        }
        o ~= b[i++];
    }
    return o;
}

char[] mergeThreadName(char[] b) pure @safe
{
    auto tid = b.indexOf(" tid=");
    if (tid > 0)
    {
        size_t j = tid + 5;
        while (j < b.length && isDigit(b[j]))
            j++;
        b = b[0 .. tid] ~ b[j .. $];
    }
    char[] o;
    o.reserve(b.length);
    size_t i = 0;
    while (i < b.length)
    {
        if (isDigit(b[i]))
        {
            while (i < b.length && isDigit(b[i]))
                i++;
            o ~= '#';
            continue;
        }
        o ~= b[i++];
    }
    return o;
}

string normalizeFrame(const(char)[] raw, ref const Options opt, bool isRootFrame) pure @safe
{
    char[] b = raw.strip.dup;
    if (b.length == 0)
        return "";
    if (!opt.keepAddresses)
    {
        b = stripOffset(b);
        if (isBareAddress(b))
            return UnknownFrame;
        b = stripRustHash(b);
        b = maskHex(b);
        b = maskDigitsAfter(b, "Lambda$");
        b = maskDigitsAfter(b, "GeneratedMethodAccessor");
        b = maskDigitsAfter(b, "GeneratedConstructorAccessor");
    }
    if (opt.stripAnnotations)
        b = stripAnnotation(b);
    if (!opt.keepLines)
        b = stripLineNumbers(b);
    if (opt.collapseGenerics)
        b = collapseGenericArgs(b);
    if (opt.mergeThreads && isRootFrame)
        b = mergeThreadName(b);
    b = b.strip;
    return b.idup;
}

bool parseFoldedLine(const(char)[] line, out const(char)[] stack, out double weight) @safe
{
    ptrdiff_t idx = -1;
    foreach_reverse (i, ch; line)
    {
        if (ch == ' ' || ch == '\t')
        {
            idx = i;
            break;
        }
    }
    if (idx < 1 || idx + 1 >= line.length)
        return false;
    auto tok = line[idx + 1 .. $];
    if (!(isDigit(tok[0]) || tok[0] == '.' || tok[0] == '+'))
        return false;
    try
        weight = to!double(tok);
    catch (Exception)
        return false;
    if (!isFinite(weight) || weight < 0)
        return false;
    stack = line[0 .. idx].stripRight;
    return stack.length > 0;
}

struct Tree
{
    size_t reps;
    size_t maxNodes;
    string[] frameNames;
    uint[string] frameIds;
    uint[] nodeFrame;
    uint[] nodeParent;
    uint[ulong] childIndex;
    double[] selfRaw;

    this(size_t reps, size_t maxNodes)
    {
        this.reps = reps;
        this.maxNodes = maxNodes;
        nodeFrame ~= internFrame(RootLabel);
        nodeParent ~= 0;
        selfRaw.length = reps;
        selfRaw[] = 0;
    }

    @property size_t nodeCount() const { return nodeFrame.length; }

    uint internFrame(string name)
    {
        if (auto p = name in frameIds)
            return *p;
        auto id = cast(uint) frameNames.length;
        frameNames ~= name;
        frameIds[name] = id;
        return id;
    }

    void addStack(const(uint)[] frames, size_t rep, double w)
    {
        uint node = 0;
        foreach (fid; frames)
        {
            ulong key = (cast(ulong) node << 32) | fid;
            if (auto p = key in childIndex)
            {
                node = *p;
                continue;
            }
            if (nodeFrame.length >= maxNodes)
                throw new InputError(format("call tree exceeds %s nodes. Raise --max-nodes, enable "
                        ~ "--collapse-recursion or filter with --only-stack.", maxNodes));
            auto id = cast(uint) nodeFrame.length;
            nodeFrame ~= fid;
            nodeParent ~= node;
            selfRaw.length += reps;
            selfRaw[$ - reps .. $] = 0;
            childIndex[key] = id;
            node = id;
        }
        selfRaw[node * reps + rep] += w;
    }

    string[] pathOf(uint node) const
    {
        string[] rev;
        while (node != 0)
        {
            rev ~= frameNames[nodeFrame[node]];
            node = nodeParent[node];
        }
        rev.reverse();
        return rev;
    }
}

struct Ingestor
{
    Tree* tree;
    const(Options)* opt;
    uint[string] rawCache;
    uint[string] rawRootCache;
    ubyte[] frameFlags;
    uint[] buf;

    uint resolve(const(char)[] raw, bool isRoot)
    {
        auto cache = (isRoot && opt.mergeThreads) ? &rawRootCache : &rawCache;
        if (auto p = cast(string) raw in *cache)
            return *p;
        auto name = normalizeFrame(raw, *opt, isRoot);
        uint id = NoFrame;
        if (name.length && !matchesAny(name, opt.skipFrames))
        {
            id = tree.internFrame(name);
            if (frameFlags.length <= id)
                frameFlags.length = id + 1;
            ubyte fl = 0;
            if (matchesAny(name, opt.excludeStacks))
                fl |= FlagExclude;
            if (matchesAny(name, opt.onlyStacks))
                fl |= FlagOnly;
            frameFlags[id] = fl;
        }
        (*cache)[raw.idup] = id;
        return id;
    }

    void ingest(Lines)(Lines lines, size_t rep, ref LoadStats st)
    {
        bool firstLine = true;
        foreach (rawLine; lines)
        {
            st.lines++;
            const(char)[] line = rawLine;
            if (firstLine)
            {
                firstLine = false;
                if (line.length >= 3 && line[0] == '\xEF' && line[1] == '\xBB' && line[2] == '\xBF')
                    line = line[3 .. $];
            }
            line = line.strip;
            if (line.length == 0 || line[0] == '#')
                continue;
            const(char)[] stack;
            double w;
            if (!parseFoldedLine(line, stack, w))
            {
                st.malformed++;
                if (st.firstBadLine == 0)
                    st.firstBadLine = st.lines;
                continue;
            }
            if (w == 0)
                continue;
            buf.length = 0;
            buf.assumeSafeAppend();
            bool excluded = false;
            bool wanted = opt.onlyStacks.length == 0;
            bool isRoot = true;
            foreach (part; stack.splitter(';'))
            {
                auto trimmed = part.strip;
                if (trimmed.length == 0)
                    continue;
                auto id = resolve(trimmed, isRoot);
                isRoot = false;
                if (id == NoFrame)
                    continue;
                auto fl = frameFlags[id];
                if (fl & FlagExclude)
                    excluded = true;
                if (fl & FlagOnly)
                    wanted = true;
                if (opt.collapseRecursion && buf.length && buf[$ - 1] == id)
                    continue;
                if (buf.length >= MaxStackDepth)
                {
                    st.truncated++;
                    break;
                }
                buf ~= id;
            }
            if (buf.length == 0)
            {
                st.malformed++;
                if (st.firstBadLine == 0)
                    st.firstBadLine = st.lines;
                continue;
            }
            if (excluded || !wanted)
            {
                st.excludedWeight += w;
                continue;
            }
            tree.addStack(buf, rep, w);
            st.keptWeight += w;
            st.stacks++;
        }
    }
}

struct Design
{
    size_t nb;
    size_t nh;
    double[] denom;
    double eb = 0;
    double eh = 0;
    bool share;
    bool samples;
    bool welch;
    double deff = 1;

    void cost(const(double)[] raw, out double b, out double h) const pure nothrow @nogc @safe
    {
        double xb = 0, xh = 0;
        foreach (r; 0 .. nb)
            xb += raw[r];
        foreach (r; nb .. nb + nh)
            xh += raw[r];
        b = xb / eb;
        h = xh / eh;
    }

    private double sideVariance(const(double)[] raw, size_t lo, size_t n, double rate, double exposure) const pure nothrow @nogc @safe
    {
        double mean = 0;
        foreach (r; lo .. lo + n)
            mean += raw[r] / denom[r];
        mean /= n;
        double ss = 0;
        foreach (r; lo .. lo + n)
        {
            auto d = raw[r] / denom[r] - mean;
            ss += d * d;
        }
        double v = ss / (n - 1);
        if (samples)
        {
            double unit = share ? rate * (1 - rate) : rate;
            double floorVar = deff * unit / (exposure / n);
            if (floorVar > v)
                v = floorVar;
        }
        return v / n;
    }

    Cmp compare(const(double)[] raw) const @safe
    {
        Cmp c;
        cost(raw, c.base, c.head);
        c.delta = c.head - c.base;
        if (welch)
        {
            double vb = sideVariance(raw, 0, nb, c.base, eb);
            double vh = sideVariance(raw, nb, nh, c.head, eh);
            double se2 = vb + vh;
            if (se2 <= 0)
            {
                c.p = c.delta == 0 ? 1.0 : double.nan;
                return c;
            }
            double mb = 0, mh = 0;
            foreach (r; 0 .. nb)
                mb += raw[r] / denom[r];
            foreach (r; nb .. nb + nh)
                mh += raw[r] / denom[r];
            double t = (mh / nh - mb / nb) / sqrt(se2);
            double dfDen = vb * vb / (nb - 1) + vh * vh / (nh - 1);
            double df = dfDen > 0 ? se2 * se2 / dfDen : cast(double)(nb + nh - 2);
            c.p = studentTwoSided(t, df);
            return c;
        }
        if (!samples)
            return c;
        double xb = c.base * eb, xh = c.head * eh;
        double pooled = (xb + xh) / (eb + eh);
        double unit = share ? pooled * (1 - pooled) : pooled;
        double var = deff * unit * (1 / eb + 1 / eh);
        if (var <= 0)
        {
            c.p = 1.0;
            return c;
        }
        c.p = normalTwoSided(c.delta / sqrt(var));
        return c;
    }
}

double normalTwoSided(double z) pure nothrow @nogc @safe
{
    return cast(double) erfc(fabs(z) / SQRT2);
}

double studentTwoSided(double t, double df) @safe
{
    if (!isFinite(t))
        return 0;
    if (df <= 0 || isNaN(df))
        return double.nan;
    double x = df / (df + t * t);
    return cast(double) betaIncomplete(df / 2, 0.5, x);
}

double[] benjaminiHochberg(const(double)[] p) @safe
{
    auto q = new double[p.length];
    q[] = double.nan;
    size_t[] idx;
    foreach (i, v; p)
        if (!isNaN(v))
            idx ~= i;
    idx.sort!((a, b) => p[a] < p[b]);
    double running = 1.0;
    foreach_reverse (k, i; idx)
    {
        double adj = p[i] * idx.length / (k + 1);
        if (adj < running)
            running = adj;
        q[i] = running;
    }
    return q;
}

double estimateDesignEffect(ref const Design d, const(double)[] frameIncl, size_t frames, size_t reps, out size_t used) @safe
{
    double chi = 0;
    double dof = 0;
    used = 0;
    void side(size_t lo, size_t n)
    {
        if (n < 2)
            return;
        double e = 0;
        foreach (r; lo .. lo + n)
            e += d.denom[r];
        foreach (f; 1 .. frames)
        {
            auto raw = frameIncl[f * reps .. f * reps + reps];
            double x = 0;
            foreach (r; lo .. lo + n)
                x += raw[r];
            double rate = x / e;
            double unit = d.share ? (1 - rate) : 1.0;
            if (unit <= 0.05)
                continue;
            double c = 0;
            bool ok = true;
            foreach (r; lo .. lo + n)
            {
                double expected = d.denom[r] * rate;
                if (expected < MinExpectedForDispersion)
                {
                    ok = false;
                    break;
                }
                double diff = raw[r] - expected;
                c += diff * diff / (expected * unit);
            }
            if (!ok)
                continue;
            chi += c;
            dof += n - 1;
            used++;
        }
    }
    side(0, d.nb);
    side(d.nb, d.nh);
    if (used < MinFramesForDispersion || dof <= 0)
        return double.nan;
    return min(MaxDesignEffect, max(1.0, chi / dof));
}

Report analyze(ref Tree tree, ref const Options opt, LoadStats[] loads)
{
    immutable R = tree.reps;
    immutable N = tree.nodeCount;
    immutable F = tree.frameNames.length;
    immutable nb = opt.baseFiles.length;
    immutable nh = opt.headFiles.length;
    assert(nb + nh == R);

    Report rep;
    rep.mode = opt.mode;
    rep.weights = opt.weights;
    rep.alpha = opt.alpha;
    rep.minDeltaPts = opt.minDeltaPts;
    rep.baseProfiles = nb;
    rep.headProfiles = nh;
    rep.loads = loads;
    rep.nodes = N - 1;
    rep.frames = F - 1;
    rep.frameNames = tree.frameNames;

    auto totRaw = tree.selfRaw.dup;
    foreach_reverse (i; 1 .. N)
    {
        auto par = tree.nodeParent[i];
        totRaw[par * R .. par * R + R] += totRaw[i * R .. i * R + R];
    }

    Design d;
    d.nb = nb;
    d.nh = nh;
    d.share = opt.mode == Mode.share;
    d.samples = opt.weights == WeightKind.samples;
    d.welch = nb >= MinWelchReplicates && nh >= MinWelchReplicates;
    d.denom = new double[R];
    foreach (r; 0 .. R)
    {
        double rootW = totRaw[r];
        if (!(rootW > 0))
            throw new InputError(format("%s has no usable stacks after filtering", loads[r].file));
        final switch (opt.mode)
        {
        case Mode.share:
            d.denom[r] = rootW;
            break;
        case Mode.perOp:
            d.denom[r] = r < nb ? opt.baseOps[r] : opt.headOps[r - nb];
            break;
        case Mode.absolute:
            d.denom[r] = 1;
            break;
        }
        if (r < nb)
        {
            d.eb += d.denom[r];
            rep.baseWeight += rootW;
        }
        else
        {
            d.eh += d.denom[r];
            rep.headWeight += rootW;
        }
    }

    auto firstOnPath = new bool[N];
    {
        auto active = new uint[F];
        auto firstChild = new uint[N];
        auto nextSibling = new uint[N];
        foreach_reverse (i; 1 .. N)
        {
            auto par = tree.nodeParent[i];
            nextSibling[i] = firstChild[par];
            firstChild[par] = cast(uint) i;
        }
        long[] stack;
        stack ~= 0;
        while (stack.length)
        {
            long top = stack[$ - 1];
            stack.length--;
            stack.assumeSafeAppend();
            if (top < 0)
            {
                active[tree.nodeFrame[cast(size_t)(~top)]]--;
                continue;
            }
            auto n = cast(uint) top;
            auto f = tree.nodeFrame[n];
            firstOnPath[n] = active[f] == 0;
            active[f]++;
            stack ~= ~top;
            for (uint c = firstChild[n]; c != 0; c = nextSibling[c])
                stack ~= c;
        }
    }

    auto frameSelf = new double[F * R];
    auto frameIncl = new double[F * R];
    auto framePaths = new size_t[F];
    frameSelf[] = 0;
    frameIncl[] = 0;
    foreach (i; 1 .. N)
    {
        auto f = tree.nodeFrame[i];
        framePaths[f]++;
        frameSelf[f * R .. f * R + R] += tree.selfRaw[i * R .. i * R + R];
        if (firstOnPath[i])
            frameIncl[f * R .. f * R + R] += totRaw[i * R .. i * R + R];
    }

    if (opt.designEffect > 0)
    {
        d.deff = opt.designEffect;
        rep.designEffectSource = "set by --design-effect";
    }
    else if (d.samples)
    {
        size_t used;
        double est = estimateDesignEffect(d, frameIncl, F, R, used);
        if (!isNaN(est))
        {
            d.deff = est;
            rep.designEffectSource = format("estimated from replicate spread across %s frames", used);
        }
        else
        {
            d.deff = DefaultDesignEffect;
            rep.designEffectSource = "default, no replicates to estimate it from";
        }
    }
    rep.designEffect = d.deff;
    if (d.welch)
        rep.testName = d.samples ? "Welch t test across replicates with a sampling variance floor"
            : "Welch t test across replicates";
    else if (d.samples)
        rep.testName = d.share ? "pooled two proportion z test" : "pooled Poisson rate z test";
    else
        rep.testName = "none";

    rep.total = d.compare(totRaw[0 .. R]);
    rep.baseTotal = rep.total.base;
    rep.headTotal = rep.total.head;
    immutable baseTotal = rep.baseTotal;
    double pts(double v) { return v / baseTotal * 100.0; }

    immutable thr = opt.minDeltaPts / 100.0 * baseTotal;
    double famThr = thr;
    if (!isNaN(opt.maxRegressionPts))
        famThr = min(famThr, opt.maxRegressionPts / 100.0 * baseTotal);

    if (d.samples && !d.welch)
    {
        double zsum = cast(double) normalDistributionInverse(1 - opt.alpha / 2) + PowerZ;
        double mde(double frac)
        {
            double unit = d.share ? frac * (1 - frac) : frac * baseTotal;
            double sd = sqrt(d.deff * unit * (1 / d.eb + 1 / d.eh));
            return pts(zsum * sd);
        }
        rep.mdeSmall = mde(0.01);
        rep.mdeLarge = mde(0.10);
    }

    auto nodeBaseTot = new double[N];
    auto nodeHeadTot = new double[N];
    auto nodeBaseSelf = new double[N];
    auto nodeHeadSelf = new double[N];
    foreach (i; 0 .. N)
    {
        d.cost(totRaw[i * R .. i * R + R], nodeBaseTot[i], nodeHeadTot[i]);
        d.cost(tree.selfRaw[i * R .. i * R + R], nodeBaseSelf[i], nodeHeadSelf[i]);
    }
    auto maxAbs = new double[N];
    foreach (i; 0 .. N)
        maxAbs[i] = max(fabs(nodeHeadTot[i] - nodeBaseTot[i]), fabs(nodeHeadSelf[i] - nodeBaseSelf[i]));
    foreach_reverse (i; 1 .. N)
    {
        auto par = tree.nodeParent[i];
        if (maxAbs[i] > maxAbs[par])
            maxAbs[par] = maxAbs[i];
    }

    double[] famP;
    int[uint] idxTot;
    int[uint] idxSelf;
    foreach (i; 1 .. N)
    {
        if (nodeBaseTot[i] + nodeHeadTot[i] < thr)
            continue;
        idxTot[cast(uint) i] = cast(int) famP.length;
        famP ~= d.compare(totRaw[i * R .. i * R + R]).p;
        if (nodeBaseSelf[i] + nodeHeadSelf[i] >= thr)
        {
            idxSelf[cast(uint) i] = cast(int) famP.length;
            famP ~= d.compare(tree.selfRaw[i * R .. i * R + R]).p;
        }
    }
    auto famQ = benjaminiHochberg(famP);
    immutable famSize = famP.count!(v => !isNaN(v)) + 1;

    {
        uint[][uint] kidsOf;
        foreach (i; 1 .. N)
            if (maxAbs[i] >= thr)
                kidsOf[tree.nodeParent[i]] ~= cast(uint) i;
        uint[] work = [0];
        auto leftRaw = new double[R];
        while (work.length)
        {
            auto n = work[$ - 1];
            work.length--;
            work.assumeSafeAppend();
            uint[] kids;
            if (auto pk = n in kidsOf)
                kids = *pk;
            double selfDelta = nodeHeadSelf[n] - nodeBaseSelf[n];
            bool selfBig = n != 0 && fabs(selfDelta) >= thr;
            leftRaw[] = totRaw[n * R .. n * R + R];
            foreach (k; kids)
                leftRaw[] -= totRaw[k * R .. k * R + R];
            if (selfBig)
                leftRaw[] -= tree.selfRaw[n * R .. n * R + R];
            foreach (ref v; leftRaw)
                if (v < 0)
                    v = 0;
            auto par = tree.nodeParent[n];
            double parSelfDelta = (n != 0 && par != 0) ? nodeHeadSelf[par] - nodeBaseSelf[par] : 0.0;
            bool offsetByCaller(double amount)
            {
                return parSelfDelta * amount < 0 && fabs(parSelfDelta) >= ShiftRatio * fabs(amount);
            }
            if (selfBig)
            {
                Culprit c;
                c.node = n;
                c.kind = Kind.self;
                c.c = d.compare(tree.selfRaw[n * R .. n * R + R]);
                c.q = famQ[idxSelf[n]];
                double opposing = 0;
                foreach (k; kids)
                {
                    double kd = nodeHeadTot[k] - nodeBaseTot[k];
                    if (kd * selfDelta < 0)
                        opposing += fabs(kd);
                }
                c.shift = opposing >= ShiftRatio * fabs(selfDelta) || offsetByCaller(selfDelta);
                rep.culprits ~= c;
            }
            auto left = d.compare(leftRaw);
            if (fabs(left.delta) >= thr)
            {
                Culprit c;
                c.node = n;
                c.c = left;
                if (kids.length == 0 && !selfBig && n != 0)
                {
                    c.kind = Kind.subtree;
                    c.q = famQ[idxTot[n]];
                    c.shift = offsetByCaller(left.delta);
                }
                else
                {
                    c.kind = Kind.remainder;
                    c.q = isNaN(left.p) ? double.nan : min(1.0, left.p * famSize);
                    c.shift = !selfBig && offsetByCaller(left.delta);
                }
                rep.culprits ~= c;
            }
            else
            {
                rep.residual += left.delta;
            }
            work ~= kids;
        }
    }
    rep.culprits.sort!((a, b) => fabs(a.c.delta) > fabs(b.c.delta));
    foreach (ref c; rep.culprits)
        rep.culpritPaths ~= tree.pathOf(c.node);

    FrameRow[] rows;
    double[] frameP;
    foreach (f; 1 .. F)
    {
        FrameRow row;
        row.frame = cast(uint) f;
        row.paths = framePaths[f];
        double sb, sh, ib, ih;
        d.cost(frameSelf[f * R .. f * R + R], sb, sh);
        d.cost(frameIncl[f * R .. f * R + R], ib, ih);
        if (ib + ih < famThr)
            continue;
        row.incl = d.compare(frameIncl[f * R .. f * R + R]);
        if (sb + sh >= famThr)
            row.self = d.compare(frameSelf[f * R .. f * R + R]);
        else
            row.self = Cmp(sb, sh, sh - sb, double.nan);
        frameP ~= row.incl.p;
        frameP ~= (sb + sh >= famThr) ? row.self.p : double.nan;
        rows ~= row;
    }
    auto frameQ = benjaminiHochberg(frameP);
    foreach (i, ref row; rows)
    {
        row.qIncl = frameQ[2 * i];
        row.qSelf = frameQ[2 * i + 1];
    }

    bool passes(double q) { return isNaN(q) || q <= opt.alpha; }

    if (!isNaN(opt.maxRegressionPts))
    {
        auto offenders = rows.filter!(r => pts(r.incl.delta) >= opt.maxRegressionPts && passes(r.qIncl)).array;
        offenders.sort!((a, b) => a.incl.delta > b.incl.delta);
        foreach (i, ref row; offenders)
        {
            if (i == MaxGateReasons)
            {
                rep.gateReasons ~= format("%s more frames over the limit", offenders.length - MaxGateReasons);
                break;
            }
            rep.gateReasons ~= format("frame %s rose by %+.2f pts inclusive, limit %.2f",
                    shorten(tree.frameNames[row.frame]), pts(row.incl.delta), opt.maxRegressionPts);
        }
    }
    if (!isNaN(opt.maxTotalRegressionPct))
    {
        double pct = rep.total.delta / baseTotal * 100.0;
        if (pct >= opt.maxTotalRegressionPct && (isNaN(rep.total.p) || rep.total.p <= opt.alpha))
            rep.gateReasons ~= format("total cost rose by %+.2f%%, limit %.2f%%", pct, opt.maxTotalRegressionPct);
    }

    auto bySelf = rows.filter!(r => fabs(r.self.delta) >= thr).array;
    bySelf.sort!((a, b) => fabs(a.self.delta) > fabs(b.self.delta));
    if (bySelf.length > opt.top)
        bySelf.length = opt.top;
    auto byIncl = rows.filter!(r => fabs(r.incl.delta) >= thr).array;
    byIncl.sort!((a, b) => fabs(a.incl.delta) > fabs(b.incl.delta));
    if (byIncl.length > opt.top)
        byIncl.length = opt.top;

    if (bySelf.length)
    {
        size_t[uint] slot;
        foreach (i, ref row; bySelf)
            slot[row.frame] = i;
        auto callers = new double[uint][bySelf.length];
        foreach (i; 1 .. N)
        {
            auto ps = tree.nodeFrame[i] in slot;
            if (ps is null)
                continue;
            double delta = nodeHeadSelf[i] - nodeBaseSelf[i];
            if (delta == 0)
                continue;
            auto par = tree.nodeParent[i];
            uint caller = par == 0 ? 0 : tree.nodeFrame[par];
            callers[*ps][caller] = callers[*ps].get(caller, 0.0) + delta;
        }
        foreach (i, ref row; bySelf)
        {
            double best = 0;
            foreach (caller, delta; callers[i])
            {
                if (delta * row.self.delta > 0 && fabs(delta) > fabs(best))
                {
                    best = delta;
                    row.topCaller = caller;
                }
            }
            if (row.topCaller != NoFrame)
                row.topCallerShare = min(1.0, best / row.self.delta);
        }
    }
    rep.selfRows = bySelf;
    rep.inclRows = byIncl;

    foreach (ref st; loads)
    {
        if (st.malformed)
            rep.warnings ~= format("%s: %s malformed lines skipped, first at line %s", st.file, st.malformed, st.firstBadLine);
        if (st.truncated)
            rep.warnings ~= format("%s: %s stacks deeper than %s frames were truncated", st.file, st.truncated, MaxStackDepth);
    }
    if (d.samples)
    {
        if (rep.baseWeight < opt.minSamples)
            rep.warnings ~= format("base has only %.0f samples, below --min-samples %.0f. Small shifts cannot be detected.",
                    rep.baseWeight, opt.minSamples);
        if (rep.headWeight < opt.minSamples)
            rep.warnings ~= format("head has only %.0f samples, below --min-samples %.0f. Small shifts cannot be detected.",
                    rep.headWeight, opt.minSamples);
    }
    else if (!d.welch)
    {
        rep.warnings ~= format("weights are values and fewer than %s profiles per side were given, so no significance "
                ~ "test is possible. Rows and gates use effect size only.", MinWelchReplicates);
    }
    {
        double headKnown = 0, baseKnown = 0, unkBase = 0, unkHead = 0;
        foreach (f; 1 .. F)
        {
            double sb, sh, ib, ih;
            d.cost(frameSelf[f * R .. f * R + R], sb, sh);
            d.cost(frameIncl[f * R .. f * R + R], ib, ih);
            if (ib > 0)
                headKnown += sh;
            if (ih > 0)
                baseKnown += sb;
            if (tree.frameNames[f] == UnknownFrame)
            {
                unkBase = sb;
                unkHead = sh;
            }
        }
        double headOverlap = rep.headTotal > 0 ? headKnown / rep.headTotal : 1;
        double baseOverlap = baseKnown / baseTotal;
        if (min(headOverlap, baseOverlap) < SymbolOverlapWarn)
            rep.warnings ~= format("only %.0f%% of head self time and %.0f%% of base self time sits in frames the other "
                    ~ "side also has. The profiles look symbolised differently, so attribution is unreliable.",
                    headOverlap * 100, baseOverlap * 100);
        double unk = max(unkBase / baseTotal, rep.headTotal > 0 ? unkHead / rep.headTotal : 0);
        if (unk > UnknownShareWarn)
            rep.warnings ~= format("%.0f%% of self time is in unresolved frames on one side. Fix symbolisation first.", unk * 100);
    }
    return rep;
}

string shorten(string s)
{
    if (s.length <= MaxFrameDisplay)
        return s;
    size_t cut = MaxFrameDisplay - 3;
    while (cut > 0 && (s[cut] & 0xC0) == 0x80)
        cut--;
    return s[0 .. cut] ~ "...";
}

string renderPath(const(string)[] path, bool full)
{
    if (path.length == 0)
        return RootLabel;
    if (full || path.length <= PathTailFrames)
        return path.map!(f => full ? f : shorten(f)).join(";");
    immutable hidden = path.length - PathTailFrames;
    return format("(%s frame%s);", hidden, hidden == 1 ? "" : "s")
        ~ path[$ - PathTailFrames .. $].map!shorten.join(";");
}

string fmtQ(double q)
{
    if (isNaN(q))
        return "n/a";
    if (q < 0.001)
        return "<0.001";
    return format("%.3f", q);
}

void renderText(ref const Report rep, ref const Options opt)
{
    immutable bt = rep.baseTotal;
    double pts(double v) { return v / bt * 100.0; }
    writeln("Flame graph regression attribution");
    writefln("  base  %s profile%s, weight %.0f", rep.baseProfiles, rep.baseProfiles == 1 ? "" : "s", rep.baseWeight);
    writefln("  head  %s profile%s, weight %.0f", rep.headProfiles, rep.headProfiles == 1 ? "" : "s", rep.headWeight);
    writefln("  tree  %s call paths, %s distinct frames", rep.nodes, rep.frames);
    final switch (rep.mode)
    {
    case Mode.share:
        writeln("  mode  share: each side is normalised to its own total, deltas are percentage points of the profile.");
        writeln("        A uniform slowdown is invisible here and a speedup elsewhere inflates every other share.");
        break;
    case Mode.perOp:
        writeln("  mode  per-op: weight divided by operation count, deltas are percent of base cost per operation");
        break;
    case Mode.absolute:
        writeln("  mode  absolute: raw weight per profile, deltas are percent of base total weight");
        break;
    }
    if (rep.testName == "none")
        writeln("  test  none");
    else if (rep.weights == WeightKind.samples)
        writefln("  test  %s, design effect %.2f (%s), FDR %.2g", rep.testName, rep.designEffect,
                rep.designEffectSource, rep.alpha);
    else
        writefln("  test  %s, FDR %.2g", rep.testName, rep.alpha);
    if (!isNaN(rep.mdeSmall))
        writefln("  power smallest shift detectable 80%% of the time: %.2f pts on a 1%% frame, %.2f pts on a 10%% frame",
                rep.mdeSmall, rep.mdeLarge);
    writeln();
    if (rep.mode == Mode.share)
        writeln("Total cost: not comparable in share mode, both sides sum to 100 by construction");
    else
        writefln("Total cost: base %.6g, head %.6g, change %+.2f%%, p %s", rep.baseTotal, rep.headTotal,
                pts(rep.total.delta), fmtQ(rep.total.p));
    writeln();
    writefln("Call path culprits (rows of at least %.2f pts, rows plus the unlisted remainder add up to the total change)",
            rep.minDeltaPts);
    if (rep.culprits.length == 0)
        writeln("  none");
    else
    {
        writeln("    delta    base    head       q  kind     path");
        size_t shown = 0;
        foreach (i, ref c; rep.culprits)
        {
            if (shown++ >= opt.top)
            {
                writefln("  ... %s more rows, raise --top to see them", rep.culprits.length - opt.top);
                break;
            }
            string tags;
            if (c.c.base == 0)
                tags ~= " [new]";
            if (c.c.head == 0)
                tags ~= " [gone]";
            if (c.shift)
                tags ~= " [shift]";
            writefln("  %+7.2f %7.2f %7.2f %7s  %-7s  %s%s", pts(c.c.delta), pts(c.c.base), pts(c.c.head), fmtQ(c.q),
                    KindNames[c.kind], renderPath(rep.culpritPaths[i], opt.fullPaths), tags);
        }
    }
    writefln("  unlisted remainder: %+.2f pts spread over paths below the threshold", pts(rep.residual));
    writeln();
    writeln("Frames by self time change (self deltas across all frames add up to the total change)");
    if (rep.selfRows.length == 0)
        writeln("  none");
    else
    {
        writeln("    delta    base    head       q  paths  frame");
        foreach (ref r; rep.selfRows)
        {
            string via;
            if (r.topCaller != NoFrame && r.paths > 1)
                via = format("  <- %.0f%% via %s", r.topCallerShare * 100,
                        r.topCaller == 0 ? "(root)" : shorten(rep.frameNames[r.topCaller]));
            writefln("  %+7.2f %7.2f %7.2f %7s %6s  %s%s", pts(r.self.delta), pts(r.self.base), pts(r.self.head),
                    fmtQ(r.qSelf), r.paths, shorten(rep.frameNames[r.frame]), via);
        }
    }
    writeln();
    writeln("Frames by inclusive time change (each stack counted once per frame, rows overlap along a call chain)");
    if (rep.inclRows.length == 0)
        writeln("  none");
    else
    {
        writeln("    delta    base    head       q  paths  frame");
        foreach (ref r; rep.inclRows)
            writefln("  %+7.2f %7.2f %7.2f %7s %6s  %s", pts(r.incl.delta), pts(r.incl.base), pts(r.incl.head),
                    fmtQ(r.qIncl), r.paths, shorten(rep.frameNames[r.frame]));
    }
    if (rep.warnings.length)
    {
        writeln();
        writeln("Warnings");
        foreach (w; rep.warnings)
            writefln("  - %s", w);
    }
    if (!isNaN(opt.maxRegressionPts) || !isNaN(opt.maxTotalRegressionPct))
    {
        writeln();
        if (rep.gateReasons.length == 0)
            writeln("Gate: pass");
        else
        {
            writeln("Gate: FAIL");
            foreach (g; rep.gateReasons)
                writefln("  - %s", g);
        }
    }
}

JSONValue jnum(double v)
{
    return isFinite(v) ? JSONValue(v) : JSONValue(null);
}

JSONValue cmpJson(ref const Cmp c, double q, double bt)
{
    JSONValue o = JSONValue.emptyObject;
    o["base"] = jnum(c.base);
    o["head"] = jnum(c.head);
    o["basePts"] = jnum(c.base / bt * 100);
    o["headPts"] = jnum(c.head / bt * 100);
    o["deltaPts"] = jnum(c.delta / bt * 100);
    o["p"] = jnum(c.p);
    o["q"] = jnum(q);
    return o;
}

void renderJson(ref const Report rep, ref const Options opt)
{
    immutable bt = rep.baseTotal;
    JSONValue root = JSONValue.emptyObject;
    root["schema"] = SchemaId;
    root["mode"] = ModeNames[rep.mode];
    root["weights"] = rep.weights == WeightKind.samples ? "samples" : "values";
    root["test"] = rep.testName;
    root["designEffect"] = jnum(rep.designEffect);
    root["designEffectSource"] = rep.designEffectSource;
    root["alpha"] = rep.alpha;
    root["minDeltaPts"] = rep.minDeltaPts;
    root["callPaths"] = rep.nodes;
    root["distinctFrames"] = rep.frames;
    JSONValue sideJson(size_t profiles, double weight, double total)
    {
        JSONValue o = JSONValue.emptyObject;
        o["profiles"] = profiles;
        o["weight"] = jnum(weight);
        o["cost"] = jnum(total);
        return o;
    }
    root["base"] = sideJson(rep.baseProfiles, rep.baseWeight, rep.baseTotal);
    root["head"] = sideJson(rep.headProfiles, rep.headWeight, rep.headTotal);
    JSONValue total = JSONValue.emptyObject;
    total["comparable"] = rep.mode != Mode.share;
    total["deltaPct"] = rep.mode == Mode.share ? JSONValue(null) : jnum(rep.total.delta / bt * 100);
    total["p"] = rep.mode == Mode.share ? JSONValue(null) : jnum(rep.total.p);
    root["total"] = total;
    JSONValue power = JSONValue.emptyObject;
    power["detectablePtsOnOnePercentFrame"] = jnum(rep.mdeSmall);
    power["detectablePtsOnTenPercentFrame"] = jnum(rep.mdeLarge);
    root["power"] = power;

    JSONValue[] culprits;
    foreach (i, ref c; rep.culprits)
    {
        JSONValue o = cmpJson(c.c, c.q, bt);
        o["kind"] = KindNames[c.kind];
        o["path"] = JSONValue(rep.culpritPaths[i].dup);
        o["isNew"] = c.c.base == 0;
        o["isGone"] = c.c.head == 0;
        o["shift"] = c.shift;
        culprits ~= o;
    }
    root["culprits"] = JSONValue(culprits);
    root["residualPts"] = jnum(rep.residual / bt * 100);

    JSONValue frameJson(ref const FrameRow r)
    {
        JSONValue o = JSONValue.emptyObject;
        o["frame"] = rep.frameNames[r.frame];
        o["paths"] = r.paths;
        o["self"] = cmpJson(r.self, r.qSelf, bt);
        o["inclusive"] = cmpJson(r.incl, r.qIncl, bt);
        if (r.topCaller != NoFrame)
        {
            o["topCaller"] = r.topCaller == 0 ? "(root)" : rep.frameNames[r.topCaller];
            o["topCallerShare"] = jnum(r.topCallerShare);
        }
        return o;
    }
    JSONValue[] selfRows, inclRows;
    foreach (ref r; rep.selfRows)
        selfRows ~= frameJson(r);
    foreach (ref r; rep.inclRows)
        inclRows ~= frameJson(r);
    root["framesBySelf"] = JSONValue(selfRows);
    root["framesByInclusive"] = JSONValue(inclRows);
    root["warnings"] = JSONValue(rep.warnings.dup);
    JSONValue gate = JSONValue.emptyObject;
    gate["configured"] = !isNaN(opt.maxRegressionPts) || !isNaN(opt.maxTotalRegressionPct);
    gate["failed"] = rep.gateReasons.length > 0;
    gate["reasons"] = JSONValue(rep.gateReasons.dup);
    root["gate"] = gate;
    JSONValue[] inputs;
    foreach (i, ref st; rep.loads)
    {
        JSONValue o = JSONValue.emptyObject;
        o["file"] = st.file;
        o["side"] = i < rep.baseProfiles ? "base" : "head";
        o["lines"] = st.lines;
        o["stacks"] = st.stacks;
        o["malformed"] = st.malformed;
        o["truncated"] = st.truncated;
        o["keptWeight"] = jnum(st.keptWeight);
        o["excludedWeight"] = jnum(st.excludedWeight);
        inputs ~= o;
    }
    root["inputs"] = JSONValue(inputs);
    writeln(root.toPrettyString(JSONOptions.doNotEscapeSlashes));
}

immutable string UsageText = `Usage:
  FlameGraphRegressionAttributor --base FILE [--base FILE ...] --head FILE [--head FILE ...] [options]

Inputs are folded stacks, one "frame;frame;frame weight" per line. Use - to read one side from stdin.
Give several --base and --head files when you have repeated runs. Three or more per side switches
the test from sample counting to a Welch t test across runs.

Normalisation
  --mode share|per-op|absolute   share (default) compares the shape of the profile.
                                 per-op divides weight by --base-ops and --head-ops.
                                 absolute compares raw weight per profile.
  --base-ops N[,N...]            operations behind each base profile, one value or one per file
  --head-ops N[,N...]            operations behind each head profile
  --weights samples|values       samples (default) are counts and get a count based test.
                                 values are nanoseconds, bytes and the like.

Statistics
  --min-delta PTS                smallest change worth a row, default 0.5
  --alpha Q                      false discovery rate, default 0.05
  --design-effect auto|N         variance inflation for correlated samples, default auto
  --min-samples N                warn when a side has fewer samples, default 2000

Frame clean up
  --collapse-recursion           fold a;a;a into a
  --collapse-generics            fold Foo<Bar<int>> into Foo<>
  --strip-annotations            drop the _[j] _[i] _[k] _[w] suffixes
  --merge-threads                mask numbers in the first frame so thread ids do not split stacks
  --keep-lines                   keep :LINE suffixes
  --keep-addresses               keep +0x offsets, raw addresses, Rust hashes and lambda ids
  --skip-frame GLOB              remove matching frames from every stack, repeatable
  --exclude-stack GLOB           drop stacks containing a matching frame, repeatable
  --only-stack GLOB              keep only stacks containing a matching frame, repeatable

Output and gating
  --top N                        rows per table, default 15
  --full-paths                   print whole call paths
  --json                         machine readable report
  --max-regression PTS           exit 2 when a frame's inclusive cost rises by PTS or more
  --max-total-regression PCT     exit 2 when total cost rises by PCT or more, not valid in share mode
  --strict                       exit 3 when there are warnings
  --max-nodes N                  call tree size limit, default 4000000

Exit codes: 0 ok, 1 usage or input error, 2 gate failed, 3 warnings under --strict.`;

double[] parseOps(string arg, string flag)
{
    double[] vals;
    foreach (tok; arg.split(","))
    {
        double v;
        try
            v = to!double(tok.strip);
        catch (Exception)
            throw new UsageError(format("%s expects numbers, got '%s'", flag, tok));
        if (!isFinite(v) || v <= 0)
            throw new UsageError(format("%s values must be positive, got '%s'", flag, tok));
        vals ~= v;
    }
    return vals;
}

double[] spreadOps(double[] ops, size_t files, string flag)
{
    if (ops.length == files)
        return ops;
    if (ops.length == 1)
        return ops[0].repeat(files).array;
    throw new UsageError(format("%s has %s values for %s files. Give one value or one per file.", flag, ops.length, files));
}

Options parseArgs(ref string[] args, out bool helpWanted)
{
    Options opt;
    string modeArg, weightsArg, deffArg = "auto", baseOpsArg, headOpsArg;
    try
    {
        auto res = getopt(args,
                std.getopt.config.caseSensitive,
                "base", &opt.baseFiles,
                "head", &opt.headFiles,
                "mode", &modeArg,
                "base-ops", &baseOpsArg,
                "head-ops", &headOpsArg,
                "weights", &weightsArg,
                "min-delta", &opt.minDeltaPts,
                "alpha", &opt.alpha,
                "design-effect", &deffArg,
                "min-samples", &opt.minSamples,
                "collapse-recursion", &opt.collapseRecursion,
                "collapse-generics", &opt.collapseGenerics,
                "strip-annotations", &opt.stripAnnotations,
                "merge-threads", &opt.mergeThreads,
                "keep-lines", &opt.keepLines,
                "keep-addresses", &opt.keepAddresses,
                "skip-frame", &opt.skipFrames,
                "exclude-stack", &opt.excludeStacks,
                "only-stack", &opt.onlyStacks,
                "top", &opt.top,
                "full-paths", &opt.fullPaths,
                "json", &opt.json,
                "max-regression", &opt.maxRegressionPts,
                "max-total-regression", &opt.maxTotalRegressionPct,
                "strict", &opt.strict,
                "max-nodes", &opt.maxNodes);
        helpWanted = res.helpWanted;
    }
    catch (Exception e)
    {
        throw new UsageError(e.msg);
    }
    if (helpWanted)
        return opt;
    if (args.length > 1)
        throw new UsageError(format("unexpected argument '%s'. Profiles go behind --base and --head.", args[1]));
    if (opt.baseFiles.length == 0 || opt.headFiles.length == 0)
        throw new UsageError("at least one --base and one --head profile are required");
    if (opt.baseFiles.count("-") + opt.headFiles.count("-") > 1)
        throw new UsageError("stdin can stand in for one profile only");
    if (baseOpsArg.length)
        opt.baseOps = parseOps(baseOpsArg, "--base-ops");
    if (headOpsArg.length)
        opt.headOps = parseOps(headOpsArg, "--head-ops");
    switch (modeArg)
    {
    case "":
        opt.mode = (opt.baseOps.length || opt.headOps.length) ? Mode.perOp : Mode.share;
        break;
    case "share":
        opt.mode = Mode.share;
        break;
    case "per-op":
        opt.mode = Mode.perOp;
        break;
    case "absolute":
        opt.mode = Mode.absolute;
        break;
    default:
        throw new UsageError(format("unknown --mode '%s'", modeArg));
    }
    if (opt.mode == Mode.perOp)
    {
        if (opt.baseOps.length == 0 || opt.headOps.length == 0)
            throw new UsageError("per-op mode needs both --base-ops and --head-ops");
        opt.baseOps = spreadOps(opt.baseOps, opt.baseFiles.length, "--base-ops");
        opt.headOps = spreadOps(opt.headOps, opt.headFiles.length, "--head-ops");
    }
    else if (opt.baseOps.length || opt.headOps.length)
    {
        throw new UsageError("--base-ops and --head-ops only apply to --mode per-op");
    }
    switch (weightsArg)
    {
    case "", "samples":
        opt.weights = WeightKind.samples;
        break;
    case "values":
        opt.weights = WeightKind.values;
        break;
    default:
        throw new UsageError(format("unknown --weights '%s'", weightsArg));
    }
    if (deffArg != "auto")
    {
        try
            opt.designEffect = to!double(deffArg);
        catch (Exception)
            throw new UsageError("--design-effect expects auto or a number");
        if (!(opt.designEffect >= 1) || opt.designEffect > MaxDesignEffect)
            throw new UsageError(format("--design-effect must be between 1 and %.0f", MaxDesignEffect));
        if (opt.weights == WeightKind.values)
            throw new UsageError("--design-effect has no meaning with --weights values");
    }
    if (!(opt.minDeltaPts > 0))
        throw new UsageError("--min-delta must be positive");
    if (!(opt.alpha > 0 && opt.alpha < 1))
        throw new UsageError("--alpha must be between 0 and 1");
    if (opt.top == 0)
        throw new UsageError("--top must be at least 1");
    if (opt.maxNodes < 2)
        throw new UsageError("--max-nodes must be at least 2");
    if (!isNaN(opt.maxRegressionPts) && !(opt.maxRegressionPts > 0))
        throw new UsageError("--max-regression must be positive");
    if (!isNaN(opt.maxTotalRegressionPct))
    {
        if (!(opt.maxTotalRegressionPct > 0))
            throw new UsageError("--max-total-regression must be positive");
        if (opt.mode == Mode.share)
            throw new UsageError("--max-total-regression needs --mode per-op or absolute. Share mode normalises "
                    ~ "both sides to the same total, so a total change is invisible.");
    }
    return opt;
}

Report run(ref const Options opt)
{
    auto files = opt.baseFiles ~ opt.headFiles;
    auto tree = Tree(files.length, opt.maxNodes);
    Ingestor ing;
    ing.tree = &tree;
    ing.opt = &opt;
    ing.frameFlags.length = 1;
    auto loads = new LoadStats[files.length];
    foreach (r, path; files)
    {
        loads[r].file = path == "-" ? "(stdin)" : path;
        File f;
        if (path == "-")
            f = stdin;
        else
        {
            try
                f = File(path, "r");
            catch (Exception e)
                throw new InputError(format("cannot open %s: %s", path, e.msg));
        }
        try
            ing.ingest(f.byLine, r, loads[r]);
        catch (InputError e)
            throw e;
        catch (Exception e)
            throw new InputError(format("cannot read %s: %s", path, e.msg));
        if (loads[r].stacks == 0)
            throw new InputError(format("%s holds no usable folded stacks (%s lines, %s malformed, weight %.0f filtered out). "
                    ~ "Expected lines like 'main;work;leaf 42'.", loads[r].file, loads[r].lines, loads[r].malformed,
                    loads[r].excludedWeight));
    }
    return analyze(tree, opt, loads);
}

version (unittest)
{
    Report analyzeLines(Options opt, string[][] base, string[][] head)
    {
        opt.baseFiles = base.length.iota.map!(i => format("base%s", i)).array;
        opt.headFiles = head.length.iota.map!(i => format("head%s", i)).array;
        auto tree = Tree(base.length + head.length, opt.maxNodes);
        Ingestor ing;
        ing.tree = &tree;
        ing.opt = &opt;
        ing.frameFlags.length = 1;
        auto loads = new LoadStats[base.length + head.length];
        foreach (i, lines; base ~ head)
        {
            loads[i].file = i < base.length ? opt.baseFiles[i] : opt.headFiles[i - base.length];
            ing.ingest(lines, i, loads[i]);
        }
        return analyze(tree, opt, loads);
    }
}

unittest
{
    const(char)[] stack;
    double w;
    assert(parseFoldedLine("main;work;leaf 42", stack, w) && stack == "main;work;leaf" && w == 42);
    assert(parseFoldedLine("main;operator new 7", stack, w) && stack == "main;operator new" && w == 7);
    assert(parseFoldedLine("a;b\t1.5", stack, w) && w == 1.5);
    assert(!parseFoldedLine("no_weight_here", stack, w));
    assert(!parseFoldedLine("a;b -3", stack, w));
    assert(!parseFoldedLine("a;b nan", stack, w));
    assert(!parseFoldedLine("a;b 12abc", stack, w));
}

unittest
{
    Options o;
    assert(normalizeFrame("  do_work+0x1f  ", o, false) == "do_work");
    assert(normalizeFrame("0x00007f3a12345678", o, false) == UnknownFrame);
    assert(normalizeFrame("7f3a12345678", o, false) == UnknownFrame);
    assert(normalizeFrame("deadbeef", o, false) == "deadbeef");
    assert(normalizeFrame("core::ptr::drop_in_place::h0123456789abcdef", o, false) == "core::ptr::drop_in_place");
    assert(normalizeFrame("handler (app/views.py:123)", o, false) == "handler (app/views.py)");
    assert(normalizeFrame("render app.js:10:5", o, false) == "render app.js");
    assert(normalizeFrame("Foo$$Lambda$123/0x0000000800c0a1b8.run", o, false) == "Foo$$Lambda$N/0x?.run");
    assert(normalizeFrame("std::foo::bar", o, false) == "std::foo::bar");
    assert(normalizeFrame("compute_[j]", o, false) == "compute_[j]");
    o.stripAnnotations = true;
    assert(normalizeFrame("compute_[j]", o, false) == "compute");
    o.collapseGenerics = true;
    assert(normalizeFrame("std::vector<std::pair<int, long>>::push_back", o, false) == "std::vector<>::push_back");
    assert(normalizeFrame("Foo.<init>", o, false) == "Foo.<init>");
    assert(normalizeFrame("operator<", o, false) == "operator<");
    assert(normalizeFrame("<module>", o, false) == "<module>");
    o.mergeThreads = true;
    assert(normalizeFrame("pool-1-thread-7", o, true) == "pool-#-thread-#");
    assert(normalizeFrame("[worker tid=4411]", o, true) == "[worker]");
    assert(normalizeFrame("pool-1-thread-7", o, false) == "pool-1-thread-7");
    o.keepLines = true;
    assert(normalizeFrame("Foo.bar:42", o, false) == "Foo.bar:42");
}

unittest
{
    assert(wildcardMatch("cpu_idle", "cpu_*"));
    assert(wildcardMatch("abc", "a?c"));
    assert(wildcardMatch("java.util.HashMap.get", "*HashMap*"));
    assert(!wildcardMatch("abc", "a?d"));
    assert(wildcardMatch("", "*"));
    assert(!wildcardMatch("x", ""));
}

unittest
{
    auto q = benjaminiHochberg([0.01, 0.04, double.nan, 0.03, 0.5]);
    assert(isNaN(q[2]));
    assert(isClose(q[0], 0.04));
    assert(isClose(q[3], 0.04 * 4 / 3));
    assert(isClose(q[1], 0.04 * 4 / 3));
    assert(isClose(q[4], 0.5));
    assert(isClose(normalTwoSided(1.959964), 0.05, 1e-4));
    assert(isClose(studentTwoSided(2.228, 10), 0.05, 1e-3));
}

unittest
{
    Options o;
    auto base = ["main;serve;parse 3000", "main;serve;render 5000", "main;serve;render;memcpy 1000", "main;gc 1000"];
    auto head = ["main;serve;parse 5200", "main;serve;render 5000", "main;serve;render;memcpy 1000", "main;gc 1000"];
    auto rep = analyzeLines(o, [base], [head]);
    assert(rep.culprits.length >= 1);
    assert(rep.culpritPaths[0] == ["main", "serve", "parse"]);
    assert(rep.culprits[0].kind == Kind.self);
    assert(rep.culprits[0].q < 0.001);
    double sum = rep.residual;
    foreach (c; rep.culprits)
        sum += c.c.delta;
    assert(fabs(sum - rep.total.delta) < 1e-9);
    assert(rep.selfRows[0].frame == rep.frameNames.countUntil("parse"));
}

unittest
{
    Options o;
    o.mode = Mode.absolute;
    auto base = ["main;bar;foo 4000", "main;bar 1000", "main;other 5000"];
    auto head = ["main;bar 5000", "main;other 5000"];
    auto rep = analyzeLines(o, [base], [head]);
    assert(rep.culprits.length == 2);
    assert(rep.culprits.all!(c => c.shift));
    assert(fabs(rep.total.delta) < 1e-9);
    o.maxRegressionPts = 5;
    rep = analyzeLines(o, [base], [head]);
    assert(rep.gateReasons.length == 0);
}

unittest
{
    Options o;
    o.mode = Mode.perOp;
    o.baseOps = [1000.0];
    o.headOps = [1000.0];
    o.maxRegressionPts = 2;
    o.maxTotalRegressionPct = 3;
    string[] base, head;
    foreach (i; 0 .. 40)
    {
        base ~= format("main;handler%s;memcpy 100", i);
        base ~= format("main;handler%s 900", i);
        head ~= format("main;handler%s;memcpy 150", i);
        head ~= format("main;handler%s 900", i);
    }
    auto rep = analyzeLines(o, [base], [head]);
    assert(rep.selfRows.length == 1 && rep.frameNames[rep.selfRows[0].frame] == "memcpy");
    assert(rep.selfRows[0].paths == 40);
    assert(isClose(rep.total.delta / rep.baseTotal, 0.05));
    assert(rep.gateReasons.length >= 2);
}

unittest
{
    Options o;
    auto base = ["main;a;a;a;leaf 500", "main;b 500"];
    auto head = ["main;a;a;a;leaf 500", "main;b 500"];
    auto rep = analyzeLines(o, [base, base, base], [head, head, head]);
    assert(rep.culprits.length == 0);
    assert(rep.testName.startsWith("Welch"));
    foreach (r; rep.inclRows)
        assert(r.incl.base <= 1.0 + 1e-9);
    o.weights = WeightKind.values;
    rep = analyzeLines(o, [base], [head]);
    assert(rep.testName == "none");
    assert(rep.warnings.any!(w => w.canFind("no significance")));
}

int main(string[] args)
{
    Options opt;
    bool helpWanted;
    try
    {
        opt = parseArgs(args, helpWanted);
    }
    catch (UsageError e)
    {
        stderr.writefln("%s: %s", ToolName, e.msg);
        stderr.writeln("Run with --help for the full option list.");
        return ExitUsage;
    }
    if (helpWanted)
    {
        writeln(UsageText);
        return ExitOk;
    }
    Report rep;
    try
    {
        rep = run(opt);
    }
    catch (InputError e)
    {
        stderr.writefln("%s: %s", ToolName, e.msg);
        return ExitUsage;
    }
    if (opt.json)
        renderJson(rep, opt);
    else
        renderText(rep, opt);
    if (rep.gateReasons.length)
        return ExitGate;
    if (opt.strict && rep.warnings.length)
        return ExitWarnings;
    return ExitOk;
}
