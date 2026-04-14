import 'dart:async';
import 'dart:convert';

/// Production-grade incremental JSON extraction for streaming LLM responses.
///
/// Feed arbitrary text deltas from SSE, WebSocket, or chunked HTTP bodies and
/// receive only complete JSON values once they become syntactically valid.
class StructuredJsonStreamDecoder {
  StructuredJsonStreamDecoder({
    this.maxBufferedChars = 512 * 1024,
    this.maxDocumentChars = 256 * 1024,
    this.maxSkippedChars = 128 * 1024,
    this.allowTopLevelPrimitives = true,
    this.stripMarkdownCodeFences = true,
    this.requireStrictJson = true,
  })  : assert(maxBufferedChars > 0),
        assert(maxDocumentChars > 0),
        assert(maxSkippedChars >= 0);

  final int maxBufferedChars;
  final int maxDocumentChars;
  final int maxSkippedChars;
  final bool allowTopLevelPrimitives;
  final bool stripMarkdownCodeFences;
  final bool requireStrictJson;

  final StringBuffer _buffer = StringBuffer();

  int _cursor = 0;
  int _discardedChars = 0;
  int _documentCounter = 0;
  bool _closed = false;

  _ScanState? _active;
  final StringBuffer _noise = StringBuffer();

  int get bufferedChars => _buffer.length;
  bool get isInsideDocument => _active != null;
  int get discardedChars => _discardedChars;

  List<JsonStreamDocument> feed(String chunk) {
    if (_closed) {
      throw StateError('Cannot feed a closed StructuredJsonStreamDecoder.');
    }
    if (chunk.isEmpty) {
      return const <JsonStreamDocument>[];
    }

    _buffer.write(chunk);
    if (_buffer.length > maxBufferedChars) {
      throw JsonStreamDecodeException(
        'Buffered content exceeded $maxBufferedChars chars before a valid JSON '
        'document could be emitted.',
      );
    }

    final produced = <JsonStreamDocument>[];
    while (true) {
      final next = _consumeNextDocument();
      if (next == null) {
        break;
      }
      produced.add(next);
    }
    _compactBufferIfNeeded();
    return produced;
  }

  List<JsonStreamDocument> close() {
    _closed = true;
    final produced = <JsonStreamDocument>[];
    while (true) {
      final next = _consumeNextDocument(isFinalChunk: true);
      if (next == null) {
        break;
      }
      produced.add(next);
    }

    if (_active != null) {
      throw JsonStreamDecodeException(
        'Stream ended with an unfinished JSON document after ${_buffer.length - _active!.start} chars.',
      );
    }

    _cursor = _buffer.length;
    _compactBufferIfNeeded(force: true);
    return produced;
  }

  StreamTransformer<String, JsonStreamDocument> transformer() {
    return StreamTransformer<String, JsonStreamDocument>.fromHandlers(
      handleData: (chunk, sink) {
        try {
          for (final document in feed(chunk)) {
            sink.add(document);
          }
        } catch (error, stackTrace) {
          sink.addError(error, stackTrace);
        }
      },
      handleDone: (sink) {
        try {
          for (final document in close()) {
            sink.add(document);
          }
          sink.close();
        } catch (error, stackTrace) {
          sink.addError(error, stackTrace);
        }
      },
    );
  }

  JsonStreamDocument? _consumeNextDocument({bool isFinalChunk = false}) {
    final text = _buffer.toString();
    if (_cursor >= text.length) {
      return null;
    }

    while (_cursor < text.length) {
      if (_active == null) {
        final start = _findDocumentStart(text, _cursor);
        if (start == null) {
          _captureNoise(text.substring(_cursor));
          _cursor = text.length;
          return null;
        }

        if (start > _cursor) {
          _captureNoise(text.substring(_cursor, start));
        }
        _active = _ScanState.startingAt(text, start);
        _cursor = start;
      }

      final completion = _advanceActiveState(text, isFinalChunk: isFinalChunk);
      if (completion == null) {
        return null;
      }

      final rawJson = text.substring(_active!.start, completion.endExclusive);
      final prefixNoise = _takeNoise();
      final suffixNoise = completion.trailingNoise;
      _captureNoise('');
      _cursor = completion.endExclusive;
      final active = _active!;
      _active = null;

      final decoded = _decodeStrict(rawJson);
      final document = JsonStreamDocument(
        index: _documentCounter++,
        rawJson: rawJson,
        value: decoded,
        prefixNoise: prefixNoise,
        suffixNoise: suffixNoise,
        topLevelKind: active.topLevelKind,
      );

      if (suffixNoise.isNotEmpty) {
        _captureNoise(suffixNoise);
      }
      return document;
    }

    return null;
  }

  int? _findDocumentStart(String text, int from) {
    var index = from;
    while (index < text.length) {
      if (stripMarkdownCodeFences && _startsWithFence(text, index)) {
        index = _skipFenceLine(text, index);
        continue;
      }

      final rune = text.codeUnitAt(index);
      if (_isWhitespace(rune)) {
        index++;
        continue;
      }

      if (text.startsWith('json', index) || text.startsWith('JSON', index)) {
        final end = index + 4;
        if (end < text.length && _isWhitespace(text.codeUnitAt(end))) {
          _captureNoise(text.substring(index, end));
          index = end;
          continue;
        }
      }

      final kind = _TopLevelKindX.tryStart(text, index, allowTopLevelPrimitives);
      if (kind != null) {
        return index;
      }

      _captureNoise(String.fromCharCode(rune));
      index++;
      if (_noise.length > maxSkippedChars) {
        throw JsonStreamDecodeException(
          'Skipped more than $maxSkippedChars chars without finding JSON. The source is probably not streaming structured output.',
        );
      }
    }
    return null;
  }

  _DocumentCompletion? _advanceActiveState(String text, {required bool isFinalChunk}) {
    final state = _active!;
    while (_cursor < text.length) {
      final char = text[_cursor];
      state.consume(char);
      _cursor++;

      if (state.length > maxDocumentChars) {
        throw JsonStreamDecodeException(
          'A single JSON document exceeded $maxDocumentChars chars.',
        );
      }

      if (state.isComplete) {
        final endExclusive = _cursor;
        var trailingNoise = '';
        var probe = _cursor;
        while (probe < text.length) {
          if (stripMarkdownCodeFences && _startsWithFence(text, probe)) {
            final fenceEnd = _skipFenceLine(text, probe);
            trailingNoise += text.substring(probe, fenceEnd);
            probe = fenceEnd;
            continue;
          }
          final rune = text.codeUnitAt(probe);
          if (_isWhitespace(rune)) {
            trailingNoise += String.fromCharCode(rune);
            probe++;
            continue;
          }
          break;
        }
        _cursor = probe;
        return _DocumentCompletion(
          endExclusive: endExclusive,
          trailingNoise: trailingNoise,
        );
      }
    }

    if (isFinalChunk && state.canCloseAtEndOfInput) {
      final rawJson = text.substring(state.start);
      _decodeStrict(rawJson);
      return _DocumentCompletion(endExclusive: text.length, trailingNoise: '');
    }

    return null;
  }

  dynamic _decodeStrict(String rawJson) {
    try {
      final value = jsonDecode(rawJson);
      if (!allowTopLevelPrimitives &&
          value is! Map<String, dynamic> &&
          value is! List<dynamic>) {
        throw JsonStreamDecodeException(
          'Only top-level objects and arrays are allowed, but received ${value.runtimeType}.',
        );
      }
      return value;
    } on JsonStreamDecodeException {
      rethrow;
    } catch (error) {
      if (!requireStrictJson) {
        return rawJson;
      }
      throw JsonStreamDecodeException(
        'Decoded a candidate JSON boundary, but the payload was not strict JSON: $error',
      );
    }
  }

  void _compactBufferIfNeeded({bool force = false}) {
    if (!force && _cursor < 64 * 1024) {
      return;
    }
    final text = _buffer.toString();
    final keepFrom = _active?.start ?? _cursor;
    if (!force && keepFrom == 0) {
      return;
    }

    final remainder = keepFrom >= text.length ? '' : text.substring(keepFrom);
    _discardedChars += keepFrom;
    _buffer
      ..clear()
      ..write(remainder);

    if (_active != null) {
      _active = _active!.shiftedLeft(keepFrom);
      _cursor -= keepFrom;
    } else {
      _cursor = 0;
    }
  }

  void _captureNoise(String text) {
    if (text.isEmpty) {
      return;
    }
    _noise.write(text);
    if (_noise.length > maxSkippedChars) {
      final content = _takeNoise();
      final trimmed = content.length > 120 ? '${content.substring(0, 120)}...' : content;
      throw JsonStreamDecodeException(
        'Noise before structured output exceeded $maxSkippedChars chars. Recent prefix: ${trimmed.replaceAll('\n', r'\n')}',
      );
    }
  }

  String _takeNoise() {
    final text = _noise.toString();
    _noise.clear();
    return text;
  }

  bool _startsWithFence(String text, int index) {
    return index + 2 < text.length && text.startsWith('```', index);
  }

  int _skipFenceLine(String text, int from) {
    final newlineIndex = text.indexOf('\n', from);
    return newlineIndex == -1 ? text.length : newlineIndex + 1;
  }

  static bool _isWhitespace(int rune) {
    return rune == 0x20 || rune == 0x09 || rune == 0x0A || rune == 0x0D;
  }
}

class JsonStreamDocument {
  const JsonStreamDocument({
    required this.index,
    required this.rawJson,
    required this.value,
    required this.prefixNoise,
    required this.suffixNoise,
    required this.topLevelKind,
  });

  final int index;
  final String rawJson;
  final dynamic value;
  final String prefixNoise;
  final String suffixNoise;
  final JsonTopLevelKind topLevelKind;

  Map<String, dynamic> expectObject() {
    final current = value;
    if (current is Map<String, dynamic>) {
      return current;
    }
    if (current is Map) {
      return current.map((key, value) => MapEntry('$key', value));
    }
    throw StateError('Expected a JSON object, but got ${current.runtimeType}.');
  }

  List<dynamic> expectArray() {
    final current = value;
    if (current is List<dynamic>) {
      return current;
    }
    if (current is List) {
      return current.cast<dynamic>();
    }
    throw StateError('Expected a JSON array, but got ${current.runtimeType}.');
  }

  String asPrettyJson({String indent = '  '}) {
    const encoder = JsonEncoder.withIndent('  ');
    final rendered = encoder.convert(value);
    return indent == '  ' ? rendered : _retab(rendered, indent);
  }

  @override
  String toString() => rawJson;

  static String _retab(String input, String indent) {
    if (indent == '  ') {
      return input;
    }
    final lines = input.split('\n');
    return lines
        .map((line) {
          var level = 0;
          while (line.startsWith('  ' * (level + 1))) {
            level++;
          }
          if (level == 0) {
            return line;
          }
          return '${indent * level}${line.substring(level * 2)}';
        })
        .join('\n');
  }
}

enum JsonTopLevelKind {
  object,
  array,
  string,
  number,
  literal,
}

class JsonStreamDecodeException implements Exception {
  JsonStreamDecodeException(this.message);

  final String message;

  @override
  String toString() => 'JsonStreamDecodeException: $message';
}

class _DocumentCompletion {
  const _DocumentCompletion({
    required this.endExclusive,
    required this.trailingNoise,
  });

  final int endExclusive;
  final String trailingNoise;
}

class _ScanState {
  _ScanState({
    required this.start,
    required this.topLevelKind,
  });

  factory _ScanState.startingAt(String text, int start) {
    final kind = _TopLevelKindX.tryStart(text, start, true);
    if (kind == null) {
      throw StateError('No valid JSON start token at index $start.');
    }

    final state = _ScanState(start: start, topLevelKind: kind);
    if (kind == JsonTopLevelKind.object || kind == JsonTopLevelKind.array) {
      state.depth = 0;
    } else if (kind == JsonTopLevelKind.string) {
      state.awaitingStringClose = false;
    }
    return state;
  }

  final int start;
  final JsonTopLevelKind topLevelKind;

  int depth = 0;
  int length = 0;
  bool inString = false;
  bool escapeNext = false;
  bool sawAnyChar = false;
  bool isComplete = false;
  bool awaitingPrimitiveDelimiter = false;
  bool awaitingStringClose = true;
  final StringBuffer primitive = StringBuffer();

  bool get canCloseAtEndOfInput {
    if (isComplete) {
      return true;
    }
    switch (topLevelKind) {
      case JsonTopLevelKind.object:
      case JsonTopLevelKind.array:
        return depth == 0 && !inString && sawAnyChar;
      case JsonTopLevelKind.string:
        return !awaitingStringClose && !inString;
      case JsonTopLevelKind.literal:
        return _primitiveLooksComplete(primitive.toString());
      case JsonTopLevelKind.number:
        return _numberLooksCompleteAtEnd(primitive.toString());
    }
  }

  void consume(String char) {
    length++;
    switch (topLevelKind) {
      case JsonTopLevelKind.object:
      case JsonTopLevelKind.array:
        _consumeNested(char);
        return;
      case JsonTopLevelKind.string:
        _consumeTopLevelString(char);
        return;
      case JsonTopLevelKind.literal:
      case JsonTopLevelKind.number:
        _consumePrimitive(char);
        return;
    }
  }

  _ScanState shiftedLeft(int offset) {
    final shifted = _ScanState(start: start - offset, topLevelKind: topLevelKind)
      ..depth = depth
      ..length = length
      ..inString = inString
      ..escapeNext = escapeNext
      ..sawAnyChar = sawAnyChar
      ..isComplete = isComplete
      ..awaitingPrimitiveDelimiter = awaitingPrimitiveDelimiter
      ..awaitingStringClose = awaitingStringClose;
    shifted.primitive.write(primitive.toString());
    return shifted;
  }

  void _consumeNested(String char) {
    sawAnyChar = true;

    if (inString) {
      if (escapeNext) {
        escapeNext = false;
        return;
      }
      if (char == r'\') {
        escapeNext = true;
        return;
      }
      if (char == '"') {
        inString = false;
      }
      return;
    }

    if (char == '"') {
      inString = true;
      return;
    }

    if (char == '{' || char == '[') {
      depth++;
      return;
    }

    if (char == '}' || char == ']') {
      depth--;
      if (depth < 0) {
        throw JsonStreamDecodeException('Encountered an unmatched closing bracket in streamed JSON.');
      }
      if (depth == 0) {
        isComplete = true;
      }
    }
  }

  void _consumeTopLevelString(String char) {
    if (!sawAnyChar) {
      if (char != '"') {
        throw JsonStreamDecodeException('Top-level JSON string must start with a quote.');
      }
      sawAnyChar = true;
      inString = true;
      awaitingStringClose = true;
      return;
    }

    if (escapeNext) {
      escapeNext = false;
      return;
    }

    if (char == r'\') {
      escapeNext = true;
      return;
    }

    if (char == '"') {
      inString = false;
      awaitingStringClose = false;
      isComplete = true;
    }
  }

  void _consumePrimitive(String char) {
    if (!sawAnyChar) {
      sawAnyChar = true;
      primitive.write(char);
      return;
    }

    if (awaitingPrimitiveDelimiter) {
      if (_isPrimitiveBoundary(char)) {
        isComplete = true;
        return;
      }
      primitive.write(char);
      throw JsonStreamDecodeException(
        'Received invalid trailing data "$char" after top-level JSON primitive ${primitive.toString()}.',
      );
    }

    primitive.write(char);
    final current = primitive.toString();
    if (topLevelKind == JsonTopLevelKind.literal) {
      if (_primitiveLooksComplete(current)) {
        awaitingPrimitiveDelimiter = true;
      } else if (!_literalCouldStillMatch(current)) {
        throw JsonStreamDecodeException('Invalid JSON literal prefix "$current".');
      }
      return;
    }

    if (_numberLooksCompleteWithDelimiter(current)) {
      awaitingPrimitiveDelimiter = true;
    } else if (!_numberCouldStillBeValid(current)) {
      throw JsonStreamDecodeException('Invalid JSON number prefix "$current".');
    }
  }

  static bool _isPrimitiveBoundary(String char) {
    if (char.isEmpty) {
      return true;
    }
    final rune = char.codeUnitAt(0);
    return rune == 0x20 || rune == 0x09 || rune == 0x0A || rune == 0x0D;
  }

  static bool _primitiveLooksComplete(String text) {
    return text == 'true' || text == 'false' || text == 'null';
  }

  static bool _literalCouldStillMatch(String text) {
    return 'true'.startsWith(text) || 'false'.startsWith(text) || 'null'.startsWith(text);
  }

  static bool _numberLooksCompleteWithDelimiter(String text) {
    if (text.isEmpty) {
      return false;
    }
    if (!_numberCouldStillBeValid(text)) {
      return false;
    }
    if (_endsWithIncompleteNumberToken(text)) {
      return false;
    }
    return true;
  }

  static bool _numberLooksCompleteAtEnd(String text) {
    return text.isNotEmpty &&
        _numberCouldStillBeValid(text) &&
        !_endsWithIncompleteNumberToken(text);
  }

  static bool _numberCouldStillBeValid(String text) {
    final regex = RegExp(r'^-?(?:0|[1-9]\d*)(?:\.\d*)?(?:[eE][+-]?\d*)?$');
    if (regex.hasMatch(text)) {
      return true;
    }
    return text == '-' ||
        text.endsWith('.') ||
        text.endsWith('e') ||
        text.endsWith('E') ||
        text.endsWith('e+') ||
        text.endsWith('e-') ||
        text.endsWith('E+') ||
        text.endsWith('E-');
  }

  static bool _endsWithIncompleteNumberToken(String text) {
    return text == '-' ||
        text.endsWith('.') ||
        text.endsWith('e') ||
        text.endsWith('E') ||
        text.endsWith('e+') ||
        text.endsWith('e-') ||
        text.endsWith('E+') ||
        text.endsWith('E-');
  }
}

extension _TopLevelKindX on JsonTopLevelKind {
  static JsonTopLevelKind? tryStart(String text, int index, bool allowTopLevelPrimitives) {
    if (index >= text.length) {
      return null;
    }
    final char = text[index];
    if (char == '{') {
      return JsonTopLevelKind.object;
    }
    if (char == '[') {
      return JsonTopLevelKind.array;
    }
    if (!allowTopLevelPrimitives) {
      return null;
    }
    if (char == '"') {
      return JsonTopLevelKind.string;
    }
    if (char == '-' || _isDigit(char)) {
      return JsonTopLevelKind.number;
    }
    if (char == 't' || char == 'f' || char == 'n') {
      return JsonTopLevelKind.literal;
    }
    return null;
  }

  static bool _isDigit(String char) {
    final rune = char.codeUnitAt(0);
    return rune >= 0x30 && rune <= 0x39;
  }
}

/*
This solves the annoying JSON streaming mess you hit when LLMs send partial objects over SSE, chunked HTTP, or WebSockets and your app needs to act before the full response is finished. Built because in April 2026 a lot of Dart and Flutter teams are wiring models into mobile apps, edge workers, CLIs, and backend tools, and the weak point is usually not the model call itself. The weak point is turning messy streamed text into safe structured data without random parser crashes, giant buffers, or brittle regex hacks.

Use it when a model streams text that should eventually become JSON, but the transport splits messages at arbitrary boundaries, wraps payloads in markdown fences, or includes extra commentary before or after the real object. The trick: this file keeps a streaming state machine for nested JSON, validates completed boundaries with the real Dart JSON decoder, preserves the skipped noise so you can inspect bad prompts, and enforces hard memory limits so one bad stream does not quietly blow up your process.

Drop this into a Dart backend, Flutter desktop tool, AI gateway, worker process, inference proxy, test harness, or developer CLI when you need reliable structured output parsing from streamed model responses. I wrote it to be simple to fork, easy to audit, and good for search terms like Dart streamed JSON parser, Flutter SSE JSON decoder, LLM structured output Dart, incremental JSON extraction, and safe JSON assembly from AI streaming responses.
*/
