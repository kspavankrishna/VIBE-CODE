#!/usr/bin/env elixir

# RagPromptFirewall.exs
# Dependency-free CI gate for RAG prompt injection, retrieval poisoning, and agent trace evidence.

defmodule RagPromptFirewall.Json do
  @number_re ~r/^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/

  def parse(binary) when is_binary(binary) do
    with {:ok, value, rest} <- parse_value(skip_ws(binary)),
         "" <- skip_ws(rest) do
      {:ok, value}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, "trailing data after JSON value: #{preview(other)}"}
    end
  end

  def encode(value), do: do_encode(value)

  defp parse_value(<<"null", rest::binary>>), do: {:ok, nil, rest}
  defp parse_value(<<"true", rest::binary>>), do: {:ok, true, rest}
  defp parse_value(<<"false", rest::binary>>), do: {:ok, false, rest}
  defp parse_value(<<"\"", rest::binary>>), do: parse_string(rest, [])
  defp parse_value(<<"[", rest::binary>>), do: parse_array(rest, [])
  defp parse_value(<<"{", rest::binary>>), do: parse_object(rest, %{})
  defp parse_value(binary), do: parse_number(binary)

  defp parse_array(rest, acc) do
    case skip_ws(rest) do
      <<"]", tail::binary>> -> {:ok, Enum.reverse(acc), tail}
      other ->
        with {:ok, value, after_value} <- parse_value(other) do
          parse_array_tail(skip_ws(after_value), [value | acc])
        end
    end
  end

  defp parse_array_tail(<<",", rest::binary>>, acc), do: parse_array(rest, acc)
  defp parse_array_tail(<<"]", rest::binary>>, acc), do: {:ok, Enum.reverse(acc), rest}
  defp parse_array_tail(other, _acc), do: {:error, "expected array comma or close bracket near #{preview(other)}"}

  defp parse_object(rest, acc) do
    case skip_ws(rest) do
      <<"}", tail::binary>> -> {:ok, acc, tail}
      <<"\"", tail::binary>> ->
        with {:ok, key, after_key} <- parse_string(tail, []),
             <<":", after_colon::binary>> <- skip_ws(after_key),
             {:ok, value, after_value} <- parse_value(skip_ws(after_colon)) do
          parse_object_tail(skip_ws(after_value), Map.put(acc, key, value))
        else
          {:error, reason} -> {:error, reason}
          _ -> {:error, "expected object colon near #{preview(rest)}"}
        end
      other -> {:error, "expected object key near #{preview(other)}"}
    end
  end

  defp parse_object_tail(<<",", rest::binary>>, acc), do: parse_object(rest, acc)
  defp parse_object_tail(<<"}", rest::binary>>, acc), do: {:ok, acc, rest}
  defp parse_object_tail(other, _acc), do: {:error, "expected object comma or close brace near #{preview(other)}"}

  defp parse_string(<<"\"", rest::binary>>, acc), do: {:ok, IO.iodata_to_binary(Enum.reverse(acc)), rest}
  defp parse_string(<<"\\", rest::binary>>, acc), do: parse_escaped(rest, acc)
  defp parse_string(<<codepoint::utf8, rest::binary>>, acc), do: parse_string(rest, [<<codepoint::utf8>> | acc])
  defp parse_string(<<>>, _acc), do: {:error, "unterminated JSON string"}
  defp parse_string(_other, _acc), do: {:error, "invalid UTF-8 inside JSON string"}

  defp parse_escaped(<<"\"", rest::binary>>, acc), do: parse_string(rest, ["\"" | acc])
  defp parse_escaped(<<"\\", rest::binary>>, acc), do: parse_string(rest, ["\\" | acc])
  defp parse_escaped(<<"/", rest::binary>>, acc), do: parse_string(rest, ["/" | acc])
  defp parse_escaped(<<"b", rest::binary>>, acc), do: parse_string(rest, ["\b" | acc])
  defp parse_escaped(<<"f", rest::binary>>, acc), do: parse_string(rest, ["\f" | acc])
  defp parse_escaped(<<"n", rest::binary>>, acc), do: parse_string(rest, ["\n" | acc])
  defp parse_escaped(<<"r", rest::binary>>, acc), do: parse_string(rest, ["\r" | acc])
  defp parse_escaped(<<"t", rest::binary>>, acc), do: parse_string(rest, ["\t" | acc])

  defp parse_escaped(<<"u", hex::binary-size(4), rest::binary>>, acc) do
    case parse_hex(hex) do
      {:ok, high} when high >= 0xD800 and high <= 0xDBFF -> parse_surrogate(high, rest, acc)
      {:ok, low} when low >= 0xDC00 and low <= 0xDFFF -> {:error, "low surrogate without high surrogate"}
      {:ok, codepoint} -> parse_string(rest, [<<codepoint::utf8>> | acc])
      :error -> {:error, "invalid unicode escape #{hex}"}
    end
  end

  defp parse_escaped(other, _acc), do: {:error, "invalid escape sequence near #{preview(other)}"}

  defp parse_surrogate(high, <<"\\u", low_hex::binary-size(4), rest::binary>>, acc) do
    case parse_hex(low_hex) do
      {:ok, low} when low >= 0xDC00 and low <= 0xDFFF ->
        codepoint = 0x10000 + ((high - 0xD800) * 0x400) + (low - 0xDC00)
        parse_string(rest, [<<codepoint::utf8>> | acc])
      {:ok, _} -> {:error, "invalid low surrogate #{low_hex}"}
      :error -> {:error, "invalid unicode escape #{low_hex}"}
    end
  end

  defp parse_surrogate(_high, _rest, _acc), do: {:error, "missing low surrogate after high surrogate"}

  defp parse_hex(hex) do
    case Integer.parse(hex, 16) do
      {value, ""} -> {:ok, value}
      _ -> :error
    end
  end

  defp parse_number(binary) do
    {token, rest} = take_number(binary, "")

    cond do
      token == "" -> {:error, "unexpected JSON token near #{preview(binary)}"}
      not Regex.match?(@number_re, token) -> {:error, "invalid JSON number #{token}"}
      String.contains?(token, [".", "e", "E"]) ->
        case Float.parse(token) do
          {value, ""} -> {:ok, value, rest}
          _ -> {:error, "invalid JSON float #{token}"}
        end
      true ->
        case Integer.parse(token) do
          {value, ""} -> {:ok, value, rest}
          _ -> {:error, "invalid JSON integer #{token}"}
        end
    end
  end

  defp take_number(<<char, rest::binary>>, acc) when char in [?-, ?+, ?., ?e, ?E] or (char >= ?0 and char <= ?9) do
    take_number(rest, acc <> <<char>>)
  end

  defp take_number(rest, acc), do: {acc, rest}

  defp skip_ws(<<char, rest::binary>>) when char in [?\s, ?\n, ?\r, ?\t], do: skip_ws(rest)
  defp skip_ws(rest), do: rest

  defp do_encode(nil), do: "null"
  defp do_encode(true), do: "true"
  defp do_encode(false), do: "false"
  defp do_encode(value) when is_integer(value), do: Integer.to_string(value)
  defp do_encode(value) when is_float(value), do: :erlang.float_to_binary(value, [:compact, {:decimals, 12}])
  defp do_encode(value) when is_binary(value), do: encode_string(value)
  defp do_encode(value) when is_atom(value), do: encode_string(Atom.to_string(value))

  defp do_encode(value) when is_list(value) do
    ["[", value |> Enum.map(&do_encode/1) |> Enum.intersperse(","), "]"] |> IO.iodata_to_binary()
  end

  defp do_encode(value) when is_map(value) do
    entries =
      value
      |> Enum.sort_by(fn {key, _} -> to_string(key) end)
      |> Enum.map(fn {key, item} -> [encode_string(to_string(key)), ":", do_encode(item)] end)
      |> Enum.intersperse(",")

    ["{", entries, "}"] |> IO.iodata_to_binary()
  end

  defp encode_string(binary) do
    escaped =
      binary
      |> String.to_charlist()
      |> Enum.map(fn
        ?" -> "\\\""
        ?\\ -> "\\\\"
        ?\b -> "\\b"
        ?\f -> "\\f"
        ?\n -> "\\n"
        ?\r -> "\\r"
        ?\t -> "\\t"
        codepoint when codepoint < 0x20 -> "\\u" <> pad4(Integer.to_string(codepoint, 16))
        codepoint -> <<codepoint::utf8>>
      end)

    ["\"", escaped, "\""] |> IO.iodata_to_binary()
  end

  defp pad4(hex), do: hex |> String.downcase() |> String.pad_leading(4, "0")
  defp preview(binary) when is_binary(binary), do: binary |> String.slice(0, 40) |> String.replace(~r/\s+/, " ")
  defp preview(other), do: inspect(other)
end

defmodule RagPromptFirewall.Cli do
  @version "2026.04.1"

  def parse(argv), do: do_parse(argv, defaults())

  def version, do: @version

  def help do
    """
    RagPromptFirewall.exs #{@version}

    Usage:
      elixir RagPromptFirewall.exs [options] trace.jsonl [more.jsonl]
      cat trace.jsonl | elixir RagPromptFirewall.exs --json-out result.json -

    Options:
      --input PATH             Read a JSONL or plain text input path. Repeatable.
      --json-out PATH          Write a machine-readable JSON report.
      --markdown-out PATH      Write a Markdown report for pull request comments.
      --sarif-out PATH         Write SARIF 2.1.0 for GitHub code scanning.
      --fail-at N              Exit 1 when any finding score is at least N. Default: 80.
      --warn-at N              Mark medium findings at N. Default: 45.
      --trusted-source VALUE   Trust an exact source string or source prefix. Repeatable.
      --allow-domain DOMAIN    Trust URLs from a domain or subdomain. Repeatable.
      --max-text-bytes N       Scan only the first N bytes of each record text. Default: 200000.
      --plain                  Treat every input line as plain text instead of auto JSONL.
      --strict                 Fail if any JSON line is malformed or any input cannot be read.
      --explain                Print a short operational explanation after writing reports.
      --help                   Show this help.
      --version                Print the version.

    Input records may use common fields such as trace_id, session_id, request_id, source,
    url, role, type, text, content, prompt, message, document, chunk, input, output,
    arguments, tool_arguments, tool_name, and metadata. Unknown JSON still gets scanned.
    """
  end

  defp defaults do
    %{
      inputs: [],
      json_out: nil,
      markdown_out: nil,
      sarif_out: nil,
      fail_at: 80,
      warn_at: 45,
      trusted_sources: MapSet.new(),
      allow_domains: MapSet.new(),
      max_text_bytes: 200_000,
      plain: false,
      strict: false,
      explain: false
    }
  end

  defp do_parse([], opts), do: {:ok, normalize_inputs(opts)}
  defp do_parse(["--help" | _], _opts), do: :help
  defp do_parse(["-h" | _], _opts), do: :help
  defp do_parse(["--version" | _], _opts), do: {:version, @version}
  defp do_parse(["--plain" | rest], opts), do: do_parse(rest, %{opts | plain: true})
  defp do_parse(["--strict" | rest], opts), do: do_parse(rest, %{opts | strict: true})
  defp do_parse(["--explain" | rest], opts), do: do_parse(rest, %{opts | explain: true})
  defp do_parse(["--input", path | rest], opts), do: do_parse(rest, %{opts | inputs: opts.inputs ++ [path]})
  defp do_parse(["--json-out", path | rest], opts), do: do_parse(rest, %{opts | json_out: path})
  defp do_parse(["--markdown-out", path | rest], opts), do: do_parse(rest, %{opts | markdown_out: path})
  defp do_parse(["--sarif-out", path | rest], opts), do: do_parse(rest, %{opts | sarif_out: path})

  defp do_parse(["--trusted-source", value | rest], opts) do
    do_parse(rest, %{opts | trusted_sources: MapSet.put(opts.trusted_sources, value)})
  end

  defp do_parse(["--allow-domain", value | rest], opts) do
    do_parse(rest, %{opts | allow_domains: MapSet.put(opts.allow_domains, String.downcase(value))})
  end

  defp do_parse(["--fail-at", raw | rest], opts) do
    with {:ok, value} <- bounded_int(raw, 1, 100, "fail-at") do
      do_parse(rest, %{opts | fail_at: value})
    end
  end

  defp do_parse(["--warn-at", raw | rest], opts) do
    with {:ok, value} <- bounded_int(raw, 1, 100, "warn-at") do
      do_parse(rest, %{opts | warn_at: value})
    end
  end

  defp do_parse(["--max-text-bytes", raw | rest], opts) do
    with {:ok, value} <- bounded_int(raw, 128, 20_000_000, "max-text-bytes") do
      do_parse(rest, %{opts | max_text_bytes: value})
    end
  end

  defp do_parse([flag | _rest], _opts) do
    if String.starts_with?(flag, "-") do
      {:error, "unknown option #{flag}"}
    else
      {:error, "unreachable parser state"}
    end
  end

  defp do_parse([path | rest], opts), do: do_parse(rest, %{opts | inputs: opts.inputs ++ [path]})

  defp normalize_inputs(opts) do
    if opts.inputs == [], do: %{opts | inputs: ["-"]}, else: opts
  end

  defp bounded_int(raw, min_value, max_value, label) do
    case Integer.parse(raw) do
      {value, ""} when value >= min_value and value <= max_value -> {:ok, value}
      _ -> {:error, "#{label} must be an integer from #{min_value} to #{max_value}"}
    end
  end
end

defmodule RagPromptFirewall.Record do
  alias RagPromptFirewall.Json

  @text_fields ~w(text content prompt message document chunk input output arguments tool_arguments toolArguments tool_input toolInput tool_output toolOutput query response completion body)
  @source_fields ~w(source source_url sourceUrl url uri path file document_url documentUrl origin retriever collection index)
  @trace_fields ~w(trace_id traceId session_id sessionId request_id requestId conversation_id conversationId run_id runId span_id spanId thread_id threadId)
  @role_fields ~w(role actor speaker author)
  @kind_fields ~w(type event kind name span_name spanName category)
  @tool_fields ~w(tool tool_name toolName function function_name functionName arguments tool_arguments toolArguments)

  def from_value(value, meta, opts) when is_map(value) do
    text = value |> extract_text() |> limit_text(opts.max_text_bytes)
    source = first_string(value, @source_fields) || meta.file
    role = first_string(value, @role_fields) || "unknown"
    kind = first_string(value, @kind_fields) || "unknown"
    trace_id = first_string(value, @trace_fields)
    surface = classify_surface(value, role, kind)

    %{
      id: first_string(value, ["id", "event_id", "eventId"]) || stable_id(meta.file, meta.line, text),
      file: meta.file,
      line: meta.line,
      trace_id: trace_id,
      source: source,
      role: role,
      kind: kind,
      surface: surface,
      trusted: trusted_source?(source, opts),
      text: text,
      raw: value
    }
  end

  def from_value(value, meta, opts) do
    text = value |> Json.encode() |> limit_text(opts.max_text_bytes)

    %{
      id: stable_id(meta.file, meta.line, text),
      file: meta.file,
      line: meta.line,
      trace_id: nil,
      source: meta.file,
      role: "unknown",
      kind: "json_value",
      surface: "plain",
      trusted: trusted_source?(meta.file, opts),
      text: text,
      raw: value
    }
  end

  def plain(line, meta, opts) do
    text = limit_text(line, opts.max_text_bytes)

    %{
      id: stable_id(meta.file, meta.line, text),
      file: meta.file,
      line: meta.line,
      trace_id: nil,
      source: meta.file,
      role: "unknown",
      kind: "plain_text",
      surface: "plain",
      trusted: trusted_source?(meta.file, opts),
      text: text,
      raw: %{"text" => text}
    }
  end

  defp extract_text(map) do
    parts =
      @text_fields
      |> Enum.flat_map(fn field -> value_to_text(Map.get(map, field)) end)
      |> Enum.reject(&(&1 == ""))

    if parts == [], do: Json.encode(map), else: Enum.join(parts, "\n")
  end

  defp value_to_text(nil), do: []
  defp value_to_text(value) when is_binary(value), do: [value]
  defp value_to_text(value) when is_integer(value) or is_float(value) or is_boolean(value), do: [to_string(value)]
  defp value_to_text(value) when is_map(value) or is_list(value), do: [Json.encode(value)]
  defp value_to_text(value), do: [inspect(value)]

  defp first_string(map, fields) do
    Enum.find_value(fields, fn field ->
      case Map.get(map, field) do
        value when is_binary(value) and value != "" -> value
        value when is_integer(value) -> Integer.to_string(value)
        value when is_atom(value) -> Atom.to_string(value)
        _ -> nil
      end
    end)
  end

  defp classify_surface(map, role, kind) do
    role = String.downcase(to_string(role))
    kind = String.downcase(to_string(kind))

    cond do
      role in ["system", "developer"] -> "trusted_prompt"
      has_any?(map, @tool_fields) or contains_any?(kind, ["tool", "function_call", "mcp"]) -> "tool_args"
      contains_any?(kind, ["retriev", "document", "chunk", "vector", "search_result"]) -> "retrieval"
      role in ["user", "assistant"] or contains_any?(kind, ["prompt", "message", "completion"]) -> "prompt"
      true -> "plain"
    end
  end

  defp has_any?(map, fields), do: Enum.any?(fields, &Map.has_key?(map, &1))
  defp contains_any?(text, needles), do: Enum.any?(needles, &String.contains?(text, &1))

  defp trusted_source?(nil, _opts), do: false

  defp trusted_source?(source, opts) do
    source = to_string(source)
    host = source_host(source)

    exact_or_prefix =
      opts.trusted_sources
      |> Enum.any?(fn trusted -> source == trusted or String.starts_with?(source, trusted) end)

    domain_allowed =
      opts.allow_domains
      |> Enum.any?(fn domain -> host == domain or String.ends_with?(host, "." <> domain) end)

    exact_or_prefix or domain_allowed
  end

  defp source_host(source) do
    case URI.parse(source) do
      %URI{host: host} when is_binary(host) -> String.downcase(host)
      _ -> String.downcase(source)
    end
  rescue
    _ -> String.downcase(source)
  end

  defp stable_id(file, line, text) do
    :crypto.hash(:sha256, [file, ":", Integer.to_string(line), ":", text])
    |> Base.encode16(case: :lower)
    |> binary_part(0, 16)
  end

  defp limit_text(text, max_bytes) when byte_size(text) <= max_bytes, do: text

  defp limit_text(text, max_bytes) do
    prefix = valid_prefix(text, max_bytes)
    prefix <> "\n[truncated by RagPromptFirewall at #{max_bytes} bytes]"
  end

  defp valid_prefix(_text, max_bytes) when max_bytes <= 0, do: ""

  defp valid_prefix(text, max_bytes) do
    size = min(max_bytes, byte_size(text))
    candidate = binary_part(text, 0, size)
    if String.valid?(candidate), do: candidate, else: valid_prefix(text, size - 1)
  end
end

defmodule RagPromptFirewall.Input do
  alias RagPromptFirewall.Json
  alias RagPromptFirewall.Record

  def read_records(paths, opts) do
    {records, errors} =
      Enum.reduce(paths, {[], []}, fn path, acc ->
        read_path(path, opts, acc)
      end)

    {Enum.reverse(records), Enum.reverse(errors)}
  end

  defp read_path(path, opts, {records, errors}) do
    stream_for(path)
    |> Stream.with_index(1)
    |> Enum.reduce({records, errors}, fn {line, line_number}, {acc_records, acc_errors} ->
      line = line |> String.trim_trailing("\n") |> String.trim_trailing("\r")
      parse_line(line, %{file: path, line: line_number}, opts, acc_records, acc_errors)
    end)
  rescue
    error -> {records, [%{file: path, line: 0, error: Exception.message(error)} | errors]}
  end

  defp stream_for("-"), do: IO.stream(:stdio, :line)
  defp stream_for(path), do: File.stream!(path, [], :line)

  defp parse_line("", _meta, _opts, records, errors), do: {records, errors}

  defp parse_line(line, meta, opts, records, errors) do
    cond do
      opts.plain ->
        {[Record.plain(line, meta, opts) | records], errors}

      jsonish?(line) ->
        case Json.parse(line) do
          {:ok, value} -> {[Record.from_value(value, meta, opts) | records], errors}
          {:error, reason} ->
            parsed_as_text = Record.plain(line, meta, opts)
            error = %{file: meta.file, line: meta.line, error: reason}
            {[parsed_as_text | records], [error | errors]}
        end

      true ->
        {[Record.plain(line, meta, opts) | records], errors}
    end
  end

  defp jsonish?(line) do
    trimmed = String.trim_leading(line)
    String.starts_with?(trimmed, ["{", "["])
  end
end

defmodule RagPromptFirewall.Rule do
  def all do
    [
      %{
        id: "RPF001",
        name: "Instruction Override",
        score: 34,
        pattern: ~r/(ignore|disregard|override|forget|bypass)[\s\S]{0,100}(previous|above|system|developer|instruction|policy|guardrail)/i,
        description: "Untrusted text appears to tell the model to override higher priority instructions.",
        remediation: "Keep retrieved text outside privileged prompts, quote it as data, and add a policy boundary before model execution."
      },
      %{
        id: "RPF002",
        name: "Secret Exfiltration Request",
        score: 42,
        pattern: ~r/(reveal|print|dump|send|exfiltrate|leak)[\s\S]{0,120}(api[_ -]?key|token|secret|credential|password|env|\.env|private[_ -]?key)/i,
        description: "The record contains language that asks for credentials or private runtime material.",
        remediation: "Block the record, redact secrets before model context assembly, and inspect trace routing for tool access."
      },
      %{
        id: "RPF003",
        name: "Tool Call Steering",
        score: 31,
        pattern: ~r/(call|invoke|use|run|execute)[\s\S]{0,80}(tool|function|mcp|browser|shell|python|curl|webhook|http request)/i,
        description: "Untrusted text is trying to steer an agent tool call or function call.",
        remediation: "Require typed tool policies, source-aware confirmation rules, and deny untrusted content from tool arguments."
      },
      %{
        id: "RPF004",
        name: "Network Dropper",
        score: 32,
        pattern: ~r/(curl|wget|fetch|httpie|powershell|Invoke-WebRequest)[\s\S]{0,120}(http|webhook|pastebin|ngrok|requestbin|post)/i,
        description: "The text includes command-shaped network egress instructions.",
        remediation: "Treat network egress as high risk, require allowlisted hosts, and store the rejected payload as evidence."
      },
      %{
        id: "RPF005",
        name: "Prompt Boundary Spoofing",
        score: 26,
        pattern: ~r/(system prompt|developer message|assistant message|new instructions|priority instruction|hidden instruction|confidential prompt)/i,
        description: "The record appears to spoof prompt roles or privileged prompt sections.",
        remediation: "Render retrieved content with explicit citations and never concatenate it into role-bearing prompt text."
      },
      %{
        id: "RPF006",
        name: "Encoded Payload Hint",
        score: 24,
        pattern: ~r/(base64|rot13|hex encoded|unicode escape|decode this|atob|fromCharCode)[\s\S]{0,100}(instruction|payload|secret|command|prompt)/i,
        description: "The record hints at encoded content used to smuggle prompt or command instructions.",
        remediation: "Decode and rescan content in an isolated stage before retrieval ranking or prompt construction."
      },
      %{
        id: "RPF007",
        name: "Data Laundering",
        score: 29,
        pattern: ~r/(summarize|translate|rewrite|compress)[\s\S]{0,100}(then|and)[\s\S]{0,120}(send|post|upload|email|commit|open a pull request)/i,
        description: "The text chains a harmless transformation into an unsafe side effect.",
        remediation: "Separate read-only transformations from write actions and require fresh authorization at the write boundary."
      },
      %{
        id: "RPF008",
        name: "Evaluation Leakage",
        score: 27,
        pattern: ~r/(benchmark|eval|holdout|golden set|test answer|grading rubric)[\s\S]{0,120}(leak|show|memorize|include|copy|answer key)/i,
        description: "The record may expose evaluation material or ask the model to contaminate a holdout set.",
        remediation: "Quarantine evaluation records and keep holdout identifiers outside training, retrieval, and cache keys."
      },
      %{
        id: "RPF009",
        name: "Policy Suppression",
        score: 33,
        pattern: ~r/(do not mention|hide|conceal|silently|without telling)[\s\S]{0,100}(policy|safety|audit|log|security|review|user)/i,
        description: "The text asks the agent to hide policy, security, logging, or review behavior.",
        remediation: "Fail closed and preserve the exact source record for a human security review."
      },
      %{
        id: "RPF010",
        name: "Repository Write Steering",
        score: 35,
        pattern: ~r/(commit|push|merge|release|tag|deploy|publish)[\s\S]{0,120}(without review|directly to main|skip ci|disable tests|force)/i,
        description: "The record appears to steer repository or deployment writes around normal controls.",
        remediation: "Route repository writes through branch protection, CI policy, and explicit source trust checks."
      }
    ]
  end

  def propagation_rule do
    %{
      id: "RPF900",
      name: "Untrusted Retrieval Propagated To Action Surface",
      score: 48,
      description: "A risky untrusted retrieval record and a prompt or tool surface share the same trace identifier.",
      remediation: "Break the propagation path, add source labels to prompt assembly, and require policy checks before tools run."
    }
  end

  def sarif_rules do
    (all() ++ [propagation_rule()])
    |> Enum.map(fn rule ->
      %{
        "id" => rule.id,
        "name" => rule.name,
        "shortDescription" => %{"text" => rule.name},
        "fullDescription" => %{"text" => rule.description},
        "help" => %{"text" => rule.remediation}
      }
    end)
  end
end

defmodule RagPromptFirewall.Engine do
  alias RagPromptFirewall.Rule

  def scan(records, parse_errors, opts) do
    direct_findings = Enum.flat_map(records, &scan_record(&1, opts))
    propagated_findings = propagation_findings(records, direct_findings, opts)

    findings =
      (direct_findings ++ propagated_findings)
      |> Enum.sort_by(fn finding -> {-finding.score, finding.file, finding.line, finding.rule_id} end)

    summary = summarize(records, findings, parse_errors, opts)

    %{
      version: RagPromptFirewall.Cli.version(),
      generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      status: status(findings, parse_errors, opts),
      summary: summary,
      thresholds: %{warn_at: opts.warn_at, fail_at: opts.fail_at, strict: opts.strict},
      findings: findings,
      parse_errors: parse_errors
    }
  end

  defp scan_record(record, opts) do
    Rule.all()
    |> Enum.filter(fn rule -> Regex.match?(rule.pattern, record.text) end)
    |> Enum.map(fn rule -> build_finding(record, rule, opts) end)
  end

  defp build_finding(record, rule, opts) do
    score = clamp(rule.score + context_score(record), 1, 100)

    %{
      id: fingerprint(record.id, rule.id),
      rule_id: rule.id,
      rule_name: rule.name,
      severity: severity(score, opts),
      score: score,
      trace_id: record.trace_id,
      file: record.file,
      line: record.line,
      record_id: record.id,
      source: record.source,
      surface: record.surface,
      trusted: record.trusted,
      reason: rule.description,
      remediation: rule.remediation,
      snippet: snippet(record.text)
    }
  end

  defp context_score(record) do
    trust_adjustment = if record.trusted, do: -18, else: 0

    surface_adjustment =
      case record.surface do
        "retrieval" -> 18
        "tool_args" -> 22
        "prompt" -> 8
        "plain" -> 4
        "trusted_prompt" -> -8
        _ -> 0
      end

    trace_adjustment = if is_binary(record.trace_id), do: 4, else: 0
    trust_adjustment + surface_adjustment + trace_adjustment
  end

  defp propagation_findings(records, direct_findings, opts) do
    risky_by_trace =
      direct_findings
      |> Enum.filter(fn finding ->
        is_binary(finding.trace_id) and finding.score >= opts.warn_at and
          finding.surface in ["retrieval", "plain"] and not finding.trusted
      end)
      |> Enum.group_by(& &1.trace_id)

    records
    |> Enum.filter(fn record -> is_binary(record.trace_id) and record.surface in ["prompt", "tool_args"] end)
    |> Enum.group_by(& &1.trace_id)
    |> Enum.flat_map(fn {trace_id, sinks} ->
      case Map.get(risky_by_trace, trace_id, []) do
        [] -> []
        risky -> [build_propagation(trace_id, risky, sinks, opts)]
      end
    end)
  end

  defp build_propagation(trace_id, risky, sinks, opts) do
    rule = Rule.propagation_rule()
    poison = Enum.max_by(risky, & &1.score)
    sink = Enum.min_by(sinks, & &1.line)
    score = clamp(max(poison.score, rule.score) + 14, 1, 100)

    %{
      id: fingerprint(trace_id, rule.id),
      rule_id: rule.id,
      rule_name: rule.name,
      severity: severity(score, opts),
      score: score,
      trace_id: trace_id,
      file: sink.file,
      line: sink.line,
      record_id: sink.id,
      source: poison.source,
      surface: sink.surface,
      trusted: false,
      reason: "Risky untrusted retrieval evidence shares trace #{trace_id} with #{sink.surface} at #{sink.file}:#{sink.line}.",
      remediation: rule.remediation,
      snippet: poison.snippet
    }
  end

  defp summarize(records, findings, parse_errors, opts) do
    by_severity =
      ["critical", "high", "medium", "low"]
      |> Map.new(fn severity -> {severity, Enum.count(findings, &(&1.severity == severity))} end)

    by_surface =
      findings
      |> Enum.group_by(& &1.surface)
      |> Map.new(fn {surface, items} -> {surface, length(items)} end)

    %{
      records_scanned: length(records),
      traces_scanned: records |> Enum.map(& &1.trace_id) |> Enum.reject(&is_nil/1) |> MapSet.new() |> MapSet.size(),
      findings_total: length(findings),
      max_score: max_score(findings),
      parse_errors_total: length(parse_errors),
      by_severity: by_severity,
      by_surface: by_surface,
      fail_at: opts.fail_at,
      warn_at: opts.warn_at
    }
  end

  defp status(findings, parse_errors, opts) do
    cond do
      opts.strict and parse_errors != [] -> "fail"
      max_score(findings) >= opts.fail_at -> "fail"
      max_score(findings) >= opts.warn_at -> "warn"
      true -> "pass"
    end
  end

  defp max_score([]), do: 0
  defp max_score(findings), do: findings |> Enum.map(& &1.score) |> Enum.max()

  defp severity(score, opts) do
    cond do
      score >= opts.fail_at -> "critical"
      score >= 70 -> "high"
      score >= opts.warn_at -> "medium"
      true -> "low"
    end
  end

  defp snippet(text) do
    text
    |> String.slice(0, 420)
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
  end

  defp clamp(value, min_value, max_value), do: value |> max(min_value) |> min(max_value)

  defp fingerprint(left, right) do
    :crypto.hash(:sha256, [to_string(left), ":", to_string(right)])
    |> Base.encode16(case: :lower)
    |> binary_part(0, 20)
  end
end

defmodule RagPromptFirewall.Report do
  alias RagPromptFirewall.Json
  alias RagPromptFirewall.Rule

  def render_json(result), do: Json.encode(result) <> "\n"

  def render_markdown(result) do
    header = [
      "# RagPromptFirewall Report",
      "",
      "- Status: #{result.status}",
      "- Records scanned: #{result.summary.records_scanned}",
      "- Traces scanned: #{result.summary.traces_scanned}",
      "- Findings: #{result.summary.findings_total}",
      "- Max score: #{result.summary.max_score}",
      "- Thresholds: warn #{result.thresholds.warn_at}, fail #{result.thresholds.fail_at}",
      ""
    ]

    findings = finding_markdown(result.findings)
    errors = parse_error_markdown(result.parse_errors)
    Enum.join(header ++ findings ++ errors, "\n") <> "\n"
  end

  def render_sarif(result) do
    sarif = %{
      "$schema" => "https://json.schemastore.org/sarif-2.1.0.json",
      "version" => "2.1.0",
      "runs" => [
        %{
          "tool" => %{
            "driver" => %{
              "name" => "RagPromptFirewall",
              "informationUri" => "https://github.com/kspavankrishna/VIBE-CODE",
              "rules" => Rule.sarif_rules()
            }
          },
          "results" => Enum.map(result.findings, &sarif_result/1)
        }
      ]
    }

    Json.encode(sarif) <> "\n"
  end

  defp finding_markdown([]), do: ["## Findings", "", "No prompt injection or retrieval poisoning findings crossed the scanner rules.", ""]

  defp finding_markdown(findings) do
    rows =
      findings
      |> Enum.take(100)
      |> Enum.map(fn finding ->
        "| #{cell(finding.severity)} | #{finding.score} | #{cell(finding.rule_id)} | #{cell(finding.file)} | #{finding.line} | #{cell(finding.surface)} | #{cell(finding.trace_id || "-")} | #{cell(finding.snippet)} |"
      end)

    [
      "## Findings",
      "",
      "| Severity | Score | Rule | File | Line | Surface | Trace | Evidence |",
      "| --- | ---: | --- | --- | ---: | --- | --- | --- |"
    ] ++ rows ++ [""]
  end

  defp parse_error_markdown([]), do: []

  defp parse_error_markdown(errors) do
    rows =
      errors
      |> Enum.take(50)
      |> Enum.map(fn error -> "| #{cell(error.file)} | #{error.line} | #{cell(error.error)} |" end)

    [
      "## Parse Errors",
      "",
      "Malformed JSONL lines were still scanned as plain text. Use --strict to fail on these errors.",
      "",
      "| File | Line | Error |",
      "| --- | ---: | --- |"
    ] ++ rows ++ [""]
  end

  defp sarif_result(finding) do
    %{
      "ruleId" => finding.rule_id,
      "level" => sarif_level(finding.severity),
      "message" => %{"text" => "#{finding.rule_name}: #{finding.reason} Evidence: #{finding.snippet}"},
      "locations" => [
        %{
          "physicalLocation" => %{
            "artifactLocation" => %{"uri" => if(finding.file == "-", do: "stdin", else: finding.file)},
            "region" => %{"startLine" => max(finding.line, 1)}
          }
        }
      ],
      "partialFingerprints" => %{"ragPromptFirewall" => finding.id},
      "properties" => %{
        "score" => finding.score,
        "surface" => finding.surface,
        "trace_id" => finding.trace_id,
        "source" => finding.source,
        "trusted" => finding.trusted,
        "remediation" => finding.remediation
      }
    }
  end

  defp sarif_level("critical"), do: "error"
  defp sarif_level("high"), do: "error"
  defp sarif_level("medium"), do: "warning"
  defp sarif_level(_), do: "note"

  defp cell(nil), do: "-"

  defp cell(value) do
    value
    |> to_string()
    |> String.replace("|", "\\|")
    |> String.replace(~r/\s+/, " ")
    |> String.slice(0, 160)
  end
end

defmodule RagPromptFirewall.Main do
  alias RagPromptFirewall.Cli
  alias RagPromptFirewall.Engine
  alias RagPromptFirewall.Input
  alias RagPromptFirewall.Report

  def run(argv) do
    case Cli.parse(argv) do
      :help ->
        IO.puts(Cli.help())
        0

      {:version, version} ->
        IO.puts(version)
        0

      {:error, message} ->
        IO.puts(:stderr, "error: #{message}")
        IO.puts(:stderr, Cli.help())
        2

      {:ok, opts} ->
        execute(opts)
    end
  end

  defp execute(opts) do
    {records, parse_errors} = Input.read_records(opts.inputs, opts)
    result = Engine.scan(records, parse_errors, opts)

    case write_reports(result, opts) do
      :ok ->
        maybe_print_stdout(result, opts)
        maybe_explain(result, opts)
        exit_code(result.status)

      {:error, message} ->
        IO.puts(:stderr, "error: #{message}")
        2
    end
  end

  defp write_reports(result, opts) do
    [
      {opts.json_out, fn -> Report.render_json(result) end},
      {opts.markdown_out, fn -> Report.render_markdown(result) end},
      {opts.sarif_out, fn -> Report.render_sarif(result) end}
    ]
    |> Enum.reduce_while(:ok, fn
      {nil, _renderer}, :ok -> {:cont, :ok}
      {path, renderer}, :ok ->
        case File.write(path, renderer.()) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, "could not write #{path}: #{:file.format_error(reason)}"}}
        end
    end)
  end

  defp maybe_print_stdout(result, opts) do
    if opts.json_out == nil and opts.markdown_out == nil and opts.sarif_out == nil do
      IO.write(Report.render_json(result))
    else
      IO.puts("RagPromptFirewall status=#{result.status} findings=#{result.summary.findings_total} max_score=#{result.summary.max_score}")
    end
  end

  defp maybe_explain(result, opts) do
    if opts.explain do
      IO.puts("Scanned #{result.summary.records_scanned} records across #{result.summary.traces_scanned} traces. Failures mean untrusted retrieval text reached a prompt, tool, or repository action surface with risky instructions.")
    end
  end

  defp exit_code("fail"), do: 1
  defp exit_code(_status), do: 0
end

RagPromptFirewall.Main.run(System.argv()) |> System.halt()

# Explanation
# This solves the April 2026 problem where RAG applications, coding agents, MCP tools, and internal search copilots pull untrusted text into prompts and then accidentally let that text steer tools, leak secrets, or push repository changes. Built because teams now ship agents that read tickets, docs, web pages, code comments, Slack exports, support logs, and vector database chunks, but many teams still review only the final answer instead of the evidence path. Use it when you need a RAG prompt injection scanner, retrieval poisoning CI gate, agent trace audit, MCP tool-call security report, GitHub SARIF output, JSONL security scanner, or prompt firewall for developer tools. The trick: this file treats every trace as evidence, scores suspicious language in untrusted records, and raises the severity when the same trace carries that record into a prompt or tool surface. Drop this into a repo, pipe JSONL traces through it, upload the SARIF report to GitHub code scanning, and fail the build before poisoned retrieval content becomes an action. I kept it dependency-free so Pavan can run it in locked-down CI, incident review boxes, and research sandboxes without waiting for packages or credentials.