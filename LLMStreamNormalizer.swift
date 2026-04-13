import Foundation

public enum LLMStreamProvider: String, Sendable {
    case unknown
    case openAIResponses = "openai-responses"
    case openAIChatCompletions = "openai-chat-completions"
    case anthropic
    case genericSSE = "generic-sse"
    case genericJSON = "generic-json"
}

public enum LLMStreamMode: Sendable {
    case auto
    case sse
    case ndjson
}

public struct LLMStreamNormalizerOptions: Sendable {
    public var mode: LLMStreamMode
    public var providerHint: LLMStreamProvider?
    public var emitRawEvents: Bool
    public var finalizeToolCallsOnCompletion: Bool

    public init(
        mode: LLMStreamMode = .auto,
        providerHint: LLMStreamProvider? = nil,
        emitRawEvents: Bool = false,
        finalizeToolCallsOnCompletion: Bool = true
    ) {
        self.mode = mode
        self.providerHint = providerHint
        self.emitRawEvents = emitRawEvents
        self.finalizeToolCallsOnCompletion = finalizeToolCallsOnCompletion
    }
}

public struct LLMUsage: Sendable {
    public var inputTokens: Int?
    public var outputTokens: Int?
    public var totalTokens: Int?
    public var cacheCreationInputTokens: Int?
    public var cacheReadInputTokens: Int?
    public var reasoningTokens: Int?

    public init(
        inputTokens: Int? = nil,
        outputTokens: Int? = nil,
        totalTokens: Int? = nil,
        cacheCreationInputTokens: Int? = nil,
        cacheReadInputTokens: Int? = nil,
        reasoningTokens: Int? = nil
    ) {
        self.inputTokens = inputTokens
        self.outputTokens = outputTokens
        self.totalTokens = totalTokens
        self.cacheCreationInputTokens = cacheCreationInputTokens
        self.cacheReadInputTokens = cacheReadInputTokens
        self.reasoningTokens = reasoningTokens
    }
}

public struct LLMTextDelta: Sendable {
    public var provider: LLMStreamProvider
    public var responseId: String?
    public var channel: String
    public var streamIndex: Int?
    public var delta: String
    public var textSoFar: String
}

public struct LLMToolCallDelta: Sendable {
    public var provider: LLMStreamProvider
    public var responseId: String?
    public var channel: String
    public var streamIndex: Int?
    public var toolCallId: String
    public var toolName: String?
    public var argumentsFragment: String
    public var argumentsSoFar: String
    public var argumentsAreLikelyComplete: Bool
}

public struct LLMToolCall: Sendable {
    public var provider: LLMStreamProvider
    public var responseId: String?
    public var channel: String
    public var streamIndex: Int?
    public var toolCallId: String
    public var toolName: String?
    public var argumentsJSON: String
    public var argumentsAreLikelyComplete: Bool
}

public struct LLMStreamCompletion: Sendable {
    public var provider: LLMStreamProvider
    public var responseId: String?
    public var reason: String?
    public var textChannels: [String: String]
    public var toolCalls: [LLMToolCall]
}

public struct LLMRawEvent: Sendable {
    public var provider: LLMStreamProvider
    public var transport: String
    public var eventName: String?
    public var data: String
}

public struct LLMStreamFailure: Sendable {
    public var provider: LLMStreamProvider
    public var responseId: String?
    public var code: String?
    public var message: String
    public var rawPayload: String?
}

public struct LLMStreamSnapshot: Sendable {
    public var provider: LLMStreamProvider
    public var responseId: String?
    public var textChannels: [String: String]
    public var toolCalls: [LLMToolCall]
    public var latestUsage: LLMUsage?
}

public enum LLMStreamEvent: Sendable {
    case textDelta(LLMTextDelta)
    case toolCallDelta(LLMToolCallDelta)
    case toolCallFinished(LLMToolCall)
    case usage(LLMUsage)
    case completed(LLMStreamCompletion)
    case keepAlive(provider: LLMStreamProvider)
    case error(LLMStreamFailure)
    case raw(LLMRawEvent)
}

public final class LLMStreamNormalizer {
    private let options: LLMStreamNormalizerOptions
    private var provider: LLMStreamProvider
    private var decoder: WireFrameDecoder
    private var responseId: String?
    private var latestUsage: LLMUsage?
    private var textChannels: [String: String] = [:]
    private var toolCalls: [String: ToolAccumulator] = [:]
    private var anthropicBlocks: [Int: AnthropicBlockState] = [:]
    private var pendingCompletionReason: String?
    private var syntheticToolCounter = 0
    private var didEmitCompletion = false

    public init(options: LLMStreamNormalizerOptions = .init()) {
        self.options = options
        self.provider = options.providerHint ?? .unknown
        self.decoder = WireFrameDecoder(mode: options.mode)
    }

    public func push(data: Data) throws -> [LLMStreamEvent] {
        try process(data: data, isFinal: false)
    }

    public func push(string: String) throws -> [LLMStreamEvent] {
        try process(data: Data(string.utf8), isFinal: false)
    }

    public func push<S: Sequence>(bytes: S) throws -> [LLMStreamEvent] where S.Element == UInt8 {
        try process(data: Data(bytes), isFinal: false)
    }

    public func finish() throws -> [LLMStreamEvent] {
        try process(data: Data(), isFinal: true)
    }

    public func snapshot() -> LLMStreamSnapshot {
        LLMStreamSnapshot(
            provider: provider,
            responseId: responseId,
            textChannels: textChannels,
            toolCalls: exportedToolCalls(),
            latestUsage: latestUsage
        )
    }

    private func process(data: Data, isFinal: Bool) throws -> [LLMStreamEvent] {
        let frames = try decoder.append(data, isFinal: isFinal)
        var events: [LLMStreamEvent] = []
        for frame in frames {
            events.append(contentsOf: try handle(frame))
        }
        if isFinal {
            events.append(contentsOf: finalizeAtEndOfStream())
        }
        return events
    }

    private func handle(_ frame: WireFrame) throws -> [LLMStreamEvent] {
        if frame.isCommentOnly {
            return [.keepAlive(provider: provider)]
        }

        let trimmedData = frame.data.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmedData.isEmpty {
            return []
        }

        if trimmedData == "[DONE]" {
            if provider == .unknown {
                provider = .openAIChatCompletions
            }
            return finalizeCompletion(reason: "done")
        }

        let jsonObject = try parseJSONObjectIfPresent(trimmedData)
        provider = selectProvider(current: provider, frame: frame, object: jsonObject)

        if let object = jsonObject {
            switch provider {
            case .openAIResponses:
                return handleOpenAIResponses(frame: frame, object: object)
            case .openAIChatCompletions:
                return handleOpenAIChat(frame: frame, object: object)
            case .anthropic:
                return handleAnthropic(frame: frame, object: object)
            case .genericJSON, .genericSSE, .unknown:
                break
            }
        }

        if frame.event == "ping" {
            return [.keepAlive(provider: provider)]
        }

        if options.emitRawEvents {
            return [
                .raw(
                    LLMRawEvent(
                        provider: provider,
                        transport: frame.transport.rawValue,
                        eventName: frame.event,
                        data: frame.data
                    )
                )
            ]
        }
        return []
    }

    private func handleOpenAIResponses(frame: WireFrame, object: [String: Any]) -> [LLMStreamEvent] {
        var events: [LLMStreamEvent] = []
        let eventType = stringValue(object["type"]) ?? frame.event ?? "unknown"

        if let response = objectValue(object["response"]),
           let id = stringValue(response["id"]) {
            responseId = id
        } else if let id = stringValue(object["response_id"]) ?? stringValue(object["id"]) {
            responseId = id
        }

        switch eventType {
        case "response.created":
            if let usage = extractOpenAIResponsesUsage(from: objectValue(object["response"])) {
                latestUsage = usage
                events.append(.usage(usage))
            }

        case "response.output_text.delta":
            let outputIndex = intValue(object["output_index"]) ?? 0
            let contentIndex = intValue(object["content_index"])
            let channel = "openai-responses:text:\(outputIndex):\(contentIndex ?? 0)"
            let delta = stringValue(object["delta"]) ?? stringValue(object["text"]) ?? ""
            if !delta.isEmpty {
                let textSoFar = appendText(delta, to: channel)
                events.append(
                    .textDelta(
                        LLMTextDelta(
                            provider: provider,
                            responseId: responseId,
                            channel: channel,
                            streamIndex: outputIndex,
                            delta: delta,
                            textSoFar: textSoFar
                        )
                    )
                )
            }

        case "response.output_item.added", "response.output_item.done":
            if let item = objectValue(object["item"]) {
                events.append(contentsOf: upsertOpenAIResponsesFunctionItem(item, eventType: eventType))
            }

        case "response.function_call_arguments.delta":
            let key = openAIResponsesToolKey(from: object)
            let delta = stringValue(object["delta"]) ?? ""
            if !delta.isEmpty {
                var accumulator = upsertToolAccumulator(
                    channel: key,
                    provider: .openAIResponses,
                    streamIndex: intValue(object["output_index"]),
                    preferredToolID: stringValue(object["call_id"]) ?? stringValue(object["item_id"]),
                    preferredToolName: nil
                )
                accumulator.append(delta)
                toolCalls[key] = accumulator
                events.append(.toolCallDelta(accumulator.exportDelta(responseId: responseId, fragment: delta)))
            }

        case "response.function_call_arguments.done":
            let key = openAIResponsesToolKey(from: object)
            var accumulator = upsertToolAccumulator(
                channel: key,
                provider: .openAIResponses,
                streamIndex: intValue(object["output_index"]),
                preferredToolID: stringValue(object["call_id"]) ?? stringValue(object["item_id"]),
                preferredToolName: nil
            )
            if let full = stringValue(object["arguments"]), !full.isEmpty {
                accumulator.replaceContents(with: full)
            }
            accumulator.isFinished = true
            toolCalls[key] = accumulator
            events.append(.toolCallFinished(accumulator.exportFinal(responseId: responseId)))

        case "response.completed":
            if let usage = extractOpenAIResponsesUsage(from: objectValue(object["response"]) ?? object) {
                latestUsage = usage
                events.append(.usage(usage))
            }
            events.append(contentsOf: finalizeCompletion(reason: "completed"))

        case "response.failed":
            let message = stringValue(objectValue(object["error"])?["message"]) ?? "OpenAI response stream failed"
            let code = stringValue(objectValue(object["error"])?["code"])
            events.append(
                .error(
                    LLMStreamFailure(
                        provider: provider,
                        responseId: responseId,
                        code: code,
                        message: message,
                        rawPayload: jsonString(from: object)
                    )
                )
            )

        default:
            if options.emitRawEvents {
                events.append(
                    .raw(
                        LLMRawEvent(
                            provider: provider,
                            transport: frame.transport.rawValue,
                            eventName: frame.event,
                            data: frame.data
                        )
                    )
                )
            }
        }

        return events
    }

    private func handleOpenAIChat(frame: WireFrame, object: [String: Any]) -> [LLMStreamEvent] {
        var events: [LLMStreamEvent] = []

        if let id = stringValue(object["id"]) {
            responseId = id
        }

        if let usage = extractOpenAIChatUsage(from: object) {
            latestUsage = usage
            events.append(.usage(usage))
        }

        for choice in objectArray(object["choices"]) {
            let choiceIndex = intValue(choice["index"]) ?? 0
            let choiceChannel = "openai-chat:choice:\(choiceIndex)"
            if let delta = objectValue(choice["delta"]) {
                if let content = stringValue(delta["content"]), !content.isEmpty {
                    let textSoFar = appendText(content, to: choiceChannel)
                    events.append(
                        .textDelta(
                            LLMTextDelta(
                                provider: provider,
                                responseId: responseId,
                                channel: choiceChannel,
                                streamIndex: choiceIndex,
                                delta: content,
                                textSoFar: textSoFar
                            )
                        )
                    )
                }

                for toolCall in objectArray(delta["tool_calls"]) {
                    let toolIndex = intValue(toolCall["index"]) ?? 0
                    let channel = "\(choiceChannel):tool:\(toolIndex)"
                    let functionObject = objectValue(toolCall["function"])
                    let preferredName = stringValue(functionObject?["name"])
                    let preferredID = stringValue(toolCall["id"])
                    var accumulator = upsertToolAccumulator(
                        channel: channel,
                        provider: .openAIChatCompletions,
                        streamIndex: toolIndex,
                        preferredToolID: preferredID,
                        preferredToolName: preferredName
                    )
                    let fragment = stringValue(functionObject?["arguments"]) ?? ""
                    if !fragment.isEmpty {
                        accumulator.append(fragment)
                        toolCalls[channel] = accumulator
                        events.append(.toolCallDelta(accumulator.exportDelta(responseId: responseId, fragment: fragment)))
                    } else {
                        toolCalls[channel] = accumulator
                    }
                }
            }

            if let finishReason = stringValue(choice["finish_reason"]), !finishReason.isEmpty {
                pendingCompletionReason = finishReason
                if options.finalizeToolCallsOnCompletion {
                    events.append(contentsOf: finalizeToolCalls(withPrefix: "\(choiceChannel):tool:"))
                }
            }
        }

        if options.emitRawEvents, events.isEmpty {
            events.append(
                .raw(
                    LLMRawEvent(
                        provider: provider,
                        transport: frame.transport.rawValue,
                        eventName: frame.event,
                        data: frame.data
                    )
                )
            )
        }

        return events
    }

    private func handleAnthropic(frame: WireFrame, object: [String: Any]) -> [LLMStreamEvent] {
        var events: [LLMStreamEvent] = []
        let eventType = frame.event ?? stringValue(object["type"]) ?? "unknown"

        switch eventType {
        case "message_start":
            if let message = objectValue(object["message"]) {
                responseId = stringValue(message["id"]) ?? responseId
                if let usage = extractAnthropicUsage(from: message) {
                    latestUsage = usage
                    events.append(.usage(usage))
                }
            }

        case "content_block_start":
            let index = intValue(object["index"]) ?? 0
            guard let block = objectValue(object["content_block"]) else { break }
            let blockType = stringValue(block["type"]) ?? "unknown"
            switch blockType {
            case "text":
                anthropicBlocks[index] = .text(channel: "anthropic:text:\(index)")
                if let text = stringValue(block["text"]), !text.isEmpty {
                    let channel = "anthropic:text:\(index)"
                    let textSoFar = appendText(text, to: channel)
                    events.append(
                        .textDelta(
                            LLMTextDelta(
                                provider: provider,
                                responseId: responseId,
                                channel: channel,
                                streamIndex: index,
                                delta: text,
                                textSoFar: textSoFar
                            )
                        )
                    )
                }

            case "tool_use":
                let channel = "anthropic:tool:\(index)"
                anthropicBlocks[index] = .tool(channel: channel)
                let toolID = stringValue(block["id"])
                let toolName = stringValue(block["name"])
                var accumulator = upsertToolAccumulator(
                    channel: channel,
                    provider: .anthropic,
                    streamIndex: index,
                    preferredToolID: toolID,
                    preferredToolName: toolName
                )
                if let input = block["input"],
                   let initialJSON = jsonString(from: input),
                   initialJSON != "{}",
                   accumulator.argumentsJSON.isEmpty {
                    accumulator.replaceContents(with: initialJSON)
                    events.append(.toolCallDelta(accumulator.exportDelta(responseId: responseId, fragment: initialJSON)))
                }
                toolCalls[channel] = accumulator

            default:
                break
            }

        case "content_block_delta":
            let index = intValue(object["index"]) ?? 0
            guard let delta = objectValue(object["delta"]) else { break }
            let deltaType = stringValue(delta["type"]) ?? "unknown"
            switch deltaType {
            case "text_delta":
                if case let .text(channel)? = anthropicBlocks[index] {
                    let text = stringValue(delta["text"]) ?? ""
                    if !text.isEmpty {
                        let textSoFar = appendText(text, to: channel)
                        events.append(
                            .textDelta(
                                LLMTextDelta(
                                    provider: provider,
                                    responseId: responseId,
                                    channel: channel,
                                    streamIndex: index,
                                    delta: text,
                                    textSoFar: textSoFar
                                )
                            )
                        )
                    }
                }

            case "input_json_delta":
                if case let .tool(channel)? = anthropicBlocks[index] {
                    let fragment = stringValue(delta["partial_json"]) ?? ""
                    if !fragment.isEmpty {
                        var accumulator = upsertToolAccumulator(
                            channel: channel,
                            provider: .anthropic,
                            streamIndex: index,
                            preferredToolID: nil,
                            preferredToolName: nil
                        )
                        accumulator.append(fragment)
                        toolCalls[channel] = accumulator
                        events.append(.toolCallDelta(accumulator.exportDelta(responseId: responseId, fragment: fragment)))
                    }
                }

            default:
                if options.emitRawEvents {
                    events.append(
                        .raw(
                            LLMRawEvent(
                                provider: provider,
                                transport: frame.transport.rawValue,
                                eventName: frame.event,
                                data: frame.data
                            )
                        )
                    )
                }
            }

        case "content_block_stop":
            let index = intValue(object["index"]) ?? 0
            if case let .tool(channel)? = anthropicBlocks[index],
               var accumulator = toolCalls[channel] {
                accumulator.isFinished = true
                toolCalls[channel] = accumulator
                events.append(.toolCallFinished(accumulator.exportFinal(responseId: responseId)))
            }
            anthropicBlocks.removeValue(forKey: index)

        case "message_delta":
            if let usage = extractAnthropicUsage(from: object) {
                latestUsage = usage
                events.append(.usage(usage))
            }
            if let delta = objectValue(object["delta"]),
               let stopReason = stringValue(delta["stop_reason"]),
               !stopReason.isEmpty {
                pendingCompletionReason = stopReason
            }

        case "message_stop":
            events.append(contentsOf: finalizeCompletion(reason: pendingCompletionReason ?? "message_stop"))

        case "ping":
            events.append(.keepAlive(provider: provider))

        case "error":
            let errorObject = objectValue(object["error"])
            let code = stringValue(errorObject?["type"])
            let message = stringValue(errorObject?["message"]) ?? "Anthropic stream error"
            events.append(
                .error(
                    LLMStreamFailure(
                        provider: provider,
                        responseId: responseId,
                        code: code,
                        message: message,
                        rawPayload: jsonString(from: object)
                    )
                )
            )

        default:
            if options.emitRawEvents {
                events.append(
                    .raw(
                        LLMRawEvent(
                            provider: provider,
                            transport: frame.transport.rawValue,
                            eventName: frame.event,
                            data: frame.data
                        )
                    )
                )
            }
        }

        return events
    }

    private func finalizeAtEndOfStream() -> [LLMStreamEvent] {
        if provider == .unknown && textChannels.isEmpty && toolCalls.isEmpty {
            return []
        }
        return finalizeCompletion(reason: pendingCompletionReason ?? "stream_end")
    }

    private func finalizeCompletion(reason: String?) -> [LLMStreamEvent] {
        if didEmitCompletion {
            return []
        }
        didEmitCompletion = true
        var events: [LLMStreamEvent] = []
        if options.finalizeToolCallsOnCompletion {
            events.append(contentsOf: finalizeToolCalls(withPrefix: nil))
        }
        events.append(
            .completed(
                LLMStreamCompletion(
                    provider: provider,
                    responseId: responseId,
                    reason: reason,
                    textChannels: textChannels,
                    toolCalls: exportedToolCalls()
                )
            )
        )
        return events
    }

    private func finalizeToolCalls(withPrefix prefix: String?) -> [LLMStreamEvent] {
        let keys = toolCalls.keys
            .filter { key in
                if let prefix {
                    return key.hasPrefix(prefix)
                }
                return true
            }
            .sorted()

        var events: [LLMStreamEvent] = []
        for key in keys {
            guard var accumulator = toolCalls[key], !accumulator.isFinished else {
                continue
            }
            accumulator.isFinished = true
            toolCalls[key] = accumulator
            events.append(.toolCallFinished(accumulator.exportFinal(responseId: responseId)))
        }
        return events
    }

    private func upsertOpenAIResponsesFunctionItem(_ item: [String: Any], eventType: String) -> [LLMStreamEvent] {
        guard let itemType = stringValue(item["type"]), itemType == "function_call" else {
            return []
        }

        let key = openAIResponsesToolKey(from: item)
        var accumulator = upsertToolAccumulator(
            channel: key,
            provider: .openAIResponses,
            streamIndex: intValue(item["output_index"]),
            preferredToolID: stringValue(item["call_id"]) ?? stringValue(item["id"]),
            preferredToolName: stringValue(item["name"])
        )

        if let rawArguments = stringValue(item["arguments"]), !rawArguments.isEmpty {
            accumulator.replaceContents(with: rawArguments)
        } else if let input = item["input"], let json = jsonString(from: input), json != "{}" {
            accumulator.replaceContents(with: json)
        }

        toolCalls[key] = accumulator

        if eventType.hasSuffix(".done") {
            var finished = accumulator
            finished.isFinished = true
            toolCalls[key] = finished
            return [.toolCallFinished(finished.exportFinal(responseId: responseId))]
        }

        return []
    }

    private func openAIResponsesToolKey(from object: [String: Any]) -> String {
        if let itemID = stringValue(object["item_id"]), !itemID.isEmpty {
            return "openai-responses:tool:item:\(itemID)"
        }
        if let callID = stringValue(object["call_id"]), !callID.isEmpty {
            return "openai-responses:tool:call:\(callID)"
        }
        if let id = stringValue(object["id"]), !id.isEmpty {
            return "openai-responses:tool:id:\(id)"
        }
        let outputIndex = intValue(object["output_index"]) ?? 0
        return "openai-responses:tool:output:\(outputIndex)"
    }

    private func upsertToolAccumulator(
        channel: String,
        provider: LLMStreamProvider,
        streamIndex: Int?,
        preferredToolID: String?,
        preferredToolName: String?
    ) -> ToolAccumulator {
        if var existing = toolCalls[channel] {
            if let preferredToolID, !preferredToolID.isEmpty {
                existing.toolCallId = preferredToolID
            }
            if let preferredToolName, !preferredToolName.isEmpty {
                existing.toolName = preferredToolName
            }
            if let streamIndex {
                existing.streamIndex = streamIndex
            }
            toolCalls[channel] = existing
            return existing
        }

        syntheticToolCounter += 1
        let toolCallId = preferredToolID?.isEmpty == false ? preferredToolID! : "tool_\(syntheticToolCounter)"
        let accumulator = ToolAccumulator(
            provider: provider,
            channel: channel,
            streamIndex: streamIndex,
            toolCallId: toolCallId,
            toolName: preferredToolName,
            argumentsJSON: "",
            tracker: JSONFragmentTracker(),
            isFinished: false
        )
        toolCalls[channel] = accumulator
        return accumulator
    }

    private func appendText(_ delta: String, to channel: String) -> String {
        textChannels[channel, default: ""].append(delta)
        return textChannels[channel] ?? delta
    }

    private func exportedToolCalls() -> [LLMToolCall] {
        toolCalls
            .values
            .sorted { lhs, rhs in lhs.channel < rhs.channel }
            .map { $0.exportFinal(responseId: responseId) }
    }

    private func parseJSONObjectIfPresent(_ text: String) throws -> [String: Any]? {
        guard let data = text.data(using: .utf8) else {
            return nil
        }

        let raw = try JSONSerialization.jsonObject(with: data)
        return raw as? [String: Any]
    }

    private func selectProvider(
        current: LLMStreamProvider,
        frame: WireFrame,
        object: [String: Any]?
    ) -> LLMStreamProvider {
        if let hint = options.providerHint {
            return hint
        }
        if current != .unknown {
            return current
        }

        if let event = frame.event, event.hasPrefix("response.") {
            return .openAIResponses
        }
        if let type = stringValue(object?["type"]), type.hasPrefix("response.") {
            return .openAIResponses
        }
        if objectValue(object?["message"]) != nil || objectValue(object?["delta"]) != nil {
            if let type = stringValue(object?["type"]),
               type.hasPrefix("message") || type.hasPrefix("content_block") || type == "error" {
                return .anthropic
            }
        }
        if objectArray(object?["choices"]).isEmpty == false {
            return .openAIChatCompletions
        }
        if frame.transport == .sse {
            return .genericSSE
        }
        if object != nil {
            return .genericJSON
        }
        return current
    }

    private func extractOpenAIResponsesUsage(from object: [String: Any]?) -> LLMUsage? {
        guard let usageObject = objectValue(object?["usage"]) else {
            return nil
        }
        let inputDetails = objectValue(usageObject["input_tokens_details"])
        let outputDetails = objectValue(usageObject["output_tokens_details"])
        return LLMUsage(
            inputTokens: intValue(usageObject["input_tokens"]),
            outputTokens: intValue(usageObject["output_tokens"]),
            totalTokens: intValue(usageObject["total_tokens"]),
            cacheCreationInputTokens: nil,
            cacheReadInputTokens: intValue(inputDetails?["cached_tokens"]),
            reasoningTokens: intValue(outputDetails?["reasoning_tokens"])
        )
    }

    private func extractOpenAIChatUsage(from object: [String: Any]) -> LLMUsage? {
        guard let usageObject = objectValue(object["usage"]) else {
            return nil
        }
        return LLMUsage(
            inputTokens: intValue(usageObject["prompt_tokens"]),
            outputTokens: intValue(usageObject["completion_tokens"]),
            totalTokens: intValue(usageObject["total_tokens"]),
            cacheCreationInputTokens: nil,
            cacheReadInputTokens: intValue(objectValue(usageObject["prompt_tokens_details"])?["cached_tokens"]),
            reasoningTokens: intValue(objectValue(usageObject["completion_tokens_details"])?["reasoning_tokens"])
        )
    }

    private func extractAnthropicUsage(from object: [String: Any]) -> LLMUsage? {
        guard let usageObject = objectValue(object["usage"]) else {
            return nil
        }
        return LLMUsage(
            inputTokens: intValue(usageObject["input_tokens"]),
            outputTokens: intValue(usageObject["output_tokens"]),
            totalTokens: sumInts(intValue(usageObject["input_tokens"]), intValue(usageObject["output_tokens"])),
            cacheCreationInputTokens: intValue(usageObject["cache_creation_input_tokens"]),
            cacheReadInputTokens: intValue(usageObject["cache_read_input_tokens"]),
            reasoningTokens: nil
        )
    }

    private func sumInts(_ lhs: Int?, _ rhs: Int?) -> Int? {
        switch (lhs, rhs) {
        case let (left?, right?):
            return left + right
        case let (left?, nil):
            return left
        case let (nil, right?):
            return right
        case (nil, nil):
            return nil
        }
    }
}

private enum WireTransport: String {
    case sse
    case ndjson
}

private struct WireFrame {
    var transport: WireTransport
    var event: String?
    var data: String
    var isCommentOnly: Bool
}

private struct PendingSSEFrame {
    var event: String?
    var dataLines: [String] = []
    var sawField = false
    var sawComment = false

    var isEmpty: Bool {
        !sawField && !sawComment && dataLines.isEmpty && event == nil
    }

    mutating func reset() {
        event = nil
        dataLines.removeAll(keepingCapacity: true)
        sawField = false
        sawComment = false
    }
}

private final class WireFrameDecoder {
    private let mode: LLMStreamMode
    private var buffer = Data()
    private var pendingSSE = PendingSSEFrame()
    private var lockedTransport: WireTransport?

    init(mode: LLMStreamMode) {
        self.mode = mode
        switch mode {
        case .auto:
            lockedTransport = nil
        case .sse:
            lockedTransport = .sse
        case .ndjson:
            lockedTransport = .ndjson
        }
    }

    func append(_ chunk: Data, isFinal: Bool) throws -> [WireFrame] {
        buffer.append(chunk)
        var frames: [WireFrame] = []
        while let newlineRange = buffer.firstRange(of: Data([0x0A])) {
            var lineData = buffer.subdata(in: 0..<newlineRange.lowerBound)
            buffer.removeSubrange(0...newlineRange.lowerBound)
            if lineData.last == 0x0D {
                lineData.removeLast()
            }
            let line = try decodeLine(lineData)
            frames.append(contentsOf: process(line: line))
        }

        if isFinal {
            if !buffer.isEmpty {
                let line = try decodeLine(buffer)
                frames.append(contentsOf: process(line: line))
                buffer.removeAll(keepingCapacity: true)
            }
            if lockedTransport == .sse, !pendingSSE.isEmpty {
                frames.append(flushSSE())
            }
        }

        return frames
    }

    private func decodeLine(_ data: Data) throws -> String {
        guard let line = String(data: data, encoding: .utf8) else {
            throw NSError(domain: "LLMStreamNormalizer", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "Encountered non-UTF8 stream content"
            ])
        }
        return line
    }

    private func process(line: String) -> [WireFrame] {
        if lockedTransport == nil {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("event:") || trimmed.hasPrefix("data:") || trimmed.hasPrefix("id:") || trimmed.hasPrefix(":") {
                lockedTransport = .sse
            } else if !trimmed.isEmpty {
                lockedTransport = .ndjson
            }
        }

        switch lockedTransport ?? .ndjson {
        case .sse:
            return processSSE(line: line)
        case .ndjson:
            let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else { return [] }
            return [
                WireFrame(
                    transport: .ndjson,
                    event: nil,
                    data: trimmed,
                    isCommentOnly: false
                )
            ]
        }
    }

    private func processSSE(line: String) -> [WireFrame] {
        if line.isEmpty {
            guard !pendingSSE.isEmpty else { return [] }
            return [flushSSE()]
        }

        if line.hasPrefix(":") {
            pendingSSE.sawComment = true
            return []
        }

        let parts = line.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)
        let field = String(parts[0])
        let value = parts.count > 1 ? String(parts[1]).trimmingPrefix(" ") : ""
        pendingSSE.sawField = true

        switch field {
        case "event":
            pendingSSE.event = value
        case "data":
            pendingSSE.dataLines.append(value)
        default:
            break
        }

        return []
    }

    private func flushSSE() -> WireFrame {
        let frame = WireFrame(
            transport: .sse,
            event: pendingSSE.event,
            data: pendingSSE.dataLines.joined(separator: "\n"),
            isCommentOnly: pendingSSE.sawComment && pendingSSE.dataLines.isEmpty
        )
        pendingSSE.reset()
        return frame
    }
}

private struct ToolAccumulator {
    var provider: LLMStreamProvider
    var channel: String
    var streamIndex: Int?
    var toolCallId: String
    var toolName: String?
    var argumentsJSON: String
    var tracker: JSONFragmentTracker
    var isFinished: Bool

    mutating func append(_ fragment: String) {
        argumentsJSON.append(fragment)
        tracker.append(fragment)
    }

    mutating func replaceContents(with fullJSON: String) {
        argumentsJSON = fullJSON
        tracker = JSONFragmentTracker(seededWith: fullJSON)
    }

    func exportDelta(responseId: String?, fragment: String) -> LLMToolCallDelta {
        LLMToolCallDelta(
            provider: provider,
            responseId: responseId,
            channel: channel,
            streamIndex: streamIndex,
            toolCallId: toolCallId,
            toolName: toolName,
            argumentsFragment: fragment,
            argumentsSoFar: argumentsJSON,
            argumentsAreLikelyComplete: tracker.isLikelyComplete
        )
    }

    func exportFinal(responseId: String?) -> LLMToolCall {
        LLMToolCall(
            provider: provider,
            responseId: responseId,
            channel: channel,
            streamIndex: streamIndex,
            toolCallId: toolCallId,
            toolName: toolName,
            argumentsJSON: argumentsJSON,
            argumentsAreLikelyComplete: tracker.isLikelyComplete
        )
    }
}

private enum AnthropicBlockState {
    case text(channel: String)
    case tool(channel: String)
}

private struct JSONFragmentTracker: Sendable {
    private var nestingDepth = 0
    private var isInString = false
    private var isEscaping = false
    private var sawNonWhitespace = false

    init() {}

    init(seededWith text: String) {
        append(text)
    }

    mutating func append(_ fragment: String) {
        for scalar in fragment.unicodeScalars {
            if isEscaping {
                isEscaping = false
                continue
            }

            if isInString {
                if scalar == "\\" {
                    isEscaping = true
                } else if scalar == "\"" {
                    isInString = false
                }
                continue
            }

            switch scalar {
            case "\"":
                sawNonWhitespace = true
                isInString = true
            case "{", "[":
                sawNonWhitespace = true
                nestingDepth += 1
            case "}", "]":
                sawNonWhitespace = true
                nestingDepth = max(0, nestingDepth - 1)
            default:
                if !CharacterSet.whitespacesAndNewlines.contains(scalar) {
                    sawNonWhitespace = true
                }
            }
        }
    }

    var isLikelyComplete: Bool {
        sawNonWhitespace && !isInString && nestingDepth == 0
    }
}

private func objectValue(_ value: Any?) -> [String: Any]? {
    value as? [String: Any]
}

private func objectArray(_ value: Any?) -> [[String: Any]] {
    value as? [[String: Any]] ?? []
}

private func stringValue(_ value: Any?) -> String? {
    switch value {
    case let string as String:
        return string
    case let number as NSNumber:
        return number.stringValue
    default:
        return nil
    }
}

private func intValue(_ value: Any?) -> Int? {
    switch value {
    case let int as Int:
        return int
    case let number as NSNumber:
        return number.intValue
    case let string as String:
        return Int(string)
    default:
        return nil
    }
}

private func jsonString(from value: Any) -> String? {
    if let string = value as? String {
        return string
    }
    if let number = value as? NSNumber {
        return number.stringValue
    }
    if value is NSNull {
        return "null"
    }
    guard JSONSerialization.isValidJSONObject(value) else {
        return nil
    }
    guard let data = try? JSONSerialization.data(withJSONObject: value, options: [.sortedKeys]) else {
        return nil
    }
    return String(data: data, encoding: .utf8)
}

private extension String {
    func trimmingPrefix(_ prefix: String) -> String {
        guard hasPrefix(prefix) else { return self }
        return String(dropFirst(prefix.count))
    }
}

/*
This solves the annoying part of streaming LLM integrations on Apple platforms: every provider sends a slightly different wire format, and tool-call arguments often arrive as broken JSON fragments spread across many events. Built because I got tired of rewriting the same fragile SSE parsing and argument assembly code every time a Swift app needed OpenAI, Anthropic, or both.

Use it when you are reading `URLSession.AsyncBytes`, server-sent events, or newline-delimited JSON from model APIs and you want one clean event model for text deltas, tool calls, usage, completion, and failures. The trick: parse the transport once, detect the provider from real payload shape, then keep a stateful accumulator that can rebuild tool-call arguments without assuming each chunk is valid JSON on its own.

Drop this into a macOS app, iOS client, Vapor service, local agent runner, or an internal SDK layer where model streams need to look boring and predictable for the rest of the codebase.
*/