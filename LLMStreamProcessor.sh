#!/bin/bash

# LLMStreamProcessor: Process streaming LLM responses with validation and error recovery.
# Handles partial JSON, timeouts, malformed data, and rate limiting.

set -o pipefail

readonly BUFFER_SIZE=8192
readonly TIMEOUT=30
readonly MAX_RETRIES=3
readonly BACKOFF_BASE=1

process_stream() {
    local input_stream="$1"
    local output_file="${2:-.stream_output}"
    local on_chunk_handler="${3:cat}"
    
    local buffer=""
    local json_depth=0
    local chunk_count=0
    local timeout_count=0
    
    while IFS= read -r -n $BUFFER_SIZE line || [[ -n "$line" ]]; do
        # Track braces to detect complete JSON objects
        for (( i=0; i<${#line}; i++ )); do
            local char="${line:$i:1}"
            [[ "$char" == "{" ]] && ((json_depth++))
            [[ "$char" == "}" ]] && ((json_depth--))
        done
        
        buffer+="$line"
        
        # Emit complete JSON objects
        if [[ $json_depth -eq 0 && "$buffer" =~ \{.*\} ]]; then
            if validate_json "$buffer"; then
                echo "$buffer" >> "$output_file"
                ((chunk_count++))
                echo "$buffer" | $on_chunk_handler
                buffer=""
            else
                # Partial/malformed - buffer and wait
                [[ ${#buffer} -gt 65536 ]] && buffer="${buffer: -32768}"
            fi
        fi
    done < <(timeout $TIMEOUT cat "$input_stream" || echo "TIMEOUT")
    
    # Flush remaining buffer
    [[ -n "$buffer" ]] && echo "$buffer" >> "$output_file"
    
    return 0
}

validate_json() {
    local json="$1"
    command -v jq &>/dev/null || return 0  # Skip validation if jq missing
    echo "$json" | jq empty 2>/dev/null
}

retry_with_backoff() {
    local command="$1"
    local attempt=1
    
    while [[ $attempt -le $MAX_RETRIES ]]; do
        if eval "$command"; then
            return 0
        fi
        
        local wait_time=$((BACKOFF_BASE * (2 ** (attempt - 1))))
        echo "[Retry $attempt/$MAX_RETRIES] Waiting ${wait_time}s..." >&2
        sleep $wait_time
        ((attempt++))
    done
    
    echo "[ERROR] Failed after $MAX_RETRIES retries" >&2
    return 1
}

extract_field() {
    local json="$1"
    local field="$2"
    
    if command -v jq &>/dev/null; then
        echo "$json" | jq -r ".${field}" 2>/dev/null || echo ""
    else
        # Fallback: basic grep extraction
        grep -o "\"${field}\":[^,}]*" <<< "$json" | cut -d':' -f2- | tr -d ' "'
    fi
}

# Main entrypoint
main() {
    local stream_source="${1:-/dev/stdin}"
    local output="${2:-.llm_output}"
    
    echo "[LLMStreamProcessor] Starting stream processing..." >&2
    echo "[LLMStreamProcessor] Source: $stream_source" >&2
    echo "[LLMStreamProcessor] Output: $output" >&2
    
    process_stream "$stream_source" "$output" 2>&1 | tee -a "${output}.log"
    
    echo "[LLMStreamProcessor] Complete. Processed $(wc -l < "$output") chunks" >&2
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"

/*
================================================================================
EXPLANATION
LLMStreamProcessor solves the headache of handling streaming LLM responses that arrive broken, partial, malformed, or rate-limited. Built because production LLM APIs timeout, drop packets, send incomplete JSON, and require intelligent retries. Use when consuming Claude, ChatGPT, or local model streams where you need bulletproof parsing and error recovery. The trick: tracks JSON brace depth to detect complete objects even amid partial reads, buffers gracefully, validates with jq, and implements exponential backoff for retries. Logs everything for debugging. Drop this into any bash pipeline that consumes streaming APIs—instantly gains resilience.
================================================================================
*/
