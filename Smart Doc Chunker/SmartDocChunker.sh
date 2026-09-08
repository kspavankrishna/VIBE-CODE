#!/bin/bash
set -euo pipefail

INPUT_DIR="${1:-.}"
OUTPUT_DIR="${2:-./chunks}"
MAX_TOKENS="${3:-2000}"
MANIFEST="$OUTPUT_DIR/manifest.json"

mkdir -p "$OUTPUT_DIR"

estimate_tokens() {
  local text="$1"
  echo $((${#text} / 4))
}

chunk_file() {
  local file="$1"
  local base=$(basename "$file" | sed 's/\.[^.]*$//')
  local chunk_id=0
  local current_chunk=""
  local current_tokens=0
  local file_chunks=()

  while IFS= read -r line || [[ -n "$line" ]]; do
    local line_tokens=$(estimate_tokens "$line")
    
    if (( current_tokens + line_tokens > MAX_TOKENS )) && [[ -n "$current_chunk" ]]; then
      local chunk_file="$OUTPUT_DIR/${base}_chunk_${chunk_id}.txt"
      echo "$current_chunk" > "$chunk_file"
      file_chunks+=("$chunk_file")
      ((chunk_id++))
      current_chunk="$line"
      current_tokens=$line_tokens
    else
      current_chunk+=$'\n'"$line"
      ((current_tokens += line_tokens))
    fi
  done < "$file"

  if [[ -n "$current_chunk" ]]; then
    local chunk_file="$OUTPUT_DIR/${base}_chunk_${chunk_id}.txt"
    echo "$current_chunk" > "$chunk_file"
    file_chunks+=("$chunk_file")
  fi

  echo "${file_chunks[@]}"
}

build_manifest() {
  echo "{"
  echo '  "chunks": ['
  
  local first=true
  for chunk in "$OUTPUT_DIR"/*.txt; do
    [[ -e "$chunk" ]] || continue
    
    if [[ "$first" == false ]]; then
      echo ","
    fi
    first=false
    
    local token_count=$(estimate_tokens "$(cat "$chunk")")
    local byte_count=$(wc -c < "$chunk")
    
    echo -n "    {\"path\": \"$(basename "$chunk")\", \"tokens\": $token_count, \"bytes\": $byte_count}"
  done
  
  echo ""
  echo "  ]"
  echo "}"
}

find "$INPUT_DIR" -maxdepth 1 -type f \( -name "*.txt" -o -name "*.md" -o -name "*.log" \) | while read -r file; do
  chunk_file "$file" > /dev/null
done

build_manifest > "$MANIFEST"
echo "✓ Chunked documents to $OUTPUT_DIR"
echo "✓ Manifest: $MANIFEST"
echo "✓ Max tokens per chunk: $MAX_TOKENS"

================================================================================
# This solves the friction of prepping document collections for RAG pipelines.
# Built because every time you're setting up semantic search, you hit the wall:
# how do you chunk without blowing your token budget or breaking context at the
# wrong boundary? Use it when you're loading files into a vector database—respects
# token limits per chunk and spits out manifest.json with exact token counts.
# The trick: walks through files line-by-line, accumulates text until the next
# line would exceed budget, then flushes the chunk. Drop this into your RAG setup
# where you're preparing knowledge bases for Claude or your vector store.
================================================================================
