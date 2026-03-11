#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$REPO_ROOT" ] || { echo "Repo root not found"; exit 1; }
cd "$REPO_ROOT"
INBOX="inputs/inbox/Centrshin"

# Самый свежий json (без find -printf, совместимо с Termux)
current="$(
  find "$INBOX" -name "*.json" -type f -exec stat -c "%Y %n" {} \; 2>/dev/null \
  | sort -rn | head -n1 | cut -d' ' -f2-
)"

[ -f "${current:-}" ] || { echo "❌ No JSON files in $INBOX"; exit 1; }

echo "📄 $(basename "$current")"
echo

# Все массивы верхнего уровня (категории) + размер
jq -r '
  to_entries[]
  | select(.value | type == "array")
  | "  • \(.key): \(.value|length) items"
' "$current"

echo
jq -r '
  "📊 Summary:",
  "  Total keys: \(length)",
  "  Arrays: \([.[] | select(type=="array")] | length)",
  "  Objects: \([.[] | select(type=="object")] | length)",
  "  Values: \([.[] | select(type!="array" and type!="object")] | length)",
  "",
  "📈 Array statistics:",
  ("  Total items in all arrays: " + (([.[] | select(type=="array") | length] | add // 0) | tostring)),
  ("  Number of arrays: " + (([.[] | select(type=="array")] | length) | tostring)),
  ("  Average per array: " + (
      ([.[] | select(type=="array") | length] | add // 0) as $sum
      | ([.[] | select(type=="array")] | length) as $n
      | (if $n==0 then 0 else ($sum / $n | floor) end | tostring)
    ))
' "$current"

echo
echo "✅ Done"
