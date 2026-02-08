#!/bin/bash
set -euo pipefail
cd "/storage/emulated/0/Download/ETL/tirehub-system" || exit 1
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
