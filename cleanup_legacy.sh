#!/bin/bash
# cleanup_legacy.sh - реальная очистка с копированием в буфер

cd ${ETL_REPO_ROOT:-$(pwd)}

# Файл для сохранения вывода
OUTPUT_FILE="$ETL_VAR_ROOT/logs/cleanup.log"
> "$OUTPUT_FILE"

# Функция для дублирования вывода
log() {
    echo "$1" | tee -a "$OUTPUT_FILE"
}

log "========================================="
log "🧹 LEGACY CLEANUP - EXECUTING"
log "$(date)"
log "========================================="
log ""

# 1. Создаём легаси-хранилище
log "📦 1. Creating legacy storage..."
mkdir -p legacy/{archive,old_scripts,old_configs,old_docs,snapshots}
log "   ✅ legacy/ created"

# 2. Перемещаем scripts/archive
log ""
log "📦 2. Moving scripts/archive..."
if [ -d "scripts/archive" ]; then
    mv scripts/archive/* legacy/archive/ 2>/dev/null
    rmdir scripts/archive 2>/dev/null
    log "   ✅ scripts/archive moved ($(ls legacy/archive/ | wc -l) files)"
else
    log "   ⚠️ scripts/archive not found"
fi

# 3. Перемещаем старый normalizer
log ""
log "📦 3. Moving old normalizer..."
if [ -f "scripts/normalization/normalizer_v1.py" ]; then
    mv scripts/normalization/normalizer_v1.py legacy/old_scripts/
    log "   ✅ normalizer_v1.py moved"
else
    log "   ⚠️ normalizer_v1.py not found"
fi

# 4. Перемещаем старый enricher
log ""
log "📦 4. Moving old enricher..."
if [ -f "scripts/enrichment/brinex_parsed_enricher_v1.py" ]; then
    mv scripts/enrichment/brinex_parsed_enricher_v1.py legacy/old_scripts/
    log "   ✅ brinex_parsed_enricher_v1.py moved"
else
    log "   ⚠️ brinex_parsed_enricher_v1.py not found"
fi

# 5. Перемещаем старые конфиги
log ""
log "📦 5. Moving old configs..."
OLD_CONFIGS=$(find config/ -type f \( -name "*.old" -o -name "*.backup" -o -name "*_legacy*" \) 2>/dev/null | wc -l)
if [ $OLD_CONFIGS -gt 0 ]; then
    find config/ -type f \( -name "*.old" -o -name "*.backup" -o -name "*_legacy*" \) 2>/dev/null | while read f; do
        mv "$f" legacy/old_configs/
    done
    log "   ✅ $OLD_CONFIGS old configs moved"
else
    log "   ⚠️ no old configs found"
fi

# 6. Создаём снапшот
log ""
log "📸 6. Creating snapshot..."
SNAPSHOT_DIR="legacy/snapshots/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$SNAPSHOT_DIR"
cp -r scripts/ "$SNAPSHOT_DIR/" 2>/dev/null
cp -r config/ "$SNAPSHOT_DIR/" 2>/dev/null
cp -r common/ "$SNAPSHOT_DIR/" 2>/dev/null
log "   ✅ Snapshot saved to $SNAPSHOT_DIR"

# 7. Создаём README
log ""
log "📄 7. Creating README..."
cat > legacy/README.md << 'EOF2'
# Legacy Code Archive

This directory contains archived code that is no longer used in the active pipeline.

## Structure
- `archive/` — old scripts/archive content
- `old_scripts/` — old script versions (v1, old enrichers)
- `old_configs/` — deprecated configurations
- `snapshots/` — snapshots of state at archive time

## Archive Date
$(date -Iseconds)

## When to Delete
- After 3 months of stable new pipeline operation
- Or keep indefinitely as safety net
EOF2
log "   ✅ README created"

# 8. Создаём чистые директории
log ""
log "📁 8. Creating fresh directories..."
mkdir -p scripts/enrichment
mkdir -p scripts/dq
mkdir -p scripts/matching
mkdir -p drop/{inbox,processing,done,failed}
mkdir -p var/artifacts
mkdir -p logs
log "   ✅ scripts/enrichment/"
log "   ✅ scripts/dq/"
log "   ✅ scripts/matching/"
log "   ✅ drop/{inbox,processing,done,failed}/"
log "   ✅ var/artifacts/"
log "   ✅ logs/"

# 9. Удаляем пустые директории
log ""
log "🧹 9. Removing empty directories..."
find scripts/ -type d -empty -delete 2>/dev/null
find config/ -type d -empty -delete 2>/dev/null
log "   ✅ Empty directories removed"

# 10. Итоговая статистика
log ""
log "========================================="
log "📊 CLEANUP SUMMARY"
log "========================================="
log ""
log "📁 scripts/etl/: $(ls scripts/etl/ 2>/dev/null | wc -l) files"
log "📁 scripts/normalization/: $(ls scripts/normalization/ 2>/dev/null | wc -l) files"
log "📁 scripts/enrichment/: $(ls scripts/enrichment/ 2>/dev/null | wc -l) files"
log "📁 common/: $(ls common/ 2>/dev/null | wc -l) files"
log ""
log "📦 legacy size: $(du -sh legacy/ 2>/dev/null | cut -f1)"
log "📦 legacy/old_scripts/: $(ls legacy/old_scripts/ 2>/dev/null | wc -l) files"
log "📦 legacy/archive/: $(ls legacy/archive/ 2>/dev/null | wc -l) files"
log "📦 snapshot: $SNAPSHOT_DIR"
log ""
log "📁 drop/:"
ls -la drop/ 2>/dev/null | tail -6 | while read line; do log "   $line"; done
log ""
log "📁 var/:"
ls -la var/ 2>/dev/null | tail -4 | while read line; do log "   $line"; done
log ""
log "========================================="
log "✅ CLEANUP COMPLETE"
log "========================================="

# Копируем в буфер обмена Termux
if command -v termux-clipboard-set &>/dev/null; then
    cat "$OUTPUT_FILE" | termux-clipboard-set
    echo ""
    echo "📋 ✅ Result copied to Termux clipboard!"
    echo "   You can paste (Ctrl+V) anywhere"
else
    echo ""
    echo "⚠️ termux-clipboard-set not found"
    echo "   Output saved to: $OUTPUT_FILE"
fi

echo ""
echo "========================================="
