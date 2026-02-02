-- v1.4a fix: расширяем CHECK constraint для совместимости с legacy 'active'
BEGIN;

-- Удаляем старый constraint
ALTER TABLE ssot_curated_internal.curated_artifacts 
DROP CONSTRAINT IF EXISTS curated_artifacts_status_chk;

-- Добавляем новый с включением 'active'
ALTER TABLE ssot_curated_internal.curated_artifacts 
ADD CONSTRAINT curated_artifacts_status_chk 
CHECK (status_v14a = ANY (ARRAY['generated', 'published', 'archived', 'active']));

COMMIT;
