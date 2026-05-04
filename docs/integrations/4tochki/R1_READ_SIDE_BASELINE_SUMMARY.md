# 4tochki R1 read-side baseline — стартовая точка рабочей ветки

## Статус
`4tochki R1 read-side baseline` закрыт на уровне runtime + clean-clone reproducibility.

**Базовый commit:** `e51047980f573b69b2832fbd82104a72274fb9df`

**Clean gate:** 9 passed in 20.30s, RC=0

## Что уже есть
- read-only adapter: `src/integrations/fourtochki/client.py`
- typed models/filters: `src/integrations/fourtochki/models.py`
- capture helper: `scripts/probes/capture_4tochki_probe.py`
- probe tests: `tests/probes/test_4tochki_read_baseline.py`
- probe inputs: `tests/probes/probe_inputs.json`
- regression runner: `scripts/ops/4tochki_read_baseline.sh`
- baseline doc: `docs/integrations/4tochki/baseline.md`
- local manifest: `docs/integrations/4tochki/MANIFEST.local.sha256`

## Execution Contract
- `pythonpath = src` в pytest.ini
- `.env.4tochki` в корне проекта

## Следующие задачи
1. Artifact hash binding
2. Execution contract фиксация
3. Warehouse fallback
4. Negative test
5. Wrapper discipline
6. Lightweight monitoring

## Остаточные риски
- API drift высокий
- warehouse volatility средний
- CI отсутствует
- artifact binding ещё не сделан
