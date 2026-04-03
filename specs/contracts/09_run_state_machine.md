# State Machine Run

## Состояния
| Состояние | Описание | Terminal |
|-----------|----------|----------|
| received | Данные получены | ❌ |
| archived | Raw сохранён | ❌ |
| extracted | Структурное извлечение выполнено | ❌ |
| normalized | Нормализация выполнена | ❌ |
| validated | Quality gates применены | ❌ |
| applied_to_current | Обновлён supplier_offers_current | ❌ |
| published | Обновлён marketplace_offers_current | ✅ |
| blocked | Заблокирован и требует manual review | ✅ |
| failed | Ошибка, run завершён неуспешно | ✅ |

## Переходы
- `received -> archived`: после сохранения raw payload
- `archived -> extracted`: после успешного structural extraction
- `extracted -> normalized`: после успешной нормализации
- `normalized -> validated`: после применения quality gates
- `validated -> applied_to_current`: после update current layer
- `applied_to_current -> published`: после matching и publish
- `любое -> failed`: при критической ошибке
- `любое -> blocked`: при результате gate action = QUARANTINE

## Retry policy
- Retry допускается только из terminal states `blocked` или `failed` через новый run.
- Не допускается мутирование старого run.
- История старого run сохраняется как факт.
