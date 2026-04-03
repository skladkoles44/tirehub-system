# Типы источников

## Поддерживаемые каналы
- API polling
- Webhook
- Direct file upload
- URL download
- Email attachment
- Folder pickup / file drop

## Intake Contract
После intake все источники становятся единой сущностью `incoming_source_object` со следующими полями:
- `source_type`
- `source_id`
- `payload`
- `received_at`
- `run_id`

## Правила
- `payload` хранит оригинальный объект источника без бизнес-нормализации.
- После intake различие между API, webhook, file upload и другими каналами не должно влиять на downstream contract.
- Каждый incoming object должен быть трассируем к конкретному run.
