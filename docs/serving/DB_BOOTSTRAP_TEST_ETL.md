DB bootstrap (TEST)

Цель: создать Postgres DB + user для serving-слоя tirehub.

Файл:
etl_ops/provision/create_db_tirehub_v1.sh

Гарантии:
- идемпотентно создаёт role/db (или ротирует пароль user, если уже есть)
- печатает DB_URL для использования в apply_to_postgres_v1.py

Запуск (на сервере, где есть права выполнить psql как postgres):
./etl_ops/provision/create_db_tirehub_v1.sh

Опциональные env:
DB_NAME, DB_USER, DB_HOST, DB_PORT
