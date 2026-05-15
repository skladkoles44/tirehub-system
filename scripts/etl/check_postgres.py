#!/usr/bin/env python3
"""
Проверка подключения к PostgreSQL
Использует DB_CONN из переменных окружения
"""

import os
import sys
import psycopg2

DB_CONN = os.getenv("DB_CONN", "dbname=canonical user=canonical host=/var/run/postgresql")

print(f"🔌 Проверяю подключение: {DB_CONN}")

try:
    conn = psycopg2.connect(DB_CONN)
    with conn.cursor() as cur:
        cur.execute("SELECT version(), current_database(), current_user, inet_server_addr(), inet_server_port()")
        version, db, user, host, port = cur.fetchone()
        print(f"✅ Подключение успешно!")
        print(f"   PostgreSQL: {version.split(',')[0]}")
        print(f"   База данных: {db}")
        print(f"   Пользователь: {user}")
        print(f"   Хост/порт: {host}:{port}")
    conn.close()
    sys.exit(0)
except psycopg2.OperationalError as e:
    print(f"❌ Ошибка подключения: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Неизвестная ошибка: {e}")
    sys.exit(1)
