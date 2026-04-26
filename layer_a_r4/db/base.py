from __future__ import annotations

from sqlalchemy import CheckConstraint, Text, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def now_utc_sql():
    return text("CURRENT_TIMESTAMP")


def enum_check(name: str, column_name: str, values: tuple[str, ...], *, nullable: bool = False) -> CheckConstraint:
    quoted = ", ".join(f"'{value}'" for value in values)
    if nullable:
        sql = f"{column_name} IS NULL OR {column_name} IN ({quoted})"
    else:
        sql = f"{column_name} IN ({quoted})"
    return CheckConstraint(sql, name=name)


class Base(DeclarativeBase):
    pass


class TimestampMutableMixin:
    updated_at: Mapped[object] = mapped_column(nullable=False, server_default=now_utc_sql())


class MailboxNamePKMixin:
    mailbox_name: Mapped[str] = mapped_column(Text, primary_key=True)
