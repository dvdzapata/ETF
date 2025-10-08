"""Data validation helpers."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Callable, Iterable


def is_valid_date(value: str | None) -> bool:
    if not value:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


_PERCENT_PATTERN = re.compile(r"\s*%\s*$")


def parse_decimal(value: Any) -> Decimal | None:
    if value in (None, "", "null"):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if _PERCENT_PATTERN.search(text):
            text = _PERCENT_PATTERN.sub("", text)
        text = text.strip()
        if not text:
            return None
        if "," in text and "." in text:
            text = text.replace(",", "")
        elif "," in text:
            text = text.replace(",", ".")
        text = text.replace(" ", "")
        try:
            return Decimal(text)
        except (InvalidOperation, ValueError):
            return None
    return None


def in_range(value: Decimal | None, minimum: Decimal | float, maximum: Decimal | float) -> bool:
    if value is None:
        return True
    return Decimal(minimum) <= value <= Decimal(maximum)


def filter_records(
    records: Iterable[dict],
    *,
    validators: Iterable[tuple[str, Callable[[Any], bool], str]],
    rejection_logger,
    context: dict,
) -> list[dict]:
    valid_records: list[dict] = []
    for record in records:
        record_valid = True
        for field, validator, message in validators:
            value = record.get(field)
            if not validator(value):
                rejection_logger.warning(
                    "Rejected record for %(symbol)s | %(field)s=%(value)s | %(message)s | context=%(context)s",
                    {
                        "symbol": context.get("symbol"),
                        "field": field,
                        "value": value,
                        "message": message,
                        "context": {**context, "record": record},
                    },
                )
                record_valid = False
                break
        if record_valid:
            valid_records.append(record)
    return valid_records


__all__ = [
    "is_valid_date",
    "parse_decimal",
    "in_range",
    "filter_records",
]
