"""Read and write the reference SDK's application errors for the Rust wire test."""

from __future__ import annotations

import json
import sys

from roomkit.turns._error_codec import ErrorClasses, error_record
from roomkit.turns.wire import ErrorRecord


class Quota(Exception):
    __module__ = "billing"

    def __init__(self, limit: int) -> None:
        self.limit = limit
        super().__init__(limit)

    def __str__(self) -> str:
        return f"quota {self.limit}"


class PlanQuota(Quota):
    pass


def emit() -> None:
    try:
        raise Quota(4)
    except Quota as cause:
        try:
            error = PlanQuota(23)
            error.plan = "premium"
            raise error from cause
        except PlanQuota as raised:
            print(error_record(raised).model_dump_json())


def receive() -> None:
    codec = ErrorClasses([PlanQuota, Quota])
    records = json.load(sys.stdin)
    relayed, native = [codec.rebuild(ErrorRecord.model_validate(row)) for row in records]
    assert type(relayed) is PlanQuota and relayed.limit == 23 and relayed.plan == "premium"
    assert type(relayed.__cause__) is Quota and relayed.__cause__.limit == 4
    assert type(native) is Quota and native.limit == 7


if __name__ == "__main__":
    if sys.argv[1] == "emit":
        emit()
    else:
        receive()
