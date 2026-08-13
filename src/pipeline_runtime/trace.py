"""The run trace from part 1 § 3.

Every line is one inbound metadata kind being resolved, or the outbound event
that resolution makes possible. Nothing in it is specific to any domain, which
is the whole claim being demonstrated — point the executor at a different
descriptor and the shape is identical.
"""

from __future__ import annotations

import sys
from typing import TextIO


class Trace:
    def __init__(self, run_id: str, stream: TextIO | None = None) -> None:
        # The first four hex characters of the run id: short enough to scan a
        # log with, and it resolves back to the full UUID on the lineage event.
        self.tag = run_id.replace("-", "")[:4]
        self.stream = stream if stream is not None else sys.stdout

    def __call__(self, verb: str, subject: str, detail: str = "") -> None:
        print(f"[{self.tag}] {verb:<11} {subject:<38} {detail}".rstrip(), file=self.stream)

    def failed(self, exc: BaseException) -> None:
        self("FAILED", f"{type(exc).__name__}:", str(exc))


def rows(n: int) -> str:
    return f"{n:,} rows"
