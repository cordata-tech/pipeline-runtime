"""Published as part 1 § 3.

In its own module so that `catalog.py` and `policy.py` can raise without
importing `run.py`, which already imports them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .expectations import Validation


class PipelineError(Exception):
    """Base for every failure the executor raises, so one boundary catches all."""

    result: Validation | None = None  # most failures have no result to report


class UnknownApiVersion(PipelineError):
    def __init__(self, found: str | None, known: list[str]) -> None:
        super().__init__(f"apiVersion {found!r} is not one of {known}")


class SchemaDrift(PipelineError):
    def __init__(self, message: str, found: int | None = None) -> None:
        super().__init__(message)
        # The version the catalog is actually at, so the trace can show the
        # mismatch on its own line before the failure rather than only inside
        # the message.
        self.found = found


class UnknownTagKey(PipelineError):
    def __init__(self, key: str, allowed: list[str]) -> None:
        super().__init__(f"lf_tag key {key!r} is not in the ontology; allowed: {allowed}")


class UnknownTagValue(PipelineError):
    def __init__(self, key: str, value: str, allowed: list[str]) -> None:
        super().__init__(f"lf_tag {key}={value!r} is not in the ontology; allowed: {allowed}")


class PublishBlocked(PipelineError):
    def __init__(self, name: str, result: Validation) -> None:
        super().__init__(f"{name}: expectations failed before publish")
        self.result = result  # the one failure that carries an assertion result


class UnknownKind(PipelineError):
    """A descriptor named a reader, writer or step kind no registry implements.

    Not in the published hierarchy, because the article's registries are
    illustrative and always complete. A runnable executor needs the case: it is
    what a reader hits the first time they add `kind: bigquery` to a descriptor
    and run it against the local backend.
    """

    def __init__(self, registry: str, kind: str, known: list[str]) -> None:
        super().__init__(f"{registry}: no implementation for kind {kind!r}; have {known}")
