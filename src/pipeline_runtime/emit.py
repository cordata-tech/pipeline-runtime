"""The outbound half — part 2.

One terminal event per run, carrying provenance and the datasets the run
actually touched — plus, when a suite ran, an `OTHER` event on the same run id
carrying the assertion results.

The assertion results go on an *input* rather than the output, because the OpenLineage
spec types `dataQualityAssertions` as an `InputDatasetFacet`: `inputFacets` is
its only standard home, and the dataset an assertion refers to is an input *to
the assertion* whatever it was to the pipeline. Both reference integrations do
the same. Part 2 said otherwise until this repo was built; see
`docs/post-corrections.md` § 1 for the working and the fix.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import attr
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
    set_producer,
)
from openlineage.client.facet_v2 import JobFacet, RunFacet, error_message_run, schema_dataset
from openlineage.client.transport.file import FileConfig, FileTransport

from . import __version__
from .descriptor import Descriptor, TableRef, quarantined
from .errors import PipelineError
from .expectations import Validation

# Becomes the producer on every event; the client supplies schemaURL.
set_producer(f"https://github.com/cordata-tech/pipeline-runtime/tree/{__version__}")

STATE = {
    "COMPLETE": RunState.COMPLETE,
    "FAIL": RunState.FAIL,
    "QUARANTINED": RunState.FAIL,  # a quarantined run did not publish
}


def _client() -> OpenLineageClient:
    """Where events go is deployment configuration, not pipeline configuration.

    A file by default so a clean clone can read what it emitted. In a deployment
    this is the HTTP transport pointed at the adapter from part 2 § 2 — or
    `AmazonDataZoneTransport`, which the client ships and which calls the same
    `PostLineageEvent` API by hand.
    """
    target = Path(os.environ.get("CORDATA_LINEAGE_OUT", "out/lineage.ndjson"))
    target.parent.mkdir(parents=True, exist_ok=True)
    # append=True gives one newline-delimited log rather than a timestamped file
    # per event, which is what makes the accumulation part 2 § 4 describes
    # visible: several events, one runId, in the order they were produced.
    return OpenLineageClient(
        transport=FileTransport(FileConfig(log_file_path=str(target), append=True))
    )


@attr.define
class ProvenanceRunFacet(RunFacet):
    """What was declared, and what authorised it — part 2 § 4.

    Custom facets are namespaced by convention; `cordata_` is ours. The point of
    the whole block is that an auditor can get from a published number back to
    the reviewed commit without asking anybody.
    """

    descriptor_path: str
    descriptor_sha256: str
    source_table: str
    source_schema_version: int
    descriptor_git_commit: str | None
    descriptor_git_commit_signed: bool
    executor_version: str
    step_params: dict[str, dict[str, str]]

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/cordata-tech/pipeline-runtime/blob/main/schemas/provenance.json"


@attr.define
class ProcessingJobFacet(JobFacet):
    """DSGVO Art. 30 fields, declared in the descriptor and travelling outward.

    A job facet rather than a run facet: purpose and legal basis are properties
    of the pipeline, not of one execution of it.
    """

    purpose: str
    legal_basis: str

    @staticmethod
    def _get_schema() -> str:
        return "https://github.com/cordata-tech/pipeline-runtime/blob/main/schemas/processing.json"


def provenance_facet(pipeline: Descriptor, descriptor_path: Path) -> ProvenanceRunFacet:
    commit, signed = _git_provenance(descriptor_path)
    return ProvenanceRunFacet(
        descriptor_path=str(descriptor_path),
        descriptor_sha256=hashlib.sha256(descriptor_path.read_bytes()).hexdigest(),
        source_table=pipeline.source.fqn,
        source_schema_version=pipeline.source.schema_version,
        descriptor_git_commit=commit,
        descriptor_git_commit_signed=signed,
        executor_version=f"pipeline-runtime {__version__}",
        step_params={s.id: s.params for s in pipeline.steps if s.params},
    )


def _git_provenance(path: Path) -> tuple[str | None, bool]:
    """The commit that last touched this descriptor, and whether it was signed.

    `descriptor_sha256` alone proves which bytes ran, not that those bytes were
    ever reviewed — and nothing stops a rewritten history from making an old
    hash resolve to new content. The commit plus its signature is what closes
    that: `%G?` returns `G` only when the signature verifies against a key in
    `allowed_signers`. A run whose provenance points at an unsigned commit is
    itself a finding.

    Returns `(None, False)` outside a repository. Absence of provenance is not
    the same as bad provenance, and an auditor reading `null` knows to ask.
    """
    try:
        out = subprocess.run(
            ["git", "log", "-1", "--format=%H %G?", "--", path.name],
            cwd=path.parent,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        ).stdout.split()
    except (subprocess.SubprocessError, OSError):
        return None, False
    if len(out) < 2:
        return None, False
    return out[0], out[1] == "G"


def dataset(ref: TableRef, domain: str, columns: Sequence[Any] | None = None, **kwargs: Any) -> Any:
    """One dataset reference, carrying its schema when the run knows it.

    The namespace is the domain rather than the storage account: under
    account-per-domain the same physical bucket can be reached by several
    identities, and lineage that keyed on the reacher would split one dataset
    into several. The domain is the thing that stays the same.
    """
    cls = kwargs.pop("cls", OutputDataset)
    facets = dict(kwargs.pop("facets", {}))
    if columns:
        facets["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[schema_dataset.SchemaDatasetFacetFields(name=n, type=t) for n, t in columns]
        )
    return cls(namespace=f"cordata://{domain}", name=ref.fqn, facets=facets, **kwargs)


def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def emit(
    pipeline: Descriptor,
    descriptor_path: Path,
    run_id: str,
    result: Validation | None,
    status: str,
    error: PipelineError | None = None,
    source_columns: Sequence[tuple[str, str]] | None = None,
    target_columns: Sequence[tuple[str, str]] | None = None,
) -> list[RunEvent]:
    """Emit the run's terminal event, plus the assertion event when a suite ran.

    The two `*_columns` arguments are what the run actually saw, not what the
    descriptor declared: the source's shape comes from the catalog and the
    target's from the frame about to be written. Reporting the declaration
    instead would make the schema facet unfalsifiable.

    Returns what was emitted so tests can assert on it — the executor itself
    ignores the return value.
    """
    client = _client()
    domain = pipeline.metadata.domain
    events: list[RunEvent] = []

    # 1. Run facets — provenance always, plus the reason if this run died (§ 4)
    run_facets: dict[str, RunFacet] = {
        "cordata_provenance": provenance_facet(pipeline, descriptor_path)
    }
    if error is not None:
        run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
            message=str(error), programmingLanguage="python"
        )

    job = Job(
        namespace=f"cordata.{domain}",
        name=pipeline.metadata.name,
        facets={
            "processing": ProcessingJobFacet(
                purpose=pipeline.processing.purpose,
                legal_basis=pipeline.processing.legal_basis,
            )
        },
    )

    # 2. Datasets — what the run actually touched. A failed run touched nothing;
    #    a quarantined one wrote to quarantined(target), not the declared one.
    match status:
        case "FAIL":
            inputs, outputs = [], []
        case "QUARANTINED":
            inputs = [dataset(pipeline.source, domain, source_columns, cls=InputDataset)]
            outputs = [dataset(quarantined(pipeline.target), domain, target_columns)]
        case _:
            inputs = [dataset(pipeline.source, domain, source_columns, cls=InputDataset)]
            outputs = [dataset(pipeline.target, domain, target_columns)]

    # 3. The terminal event — one per run, always
    terminal = RunEvent(
        eventType=STATE[status],
        eventTime=now_utc_iso(),
        run=Run(runId=run_id, facets=run_facets),
        job=job,
        inputs=inputs,
        outputs=outputs,
    )
    client.emit(terminal)
    events.append(terminal)

    # 4. The assertion result, when there is one to report (§ 3), against the
    #    dataset that was actually tested. `OTHER` because part 1 § 3 guarantees exactly one
    #    *terminal* event per run and this is not it — the spec reserves OTHER
    #    for exactly this, additional metadata accumulating against a runId.
    if result is not None:
        tested = quarantined(pipeline.target) if status == "QUARANTINED" else pipeline.target
        assertions = RunEvent(
            eventType=RunState.OTHER,
            eventTime=now_utc_iso(),
            run=Run(runId=run_id, facets={}),
            job=Job(namespace=f"cordata.{domain}", name=f"{pipeline.metadata.name}.validate"),
            inputs=[
                dataset(
                    tested,
                    domain,
                    target_columns,
                    cls=InputDataset,
                    inputFacets={"dataQualityAssertions": result.as_facet()},
                )
            ],
            outputs=[],
        )
        client.emit(assertions)
        events.append(assertions)

    return events
