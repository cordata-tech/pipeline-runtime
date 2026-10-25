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
from urllib.parse import urlsplit, urlunsplit

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
from openlineage.client.facet_v2 import (
    JobFacet,
    RunFacet,
    error_message_run,
    schema_dataset,
    source_code_location_job,
)
from openlineage.client.generated import tags_dataset
from openlineage.client.transport.file import FileConfig, FileTransport

from . import __version__
from .catalog import Publication
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
    this is the HTTP transport pointed at the adapter from part 2 § 2, or
    `AmazonDataZoneTransport`, which the client ships. Both are reachable by
    configuration alone:
    https://openlineage.io/docs/client/python/configuration/
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
    # Absent, like the two fields below, when no commit was found: `false` would
    # report a commit as unsigned, and there is no commit. The two move
    # together, which `schemas/provenance.json` states as `dependentRequired`
    # rather than as a comment nobody downstream reads (#11).
    descriptor_git_commit_signed: bool | None
    executor_version: str
    step_params: dict[str, dict[str, str]]
    # The application release behind the source version this run read. Unset
    # when the catalog records no producer for it — a Glue version written by a
    # crawler, or by a job that set no parameters, looks exactly like that — and
    # unset on a run that read nothing. The client drops a None-valued field on
    # the way out, so an absent key is the wire spelling of "nobody recorded
    # it", which is the answer a consumer can act on: ask, rather than be told
    # a release that did not publish this version.
    source_published_by: str | None = None
    source_published_release: str | None = None

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


def provenance_facet(
    pipeline: Descriptor, descriptor_path: Path, source: Publication | None = None
) -> ProvenanceRunFacet:
    commit, signed = _git_provenance(descriptor_path)
    return ProvenanceRunFacet(
        descriptor_path=_descriptor_location(descriptor_path),
        descriptor_sha256=hashlib.sha256(descriptor_path.read_bytes()).hexdigest(),
        source_table=pipeline.source.fqn,
        source_schema_version=pipeline.source.schema_version,
        descriptor_git_commit=commit,
        descriptor_git_commit_signed=signed,
        executor_version=f"pipeline-runtime {__version__}",
        step_params={s.id: s.params for s in pipeline.steps if s.params},
        source_published_by=source.producer if source else None,
        source_published_release=source.release if source else None,
    )


def source_code_location(descriptor_path: Path, provenance: ProvenanceRunFacet) -> JobFacet | None:
    """The commit in the standard facet, so a consumer need not know ours (#13).

    `cordata_provenance` has carried the commit since the facet was published,
    and a reader that does not know this project finds nothing — OpenLineage has
    `SourceCodeLocationJobFacet` for exactly this. What stays custom is what the
    standard cannot say: whether the signature verified, and which bytes ran.

    Built from the provenance facet rather than from git a second time, so the
    commit and the path cannot disagree between the two facets: there is one
    `_git_provenance` call per run and one `_descriptor_location` call, and both
    facets read their results.

    None rather than a partial facet, which is #11's rule again. Three things
    have to hold: the descriptor is in a work tree (so there is a commit at
    all), that work tree has a remote, and the remote is an http URL. The last
    one matters more than it looks — a remote that is a local path would put a
    filesystem location back on the wire, which is what #12 removed.
    """
    root = _work_tree(descriptor_path)
    if root is None or provenance.descriptor_git_commit is None:
        return None
    remote = _normalize_remote(_remote_url(root))
    if remote is None:
        return None
    return source_code_location_job.SourceCodeLocationJobFacet(
        type="git",
        # The spec wants an http URL locating the source. A per-file URL would
        # need the host's path template — `/blob/<sha>/<path>` on GitHub,
        # `/-/blob/` on GitLab, `/src/` on Bitbucket — which a remote alone does
        # not reveal, and a wrong one is a dead link that reads as authoritative.
        # So both fields carry the repository, and `path` plus `version` say
        # which file at which commit.
        url=remote,
        repoUrl=remote,
        path=provenance.descriptor_path,
        version=provenance.descriptor_git_commit,
    )


def _remote_url(root: Path) -> str | None:
    """`origin`'s URL, or None when there is no remote to name."""
    try:
        out = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        ).stdout.strip()
    except (subprocess.SubprocessError, OSError):
        return None
    return out or None


def _normalize_remote(raw: str | None) -> str | None:
    """A remote as an http URL that names no secret, or None.

    Credentials first, because `git remote get-url` returns whatever is
    configured and an https remote can carry a token in its userinfo —
    `https://x-access-token:ghp_…@github.com/org/repo`, which is what a CI
    checkout often leaves behind. Emitting that would publish a credential to
    every lineage consumer, so userinfo is dropped rather than trusted.

    SSH remotes are rewritten to https: `git@github.com:org/repo.git` is the
    usual form in a developer's checkout, and it is the same repository a
    reader reaches over https. Anything that is not http, https or ssh — a
    local path, a bundle, `file://` — returns None, since the field is
    specified as an http URL and a path would name the host.
    """
    if not raw:
        return None
    candidate = raw.strip()

    # scp-like syntax, which has no scheme: [user@]host:path
    if "://" not in candidate and ":" in candidate:
        host, _, repo_path = candidate.partition(":")
        host = host.rpartition("@")[2]
        if not host or repo_path.startswith("/"):
            return None
        candidate = f"https://{host}/{repo_path.lstrip('/')}"

    parsed = urlsplit(candidate)
    if parsed.scheme in ("ssh", "git", "git+ssh"):
        parsed = parsed._replace(scheme="https")
    elif parsed.scheme not in ("http", "https"):
        return None

    host = parsed.hostname or ""
    if not host:
        return None
    if parsed.port and parsed.scheme == "https":
        host = f"{host}:{parsed.port}" if parsed.port != 443 else host

    cleaned = urlunsplit((parsed.scheme, host, parsed.path, "", ""))
    return cleaned.removesuffix("/").removesuffix(".git") or None


def _descriptor_location(path: Path) -> str:
    """Where the descriptor sits, named so that two machines agree.

    The work tree root when there is one, so a checkout on a laptop and the
    same commit in CI report one string, and `descriptor_git_commit` resolves
    against it. Outside a work tree — which is what an installed copy looks
    like — the domain directory's parent, giving
    `fraud/pipelines/transactions_scored.yml`: `run()` already treats the
    descriptor's grandparent as the domain root, so this names the same
    structure one level up rather than inventing one.

    Never the path as given, which is what this was until 0.2.5 (#12). That
    disagreed with `schemas/provenance.json`, which has promised a relative
    path since the facet was published, and from an installed copy it carried
    the host's filesystem layout into lineage — a site-packages path under
    whatever temporary directory the run happened to use. `descriptor_sha256`
    is what proves which bytes ran, and it is unaffected.

    A descriptor too shallow to have a domain directory keeps its file name:
    the field is in `required`, a run always executed some file, and a name
    with no path names no host.

    A descriptor under `site-packages` is treated as installed even when a work
    tree encloses it, because the usual Python layout puts the virtual
    environment inside the project — so the work tree found there is the
    reader's own repository, and naming the example relative to it would emit
    `.venv/lib/python3.13/site-packages/…`, which describes where the wheel was
    unpacked rather than which descriptor ran.
    """
    resolved = path.resolve()
    root = _work_tree(resolved)
    if root is not None:
        try:
            return resolved.relative_to(root).as_posix()
        except ValueError:  # a work tree that does not contain it, via a symlink
            pass
    if len(resolved.parents) >= 3:
        return resolved.relative_to(resolved.parents[1].parent).as_posix()
    return resolved.name


def _work_tree(path: Path) -> Path | None:
    """The work tree holding this descriptor, or None when it is not in one.

    A descriptor under `site-packages` is in none, whatever encloses it: the
    usual Python layout puts the virtual environment inside the project, so the
    work tree found there is the reader's own repository rather than the one the
    descriptor came from (#12). Both the path and the source-code-location
    facet depend on that distinction, which is why it lives here rather than in
    either of them.
    """
    resolved = path.resolve()
    if any(parent.name == "site-packages" for parent in resolved.parents):
        return None
    return _work_tree_root(resolved)


def _work_tree_root(path: Path) -> Path | None:
    """The root of the work tree holding `path`, or None when there is none."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=path.parent,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        ).stdout.strip()
    except (subprocess.SubprocessError, OSError):
        return None
    return Path(out).resolve() if out else None


def _git_provenance(path: Path) -> tuple[str | None, bool | None]:
    """The commit that last touched this descriptor, and whether it was signed.

    `descriptor_sha256` alone proves which bytes ran, not that those bytes were
    ever reviewed — and nothing stops a rewritten history from making an old
    hash resolve to new content. The commit plus its signature is what closes
    that: `%G?` returns `G` only when the signature verifies against a key in
    `allowed_signers`. A run whose provenance points at an unsigned commit is
    itself a finding.

    Returns `(None, None)` outside a repository, so neither key reaches the
    wire. Absence of provenance is not the same as bad provenance, and until
    0.2.4 this returned `(None, False)` — a negative signature verdict about a
    commit nothing named, which a consumer could only read as a finding (#11).
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
        return None, None
    if len(out) < 2:
        return None, None
    return out[0], out[1] == "G"


# Named on every tag this runtime emits, so a consumer can tell a classification
# resolved against the governance ontology from one a crawler guessed.
TAG_SOURCE = "CORDATA_PIPELINE_RUNTIME"


def dataset(
    ref: TableRef,
    domain: str,
    columns: Sequence[Any] | None = None,
    tags: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    """One dataset reference, carrying its schema and classification when the run knows them.

    The namespace is the domain rather than the storage account: under
    account-per-domain the same physical bucket can be reached by several
    identities, and lineage that keyed on the reacher would split one dataset
    into several. The domain is the thing that stays the same — provided it is
    the domain that owns the table, so the caller passes the owner's for an
    input, not its own.

    `TagsDatasetFacet` is a plain `DatasetFacet`, so unlike the quality facet in
    `docs/post-corrections.md` § 1 it belongs in `facets` and is defined for an
    input and an output alike.
    """
    cls = kwargs.pop("cls", OutputDataset)
    facets = dict(kwargs.pop("facets", {}))
    if columns:
        facets["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[schema_dataset.SchemaDatasetFacetFields(name=n, type=t) for n, t in columns]
        )
    if tags:
        facets["tags"] = tags_dataset.TagsDatasetFacet(
            tags=[
                tags_dataset.TagsDatasetFacetFields(key=k, value=v, source=TAG_SOURCE)
                for k, v in sorted(tags.items())
            ]
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
    source_publication: Publication | None = None,
    tags: dict[str, str] | None = None,
    source_tags: dict[str, str] | None = None,
) -> list[RunEvent]:
    """Emit the run's terminal event, plus the assertion event when a suite ran.

    The two `*_columns` arguments are what the run actually saw, not what the
    descriptor declared: the source's shape comes from the catalog and the
    target's from the frame about to be written. Reporting the declaration
    instead would make the schema facet unfalsifiable.

    `source_publication` is the release that published the version the run
    read, and it is absent on a run that read nothing — a run that died on
    drift touched no version, so claiming one on its event would be a guess.

    The two tag arguments come from different authorities on purpose, which is
    the decision recorded on #3. `tags` are the contract's own, resolved against
    the governance ontology and applied by the writer, so they describe what
    this run published. `source_tags` are whatever the catalog already holds for
    the table this run read, because the classification of someone else's table
    is not this descriptor's to assert — and it is absent when the catalog holds
    none.

    Returns what was emitted so tests can assert on it — the executor itself
    ignores the return value.
    """
    client = _client()
    domain = pipeline.metadata.domain
    events: list[RunEvent] = []

    # 1. Run facets — provenance always, plus the reason if this run died (§ 4)
    provenance = provenance_facet(pipeline, descriptor_path, source_publication)
    run_facets: dict[str, RunFacet] = {"cordata_provenance": provenance}
    if error is not None:
        run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
            message=str(error), programmingLanguage="python"
        )

    job_facets: dict[str, JobFacet] = {
        "processing": ProcessingJobFacet(
            purpose=pipeline.processing.purpose,
            legal_basis=pipeline.processing.legal_basis,
        )
    }
    # The commit in the facet a consumer already knows, built from the
    # provenance facet above so the two cannot disagree, and absent when this
    # run cannot name a repository (#13).
    location = source_code_location(descriptor_path, provenance)
    if location is not None:
        job_facets["sourceCodeLocation"] = location

    job = Job(namespace=f"cordata.{domain}", name=pipeline.metadata.name, facets=job_facets)

    # 2. Datasets — what the run actually touched. A failed run touched nothing;
    #    a quarantined one wrote to quarantined(target), not the declared one.
    #    The input is named by the domain that owns it, which the catalog records
    #    as its `domain` tag, not by the domain reading it: a table two domains
    #    read is one dataset, and naming it by the reader made it two (#7).
    owner = (source_tags or {}).get("domain", domain)
    match status:
        case "FAIL":
            inputs, outputs = [], []
        case "QUARANTINED":
            # The quarantined copy carries the same classification as the
            # declared target: failing a quality rule does not make the data
            # less personal, and the side copy is the one somebody eventually
            # opens without the descriptor in front of them.
            inputs = [
                dataset(pipeline.source, owner, source_columns, source_tags, cls=InputDataset)
            ]
            outputs = [dataset(quarantined(pipeline.target), domain, target_columns, tags)]
        case _:
            inputs = [
                dataset(pipeline.source, owner, source_columns, source_tags, cls=InputDataset)
            ]
            outputs = [dataset(pipeline.target, domain, target_columns, tags)]

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
    #
    #    The job is the pipeline's with a different name, so the `processing`
    #    facet travels with it: checking a pipeline's output is part of the same
    #    processing activity, and the purpose and legal basis that justify
    #    writing the rows justify checking them (#9). Built from scratch, as it
    #    was until 0.2.3, it carried no facets, and an Art. 30 record rebuilt
    #    from the events listed every `.validate` job as an activity with no
    #    declared purpose. `attr.evolve` rather than part 2 § 4's
    #    `dataclasses.replace`, which does not work here; see
    #    `docs/post-corrections.md` § 6.
    if result is not None:
        tested = quarantined(pipeline.target) if status == "QUARANTINED" else pipeline.target
        assertions = RunEvent(
            eventType=RunState.OTHER,
            eventTime=now_utc_iso(),
            run=Run(runId=run_id, facets={}),
            job=attr.evolve(job, name=f"{job.name}.validate"),
            inputs=[
                dataset(
                    tested,
                    domain,
                    target_columns,
                    # The contract's own tags, as the terminal event's output
                    # carries them. `dataset()` withholds them from an input
                    # because classifying a table this pipeline merely read is
                    # not its descriptor's to assert — and this input is the
                    # table it just wrote, so the rule is applied rather than
                    # broken. Without them one dataset arrived classified in
                    # one event of a run and unclassified in the next (#10).
                    tags,
                    cls=InputDataset,
                    inputFacets={"dataQualityAssertions": result.as_facet()},
                )
            ],
            outputs=[],
        )
        client.emit(assertions)
        events.append(assertions)

    return events
