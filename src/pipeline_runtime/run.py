"""The generic executor — part 1 § 3.

Contains no table names, no column lists, no SQL, and no domain logic of any
kind: everything that differs between pipelines already lives in the descriptor.
What is left is dispatch, and dispatch does not grow when a domain adds a
product. Every descriptor across every domain runs through this file.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from . import backends, catalog, policy
from .descriptor import Descriptor, OnFailure, quarantined
from .emit import emit
from .errors import (
    PipelineError,
    PublishBlocked,
    SchemaDrift,
    UnknownApiVersion,
    UnknownKind,
)
from .expectations import Validation, load_suite, validate
from .trace import Trace, rows

log = logging.getLogger(__name__)

# apiVersion is a dispatch key, not decoration. A future v2 model is registered
# beside v1 rather than replacing it, so descriptors written against v1 keep
# parsing while domains migrate one at a time.
MODELS: dict[str, type[Descriptor]] = {"cordata.tech/v1": Descriptor}


def load(path: Path) -> Descriptor:
    raw = yaml.safe_load(path.read_text())
    if (model := MODELS.get(raw.get("apiVersion"))) is None:
        raise UnknownApiVersion(raw.get("apiVersion"), known=sorted(MODELS))
    return model.model_validate(raw)


def _dispatch(registry: dict, name: str, kind: str):
    if (fn := registry.get(kind)) is None:
        raise UnknownKind(name, kind, known=sorted(registry))
    return fn


def run(descriptor_path: Path, run_id: str, trace: Trace | None = None) -> None:
    """Entry point. Guarantees exactly one terminal event per run."""
    pipeline = load(descriptor_path)  # UnknownApiVersion escapes — see below
    trace = trace or Trace(run_id)
    trace(
        "descriptor",
        f"{pipeline.metadata.name} ({pipeline.metadata.domain})",
        f"apiVersion {pipeline.apiVersion.split('/')[-1]} OK",
    )
    try:
        execute(pipeline, descriptor_path, descriptor_path.parents[1], run_id, trace)
    except PipelineError as exc:
        trace.failed(exc)
        emit(pipeline, descriptor_path, run_id, exc.result, status="FAIL", error=exc)
        raise


def execute(
    pipeline: Descriptor,
    descriptor_path: Path,
    domain_root: Path,
    run_id: str,
    trace: Trace,
) -> None:
    # Paths inside a descriptor (`sql/…`, `expectations/…`) are relative to the
    # domain root, not to the descriptor — descriptors sit in <domain>/pipelines/.
    backend = backends.load()

    # 1. source schema — resolved from the catalog, never hardcoded (§ 4)
    pinned = pipeline.source.schema_version
    try:
        schema = catalog.resolve(pipeline.source)
    except SchemaDrift as exc:
        # The mismatch gets its own trace line even though the run is about to
        # die, because "pinned v7, catalog v8" is the sentence the person
        # reading the log needs, and burying it inside the error text makes
        # them read the error to find out where it stopped.
        trace("schema", pipeline.source.fqn, f"pinned v{pinned}, catalog v{exc.found}")
        raise
    trace("schema", pipeline.source.fqn, f"pinned v{pinned}, catalog v{schema.version} OK")

    reader = _dispatch(backend.READERS, "READERS", pipeline.source.kind)
    frame = reader(pipeline.source, schema)
    trace("read", pipeline.source.kind, rows(len(frame)))

    # 2. the declared steps — the domain's SQL, in order
    for step in pipeline.steps:
        runner = _dispatch(backend.STEPS, "STEPS", step.kind)
        frame = runner(frame, step, domain_root)
        trace("step", f"{step.id:<21} ({step.kind})", rows(len(frame)))

    # 3. policy metadata — tag values resolved against the ontology (§ 6)
    tags = policy.resolve(pipeline.contract.lf_tags)
    trace("policy", " ".join(f"{k}={v}" for k, v in tags.items()), "resolved against ontology")

    # 4. the declared expectation suite — rules in, result out (§ 5)
    result: Validation = validate(frame, load_suite(domain_root / pipeline.expectations.suite))
    trace("expect", pipeline.expectations.suite.split("/")[-1].removesuffix(".yml"), result.summary)

    writer = _dispatch(backend.WRITERS, "WRITERS", pipeline.target.kind)

    # What the run actually saw, for the schema facets on the event. Reported
    # from the catalog and the frame rather than from the descriptor, so a
    # mismatch between declaration and reality shows up instead of being echoed.
    shapes = dict(
        source_columns=[(c.name, c.type) for c in schema.columns],
        target_columns=[(str(n), str(t).upper()) for n, t in frame.dtypes.items()],
    )

    if not result.success:
        match pipeline.expectations.on_failure:
            case OnFailure.BLOCK_PUBLISH:
                raise PublishBlocked(pipeline.metadata.name, result)
            case OnFailure.QUARANTINE:
                target = quarantined(pipeline.target)
                writer(frame, target, tags)
                trace("write", target.fqn, f"{pipeline.target.kind}, quarantined")
                emit(pipeline, descriptor_path, run_id, result, status="QUARANTINED", **shapes)
                trace("emit", "QUARANTINED → lineage adapter", "inputs=1 outputs=1")
                return
            case OnFailure.WARN:
                log.warning("%s: %s", pipeline.metadata.name, result.summary)
                trace("warn", pipeline.metadata.name, result.summary)

    # 5. write, carrying the contract
    writer(frame, pipeline.target, tags)
    keys = len(pipeline.target.partition_by)
    trace(
        "write",
        pipeline.target.fqn,
        f"{pipeline.target.kind}, {keys} partition key{'' if keys == 1 else 's'}",
    )
    emit(pipeline, descriptor_path, run_id, result, status="COMPLETE", **shapes)
    trace("emit", "COMPLETE → lineage adapter", "inputs=1 outputs=1")
