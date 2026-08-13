"""Conformance tests: does the code published in the post actually hold up?

These run against fixtures extracted from the blog post by
`tools/extract_from_post.py`, so they fail loudly whenever the article and this
repo drift apart. Every assertion here corresponds to a claim the post makes in
prose — if a test fails, either the code is wrong or the article is lying.
"""

from __future__ import annotations

import copy
import importlib.util
import pathlib

import pytest
import yaml
from pydantic import ValidationError

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def model_ns() -> dict:
    """Import the published descriptor model as a real module.

    It has to be a module rather than an `exec()` into a bare dict: pydantic
    resolves annotations against the defining module's globals, and a plain
    namespace leaves `Descriptor` "not fully defined".
    """
    path = FIXTURES / "descriptor_model.py"
    assert path.is_file(), "no model fixture — run tools/extract_from_post.py"
    spec = importlib.util.spec_from_file_location("descriptor_model", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return vars(module)


@pytest.fixture(scope="session")
def descriptors() -> dict[str, dict]:
    return {p.stem: yaml.safe_load(p.read_text()) for p in sorted(FIXTURES.glob("*.yml"))}


def test_every_published_descriptor_validates(model_ns, descriptors):
    """§ 2's YAML must satisfy § 3's model — no field drift between them."""
    assert descriptors, "no descriptor fixtures — run tools/extract_from_post.py"
    for name, raw in descriptors.items():
        try:
            model_ns["Descriptor"].model_validate(raw)
        except Exception as exc:  # noqa: BLE001 — surface which descriptor drifted
            pytest.fail(f"published descriptor {name!r} no longer validates: {exc}")


def test_one_executor_many_descriptors(model_ns, descriptors):
    """The post's central claim: the descriptors differ only in values."""
    assert len(descriptors) >= 2, "the reuse claim needs at least two descriptors"
    parsed = [model_ns["Descriptor"].model_validate(r) for r in descriptors.values()]
    assert len({d.source.kind for d in parsed}) > 1, "descriptors should exercise different readers"
    assert len({d.target.kind for d in parsed}) > 1, "descriptors should exercise different writers"


# Each case below is a rejection the post explicitly promises.
REJECTIONS = [
    ("typo'd key is a failed PR",
     lambda d: d["expectations"].update({"on_fail": "block"})),
    ("invented legal_basis cannot reach the RoPA",
     lambda d: d["processing"].update({"legal_basis": "because-we-can"})),
    ("step with both query_file and module",
     lambda d: d["steps"][0].update({"module": "domains.fraud.x"})),
    ("step with neither query_file nor module",
     lambda d: d["steps"][0].pop("query_file", None)),
    ("schema_ref without an @vN pin",
     lambda d: d["source"].update({"schema_ref": "catalog://fraud_raw/transactions"})),
    ("metadata.name with spaces and capitals",
     lambda d: d["metadata"].update({"name": "Transactions Daily"})),
]


@pytest.mark.parametrize("label,mutate", REJECTIONS, ids=[c[0] for c in REJECTIONS])
def test_model_rejects_what_the_post_claims(model_ns, descriptors, label, mutate):
    base = copy.deepcopy(next(iter(descriptors.values())))
    mutate(base)
    # Deliberately narrow: catching bare Exception here would let a broken
    # model masquerade as correct rejection.
    try:
        model_ns["Descriptor"].model_validate(base)
    except ValidationError:
        return
    pytest.fail(f"model accepted what the post promises it rejects: {label}")


def test_descriptor_paths_resolve_from_the_domain_root(model_ns, descriptors):
    """`sql/…` and `expectations/…` are domain-root relative, not descriptor relative.

    Regression guard: the post originally resolved the suite against the
    descriptor's own directory, which pointed one level too deep.
    """
    for raw in descriptors.values():
        d = model_ns["Descriptor"].model_validate(raw)
        assert not d.expectations.suite.startswith(("/", "../")), d.expectations.suite
        for step in d.steps:
            if step.query_file:
                assert not step.query_file.startswith(("/", "../")), step.query_file
