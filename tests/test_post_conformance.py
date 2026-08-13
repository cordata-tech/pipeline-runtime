"""Does this repo still say what the articles say?

The posts and this repo are two copies of one design, and two copies drift. This
module extracts the published code and compares behaviour — not text, because
formatting differences would make it fail for reasons nobody cares about while
still missing a renamed field.

Skipped when `cordata-platform` is not checked out alongside, which is the
normal case for anyone but us: that repository is private, and a contributor
should not see a failing suite because of a file they cannot have.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys

import pytest
import yaml

from pipeline_runtime.descriptor import Descriptor

from .paths import CLAIMS, FRAUD, REPO

PUBLISHED = REPO / "tests/fixtures/published"
POSTS = REPO.parent / "cordata-platform/content/blog"

pytestmark = pytest.mark.skipif(
    not POSTS.is_dir(), reason="cordata-platform not checked out alongside — nothing to compare"
)


@pytest.fixture(scope="module", autouse=True)
def extracted():
    result = subprocess.run(
        [sys.executable, "tools/extract_from_post.py"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"extraction failed: {result.stderr.strip()}")


@pytest.fixture(scope="module")
def published_model(extracted):
    """Import the published model as a real module.

    It has to be a module rather than an `exec()` into a bare dict: pydantic
    resolves annotations against the defining module's globals, and a plain
    namespace leaves `Descriptor` "not fully defined".
    """
    path = PUBLISHED / "descriptor_model.py"
    spec = importlib.util.spec_from_file_location("published_descriptor_model", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Descriptor


def _typename(annotation) -> str:
    """Compare types by shape, not by where they were defined.

    The two models are the same classes imported under different module names,
    so the module prefix is the one difference that carries no meaning.
    """
    text = str(annotation)
    for module in ("pipeline_runtime.descriptor.", "published_descriptor_model."):
        text = text.replace(module, "")
    return text


def fields(model, name="Descriptor", seen=None):
    """Every (path, type) pair in a pydantic model, nested."""
    seen = seen or set()
    if name in seen:
        return set()
    seen.add(name)

    out = set()
    for key, info in model.model_fields.items():
        annotation = info.annotation
        out.add((f"{name}.{key}", _typename(annotation)))
        nested = getattr(annotation, "__args__", (annotation,))
        for candidate in nested:
            if hasattr(candidate, "model_fields"):
                out |= fields(candidate, f"{name}.{key}", seen)
    return out


def test_the_repo_model_matches_the_published_one(published_model):
    """Field for field, type for type. A field this repo has and the article
    does not is a field a reader will be surprised by."""
    assert fields(Descriptor) == fields(published_model)


@pytest.mark.parametrize("path", [FRAUD, CLAIMS], ids=lambda p: p.stem)
def test_the_example_descriptors_are_the_published_ones(extracted, path):
    """Byte-for-byte here, because a descriptor is the artefact a reader copies.

    Whitespace and comments included: the article's comments explain which four
    fields differ between the two pipelines, and a repo that quietly drops them
    has dropped the explanation.
    """
    name = yaml.safe_load(path.read_text())["metadata"]["name"]
    published = PUBLISHED / f"descriptor-{name}.yml"
    assert published.is_file(), f"part 1 no longer publishes a descriptor named {name}"
    assert path.read_text() == published.read_text()


def test_the_published_descriptors_validate_against_this_repos_model(extracted):
    """§ 2's YAML must satisfy § 3's model — no field drift between them."""
    for published in PUBLISHED.glob("descriptor-*.yml"):
        Descriptor.model_validate(yaml.safe_load(published.read_text()))


def test_the_known_divergence_is_still_the_only_one(extracted):
    """Part 2 § 4 attaches `dataQualityAssertions` to the output dataset.

    That is wrong — see `docs/post-corrections.md` — and this repo does it
    differently on purpose. Asserting the divergence keeps it deliberate: if the
    article is corrected, this fails and the note comes out. If someone
    "restores fidelity" here, it fails too.
    """
    emit = (PUBLISHED / "emit.py").read_text()
    assert "outputs = [dataset(pipeline.target, facets=quality)]" in emit, (
        "part 2 no longer emits assertions on the output dataset — "
        "if it has been corrected, drop docs/post-corrections.md § 1 and this test"
    )

    ours = (REPO / "src/pipeline_runtime/emit.py").read_text()
    assert 'inputFacets={"dataQualityAssertions"' in ours
