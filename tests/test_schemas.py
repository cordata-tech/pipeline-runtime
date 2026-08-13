"""The custom facets' `_schemaURL` has to resolve to something.

`_schemaURL` is the field that tells a consumer what a facet it has never seen
before means. Pointing it at a path that does not exist is worse than pointing
it nowhere: it looks resolvable and is not, and this repo publishes a facet
whose whole argument is that claims should be checkable.
"""

from __future__ import annotations

import json

import pytest

from pipeline_runtime.emit import ProcessingJobFacet, ProvenanceRunFacet

from .paths import REPO

FACETS = [ProvenanceRunFacet, ProcessingJobFacet]


@pytest.mark.parametrize("facet", FACETS, ids=lambda f: f.__name__)
def test_the_schema_url_points_at_a_file_in_this_repo(facet):
    url = facet._get_schema()
    prefix = "https://github.com/cordata-tech/pipeline-runtime/blob/main/"
    assert url.startswith(prefix), url

    path = REPO / url.removeprefix(prefix)
    assert path.is_file(), f"{facet.__name__} points at {path}, which does not exist"
    json.loads(path.read_text())


@pytest.mark.parametrize("facet", FACETS, ids=lambda f: f.__name__)
def test_the_schema_describes_the_fields_the_facet_actually_emits(facet):
    """Not a full validation pass — just that the two have not drifted apart,
    which is the failure a schema file quietly develops once nobody reads it."""
    import attr

    url = facet._get_schema()
    prefix = "https://github.com/cordata-tech/pipeline-runtime/blob/main/"
    schema = json.loads((REPO / url.removeprefix(prefix)).read_text())

    described = set(schema["allOf"][1]["properties"])
    emitted = {f.name for f in attr.fields(facet) if not f.name.startswith("_")}
    assert described == emitted, f"schema and facet disagree: {described ^ emitted}"
