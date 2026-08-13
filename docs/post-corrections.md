# Where this repo and the articles disagreed

Building the runnable version turned up five places where the published posts
were wrong, incomplete, or would not run as written. They are listed here rather
than quietly fixed in the code, because a reference repo that silently departs
from the article it references is the thing it exists to prevent.

Each entry says what the article said, what is actually true, how it was
verified, and where it stands now.

| | Article | Status |
| --- | --- | --- |
| [1](#1-dataqualityassertions-belongs-on-an-input-not-an-output) | part 2 § 3, § 4 | **fixed 2026-08-13** |
| [2](#2-partition_object-baseline_fraud_score-will-not-run) | part 1 § 5 | open |
| [3](#3-the-openlineage-client-already-ships-a-datazone-transport) | part 2 § 2 | **fixed 2026-08-13** |
| [4](#4-the-trace-shows-expect-before-policy-the-code-resolves-policy-first) | part 1 § 3 | open, cosmetic |
| [5](#5-dedupe_cdc-reads-as-redundant-next-to-collapse_cdc) | part 1 § 2, § 3 | open, cosmetic |

Verified against `openlineage-python 1.52.0`, `openlineage-integration-common`,
`great_expectations 1.20.0` and the OpenLineage 2-0-2 spec on 2026-08-13.

---

## 1. `dataQualityAssertions` belongs on an input, not an output

The one that mattered: it defeated the purpose of emitting the facet at all.
Corrected in both language versions on 2026-08-13.

Part 2 § 3 showed the assertion result inside the output dataset:

```json
"outputs": [{
  "facets": {
    "dataQualityAssertions": { ... }
  }
}]
```

and § 4's `emit()` produced exactly that:

```python
quality = {"dataQualityAssertions": result.as_facet()} if result else {}
outputs = [dataset(pipeline.target, facets=quality)]
```

**What is actually true.** The spec types the facet as an `InputDatasetFacet`:

```
DataQualityAssertionsDatasetFacet.json §
  "$ref": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/InputDatasetFacet"
```

An `InputDatasetFacet` has exactly one standard home: `InputDataset.inputFacets`.
An output dataset has `facets` (typed `DatasetFacet`) and `outputFacets` (typed
`OutputDatasetFacet`), and the quality facet is neither.

Both reference integrations agree, and both do the same thing — emit the
assertions against a *validation-shaped* event whose input is the dataset that
was tested:

- `openlineage/common/provider/dbt/processor.py` builds `input_facets["dataQualityAssertions"]`
  on a job named `<model>.test` with `jobType: TEST`
- `openlineage/common/provider/great_expectations/action.py` emits
  `inputs=[dataset(..., input_facets=self.results_facet(...))], outputs=[]`

The event the article emitted was still well-formed — OpenLineage facet maps are
open, so nothing rejects it — which is why this was easy to miss. It simply put
the result where no standard consumer looks for it, and that result being
readable by a consumer is the entire reason § 3 emits it.

**What both now do.** One terminal event per run, exactly as part 1 § 3
guarantees, plus a second `OTHER` event on the same `runId` carrying the
assertions against the tested dataset as an input. `OTHER` because the spec
reserves it for additional metadata accumulating against a run — the same
accumulation part 2 § 4 already relied on — and because a second `COMPLETE` on
one `runId` would be illegal.

The correction also removed a limitation the article had apologised for: a
blocked publish writes nothing, so it has no output dataset to hang a result
on. Attached to the *tested* dataset it survives, and
`tests/test_failure_modes.py::test_block_publish_carries_the_result_it_died_on`
holds it there.

See `src/pipeline_runtime/emit.py` and `tests/test_lineage.py`.

---

## 2. `partition_object: baseline_fraud_score` will not run

Anyone copying the suite out of the article gets an error. Still open.

Part 1 § 5's suite reads:

```yaml
- type: expect_column_kl_divergence_to_be_less_than
  kwargs: {column: fraud_score, partition_object: baseline_fraud_score, threshold: 0.15}
```

`partition_object` is a Great Expectations kwarg that takes a *partition
object* — `{"bins": [...], "weights": [...]}` — not a name. GX has no registry
that resolves a bare string to a stored baseline, so as written this raises.

The article's surrounding prose is right: "KL divergence against a stored
baseline" is the correct idea, and a hundred bin weights inline would make the
suite unreadable. The name is doing real work; it just needs something to
resolve it.

**What this repo does.** `load_suite` expands a string `partition_object` into
`<domain>/baselines/<name>.json` before the expectation is built, so the
published spelling works. That also makes re-baselining a pull request with an
author on it, which is the behaviour the prose implies — see
`tools/baseline.py`.

**Planned fix.** A half-sentence after the suite: the platform resolves a named
baseline from the domain's `baselines/` directory, which is why re-baselining
shows up in a diff.

---

## 3. The OpenLineage client already ships a DataZone transport

An omission rather than an error, but one an informed reader would notice.
Corrected in both language versions on 2026-08-13.

Part 2 § 2 builds a Lambda adapter that calls `PostLineageEvent`. It never
mentioned that `openlineage-python` ships `AmazonDataZoneTransport`
(`openlineage/client/transport/amazon_datazone.py`), which makes the same call.

The adapter is not wasted work — the topology § 2 describes is fan-in from
producers that are not Python processes (Spark, dbt, Airflow), and a transport
inside the Python client cannot serve those. But a reader who knows the
transport exists would wonder why the article had them write one.

Worth noting in the article's favour: the built-in transport passes no
`clientToken` at all, so § 2's idempotency argument is *stronger* than the
library's own implementation, not weaker. The added paragraph says both.

---

## 4. The trace shows `expect` before `policy`; the code resolves policy first

Cosmetic. Still open.

Part 1 § 3's sample trace lists `expect` above `policy`. The executor in the
same section resolves policy at step 3 and validates at step 4 — which is the
correct order, because the quarantine branch writes, and a write needs resolved
tags.

This repo emits the lines in code order. Nothing depends on it; the trace is
illustrative.

---

## 5. `dedupe_cdc` reads as redundant next to `collapse_cdc`

Cosmetic, but it invites a "wait, twice?" from a careful reader. Still open.

Part 1 § 2's `claims-ingest` descriptor declares a step `dedupe_cdc`, and § 3's
`read_dms_landing` already collapses the CDC log. Nothing says how the two
differ.

They can differ, and defensibly: the reader collapses the change log to one row
per key, which is DMS mechanics and belongs to the platform; the step drops the
domain's own notion of a duplicate — the same claim submitted twice under two
ids — which only the policy team can define. This repo implements exactly that
split, in `example/domains/policy/sql/dedupe_cdc.sql`.

**Planned fix.** One clause where the step is introduced, naming the split. It
reinforces the ownership argument § 7 makes rather than distracting from it.
