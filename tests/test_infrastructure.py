"""Consistency checks between the SAM template, state machine, Glue scripts and SQL."""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

import bronze_to_silver as b2s
import ingest_handler as ih
import rules
import silver_to_gold as s2g
from fake_youtube_api import FakeYouTubeApi

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "infrastructure" / "template.yaml"
ASL = ROOT / "infrastructure" / "statemachine" / "pipeline.asl.json"


class CfnLoader(yaml.SafeLoader):
    pass


def _any_tag(loader, tag_suffix, node):
    if isinstance(node, yaml.ScalarNode):
        return {tag_suffix: loader.construct_scalar(node)}
    if isinstance(node, yaml.SequenceNode):
        return {tag_suffix: loader.construct_sequence(node, deep=True)}
    return {tag_suffix: loader.construct_mapping(node, deep=True)}


CfnLoader.add_multi_constructor("!", _any_tag)


@pytest.fixture(scope="module")
def template():
    return yaml.load(TEMPLATE.read_text(), Loader=CfnLoader)


@pytest.fixture(scope="module")
def resources(template):
    return template["Resources"]


@pytest.fixture(scope="module")
def asl():
    return json.loads(ASL.read_text())


def glue_tables(resources):
    return {
        r["Properties"]["TableInput"]["Name"]: r["Properties"]["TableInput"]
        for r in resources.values()
        if r["Type"] == "AWS::Glue::Table"
    }


# ------------------------------------------------------------------ catalog vs Spark


@pytest.fixture(scope="module")
def spark_schemas(tmp_path_factory, spark):
    root = tmp_path_factory.mktemp("schema_lake")
    now = datetime(2026, 10, 25, 9, tzinfo=timezone.utc)
    c = ih.build_run_context({"run_id": "r", "execution_start": now.isoformat()})
    ih.ingest(["US", "GB"], "k", ih.LocalStorage(root), c, http_get=FakeYouTubeApi(now=now, videos_per_region=12))
    b2s.run(spark, str(root), "2026-10-25", "09")
    s2g.run(spark, str(root), "2026-10-25")
    locations = {
        "silver_trending_videos": "silver/trending_videos",
        "silver_quarantine_trending_videos": "silver/quarantine_trending_videos",
        **{f"gold_{t}": f"gold/{t}" for t in s2g.GOLD_TABLES},
    }
    return {
        name: [(f.name, f.dataType.simpleString()) for f in spark.read.parquet(f"{root}/{loc}").schema.fields]
        for name, loc in locations.items()
    }


def test_every_spark_output_has_a_matching_catalog_table(resources, spark_schemas):
    tables = glue_tables(resources)
    for name, fields in spark_schemas.items():
        assert name in tables, f"missing Glue table {name}"
        table = tables[name]
        keys = [k["Name"] for k in table["PartitionKeys"]]
        columns = [(c["Name"], c["Type"]) for c in table["StorageDescriptor"]["Columns"]]
        assert columns == [f for f in fields if f[0] not in keys], f"column mismatch in {name}"
        assert keys == [f[0] for f in fields if f[0] in keys], f"partition mismatch in {name}"
        assert table["Parameters"]["projection.enabled"] == "true"


def test_table_locations_match_job_output_paths(resources):
    for name, table in glue_tables(resources).items():
        location = table["StorageDescriptor"]["Location"]["Sub"]
        prefix = location.split("}/", 1)[1]
        if name.startswith("gold_"):
            assert prefix == f"gold/{name[5:]}/"


def test_dq_rules_only_reference_cataloged_tables(resources):
    assert set(rules.TABLES.values()) <= set(glue_tables(resources))


# ------------------------------------------------------------------ Athena queries


def test_named_queries_match_sql_files(resources):
    named = [
        r["Properties"]["QueryString"].strip() for r in resources.values() if r["Type"] == "AWS::Athena::NamedQuery"
    ]
    files = [p.read_text().strip() for p in sorted((ROOT / "sql").glob("*.sql"))]
    assert sorted(named) == sorted(files)


# ------------------------------------------------------------------ Step Functions


def _next_targets(state):
    targets = [state.get("Next"), state.get("Default")]
    targets += [c.get("Next") for c in state.get("Choices", [])]
    targets += [c.get("Next") for c in state.get("Catch", [])]
    return [t for t in targets if t]


def test_state_machine_graph_is_valid_and_fully_reachable(asl):
    states = asl["States"]
    seen, stack = set(), [asl["StartAt"]]
    while stack:
        name = stack.pop()
        if name in seen:
            continue
        assert name in states, f"unknown state {name}"
        seen.add(name)
        stack.extend(_next_targets(states[name]))
    assert seen == set(states)
    for name, state in states.items():
        if state["Type"] not in {"Succeed", "Fail", "Choice"}:
            assert "Next" in state or state.get("End"), f"{name} has no transition"


def test_sequential_order(asl):
    order, name = [], asl["StartAt"]
    states = asl["States"]
    while name:
        order.append(name)
        s = states[name]
        name = s.get("Default") if s["Type"] == "Choice" else s.get("Next")
    assert order[:7] == [
        "IngestTrendingData",
        "BronzeToSilver",
        "SilverQualityChecks",
        "SilverQualityGate",
        "SilverToGold",
        "GoldQualityChecks",
        "GoldQualityGate",
    ]


def test_every_work_task_alerts_on_error(asl):
    for name, state in asl["States"].items():
        if state["Type"] == "Task" and "sns:publish" not in state["Resource"]:
            assert any(c["Next"] == "NotifyPipelineError" for c in state.get("Catch", [])), name


def test_quality_gates_route_failures(asl):
    for gate in ("SilverQualityGate", "GoldQualityGate"):
        choice = asl["States"][gate]["Choices"][0]
        assert choice["StringEquals"] == "FAILED" and choice["Next"] == "NotifyQualityFailure"


def test_definition_substitutions_cover_placeholders(resources):
    placeholders = set(re.findall(r"\$\{(\w+)\}", ASL.read_text()))
    subs = resources["PipelineStateMachine"]["Properties"]["DefinitionSubstitutions"]
    assert placeholders == set(subs)


def _resolved_options(script: Path) -> set[str]:
    match = re.search(r"getResolvedOptions\(sys\.argv, \[(.*?)\]\)", script.read_text())
    return {a.strip().strip('"') for a in match.group(1).split(",")} - {"JOB_NAME"}


@pytest.mark.parametrize(
    "state,job,script",
    [
        ("BronzeToSilver", "BronzeToSilverJob", "bronze_to_silver.py"),
        ("SilverToGold", "SilverToGoldJob", "silver_to_gold.py"),
    ],
)
def test_glue_arguments_are_supplied(asl, resources, state, job, script):
    required = _resolved_options(ROOT / "src" / "glue" / script)
    from_sfn = {k.removeprefix("--").removesuffix(".$") for k in asl["States"][state]["Parameters"]["Arguments"]}
    defaults = {k.removeprefix("--") for k in resources[job]["Properties"]["DefaultArguments"]}
    assert required <= from_sfn | defaults
    assert resources[job]["Properties"]["Command"]["ScriptLocation"]["Sub"].endswith(f"/artifacts/glue/{script}")


# ------------------------------------------------------------------ Lambda wiring


@pytest.mark.parametrize("logical_id", ["IngestFunction", "DataQualityFunction"])
def test_lambda_handlers_exist(resources, logical_id):
    props = resources[logical_id]["Properties"]
    module, func = props["Handler"].split(".")
    source = (TEMPLATE.parent / props["CodeUri"] / f"{module}.py").resolve()
    assert source.exists()
    assert f"def {func}(" in source.read_text()


def test_region_defaults_are_consistent(template):
    assert template["Parameters"]["Regions"]["Default"] == ih.DEFAULT_REGIONS
    assert template["Parameters"]["Regions"]["Default"].split(",") == b2s.VALID_REGIONS
    assert template["Parameters"]["ExpectedRegionCount"]["Default"] == len(b2s.VALID_REGIONS)
