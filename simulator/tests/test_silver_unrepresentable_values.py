"""QA D1/D2 on PR #10 (DE-06): JSON that is valid *syntax* but cannot be
written back as strict UTF-8 JSON - a number overflowing float64 (`1e400`
parses to inf) or a lone surrogate escape (`"\\ud800"`). Bronze stores such a
line as a normal object record; Silver must quarantine it losslessly (exact
`raw_payload_text`, parsed form omitted with a stated reason) and commit the
run, never abort it. A non-finite number must never reach the clean table,
whatever the configured thresholds."""

from __future__ import annotations

import json
from typing import Any

import pytest

from simulator.bronze.store import STATUS_COMMITTED
from simulator.silver.pipeline import (
    OMITTED_NON_FINITE_NUMBER,
    OMITTED_UNPAIRED_SURROGATE,
    strict_json_problems,
)
from simulator.tests._silver_helpers import (
    PROCESSING_TIME,
    THRESHOLDS,
    bronze_from_lines,
    clean_payloads,
    silver,
    table,
    valid_payload,
    validate,
    validator,
)

MARKER = 987654.25  # replaced textually by the unrepresentable JSON spelling


def _line(payload: dict[str, Any], spelling: str) -> str:
    """`payload` serialised with MARKER replaced by `spelling` (raw JSON text)."""
    text = json.dumps(payload)
    assert text.count(str(MARKER)) == 1
    return text.replace(str(MARKER), spelling)


GOOD = [json.dumps(p) for p in clean_payloads(3)]

# (Bronze input line, expected primary rule, expected omitted reasons, expected event_id)
BAD: list[tuple[str, str, list[str], str | None]] = [
    # D1: overflow in a validated field (no model upper bound on speed_kmh).
    (_line(valid_payload(event_id="INF-SPEED", speed_kmh=MARKER), "1e400"),
     "schema_conformance", [OMITTED_NON_FINITE_NUMBER], "INF-SPEED"),
    # D1: overflow in an extra (unknown) field.
    (_line(valid_payload(event_id="INF-EXTRA", extra=MARKER), "-1e999"),
     "schema_conformance", [OMITTED_NON_FINITE_NUMBER], "INF-EXTRA"),
    # D2: lone surrogate in event_id (a validated string field).
    (json.dumps(valid_payload(event_id="SUR-\ud800")),
     "schema_conformance", [OMITTED_UNPAIRED_SURROGATE], None),
    # D2: lone surrogate in an extra field's value and in a key.
    (json.dumps(valid_payload(event_id="SUR-EXTRA", note="x\udc00y")),
     "schema_conformance", [OMITTED_UNPAIRED_SURROGATE], "SUR-EXTRA"),
    (json.dumps(valid_payload(event_id="SUR-KEY", **{"k\ud83d": 1})),
     "schema_conformance", [OMITTED_UNPAIRED_SURROGATE], "SUR-KEY"),
    # Both problems in one record.
    (_line(valid_payload(event_id="BOTH", note="\udfff", extra=MARKER), "1E+400"),
     "schema_conformance", [OMITTED_NON_FINITE_NUMBER, OMITTED_UNPAIRED_SURROGATE], "BOTH"),
]


def _strict_loads(line: str) -> Any:
    def reject(name: str) -> Any:
        raise AssertionError(f"non-standard JSON constant {name!r} written")

    return json.loads(line, parse_constant=reject)


def test_lines_are_ascii_json_that_bronze_stores_as_objects():
    for line, *_ in BAD:
        assert line.isascii()
        assert isinstance(json.loads(line), dict)


def test_unrepresentable_values_are_quarantined_and_the_run_commits(tmp_path):
    lines = [GOOD[0], *(line for line, *_ in BAD[:3]), GOOD[1], *(line for line, *_ in BAD[3:]), GOOD[2]]
    bronze = bronze_from_lines(tmp_path / "bronze", lines)
    result = silver(bronze)

    assert result.status == STATUS_COMMITTED
    assert result.manifest["status"] == STATUS_COMMITTED
    counters = result.manifest["counters"]
    assert counters["extracted"] == len(lines)
    assert counters["valid_loaded"] == len(GOOD)
    assert counters["quarantined"] == len(BAD)
    assert counters["duplicates_excluded"] == 0
    assert counters["schema_failures"] == len(BAD)
    assert counters["quarantined_by_rule"]["schema_conformance"] == len(BAD)
    assert sum(counters["quarantined_by_rule"].values()) == len(BAD)
    reconciliation = result.manifest["reconciliation"]
    assert reconciliation["holds"] is True
    assert all(reconciliation["checks"].values())

    clean = table(result, "clean")
    assert [r["event_id"] for r in clean] == [json.loads(g)["event_id"] for g in GOOD]

    quarantine = table(result, "quarantine")
    assert len(quarantine) == len(BAD)
    for record, (line, rule, reasons, event_id) in zip(quarantine, BAD, strict=True):
        assert record["primary_rule"] == rule
        assert record["rejection_category"] == "schema"
        assert record["raw_format"] == "json_object"
        assert record["raw_payload"] is None
        assert record["raw_payload_omitted_reasons"] == reasons
        assert record["event_id"] == event_id
        # Lossless: byte-identical to the Bronze input line.
        assert record["raw_payload_text"].encode("utf-8") == line.encode("utf-8")
        assert record["raw_encoding"] == "utf-8"
        # The original parses back from the kept text.
        assert json.loads(record["raw_payload_text"]).keys() == json.loads(line).keys()

    # Every written line is strict JSON in valid UTF-8 (no NaN/Infinity tokens).
    root = result.manifest_path.parent.parent
    for role in ("clean", "quarantine", "duplicates"):
        data = (root / result.manifest["tables"][role]["path"]).read_bytes()
        for raw in data.decode("utf-8").splitlines():
            _strict_loads(raw)


@pytest.mark.parametrize(("line", "rule", "reasons", "event_id"), BAD, ids=[b[3] or "SUR-EVENT-ID" for b in BAD])
def test_each_unrepresentable_record_alone_is_quarantined_and_commits(tmp_path, line, rule, reasons, event_id):
    result = silver(bronze_from_lines(tmp_path / "bronze", [GOOD[0], line, GOOD[1]]))
    assert result.status == STATUS_COMMITTED
    counters = result.manifest["counters"]
    assert (counters["extracted"], counters["valid_loaded"], counters["quarantined"]) == (3, 2, 1)
    assert result.manifest["reconciliation"]["holds"] is True
    (record,) = table(result, "quarantine")
    assert record["primary_rule"] == rule
    assert record["raw_payload"] is None
    assert record["raw_payload_omitted_reasons"] == reasons
    assert record["event_id"] == event_id
    assert record["raw_payload_text"].encode("utf-8") == line.encode("utf-8")


def test_rerun_over_the_same_bronze_store_is_a_no_op(tmp_path):
    bronze = bronze_from_lines(tmp_path / "bronze", [GOOD[0], *(line for line, *_ in BAD)])
    first = silver(bronze)
    second = silver(bronze)
    assert first.status == STATUS_COMMITTED
    assert second.status == "ALREADY_COMMITTED"
    assert second.manifest["tables"] == first.manifest["tables"]


def test_non_finite_speed_never_reaches_clean_even_with_an_infinite_max_speed(tmp_path):
    # TOML accepts `inf`; with max_speed_kmh = inf the threshold rule cannot
    # reject speed inf, so the explicit finiteness check must.
    thresholds = THRESHOLDS.model_copy(update={"max_speed_kmh": float("inf")})
    inf_line = BAD[0][0]
    bronze = bronze_from_lines(tmp_path / "bronze", [GOOD[0], inf_line, GOOD[1]])
    result = silver(bronze, thresholds=thresholds)
    assert result.status == STATUS_COMMITTED
    assert [r["event_id"] for r in table(result, "clean")] == [json.loads(g)["event_id"] for g in GOOD[:2]]
    (record,) = table(result, "quarantine")
    assert record["event_id"] == "INF-SPEED"
    assert record["primary_rule"] == "schema_conformance"
    assert "speed_kmh: non-finite number inf" in record["rejections"][0]["reason"]
    assert record["raw_payload_text"] == inf_line
    assert result.manifest["reconciliation"]["holds"] is True


def test_duplicate_of_a_valid_event_cannot_carry_unrepresentable_values(tmp_path):
    # A copy of a valid event_id that carries inf / a surrogate is not a
    # duplicate: it fails validation and is quarantined, the original stays
    # clean, and the duplicates table only ever receives validated events.
    base = clean_payloads(1)[0]
    copy_inf = _line({**base, "speed_kmh": MARKER}, "1e400")
    copy_sur = json.dumps({**base, "vehicle_id": "V\ud800"})
    bronze = bronze_from_lines(tmp_path / "bronze", [json.dumps(base), copy_inf, copy_sur, json.dumps(base)])
    result = silver(bronze)
    counters = result.manifest["counters"]
    assert (counters["valid_loaded"], counters["quarantined"], counters["duplicates_excluded"]) == (1, 2, 1)
    assert [r["raw_payload_omitted_reasons"] for r in table(result, "quarantine")] == [
        [OMITTED_NON_FINITE_NUMBER], [OMITTED_UNPAIRED_SURROGATE],
    ]
    (duplicate,) = table(result, "duplicates")
    assert duplicate["event_id"] == base["event_id"] and duplicate["payload_matches_kept"] is True
    assert result.manifest["reconciliation"]["holds"] is True


def test_validator_rejects_non_finite_in_every_float_field():
    for field in ("latitude", "longitude", "speed_kmh", "heading"):
        for spelling in ("1e400", "-1e400"):
            payload = valid_payload(**{field: MARKER})
            text = _line(payload, spelling)
            outcome = validator(max_speed_kmh=float("inf")).validate(
                json.loads(text), text, ingestion_time=PROCESSING_TIME
            )
            assert not outcome.is_valid, (field, spelling)


def test_validator_rejects_lone_surrogates_in_string_fields():
    for field in ("event_id", "vehicle_id", "route_id", "status", "schema_version"):
        outcome = validate(valid_payload(**{field: "A\ud800"}))
        assert not outcome.is_valid, field


def test_strict_json_problems():
    assert strict_json_problems({"a": [1, 2.5, "x", None, True, {"b": "é"}]}) == []
    assert strict_json_problems({"a": [{"b": float("-inf")}]}) == [OMITTED_NON_FINITE_NUMBER]
    assert strict_json_problems({"a": float("nan")}) == [OMITTED_NON_FINITE_NUMBER]
    assert strict_json_problems({"\udc00": 1}) == [OMITTED_UNPAIRED_SURROGATE]
    assert strict_json_problems(["ok", ["x\ud800"], 1e308]) == [OMITTED_UNPAIRED_SURROGATE]

