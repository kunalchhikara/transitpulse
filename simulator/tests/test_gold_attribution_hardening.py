"""DE-08 Gold attribution hardening (BACKLOG FU-10, FU-11):

* a quarantined record rejected by ANY Silver event-time rule
  (`timestamp_bounds`, `future_time_tolerance`, `watermark_guard`) is
  unattributed (`event_time_rejected`);
* a `supported_schema_version` failure is unattributed
  (`schema_version_unsupported`): field meaning is undefined under an unknown
  schema;
* Silver evaluates stage-2 rules only for records that pass stage 1, so Gold
  re-applies the event-time rules (bounds, future-time tolerance, watermark
  guard - with the thresholds and Silver config recorded in the Silver
  manifest) to every quarantined record it would attribute
  (`event_time_rejected_on_recheck`; `event_time_unverifiable` when the
  reference instant is unusable);
* no Gold row has a period outside the configured timestamp bounds.

The expected Gold numbers for `HARDENING_LINES` were computed by hand from the
table below, not by calling Gold code.

| id     | route    | event_time / generated_at          | defect        | Silver               | Gold                            |
|--------|----------|------------------------------------|---------------|----------------------|---------------------------------|
| E1     | ROUTE-01 | 00:10 / 00:10                      | -             | clean in_order       | R01 h00                         |
| QS     | ROUTE-01 | 00:20 / 00:20, schema 2.0.0        | version       | supported_schema_ver | schema_version_unsupported      |
| QB1    | ROUTE-01 | 0001-01-01 / same                  | speed -5      | non_negative_speed   | event_time_rejected_on_recheck  |
| QB9    | ROUTE-01 | 9999-12-31T23:59:59 / same         | speed -5      | non_negative_speed   | event_time_rejected_on_recheck  |
| QF     | ROUTE-01 | 2030-01-01 / 00:30                 | speed -5      | non_negative_speed   | event_time_rejected_on_recheck  |
| QG     | ROUTE-01 | 2090-01-01 / same                  | speed -5      | non_negative_speed   | event_time_rejected_on_recheck  |
| QT     | ROUTE-02 | 9999-12-31T23:59:59 / same         | -             | timestamp_bounds     | event_time_rejected             |
| QW     | ROUTE-02 | 2090-01-01 / same                  | -             | watermark_guard      | event_time_rejected             |
| QU     | ROUTE-02 | 00:40 / "bad"                      | -             | parsable_timestamps  | event_time_unverifiable         |
| Q1     | ROUTE-02 | 00:50 / 00:50                      | speed -5      | non_negative_speed   | R02 h00                         |

Rows: R01 h00 (1 event, delay 60, 0 quarantined, 100 %); R02 h00 (0 events,
1 quarantined, 0 %). Totals: 10 extracted = 1 clean + 9 quarantined;
attributed 1, unattributed 8 (1 + 2 + 4 + 1); overall 1 / 10 = 10 %.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest

from simulator.bronze import fixed_clock
from simulator.gold import GoldInputError, Periods, attribute_quarantined
from simulator.gold.aggregate import (
    UNATTRIBUTED_EVENT_TIME_RECHECK,
    UNATTRIBUTED_EVENT_TIME_REJECTED,
    UNATTRIBUTED_EVENT_TIME_UNPARSEABLE,
    UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE,
    UNATTRIBUTED_REASONS,
    UNATTRIBUTED_SCHEMA_VERSION,
    EventTimeCheck,
    aggregate_route_performance,
)
from simulator.tests._bronze_helpers import REPO_ROOT
from simulator.tests._gold_helpers import (
    gold,
    gold_rows,
    silver_from_lines,
    silver_table,
    store_files,
)
from simulator.tests._silver_helpers import (
    SILVER_CONFIG,
    THRESHOLDS,
    silver,
    valid_payload,
)

HOURLY = Periods(period_minutes=60, utc_offset_hours=10)
KNOWN = frozenset({"ROUTE-01", "ROUTE-02"})
DAY = "2025-01-01"
INGESTED = "2025-02-01T09:30:00.000000Z"
PROCESSED = "2025-02-01T10:00:00.000000Z"


def _t(hhmm: str) -> str:
    return f"{DAY}T{hhmm}:00Z"


def _event(event_id, route_id, event_time, generated_at=None, **overrides):
    return valid_payload(event_id=event_id, route_id=route_id, event_time=event_time,
                         generated_at=generated_at if generated_at is not None else event_time,
                         trip_id=f"TRIP-{route_id}-0000", delay_seconds=60, **overrides)


Y1, Y9, Y2090 = "0001-01-01T00:00:00Z", "9999-12-31T23:59:59Z", "2090-01-01T00:00:00Z"

HARDENING_LINES = [
    json.dumps(_event("E1", "ROUTE-01", _t("00:10"))),
    json.dumps(_event("QS", "ROUTE-01", _t("00:20"), schema_version="2.0.0")),
    json.dumps(_event("QB1", "ROUTE-01", Y1, speed_kmh=-5.0)),
    json.dumps(_event("QB9", "ROUTE-01", Y9, speed_kmh=-5.0)),
    json.dumps(_event("QF", "ROUTE-01", "2030-01-01T00:00:00Z", _t("00:30"), speed_kmh=-5.0)),
    json.dumps(_event("QG", "ROUTE-01", Y2090, speed_kmh=-5.0)),
    json.dumps(_event("QT", "ROUTE-02", Y9)),
    json.dumps(_event("QW", "ROUTE-02", Y2090)),
    json.dumps(_event("QU", "ROUTE-02", _t("00:40"), "bad")),
    json.dumps(_event("Q1", "ROUTE-02", _t("00:50"), speed_kmh=-5.0)),
]

EXPECTED_UNATTRIBUTED = {
    **dict.fromkeys(UNATTRIBUTED_REASONS, 0),
    UNATTRIBUTED_SCHEMA_VERSION: 1,
    UNATTRIBUTED_EVENT_TIME_REJECTED: 2,
    UNATTRIBUTED_EVENT_TIME_RECHECK: 4,
    UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE: 1,
}


def _hardening_gold(tmp_path):
    silver_from_lines(tmp_path, HARDENING_LINES)
    return gold(tmp_path)


def test_silver_outcome_is_what_the_hand_computation_assumes(tmp_path):
    result = silver_from_lines(tmp_path, HARDENING_LINES)
    assert [r["event_id"] for r in silver_table(result, "clean")] == ["E1"]
    assert {r["event_id"]: r["primary_rule"] for r in silver_table(result, "quarantine")} == {
        "QS": "supported_schema_version", "QB1": "non_negative_speed", "QB9": "non_negative_speed",
        "QF": "non_negative_speed", "QG": "non_negative_speed", "QT": "timestamp_bounds",
        "QW": "watermark_guard", "QU": "parsable_timestamps", "Q1": "non_negative_speed",
    }


def test_gold_attribution_matches_the_hand_computation(tmp_path):
    """Before DE-08: QS, QF, QG and QB1 were attributed (QB1 to a 0001-01-01
    row, QF to 2030, QG to 2090) and QT/QW loaded as clean, so the run failed
    on QT's unrepresentable period."""
    result = _hardening_gold(tmp_path)
    counters = result.manifest["counters"]
    assert counters["unattributed_by_reason"] == EXPECTED_UNATTRIBUTED
    assert (counters["quarantined_attributed"], counters["quarantined_unattributed"]) == (1, 8)
    assert counters["overall_valid_percentage"] == 10.0
    assert [(r["route_id"], r["period_start"], r["event_count"], r["quarantined_count"], r["valid_percentage"])
            for r in gold_rows(result)] == [
        ("ROUTE-01", f"{DAY}T00:00:00.000000Z", 1, 0, 100.0),
        ("ROUTE-02", f"{DAY}T00:00:00.000000Z", 0, 1, 0.0),
    ]
    assert result.manifest["reconciliation"]["holds"]


def test_no_gold_row_has_a_period_outside_the_configured_bounds(tmp_path):
    rows = gold_rows(_hardening_gold(tmp_path))
    lowest = THRESHOLDS.min_timestamp - timedelta(days=1)
    for row in rows:
        start = datetime.fromisoformat(row["period_start"])
        assert lowest <= start <= THRESHOLDS.max_timestamp, row


def test_attribution_settings_are_recorded_in_the_gold_manifest(tmp_path):
    manifest = _hardening_gold(tmp_path).manifest
    attribution = manifest["attribution"]
    assert attribution["event_time_rules"] == ["timestamp_bounds", "future_time_tolerance", "watermark_guard"]
    assert attribution["min_timestamp"] == "2000-01-01T00:00:00Z"
    assert attribution["max_timestamp"] == "2100-01-01T00:00:00Z"
    assert attribution["future_time_reference"] == "generated_at"
    assert attribution["watermark_guard"]["mode"] == "quarantine"


def test_the_qa_year_0001_record_is_no_longer_attributed(tmp_path):
    """QA of PR #12 (O2): a quarantined year-0001 record was attributed to a
    0001-01-01 Gold row."""
    silver_from_lines(tmp_path, [json.dumps(_event("QB1", "ROUTE-01", Y1, speed_kmh=-5.0))])
    result = gold(tmp_path)
    assert gold_rows(result) == []
    assert result.manifest["counters"]["unattributed_by_reason"][UNATTRIBUTED_EVENT_TIME_RECHECK] == 1


def test_pre_de08_silver_manifest_is_refused(tmp_path):
    """Gold re-applies the thresholds Silver recorded; a Silver manifest
    written before DE-08 has no timestamp bounds or watermark guard, so Gold
    refuses it (fail closed) rather than guess."""
    silver_from_lines(tmp_path, HARDENING_LINES[:1])
    path = tmp_path / "silver" / "_runs" / "s1.json"
    manifest = json.loads(path.read_text())
    del manifest["validation_thresholds"]["max_timestamp"]
    del manifest["config"]["watermark_guard"]
    path.write_text(json.dumps(manifest))
    with pytest.raises(GoldInputError, match="re-run Silver"):
        gold(tmp_path)
    assert not (tmp_path / "gold" / "route_performance").exists()


def test_committed_silver_manifest_with_failed_reconciliation_blocks_every_gold_run(tmp_path):
    """FU-11 fail-closed note (README DE-07 "Input"): the bad manifest blocks
    Gold even when Gold is pinned to a different, healthy Silver run."""
    silver_from_lines(tmp_path, HARDENING_LINES[:1])  # s1: healthy
    silver(tmp_path / "bronze", tmp_path / "silver", run_id="s2",
           clock=fixed_clock(datetime(2025, 2, 1, 12, 0, tzinfo=UTC)))
    path = tmp_path / "silver" / "_runs" / "s2.json"
    manifest = json.loads(path.read_text())
    manifest["reconciliation"]["holds"] = False
    path.write_text(json.dumps(manifest))
    for pinned in (None, "s1"):
        with pytest.raises(GoldInputError, match="reconciliation does not hold"):
            gold(tmp_path, silver_run_id=pinned)
    assert not (tmp_path / "gold" / "_runs").exists()


def test_cli_chain_is_byte_identical_across_two_processes(tmp_path):
    source = tmp_path / "events.ndjson"
    source.write_text("\n".join(HARDENING_LINES) + "\n")

    def run(*args: str) -> None:
        subprocess.run([sys.executable, "-m", *args], cwd=REPO_ROOT, check=True, capture_output=True)

    for name in ("a", "b"):
        root = tmp_path / name
        run("simulator.bronze", "stream", "--input", str(source), "--run-id", "b1", "--output-dir",
            str(root / "bronze"), "--fixed-ingestion-time", "2025-02-01T09:30:00Z")
        run("simulator.silver", "--run-id", "s1", "--bronze-dir", str(root / "bronze"), "--output-dir",
            str(root / "silver"), "--fixed-processing-time", "2025-02-01T10:00:00Z")
        run("simulator.gold", "--run-id", "g1", "--silver-dir", str(root / "silver"), "--output-dir",
            str(root / "gold"), "--fixed-processing-time", "2025-02-01T11:00:00Z")
    for store in ("silver", "gold"):
        assert store_files(tmp_path / "a" / store) == store_files(tmp_path / "b" / store), store
    manifest = json.loads((tmp_path / "a" / "gold" / "_runs" / "g1.json").read_text())
    assert manifest["counters"]["unattributed_by_reason"] == EXPECTED_UNATTRIBUTED


# -- attribution rule, unit level ---------------------------------------------------------

SILVER_MANIFEST = {"validation_thresholds": THRESHOLDS.model_dump(mode="json"),
                   "config": SILVER_CONFIG.fingerprint_fields()}
CHECK = EventTimeCheck.from_silver_manifest(SILVER_MANIFEST)


def _check(**config) -> EventTimeCheck:
    return EventTimeCheck.from_silver_manifest(
        {**SILVER_MANIFEST, "config": {**SILVER_MANIFEST["config"], **config}}
    )


def _q(payload, *, rules=("non_negative_speed",), ingestion_time=INGESTED, processing_time=PROCESSED):
    record = {"raw_format": "json_object", "raw_payload": payload, "rejection_rules": list(rules),
              "ingestion_time": ingestion_time, "processing_time": processing_time}
    return {k: v for k, v in record.items() if v is not None}


def _p(event_time, generated_at=None, route="ROUTE-01"):
    return {"route_id": route, "event_time": event_time,
            "generated_at": generated_at if generated_at is not None else event_time}


R01_H00 = ("ROUTE-01", datetime(2025, 1, 1, tzinfo=UTC))


@pytest.mark.parametrize(
    ("record", "check", "expected"),
    [
        # FU-11: unsupported schema version - unattributed with or without a check.
        (_q(_p(_t("00:10")), rules=("supported_schema_version",)), None, UNATTRIBUTED_SCHEMA_VERSION),
        (_q(_p(_t("00:10")), rules=("supported_schema_version",)), CHECK, UNATTRIBUTED_SCHEMA_VERSION),
        # Every Silver event-time rule -> event_time_rejected.
        (_q(_p(Y9), rules=("timestamp_bounds",)), CHECK, UNATTRIBUTED_EVENT_TIME_REJECTED),
        (_q(_p(Y9), rules=("timestamp_bounds",)), None, UNATTRIBUTED_EVENT_TIME_REJECTED),
        (_q(_p(Y2090), rules=("watermark_guard",)), CHECK, UNATTRIBUTED_EVENT_TIME_REJECTED),
        (_q(_p(_t("02:00"), _t("00:00")), rules=("future_time_tolerance",)), CHECK,
         UNATTRIBUTED_EVENT_TIME_REJECTED),
        # Re-check of a stage-1 reject: bounds (event_time, generated_at), future tolerance, guard.
        (_q(_p(Y1)), CHECK, UNATTRIBUTED_EVENT_TIME_RECHECK),
        (_q(_p(Y9)), CHECK, UNATTRIBUTED_EVENT_TIME_RECHECK),
        (_q(_p(_t("00:10"), "1999-12-31T23:59:59Z")), CHECK, UNATTRIBUTED_EVENT_TIME_RECHECK),
        (_q(_p("2030-01-01T00:00:00Z", _t("00:00"))), CHECK, UNATTRIBUTED_EVENT_TIME_RECHECK),
        (_q(_p(_t("00:02"), _t("00:00"))), CHECK, R01_H00),  # exactly at the 120 s tolerance
        (_q(_p("2025-01-01T00:02:00.000001Z", _t("00:00"))), CHECK, UNATTRIBUTED_EVENT_TIME_RECHECK),
        (_q(_p(Y2090)), CHECK, UNATTRIBUTED_EVENT_TIME_RECHECK),
        # Without a check (DE-07 behaviour) the far-future stage-1 reject would be attributed.
        (_q(_p(Y2090)), None, ("ROUTE-01", datetime(2090, 1, 1, tzinfo=UTC))),
        # Guard off / flag: the guard is not a Silver rule then, so it is not re-applied.
        (_q(_p(Y2090)), _check(watermark_guard={"mode": "off", "reference": "ingestion_time",
                                                "max_skew_seconds": 300}),
         ("ROUTE-01", datetime(2090, 1, 1, tzinfo=UTC))),
        (_q(_p(Y2090)), _check(watermark_guard={"mode": "flag", "reference": "ingestion_time",
                                                "max_skew_seconds": 300}),
         ("ROUTE-01", datetime(2090, 1, 1, tzinfo=UTC))),
        # Guard reference = processing_time: 20 min after ingestion, before processing -> fine.
        (_q(_p("2025-02-01T09:50:00Z")), _check(watermark_guard={
            "mode": "quarantine", "reference": "processing_time", "max_skew_seconds": 300}),
         ("ROUTE-01", datetime(2025, 2, 1, 9, tzinfo=UTC))),
        (_q(_p("2025-02-01T09:50:00Z")), CHECK, UNATTRIBUTED_EVENT_TIME_RECHECK),
        # future_time_reference = ingestion_time.
        (_q(_p("2025-02-01T09:33:00Z", "2025-02-01T09:33:00Z")), _check(future_time_reference="ingestion_time"),
         UNATTRIBUTED_EVENT_TIME_RECHECK),
        (_q(_p("2025-02-01T09:31:00Z", "2025-02-01T09:31:00Z")), _check(future_time_reference="ingestion_time"),
         ("ROUTE-01", datetime(2025, 2, 1, 9, tzinfo=UTC))),
        # Unverifiable: the reference instant is missing or unparseable.
        (_q(_p(_t("00:10"), "bad"), rules=("parsable_timestamps",)), CHECK, UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE),
        (_q({"route_id": "ROUTE-01", "event_time": _t("00:10")}, rules=("required_fields",)), CHECK,
         UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE),
        (_q(_p(_t("00:10")), ingestion_time=None), CHECK, UNATTRIBUTED_EVENT_TIME_UNVERIFIABLE),
        (_q(_p(_t("00:10")), ingestion_time=None),
         _check(watermark_guard={"mode": "off", "reference": "ingestion_time", "max_skew_seconds": 300}),
         R01_H00),
        # Precedence: an unparseable event_time is reported before any check.
        (_q(_p("not-a-time", _t("00:00"))), CHECK, UNATTRIBUTED_EVENT_TIME_UNPARSEABLE),
        # A normal stage-1 reject is still attributed.
        (_q(_p(_t("00:10"))), CHECK, R01_H00),
    ],
)
def test_attribution_with_event_time_check(record, check, expected):
    assert attribute_quarantined(record, KNOWN, HOURLY, event_time_check=check) == expected


def test_clean_record_outside_the_bounds_breaks_the_silver_contract():
    clean = [{"event_id": "X", "route_id": "ROUTE-01", "event_time": "2100-01-01T00:00:00.000001Z",
              "delay_seconds": 1, "_silver_event_time_status": "in_order"}]
    with pytest.raises(GoldInputError, match="outside the Silver timestamp bounds"):
        aggregate_route_performance(clean, [], periods=HOURLY, reference_routes={"ROUTE-01": ("100", "x")},
                                    lineage={"gold_run_id": "g", "silver_run_id": "s", "processing_time": "p"},
                                    event_time_check=CHECK)


@pytest.mark.parametrize(
    "manifest",
    [
        {"config": SILVER_MANIFEST["config"]},
        {"validation_thresholds": SILVER_MANIFEST["validation_thresholds"]},
        {**SILVER_MANIFEST, "config": {**SILVER_MANIFEST["config"], "watermark_guard": {"mode": "x"}}},
    ],
)
def test_event_time_check_needs_a_de08_silver_manifest(manifest):
    with pytest.raises(GoldInputError, match="re-run Silver"):
        EventTimeCheck.from_silver_manifest(manifest)
