"""Per-dataset current state and the two merge semantics of batch Silver.

Merge mode (DE-04 catalogue datasets), per valid row, against the current
row with the same primary key (rows earlier in the same run included):

| outcome     | condition                                              | effect                         |
|-------------|--------------------------------------------------------|--------------------------------|
| `inserted`  | key not present                                        | row added                      |
| `updated`   | incoming `updated_at` newer                            | row replaced                   |
| `unchanged` | same `updated_at`, same standardised payload           | nothing (lineage kept)         |
| `stale`     | incoming `updated_at` older                            | excluded, written to `stale_*` |
| `conflict`  | same `updated_at`, different payload                   | quarantined (`merge_conflict`) |

Snapshot mode (DE-01 reference entities loaded in `full` by DE-11): each
Bronze run is a complete snapshot. Valid rows (first occurrence per key;
repeats quarantined as `duplicate_key_in_snapshot`) become the current
state: `inserted` (new or previously deleted key), `updated` (payload
changed), `unchanged`; every active key absent from the valid snapshot is
**soft-deleted** (`_silver_is_deleted = true`, row kept as a tombstone) and
counted as `deleted`. A snapshot older than the last applied one for the
dataset (by DE-11 `as_of`, else Bronze `ingestion_time`) is `stale` as a
whole: its valid rows are counted and audited as stale and nothing changes.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from simulator.bronze.records import (
    INGESTION_TIME,
    LINE_NUMBER,
    PARSE_ERROR,
    PARSE_ERROR_DETAIL,
    RAW_ENCODING,
    RAW_LINE,
    RUN_ID,
    SOURCE,
    SOURCE_FILE,
    strip_metadata,
)
from simulator.silver_batch.bronze_input import (
    EXTRACT_FOUND,
    OUTCOME_BRONZE_PARSE_ERROR,
    OUTCOME_STORED_REQUIRES_QUARANTINE,
    BronzeRow,
    De11Listing,
)
from simulator.silver_batch.datasets import ForeignKeySpec, LoadMode, SilverDataset
from simulator.silver_batch.errors import BronzeContractError, SnapshotDeleteGuardError
from simulator.silver_batch.rules import (
    CATEGORY_MERGE,
    CATEGORY_PARSE,
    CATEGORY_SCHEMA,
    CATEGORY_SOURCE,
    RULE_BRONZE_PARSE_ERROR,
    RULE_DUPLICATE_KEY_IN_SNAPSHOT,
    RULE_MERGE_CONFLICT,
    RULE_PRESERVED_BAD_SOURCE_ROW,
    Violation,
    ordered,
    primary_key_of,
    row_hash,
    validate_record,
)

INSERTED = "inserted"
UPDATED = "updated"
UNCHANGED = "unchanged"
STALE = "stale"
QUARANTINED = "quarantined"
DELETED = "deleted"

STALE_OLDER_UPDATED_AT = "older_updated_at"
STALE_OLDER_SNAPSHOT = "older_snapshot"

#: Why a quarantined object's parsed form is null (the exact text is kept).
OMITTED_NON_FINITE_NUMBER = "non_finite_number"
OMITTED_UNPAIRED_SURROGATE = "unpaired_surrogate"

_CATEGORY_COUNTER = {
    CATEGORY_PARSE: "parse_errors",
    CATEGORY_SCHEMA: "schema_failures",
    CATEGORY_SOURCE: "source_flagged",
    CATEGORY_MERGE: "merge_quarantined",
}


def dumps_line(record: dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"


def _has_surrogate(text: str) -> bool:
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return True
    return False


def json_problems(value: Any) -> list[str]:
    """Parts of a parsed JSON value strict UTF-8 JSON cannot write."""
    problems: set[str] = set()
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, float) and not math.isfinite(item):
            problems.add(OMITTED_NON_FINITE_NUMBER)
        elif isinstance(item, str) and _has_surrogate(item):
            problems.add(OMITTED_UNPAIRED_SURROGATE)
        elif isinstance(item, dict):
            stack.extend(item.keys())
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return sorted(problems)


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(UTC)


@dataclass
class Counters:
    extracted: int = 0
    bronze_records: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    stale: int = 0
    quarantined: int = 0
    conflicts: int = 0
    duplicate_keys_in_snapshot: int = 0
    deleted: int = 0
    deleted_with_quarantined_row: int = 0
    parse_errors: int = 0
    schema_failures: int = 0
    source_flagged: int = 0
    merge_quarantined: int = 0
    de11_listed_rows: int = 0
    de11_listed_stored_rows_quarantined: int = 0
    de11_listed_parse_error_rows_quarantined: int = 0
    quarantined_by_rule: Counter = field(default_factory=Counter)
    rule_violations: Counter = field(default_factory=Counter)
    parse_errors_by_code: Counter = field(default_factory=Counter)

    def add(self, other: Counters) -> None:
        for name, value in vars(other).items():
            if isinstance(value, Counter):
                getattr(self, name).update(value)
            else:
                setattr(self, name, getattr(self, name) + value)

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for name, value in vars(self).items():
            out[name] = dict(sorted(value.items())) if isinstance(value, Counter) else value
        return out


@dataclass
class RunContext:
    silver_run_id: str
    processing_time: str
    max_snapshot_delete_fraction: float
    log_rejection: Callable[[str, dict[str, Any], list[Violation]], None]


@dataclass
class DatasetState:
    dataset: SilverDataset
    rows: dict[tuple, dict[str, Any]]
    rows_before: int
    tombstones_before: int
    last_snapshot_as_of: str | None
    counters: Counters = field(default_factory=Counters)
    steps: list[dict[str, Any]] = field(default_factory=list)
    quarantine_lines: list[str] = field(default_factory=list)
    stale_lines: list[str] = field(default_factory=list)
    #: keys inserted/updated (and still active) by this run, for FU-02 counts
    written_keys: set[tuple] = field(default_factory=set)
    changed: bool = False
    last_valid_snapshot_keys: int | None = None

    @property
    def active_rows(self) -> int:
        return sum(1 for row in self.rows.values() if not row["_silver_is_deleted"])

    @property
    def tombstones(self) -> int:
        return len(self.rows) - self.active_rows

    def key_of(self, payload: dict[str, Any]) -> tuple:
        return tuple(payload[column] for column in self.dataset.primary_key)


def _lineage(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "ingestion_time": record[INGESTION_TIME],
        "source": record[SOURCE],
        "bronze_run_id": record[RUN_ID],
        SOURCE_FILE: record[SOURCE_FILE],
        LINE_NUMBER: record[LINE_NUMBER],
    }


def _current_row(ctx: RunContext, payload: dict[str, Any], digest: str, record: dict[str, Any]) -> dict[str, Any]:
    return {
        **payload,
        "_silver_row_hash": digest,
        "_silver_is_deleted": False,
        "_silver_deleted_by_run_id": None,
        "_silver_batch_run_id": ctx.silver_run_id,
        "_silver_processing_time": ctx.processing_time,
        "_bronze_run_id": record[RUN_ID],
        "_bronze_ingestion_time": record[INGESTION_TIME],
        "_bronze_source": record[SOURCE],
        "_bronze_source_file": record[SOURCE_FILE],
        "_bronze_line_number": record[LINE_NUMBER],
    }


def _quarantine_record(
    ctx: RunContext,
    dataset: SilverDataset,
    row: BronzeRow,
    violations: list[Violation],
    listed: dict[str, Any] | None,
    conflict_with: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = row.record
    primary = violations[0]
    common = {
        "silver_batch_run_id": ctx.silver_run_id,
        "processing_time": ctx.processing_time,
        "dataset": dataset.name,
        **_lineage(record),
        "primary_rule": primary.rule,
        "rejection_category": primary.category,
        "rejection_rules": [v.rule for v in violations],
        "rejections": [v.as_dict() for v in violations],
        "de11_preserved_bad_row": listed,
        "conflict_with": conflict_with,
    }
    if row.is_parse_error:
        return {
            **common,
            "primary_key": None,
            "raw_format": "bronze_parse_error",
            "raw_payload": None,
            "raw_payload_omitted_reasons": [],
            "raw_payload_text": record[RAW_LINE],
            "raw_encoding": record[RAW_ENCODING],
            "bronze_parse_error": record[PARSE_ERROR],
            "bronze_parse_error_detail": record[PARSE_ERROR_DETAIL],
        }
    original = strip_metadata(record)
    omitted = json_problems(original)
    return {
        **common,
        "primary_key": primary_key_of(dataset, original),
        "raw_format": "json_object",
        "raw_payload": None if omitted else original,
        "raw_payload_omitted_reasons": omitted,
        "raw_payload_text": row.original_text,
        "raw_encoding": "utf-8",
        "bronze_parse_error": None,
        "bronze_parse_error_detail": None,
    }


def _stale_record(
    ctx: RunContext,
    dataset: SilverDataset,
    payload: dict[str, Any],
    digest: str,
    record: dict[str, Any],
    reason: str,
    current: dict[str, Any] | None,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "silver_batch_run_id": ctx.silver_run_id,
        "processing_time": ctx.processing_time,
        "dataset": dataset.name,
        **_lineage(record),
        "stale_reason": reason,
        "primary_key": {column: payload[column] for column in dataset.primary_key},
        "row_hash": digest,
        "current_row_hash": current["_silver_row_hash"] if current else None,
        "current_bronze_run_id": current["_bronze_run_id"] if current else None,
        "current_updated_at": current.get(dataset.watermark_column) if current and dataset.watermark_column else None,
        "snapshot": snapshot,
        "payload": payload,
    }


class _Step:
    """Applies one Bronze table run to one dataset state."""

    def __init__(self, ctx: RunContext, state: DatasetState, listing: De11Listing, run_id: str) -> None:
        self.ctx = ctx
        self.state = state
        self.listing = listing
        self.run_id = run_id
        self.counters = Counters()
        self.seen_listed: set[int] = set()

    def quarantine(
        self,
        row: BronzeRow,
        violations: list[Violation],
        listed: dict[str, Any] | None,
        conflict_with: dict[str, Any] | None = None,
    ) -> None:
        violations = ordered(violations)
        c = self.counters
        c.quarantined += 1
        c.quarantined_by_rule[violations[0].rule] += 1
        setattr(c, _CATEGORY_COUNTER[violations[0].category], getattr(c, _CATEGORY_COUNTER[violations[0].category]) + 1)
        for violation in violations:
            c.rule_violations[violation.rule] += 1
        if any(v.rule == RULE_PRESERVED_BAD_SOURCE_ROW for v in violations):
            if row.is_parse_error:
                c.de11_listed_parse_error_rows_quarantined += 1
            else:
                c.de11_listed_stored_rows_quarantined += 1
        record = _quarantine_record(self.ctx, self.state.dataset, row, violations, listed, conflict_with)
        self.state.quarantine_lines.append(dumps_line(record))
        self.ctx.log_rejection(self.state.dataset.name, record, violations)

    def validate(self, row: BronzeRow) -> tuple[dict[str, Any] | None, list[Violation], dict[str, Any] | None]:
        """(standardised payload, violations, DE-11 listing entry)."""
        self.counters.extracted += 1
        listed = None
        if self.listing.status == EXTRACT_FOUND:
            listed = self.listing.rows.get(row.line_number)
        violations: list[Violation] = []
        payload = None
        if row.is_parse_error:
            self.counters.parse_errors_by_code[row.record[PARSE_ERROR]] += 1
            violations.append(
                Violation(RULE_BRONZE_PARSE_ERROR,
                          f"Bronze parse error {row.record[PARSE_ERROR]}: {row.record[PARSE_ERROR_DETAIL]}")
            )
        else:
            assert row.original_text is not None
            result = validate_record(self.state.dataset, strip_metadata(row.record), row.original_text)
            violations.extend(result.violations)
            payload = result.payload
        if listed is not None:
            self.seen_listed.add(row.line_number)
            self.counters.de11_listed_rows += 1
            outcome = listed.get("bronze_outcome")
            expected = OUTCOME_BRONZE_PARSE_ERROR if row.is_parse_error else OUTCOME_STORED_REQUIRES_QUARANTINE
            if outcome != expected:
                raise BronzeContractError(
                    f"DE-11 lists line {row.line_number} of Bronze run {self.run_id!r} as {outcome!r}, "
                    f"but Bronze holds a {'parse-error' if row.is_parse_error else 'stored'} record"
                )
            violations.append(
                Violation(
                    RULE_PRESERVED_BAD_SOURCE_ROW,
                    f"DE-11 preserved bad source line {listed.get('source_line')} ({listed.get('reason')}); "
                    f"bronze_outcome={outcome}",
                )
            )
        return (None if violations else payload), violations, listed

    def finish(self) -> None:
        missing = sorted(set(self.listing.rows) - self.seen_listed) if self.listing.status == EXTRACT_FOUND else []
        if missing:
            raise BronzeContractError(
                f"DE-11 lists extract line(s) {missing[:10]} that Bronze run {self.run_id!r} does not hold"
            )


def apply_merge_run(
    ctx: RunContext, state: DatasetState, rows: Iterable[BronzeRow], listing: De11Listing, run_id: str
) -> Counters:
    step = _Step(ctx, state, listing, run_id)
    ds = state.dataset
    assert ds.watermark_column is not None
    c = step.counters
    for row in rows:
        payload, violations, listed = step.validate(row)
        if violations:
            step.quarantine(row, violations, listed)
            continue
        assert payload is not None
        key = state.key_of(payload)
        digest = row_hash(payload)
        current = state.rows.get(key)
        if current is None:
            state.rows[key] = _current_row(ctx, payload, digest, row.record)
            state.written_keys.add(key)
            c.inserted += 1
            continue
        incoming_at, current_at = _utc(payload[ds.watermark_column]), _utc(current[ds.watermark_column])
        if incoming_at > current_at:
            state.rows[key] = _current_row(ctx, payload, digest, row.record)
            state.written_keys.add(key)
            c.updated += 1
        elif incoming_at < current_at:
            c.stale += 1
            state.stale_lines.append(
                dumps_line(_stale_record(ctx, ds, payload, digest, row.record, STALE_OLDER_UPDATED_AT, current))
            )
        elif digest == current["_silver_row_hash"]:
            c.unchanged += 1
        else:
            c.conflicts += 1
            step.quarantine(
                row,
                [Violation(RULE_MERGE_CONFLICT,
                           f"same key and {ds.watermark_column} {payload[ds.watermark_column]} as the current row "
                           f"(Bronze run {current['_bronze_run_id']} line {current['_bronze_line_number']}) "
                           "but a different payload; the current row is kept")],
                listed,
                conflict_with={
                    "bronze_run_id": current["_bronze_run_id"],
                    "_bronze_source_file": current["_bronze_source_file"],
                    "_bronze_line_number": current["_bronze_line_number"],
                    "row_hash": current["_silver_row_hash"],
                    "incoming_row_hash": digest,
                },
            )
    step.finish()
    if c.inserted or c.updated:
        state.changed = True
    return c


def apply_snapshot_run(
    ctx: RunContext,
    state: DatasetState,
    rows: Iterable[BronzeRow],
    listing: De11Listing,
    run_id: str,
    snapshot_as_of: str,
) -> tuple[Counters, bool]:
    """Returns (counters, snapshot_was_stale)."""
    step = _Step(ctx, state, listing, run_id)
    ds = state.dataset
    c = step.counters
    valid: dict[tuple, tuple[dict[str, Any], str, dict[str, Any]]] = {}
    quarantined_keys: set[tuple] = set()
    for row in rows:
        payload, violations, listed = step.validate(row)
        if violations:
            key = primary_key_of(ds, strip_metadata(row.record)) if not row.is_parse_error else None
            if key is not None:
                quarantined_keys.add(tuple(key[col] for col in ds.primary_key))
            step.quarantine(row, violations, listed)
            continue
        assert payload is not None
        key = state.key_of(payload)
        if key in valid:
            c.duplicate_keys_in_snapshot += 1
            first = valid[key][2]
            step.quarantine(
                row,
                [Violation(RULE_DUPLICATE_KEY_IN_SNAPSHOT,
                           f"key repeated in one snapshot; first occurrence at line {first[LINE_NUMBER]} wins")],
                listed,
            )
            continue
        valid[key] = (payload, row_hash(payload), row.record)
    step.finish()

    stale = state.last_snapshot_as_of is not None and _utc(snapshot_as_of) < _utc(state.last_snapshot_as_of)
    snapshot_info = {"snapshot_as_of": snapshot_as_of, "last_applied_snapshot_as_of": state.last_snapshot_as_of}
    if stale:
        for key, (payload, digest, record) in valid.items():
            c.stale += 1
            state.stale_lines.append(
                dumps_line(_stale_record(ctx, ds, payload, digest, record, STALE_OLDER_SNAPSHOT,
                                         state.rows.get(key), snapshot_info))
            )
        return c, True

    active_before = state.active_rows
    for key, (payload, digest, record) in valid.items():
        current = state.rows.get(key)
        if current is None or current["_silver_is_deleted"]:
            state.rows[key] = _current_row(ctx, payload, digest, record)
            state.written_keys.add(key)
            c.inserted += 1
        elif digest != current["_silver_row_hash"]:
            state.rows[key] = _current_row(ctx, payload, digest, record)
            state.written_keys.add(key)
            c.updated += 1
        else:
            c.unchanged += 1
    for key, current in state.rows.items():
        if current["_silver_is_deleted"] or key in valid:
            continue
        current.update(
            _silver_is_deleted=True,
            _silver_deleted_by_run_id=ctx.silver_run_id,
            _silver_batch_run_id=ctx.silver_run_id,
            _silver_processing_time=ctx.processing_time,
        )
        state.written_keys.discard(key)
        c.deleted += 1
        if key in quarantined_keys:
            c.deleted_with_quarantined_row += 1
    if active_before and c.deleted / active_before > ctx.max_snapshot_delete_fraction:
        raise SnapshotDeleteGuardError(
            f"{ds.name}: snapshot from Bronze run {run_id!r} would soft-delete {c.deleted} of {active_before} "
            f"active rows (> max_snapshot_delete_fraction {ctx.max_snapshot_delete_fraction}); nothing committed"
        )
    state.last_snapshot_as_of = snapshot_as_of
    state.last_valid_snapshot_keys = len(valid)
    if c.inserted or c.updated or c.deleted:
        state.changed = True
    return c, False


# -- FU-02: unresolved references ------------------------------------------------


def parent_key_sets(states: dict[str, DatasetState]) -> dict[tuple[str, str], set[Any]]:
    """Every value of every referenced parent column, over all rows ever
    loaded (tombstones included: a fact may reference a retired entity)."""
    wanted = {(fk.parent, fk.parent_column) for st in states.values() for fk in st.dataset.foreign_keys}
    out: dict[tuple[str, str], set[Any]] = {}
    for parent, column in wanted:
        state = states.get(parent)
        out[(parent, column)] = {row[column] for row in state.rows.values()} if state else set()
    return out


def _unresolved(fk: ForeignKeySpec, value: Any, parents: set[Any]) -> bool:
    if value is None:
        return False  # nullable (non-nullable nulls never pass validation)
    if fk.is_list:
        return any(item not in parents for item in value)
    return value not in parents


def unresolved_counts(
    state: DatasetState, parents: dict[tuple[str, str], set[Any]], keys: Iterable[tuple] | None = None
) -> dict[str, int]:
    """Per foreign key: active rows (of `keys`, default all) whose reference
    does not resolve to a loaded parent row. Such rows are loaded anyway."""
    selected = state.rows.values() if keys is None else (state.rows[k] for k in keys if k in state.rows)
    rows = [row for row in selected if not row["_silver_is_deleted"]]
    return {
        fk.name: sum(1 for row in rows if _unresolved(fk, row[fk.column], parents[(fk.parent, fk.parent_column)]))
        for fk in state.dataset.foreign_keys
    }


def is_snapshot(dataset: SilverDataset) -> bool:
    return dataset.mode is LoadMode.SNAPSHOT
