"""Source reading, window selection and extract writing for the DE-11 loader.

* **Incremental sources** are DE-04 dataset files (`<dataset>.jsonl` listed
  in the DE-04 `manifest.json`). Every physical line is classified; good
  lines carry a parsed watermark (`updated_at` by catalogue). Selected lines
  are copied **byte-for-byte** into the extract (only the line terminator is
  normalised to `\\n`), so Bronze stores exactly what the source held.
* **Full sources** are DE-01 reference entity files (`<entity>.json`, a JSON
  array, as written by `python -m simulator.reference`). Each element becomes
  one canonical JSON line (sorted keys, compact separators) - the JSON-lines
  shape Bronze and Spark expect. Elements must carry `schema_version`.

An extract is written as a DE-04-shaped directory (`<dataset>.jsonl` +
`manifest.json` with `row_count`/`sha256`), which `simulator.bronze.ingest_batch`
verifies before it commits.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from simulator.incremental.clock import format_ts, parse_aware
from simulator.incremental.config import DatasetPlan

EXTRACT_SCHEMA_VERSION = "1.0.0"
EXTRACT_MANIFEST_FILE = "manifest.json"

# Bad-line reason codes (audit `bad_source_rows_by_reason`).
BAD_INVALID_UTF8 = "invalid_utf8"
BAD_INVALID_JSON = "invalid_json"
BAD_DUPLICATE_KEY = "duplicate_json_key"
BAD_NOT_OBJECT = "not_a_json_object"
BAD_MISSING_WATERMARK = "missing_watermark"
BAD_INVALID_WATERMARK = "invalid_watermark"
BAD_MISSING_SCHEMA_VERSION = "missing_schema_version"
BAD_REASONS: tuple[str, ...] = (
    BAD_INVALID_UTF8,
    BAD_INVALID_JSON,
    BAD_DUPLICATE_KEY,
    BAD_NOT_OBJECT,
    BAD_MISSING_WATERMARK,
    BAD_INVALID_WATERMARK,
    BAD_MISSING_SCHEMA_VERSION,
)
#: What Bronze (DE-05) makes of a bad line landed under the preserve policy.
#: Not UTF-8 / not JSON / duplicate keys / not an object -> a Bronze
#: parse-error record. A JSON object whose watermark is missing or invalid
#: (or a reference element without `schema_version`) is a well-formed
#: object to Bronze, so it is stored as a NORMAL record: DE-12 must
#: quarantine it (Lead decision D2), which is why every such row is listed.
BRONZE_PARSE_ERROR_RECORD = "bronze_parse_error_record"
STORED_RECORD_REQUIRES_QUARANTINE = "stored_record_requires_quarantine"
BRONZE_OUTCOME_BY_REASON: dict[str, str] = {
    BAD_INVALID_UTF8: BRONZE_PARSE_ERROR_RECORD,
    BAD_INVALID_JSON: BRONZE_PARSE_ERROR_RECORD,
    BAD_DUPLICATE_KEY: BRONZE_PARSE_ERROR_RECORD,
    BAD_NOT_OBJECT: BRONZE_PARSE_ERROR_RECORD,
    BAD_MISSING_WATERMARK: STORED_RECORD_REQUIRES_QUARANTINE,
    BAD_INVALID_WATERMARK: STORED_RECORD_REQUIRES_QUARANTINE,
    BAD_MISSING_SCHEMA_VERSION: STORED_RECORD_REQUIRES_QUARANTINE,
}


class SourceError(Exception):
    """The source cannot be read as a whole (missing, unparseable file,
    dataset absent from the DE-04 manifest). Never retried."""


class SourceManifestMismatchError(SourceError):
    """A DE-04 source file disagrees with its manifest (sha256/row_count)."""


@dataclass(frozen=True)
class SourceLine:
    """One non-blank source line/element."""

    line_number: int
    raw: bytes  # exact bytes, without the line terminator
    watermark: datetime | None  # incremental good lines only
    bad_reason: str | None

    @property
    def is_bad(self) -> bool:
        return self.bad_reason is not None

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass
class SourceSnapshot:
    dataset: str
    file_name: str
    sha256: str
    lines: list[SourceLine]
    blank_lines: int = 0
    manifest_check: dict[str, Any] | None = None

    @property
    def bad_lines(self) -> list[SourceLine]:
        return [line for line in self.lines if line.is_bad]

    def bad_by_reason(self) -> dict[str, int]:
        counts = {reason: 0 for reason in BAD_REASONS}
        for line in self.bad_lines:
            counts[line.bad_reason] += 1  # type: ignore[index]
        return {reason: count for reason, count in counts.items() if count}


@dataclass
class Selection:
    """What one window extracts from a source snapshot, in source order."""

    lines: list[SourceLine] = field(default_factory=list)
    in_window: int = 0  # good rows with low < watermark <= high (all rows for full)
    bad_preserved: int = 0  # bad lines landed by this extract (preserve policy)
    below_or_at_low: int = 0  # good rows with watermark <= low (already loaded)
    after_high: int = 0  # good rows with watermark > high (later windows)
    max_watermark: datetime | None = None  # max watermark of the selected good rows

    @property
    def count(self) -> int:
        return len(self.lines)

    @property
    def good_lines(self) -> list[SourceLine]:
        return [line for line in self.lines if not line.is_bad]


@dataclass(frozen=True)
class ExtractInfo:
    directory: Path
    file_name: str
    row_count: int
    sha256: str


# -- parsing -----------------------------------------------------------------


class _DuplicateKeyError(ValueError):
    pass


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError(key)
        result[key] = value
    return result


def _reject_constant(name: str) -> Any:
    raise ValueError(f"non-standard JSON constant {name!r}")


def _parse_object(raw: bytes) -> tuple[dict[str, Any] | None, str | None]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, BAD_INVALID_UTF8
    try:
        value = json.loads(text, object_pairs_hook=_no_duplicates, parse_constant=_reject_constant)
    except _DuplicateKeyError:
        return None, BAD_DUPLICATE_KEY
    except (ValueError, RecursionError):
        return None, BAD_INVALID_JSON
    if not isinstance(value, dict):
        return None, BAD_NOT_OBJECT
    return value, None


def classify_line(raw: bytes, line_number: int, watermark_column: str) -> SourceLine:
    obj, reason = _parse_object(raw)
    if obj is None:
        return SourceLine(line_number, raw, None, reason)
    value = obj.get(watermark_column)
    if value is None:
        return SourceLine(line_number, raw, None, BAD_MISSING_WATERMARK)
    if not isinstance(value, str):
        return SourceLine(line_number, raw, None, BAD_INVALID_WATERMARK)
    try:
        watermark = parse_aware(value, watermark_column)
    except ValueError:
        return SourceLine(line_number, raw, None, BAD_INVALID_WATERMARK)
    return SourceLine(line_number, raw, watermark, None)


def split_lines(data: bytes) -> list[bytes]:
    """Physical lines without `\\n` (same rule as DE-05 Bronze): a trailing
    newline does not start a new line; an unterminated final line is kept."""
    if not data:
        return []
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()
    return lines


def _safe_child(directory: Path, name: str, what: str) -> Path:
    if not isinstance(name, str) or not name or Path(name).name != name or name in (".", ".."):
        raise SourceError(f"unsafe {what} file name {name!r}")
    return directory / name


def read_incremental_source(
    source_dir: Path,
    plan: DatasetPlan,
    *,
    manifest_file: str,
    verify_manifest: bool,
) -> SourceSnapshot:
    """Read and classify one DE-04 dataset file."""
    assert plan.watermark_column is not None
    manifest_path = source_dir / manifest_file
    entry: dict[str, Any] | None = None
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            entry = manifest["datasets"].get(plan.name)
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise SourceError(f"unreadable source manifest {manifest_file}: {type(exc).__name__}") from exc
        if entry is None:
            raise SourceError(f"dataset {plan.name!r} is not listed in source {manifest_file}")
    elif verify_manifest:
        raise SourceError(f"source {manifest_file} not found in the batch source directory")
    file_name = entry.get("file") if entry is not None else f"{plan.name}.jsonl"
    path = _safe_child(source_dir, file_name, "source")
    if not path.is_file():
        raise SourceError(f"source file {file_name!r} for dataset {plan.name!r} is missing")
    data = path.read_bytes()
    snapshot = SourceSnapshot(plan.name, file_name, hashlib.sha256(data).hexdigest(), [])
    for number, raw in enumerate(split_lines(data), start=1):
        if not raw.strip():
            snapshot.blank_lines += 1
            continue
        snapshot.lines.append(classify_line(raw, number, plan.watermark_column))
    if entry is not None:
        expected_rows, expected_sha = entry.get("row_count"), entry.get("sha256")
        matched = expected_rows == len(snapshot.lines) and expected_sha == snapshot.sha256
        snapshot.manifest_check = {
            "expected_row_count": expected_rows,
            "expected_sha256": expected_sha,
            "status": "MATCHED" if matched else "MISMATCH",
            "verified": verify_manifest,
        }
        if not matched and verify_manifest:
            raise SourceManifestMismatchError(
                f"source file {file_name!r} disagrees with {manifest_file} "
                f"(rows {len(snapshot.lines)} vs {expected_rows}, sha256 match: {expected_sha == snapshot.sha256})"
            )
    return snapshot


def canonical_line(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
        "utf-8"
    )


def read_reference_source(source_dir: Path, plan: DatasetPlan) -> SourceSnapshot:
    """Read one DE-01 reference entity file (JSON array) as canonical lines."""
    assert plan.source_file is not None
    path = _safe_child(source_dir, plan.source_file, "reference")
    if not path.is_file():
        raise SourceError(f"reference file {plan.source_file!r} for {plan.name!r} is missing")
    data = path.read_bytes()
    try:
        elements = json.loads(data.decode("utf-8"), parse_constant=_reject_constant)
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise SourceError(f"reference file {plan.source_file!r} is not valid JSON: {type(exc).__name__}") from exc
    if not isinstance(elements, list):
        raise SourceError(f"reference file {plan.source_file!r} is not a JSON array")
    snapshot = SourceSnapshot(plan.name, plan.source_file, hashlib.sha256(data).hexdigest(), [])
    for number, element in enumerate(elements, start=1):
        raw = canonical_line(element)
        if not isinstance(element, dict):
            reason: str | None = BAD_NOT_OBJECT
        elif "schema_version" not in element:
            reason = BAD_MISSING_SCHEMA_VERSION
        else:
            reason = None
        snapshot.lines.append(SourceLine(number, raw, None, reason))
    return snapshot


# -- selection ---------------------------------------------------------------


def select_window(
    snapshot: SourceSnapshot,
    *,
    low: datetime | None,
    high: datetime,
    preserved_bad_lines: dict[str, int],
    preserve_bad: bool,
) -> Selection:
    """Incremental selection: good lines with `low < watermark <= high`
    (`watermark <= high` when `low` is None), plus - under the preserve
    policy - every bad line not landed by an earlier extract (tracked by raw
    sha256 and occurrence count). Source order is kept."""
    selection = Selection()
    seen_bad: dict[str, int] = {}
    for line in snapshot.lines:
        if line.is_bad:
            if not preserve_bad:
                continue
            digest = line.sha256
            seen_bad[digest] = seen_bad.get(digest, 0) + 1
            if seen_bad[digest] > preserved_bad_lines.get(digest, 0):
                selection.lines.append(line)
                selection.bad_preserved += 1
            continue
        assert line.watermark is not None
        if low is not None and line.watermark <= low:
            selection.below_or_at_low += 1
        elif line.watermark > high:
            selection.after_high += 1
        else:
            selection.lines.append(line)
            selection.in_window += 1
            if selection.max_watermark is None or line.watermark > selection.max_watermark:
                selection.max_watermark = line.watermark
    return selection


def select_full(snapshot: SourceSnapshot, *, preserve_bad: bool) -> Selection:
    selection = Selection()
    for line in snapshot.lines:
        if line.is_bad and not preserve_bad:
            continue
        selection.lines.append(line)
        if line.is_bad:
            selection.bad_preserved += 1
        else:
            selection.in_window += 1
    return selection


def count_at_or_below(snapshot: SourceSnapshot, watermark: datetime) -> int:
    return sum(1 for line in snapshot.lines if not line.is_bad and line.watermark <= watermark)  # type: ignore[operator]


def extract_bytes(lines: Sequence[SourceLine]) -> bytes:
    """Extract file content: each selected line's exact bytes + `\\n`."""
    return b"".join(line.raw + b"\n" for line in lines)


def preserved_bad_rows(lines: Sequence[SourceLine]) -> list[dict[str, Any]]:
    """Deterministic listing of the bad lines an extract carries (preserve
    policy), in extract order: 1-based line number within the extract, line
    number in the source (element index for a reference file), reason,
    sha256 of the exact line bytes, and what Bronze makes of it. Written to
    both the extract manifest and the attempt's audit record (Lead D2)."""
    return [
        {
            "extract_line": number,
            "source_line": line.line_number,
            "reason": line.bad_reason,
            "sha256": line.sha256,
            "bronze_outcome": BRONZE_OUTCOME_BY_REASON[line.bad_reason],  # type: ignore[index]
        }
        for number, line in enumerate(lines, start=1)
        if line.is_bad
    ]


# -- writing -----------------------------------------------------------------


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def write_extract(
    directory: Path,
    plan: DatasetPlan,
    lines: Sequence[SourceLine],
    *,
    window: dict[str, str | None],
    bronze_run_id: str,
) -> ExtractInfo:
    """Write `<dataset>.jsonl` + a DE-04-shaped `manifest.json` (written
    last). Rewriting the same window produces byte-identical files."""
    directory.mkdir(parents=True, exist_ok=True)
    file_name = f"{plan.name}.jsonl"
    data = extract_bytes(lines)
    _atomic_write(directory / file_name, data)
    watermarks = sorted(line.watermark for line in lines if line.watermark is not None)
    sha256 = hashlib.sha256(data).hexdigest()
    manifest = {
        "generator": "simulator.incremental",
        "extract_schema_version": EXTRACT_SCHEMA_VERSION,
        "mode": plan.mode.value,
        "bronze_run_id": bronze_run_id,
        "window": window,
        "datasets": {
            plan.name: {
                "file": file_name,
                "row_count": len(lines),
                "sha256": sha256,
                "primary_key": list(plan.primary_key),
                "watermark_column": plan.watermark_column,
                "partition_column": plan.partition_column,
                "min_watermark": format_ts(watermarks[0]) if watermarks else None,
                "max_watermark": format_ts(watermarks[-1]) if watermarks else None,
                "preserved_bad_rows": preserved_bad_rows(lines),
            }
        },
    }
    text = json.dumps(manifest, sort_keys=True, indent=2) + "\n"
    _atomic_write(directory / EXTRACT_MANIFEST_FILE, text.encode("utf-8"))
    return ExtractInfo(directory, file_name, len(lines), sha256)


def count_file_lines(path: Path) -> tuple[int, str]:
    """(line count, sha256) of a file as it is on disk - the independent
    re-count used by the pre-Bronze reconciliation check."""
    data = path.read_bytes()
    return data.count(b"\n"), hashlib.sha256(data).hexdigest()
