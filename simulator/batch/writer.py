"""Write DE-04 datasets to local JSON-lines files plus a run manifest.

Format: one `<dataset>.jsonl` file per dataset (one canonical JSON object per
line - see `generator.serialize_row`), chosen over CSV because it:
- preserves types and explicit nulls (CSV turns every value into an
  ambiguous string, e.g. a null `vehicle_id` vs an empty one);
- matches DE-02's streaming output (newline-delimited JSON), so DE-05's
  Bronze ingestion can read both source shapes with one reader;
- is natively readable by Spark/Fabric (`spark.read.json`) and appendable
  line-by-line, which suits incremental extracts.

`manifest.json` records, per dataset, its row count, SHA-256, primary key,
watermark/partition columns and min/max watermark - a deterministic
(no wall-clock fields) reconciliation anchor for later Bronze loads.

Files are written to a temporary name and atomically renamed, so a rerun
into the same directory either fully replaces a file or leaves the previous
one intact (idempotent rerun).
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from simulator.batch.config import BatchConfig
from simulator.batch.generator import BatchDatasets, serialize_row
from simulator.batch.schemas import BATCH_SCHEMA_VERSION, DATASET_CATALOGUE
from simulator.config.models import ScaleConfig

MANIFEST_FILE_NAME = "manifest.json"


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _config_digest(batch_config: BatchConfig, scale_config: ScaleConfig) -> str:
    payload = json.dumps(
        {"batch": batch_config.model_dump(mode="json"), "scale": scale_config.model_dump(mode="json")},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_batch_datasets(
    datasets: BatchDatasets,
    output_dir: str | Path,
    *,
    batch_config: BatchConfig,
    scale_config: ScaleConfig,
) -> dict:
    """Write every dataset + `manifest.json` into `output_dir`; return the manifest."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest_datasets: dict[str, dict] = {}
    for name, spec in DATASET_CATALOGUE.items():
        rows = datasets.rows(name)
        lines = [serialize_row(row) for row in rows]
        data = "".join(line + "\n" for line in lines).encode("utf-8")
        _atomic_write(out_dir / spec.file_name, data)

        watermarks = sorted(getattr(row, spec.watermark_column) for row in rows)
        schema_versions = sorted({row.schema_version for row in rows})
        manifest_datasets[name] = {
            "file": spec.file_name,
            "row_count": len(rows),
            "sha256": hashlib.sha256(data).hexdigest(),
            "schema_versions": schema_versions,
            "primary_key": list(spec.primary_key),
            "watermark_column": spec.watermark_column,
            "partition_column": spec.partition_column,
            "min_watermark": watermarks[0].isoformat().replace("+00:00", "Z") if watermarks else None,
            "max_watermark": watermarks[-1].isoformat().replace("+00:00", "Z") if watermarks else None,
            "partition_count": len({getattr(row, spec.partition_column) for row in rows}),
        }

    manifest = {
        "generator": "simulator.batch",
        "batch_schema_version": BATCH_SCHEMA_VERSION,
        "batch_seed": batch_config.batch.seed,
        "reference_seed": scale_config.seed,
        "start_date": batch_config.batch.start_date.isoformat(),
        "service_days": batch_config.batch.service_days,
        "config_sha256": _config_digest(batch_config, scale_config),
        "vehicle_overlap_count": datasets.vehicle_overlap_count,
        "datasets": manifest_datasets,
    }
    _atomic_write(
        out_dir / MANIFEST_FILE_NAME,
        (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )
    return manifest
