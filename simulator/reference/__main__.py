"""CLI: generate the reference-entity dataset to local JSON files.

Usage:
    python -m simulator.reference --output-dir simulator/_out/reference

Writes depots.json, stops.json, routes.json and vehicles.json. Re-running
with the same config/seed overwrites with byte-identical content (see
`simulator/tests/test_reference_cli_determinism.py`).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from simulator.config import load_scale_config
from simulator.reference.generator import generate_reference_dataset


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scale-config",
        type=Path,
        default=None,
        help="Path to an alternate scale-config TOML file (default: bundled default_scale.toml).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("simulator/_out/reference"),
        help="Directory to write depots.json/stops.json/routes.json/vehicles.json into.",
    )
    args = parser.parse_args()

    config = load_scale_config(args.scale_config)
    dataset = generate_reference_dataset(config)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    data = dataset.to_dict()
    for name, rows in data.items():
        out_path = args.output_dir / f"{name}.json"
        out_path.write_text(json.dumps(rows, sort_keys=True, indent=2) + "\n")
        print(f"wrote {len(rows)} {name} -> {out_path}")


if __name__ == "__main__":
    main()
