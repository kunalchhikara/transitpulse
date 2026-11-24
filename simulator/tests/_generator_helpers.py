"""Shared test helpers for the DE-02 generator test suite (not collected by
pytest itself -- no `test_` prefix)."""

from __future__ import annotations

from simulator.config import load_scale_config
from simulator.config.models import ScaleConfig
from simulator.generator.config import GeneratorConfig, load_generator_config
from simulator.generator.core import TelemetryGenerator
from simulator.reference.generator import generate_reference_dataset


def build_generator(
    *,
    scale_config: ScaleConfig | None = None,
    generator_config: GeneratorConfig | None = None,
) -> TelemetryGenerator:
    scale_config = scale_config if scale_config is not None else load_scale_config()
    generator_config = (
        generator_config if generator_config is not None else load_generator_config()
    )
    reference = generate_reference_dataset(scale_config)
    return TelemetryGenerator(
        scale_config=scale_config,
        generator_config=generator_config,
        reference=reference,
    )
