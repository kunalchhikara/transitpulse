"""Shared test helpers for the DE-03 incident test suite (not collected by
pytest itself -- no `test_` prefix)."""

from __future__ import annotations

from simulator.generator.core import TelemetryGenerator
from simulator.incidents.config import IncidentsConfig
from simulator.incidents.injector import IncidentInjector
from simulator.tests._generator_helpers import build_generator

#: A fixed seed for the incidents' own RNG, distinct from the generator
#: config's seed, used throughout this test suite unless a test needs a
#: specific value to exercise a particular draw outcome.
INCIDENTS_SEED = 909090


def baseline_incidents_config(**overrides: object) -> IncidentsConfig:
    """An `IncidentsConfig` with every scenario disabled (DE-02 baseline),
    optionally overridden -- e.g.
    `baseline_incidents_config(malformed=MalformedDataConfig(enabled=True, probability_per_tick=1.0))`.
    """
    config = IncidentsConfig(seed=INCIDENTS_SEED)
    return config.model_copy(update=overrides) if overrides else config


def build_injector(
    *,
    generator: TelemetryGenerator | None = None,
    incidents_config: IncidentsConfig | None = None,
    **overrides: object,
) -> IncidentInjector:
    generator = generator if generator is not None else build_generator()
    incidents_config = (
        incidents_config if incidents_config is not None else baseline_incidents_config(**overrides)
    )
    return IncidentInjector(generator, incidents_config)
