"""DE-03 incident configuration: one sub-model per `PROJECT.md` section 13
scenario, each independently `enabled` with its own configurable intensity
(probability, skew, multiplier, window, ...), externalized to
`simulator/incidents/default_incidents.toml` per section 19 ("do not scatter
magic operational constants"). Deliberately a *separate* config surface from
`simulator/generator/config.py` (DE-02's `GeneratorConfig`, which this story
does not modify) and from `simulator/config/` (DE-01's scale/validation
contract, also untouched).

All scenarios default to `enabled = false` so a plain `IncidentsConfig()` (or
the bundled default TOML) reproduces DE-02's baseline, incident-free
behaviour -- incidents are strictly opt-in, per scenario.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from simulator.incidents.malformed import MALFORMED_MODES

_INCIDENTS_DIR = Path(__file__).parent
DEFAULT_INCIDENTS_CONFIG_PATH = _INCIDENTS_DIR / "default_incidents.toml"


class RecoveryWindow(BaseModel):
    """A `[start_tick, start_tick + duration_ticks)` generator-tick window
    used by the three duration-based scenarios (congestion, volume spike,
    breakdown) to demonstrate PROJECT.md section 13 item 8 (recovery):
    behaviour is only anomalous *inside* the window and must revert to
    exactly baseline behaviour once the window has elapsed.
    """

    model_config = {"extra": "forbid"}

    start_tick: int = Field(ge=0)
    duration_ticks: int = Field(gt=0)

    def contains(self, tick: int) -> bool:
        return self.start_tick <= tick < self.start_tick + self.duration_ticks


class MalformedDataConfig(BaseModel):
    """PROJECT.md section 13 item 1: null vehicle IDs, invalid coordinates,
    negative speeds, malformed timestamps, unknown routes, missing fields.

    Each triggered tick deterministically round-robins through `modes`
    (tracked by `IncidentInjector`'s own counter) so a sufficiently long run
    is guaranteed to exercise every configured mode at least once, rather
    than leaving it to chance which anomalies a given run happens to show.
    """

    model_config = {"extra": "forbid"}

    enabled: bool = False
    probability_per_tick: float = Field(ge=0.0, le=1.0, default=0.05)
    modes: list[str] = Field(default_factory=lambda: list(MALFORMED_MODES))

    @model_validator(mode="after")
    def _check_modes(self) -> MalformedDataConfig:
        if not self.modes:
            raise ValueError("modes must not be empty")
        unknown = set(self.modes) - set(MALFORMED_MODES)
        if unknown:
            raise ValueError(f"unknown malformed mode(s): {sorted(unknown)}")
        return self


class DuplicateConfig(BaseModel):
    """PROJECT.md section 13 item 2: replay a prior event_id verbatim."""

    model_config = {"extra": "forbid"}

    enabled: bool = False
    probability_per_tick: float = Field(ge=0.0, le=1.0, default=0.05)
    #: How many of the most recent delivered payloads are kept available to
    #: replay from.
    replay_window: int = Field(gt=0, default=20)


class LateEventConfig(BaseModel):
    """PROJECT.md section 13 item 3: `event_time` significantly earlier than
    `generated_at`/arrival. `late_threshold_seconds` is the configurable
    threshold a consumer would use to call an event "late" -- `min_skew_
    seconds` must be >= it, so every triggered event is unambiguously late
    by this scenario's own definition, not just marginally earlier.
    """

    model_config = {"extra": "forbid"}

    enabled: bool = False
    probability_per_tick: float = Field(ge=0.0, le=1.0, default=0.05)
    min_skew_seconds: int = Field(gt=0, default=120)
    max_skew_seconds: int = Field(gt=0, default=600)
    late_threshold_seconds: int = Field(gt=0, default=60)

    @model_validator(mode="after")
    def _check_ranges(self) -> LateEventConfig:
        if self.min_skew_seconds > self.max_skew_seconds:
            raise ValueError("min_skew_seconds must be <= max_skew_seconds")
        if self.min_skew_seconds < self.late_threshold_seconds:
            raise ValueError(
                "min_skew_seconds must be >= late_threshold_seconds, so every "
                "triggered event is unambiguously late"
            )
        return self


class OutOfOrderConfig(BaseModel):
    """PROJECT.md section 13 item 4: out-of-order delivery.

    Implemented as a small reorder buffer, pre-filled with `hold_ticks`
    events before the main run begins (see `IncidentInjector.__init__`) so
    every produced event can still be delivered exactly once (no loss, no
    duplication) while still allowing genuine reordering once the buffer is
    warm -- see `simulator/incidents/injector.py` module docstring for the
    full rationale.
    """

    model_config = {"extra": "forbid"}

    enabled: bool = False
    probability_per_tick: float = Field(ge=0.0, le=1.0, default=0.3)
    hold_ticks: int = Field(ge=2, default=3)


class CongestionConfig(BaseModel):
    """PROJECT.md section 13 item 5: lower speed and raise delay on a route
    so its KPIs visibly degrade, for a configured recovery window."""

    model_config = {"extra": "forbid"}

    enabled: bool = False
    route_id: str | None = None
    window: RecoveryWindow = Field(default_factory=lambda: RecoveryWindow(start_tick=0, duration_ticks=1))
    speed_multiplier: float = Field(gt=0.0, le=1.0, default=0.3)
    extra_delay_seconds: int = Field(ge=0, default=240)

    @model_validator(mode="after")
    def _check_route(self) -> CongestionConfig:
        if self.enabled and not self.route_id:
            raise ValueError("route_id is required when congestion is enabled")
        return self


class VolumeSpikeConfig(BaseModel):
    """PROJECT.md section 13 item 6: temporarily multiply the emission rate
    for a configured window, then return to baseline."""

    model_config = {"extra": "forbid"}

    enabled: bool = False
    window: RecoveryWindow = Field(default_factory=lambda: RecoveryWindow(start_tick=0, duration_ticks=1))
    rate_multiplier: float = Field(gt=1.0, default=6.0)


class BreakdownConfig(BaseModel):
    """PROJECT.md section 13 item 7: status becomes BREAKDOWN for the
    configured vehicle(s), for a configured recovery window. Downstream
    creation of a service-incident record is explicitly out of this story's
    scope (see `docs/BACKLOG.md` DE-03 and PROJECT.md section 13 item 7's
    "a service incident is created" clause -- deferred to a later story)."""

    model_config = {"extra": "forbid"}

    enabled: bool = False
    vehicle_ids: list[str] = Field(default_factory=list)
    window: RecoveryWindow = Field(default_factory=lambda: RecoveryWindow(start_tick=0, duration_ticks=1))

    @model_validator(mode="after")
    def _check_vehicles(self) -> BreakdownConfig:
        if self.enabled and not self.vehicle_ids:
            raise ValueError("vehicle_ids is required when breakdown is enabled")
        return self


class IncidentsConfig(BaseModel):
    """Top-level DE-03 configuration: one independently-enabled sub-config
    per `PROJECT.md` section 13 scenario (1-7; scenario 8, recovery, is the
    cross-cutting behaviour demonstrated by the duration-window scenarios
    reverting to baseline -- see `RecoveryWindow`)."""

    model_config = {"extra": "forbid"}

    #: Seeds a `random.Random` instance dedicated to incident decisions,
    #: kept separate from `GeneratorConfig.seed` so enabling/disabling
    #: incidents never perturbs the underlying DE-02 event content -- only
    #: this story's own anomaly-injection decisions are drawn from it, in a
    #: fixed per-tick order (see `IncidentInjector`).
    seed: int = 20250202

    malformed: MalformedDataConfig = Field(default_factory=MalformedDataConfig)
    duplicate: DuplicateConfig = Field(default_factory=DuplicateConfig)
    late_event: LateEventConfig = Field(default_factory=LateEventConfig)
    out_of_order: OutOfOrderConfig = Field(default_factory=OutOfOrderConfig)
    congestion: CongestionConfig = Field(default_factory=CongestionConfig)
    volume_spike: VolumeSpikeConfig = Field(default_factory=VolumeSpikeConfig)
    breakdown: BreakdownConfig = Field(default_factory=BreakdownConfig)


def load_incidents_config(path: str | Path | None = None) -> IncidentsConfig:
    """Load and validate DE-03's incident configuration.

    Args:
        path: Optional path to an alternate incidents-config TOML file.
            Defaults to `simulator/incidents/default_incidents.toml`, in
            which every scenario is `enabled = false` (DE-02 baseline
            behaviour).
    """
    resolved = Path(path) if path is not None else DEFAULT_INCIDENTS_CONFIG_PATH
    with open(resolved, "rb") as fh:
        data = tomllib.load(fh)
    return IncidentsConfig.model_validate(data["incidents"])
