"""DE-03: configurable, individually-triggerable incident injection for the
DE-02 telemetry generator -- see `docs/BACKLOG.md` DE-03 and `PROJECT.md`
section 13.

This package is purely additive on top of `simulator/generator/`: it wraps
`simulator.generator.core.TelemetryGenerator` rather than forking it (see
`simulator.incidents.injector.IncidentInjector`), and reuses
`simulator.generator.runner.run`'s fixed-rate scheduler via the one small,
additive `rate_multiplier` hook documented in `simulator/generator/runner.py`.
"""

from __future__ import annotations
