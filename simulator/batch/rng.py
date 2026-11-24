"""Deterministic, independent RNG streams for DE-04.

Rather than one global `random.Random(seed)` (DE-01/DE-02's approach), DE-04
derives a separate stream per logical unit - per route timetable, per
service day, per trip, per weather station-day - keyed on the batch seed
plus stable string identifiers. `random.Random(str)` hashes string seeds
with SHA-512 (seed version 2), so the stream is identical across processes
and independent of `PYTHONHASHSEED`.

Why streams instead of one RNG: it makes the output *prefix-stable*.
Extending `service_days` appends new days without changing a single byte of
the days already generated, and tweaking one dataset's settings (e.g.
weather) does not reshuffle another's. That mirrors how a real source system
behaves for incremental (watermark-based) extraction: history, once written,
does not silently change.
"""

from __future__ import annotations

import random


def derive_rng(seed: int, *parts: str) -> random.Random:
    """Return a `random.Random` uniquely and deterministically keyed on
    `seed` and the ordered `parts` (e.g. `derive_rng(7, "trip", trip_id)`)."""
    return random.Random("|".join(("transitpulse-batch", str(seed), *parts)))
