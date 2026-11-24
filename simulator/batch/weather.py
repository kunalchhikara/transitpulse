"""Optional synthetic weather observations: one station per DE-01 depot,
observing every `observation_interval_minutes` across each local day.

Temperature follows a diurnal cosine curve peaking at
`peak_temperature_hour` plus bounded uniform noise; rain, wind and cloud are
independent per-observation draws; `condition` is derived from them using the
configured thresholds. Purely synthetic - not derived from any real weather
source.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

from simulator.batch.config import BatchConfig
from simulator.batch.rng import derive_rng
from simulator.batch.schemas import WeatherCondition, WeatherObservation
from simulator.batch.timetable import local_midnight_utc
from simulator.reference.generator import ReferenceDataset


def generate_day_weather(
    service_date: date, reference: ReferenceDataset, config: BatchConfig
) -> list[WeatherObservation]:
    cfg = config.weather
    if not cfg.enabled:
        return []
    midnight = local_midnight_utc(service_date, config.batch.utc_offset_hours)
    rows: list[WeatherObservation] = []
    for depot in reference.depots:
        rng = derive_rng(config.batch.seed, "weather", depot.depot_id, service_date.isoformat())
        station_id = f"WX-{depot.depot_id}"
        for i in range(cfg.observations_per_day):
            minutes = i * cfg.observation_interval_minutes
            local_hour = minutes / 60.0
            temperature = (
                cfg.base_temperature_c
                + cfg.diurnal_amplitude_c
                * math.cos(2 * math.pi * (local_hour - cfg.peak_temperature_hour) / 24.0)
                + rng.uniform(-cfg.temperature_noise_c, cfg.temperature_noise_c)
            )
            raining = rng.random() < cfg.rain_probability
            rain_amount = rng.uniform(0.1, cfg.max_rainfall_mm) if cfg.max_rainfall_mm > 0.1 else 0.0
            rainfall = round(rain_amount, 1) if raining else 0.0
            wind = round(rng.uniform(0.0, cfg.max_wind_speed_kmh), 1)
            cloudy = rng.random() < cfg.cloudy_probability

            if rainfall > 0 and wind >= cfg.storm_wind_threshold_kmh:
                condition = WeatherCondition.STORM
            elif rainfall > 0 and rainfall >= cfg.heavy_rain_threshold_mm:
                condition = WeatherCondition.HEAVY_RAIN
            elif rainfall > 0:
                condition = WeatherCondition.RAIN
            elif cloudy:
                condition = WeatherCondition.CLOUDY
            else:
                condition = WeatherCondition.CLEAR

            observed_at = midnight + timedelta(minutes=minutes)
            local_stamp = f"{service_date.strftime('%Y%m%d')}{minutes // 60:02d}{minutes % 60:02d}"
            rows.append(
                WeatherObservation(
                    event_date=service_date,
                    updated_at=observed_at,
                    observation_id=f"{station_id}-{local_stamp}",
                    station_id=station_id,
                    depot_id=depot.depot_id,
                    latitude=depot.latitude,
                    longitude=depot.longitude,
                    observed_at=observed_at,
                    temperature_c=round(temperature, 1),
                    rainfall_mm=rainfall,
                    wind_speed_kmh=wind,
                    condition=condition,
                )
            )
    return rows
