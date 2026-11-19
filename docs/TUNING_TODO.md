# Tuning TODO

Deferred tuning and follow-up work that needs real operating data.

---

## End of winter 2026/27: replace the crude COP floor with a seasonal fit

**Target date:** around 2027-04-01, once the heating season has produced
cold-weather data. Do this before the next heating season.

There is a one-shot HA automation for this: `wpms3_end_of_winter_refit` in
`ha/homeassistant-alerts.yaml`, scheduled for 2027-04-01 09:00. It sends
both a mobile push and a persistent notification.

### Why

The current alert `wpms3_cop_floor` in `ha/homeassistant-alerts.yaml` is a
crude guard: daily heating COP below 4.0 on a day with a 24 h mean outdoor
temperature above 5 °C. It is deliberately conservative and will not catch
gradual degradation.

A proper "COP below expectation" alert needs a model of how COP depends on
outdoor temperature for this installation.

### Current preliminary fit (NOT good enough for alerts)

Fitted on 2026-11-17 from the InfluxDB `heatpump` bucket:

```
COP_expected = 3.20 + 0.544 * T_outdoor
```

- 30 usable days (days with at least 1 kWh heating electricity)
- outdoor range only **0.9 … 12.8 °C**
- R² = 0.56, residual standard deviation = 1.22
- Warm shoulder days (COP 10–14) are low-load artifacts and distort the fit
- Only two days below 1 °C, so real winter is not represented

Do not use this line for alerts yet. It is recorded here so the work is not
forgotten and so the improvement over this baseline can be measured.

### Data source

InfluxDB at `http://192.168.99.11:8086`, org `home`, bucket `heatpump`.
Relevant fields (written by telegraf from the `wpms3/info` MQTT topic):

- `waermeertrag_heiz_tag_kwh`
- `el_aufnahmeleistung_heiz_tag_kwh`
- `aussentemperatur`
- `vorlaufisttemperatur`
- `ruecklaufisttemperatur`
- `pufferisttemperatur`
- `radiator_vorlauf_ist_hk_2` is **not** a field; use `isttemperatur_hk_2`
  (the MQTT JSON key is `isttemperatur_hk_2`)

The daily TAG counters reset at local midnight, so aggregate with `max` per
day, not `last`. Filter out days with `el < 1 kWh`.

Sketch (Flux):

```flux
heat = from(bucket:"heatpump") |> range(start: 2026-10-01T00:00:00Z)
  |> filter(fn:(r) => r._field == "waermeertrag_heiz_tag_kwh")
  |> aggregateWindow(every: 1d, fn: max, createEmpty: false)
  |> rename(columns: {_value: "heat"})
  |> keep(columns: ["_time", "heat"])

el = from(bucket:"heatpump") |> range(start: 2026-10-01T00:00:00Z)
  |> filter(fn:(r) => r._field == "el_aufnahmeleistung_heiz_tag_kwh")
  |> aggregateWindow(every: 1d, fn: max, createEmpty: false)
  |> rename(columns: {_value: "el"})
  |> keep(columns: ["_time", "el"])

tout = from(bucket:"heatpump") |> range(start: 2026-10-01T00:00:00Z)
  |> filter(fn:(r) => r._field == "aussentemperatur")
  |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
  |> rename(columns: {_value: "tout"})
  |> keep(columns: ["_time", "tout"])

j1 = join(tables: {h: heat, e: el}, on: ["_time"])
join(tables: {a: j1, b: tout}, on: ["_time"])
  |> keep(columns: ["_time", "heat", "el", "tout"])
```

Use a read token supplied via the environment; do not commit tokens to the
repository.

### What to do

1. Refit `COP_expected` against daily mean outdoor temperature using a full
   heating season (exclude days with `el < 1 kWh` and low-load shoulder days
   with COP > 10).
2. Consider a two-term model using `(flow temperature - outdoor temperature)`
   instead of just outdoor temperature; the condensing temperature is the
   physically meaningful driver.
3. Add template sensors in `ha/homeassistant.yaml`:
   - `sensor.wpms3_cop_heizen_erwartet`
   - `sensor.wpms3_cop_heizen_residual` = actual daily COP − expected
4. Replace `wpms3_cop_floor` with an alert on the residual, e.g. residual
   below −1.0 for two consecutive days. Keep the old floor as a backstop.
5. Add the residual to the Effizienz view and a 30-day residual history.

---

## Revisit after the first full season

- HK1/HK2 1 K setpoint difference: compare `puffer_headroom_hk2`,
  `hk2_regelabweichung`, and `puffer_mischer_spreizung` trends over a full
  season. Decide whether the 1 K margin is still the best compromise.
  Caveat: the earlier 1 K analysis used the buffer bottom/return lower
  bound. The buffer top is not measured; use `puffer_mischer_spreizung`
  (bottom lower bound) and `puffer_mischer_spreizung_hp_flow` (HP-flow
  upper bound) together.
- Buffer sensor mapping: 2b = `180.0078` (buffer bottom/return); 2d =
  `180.000E` (DHW top, `warmwasser_isttemperatur`). Buffer top/middle
  (`180.0076`/`180.0077`) are not wired; the middle gauge is local.
- Buffer mixing loss: the target is 1–2 K; >3 K is excess. Review both the
  bottom-based lower bound and the HP-flow upper bound, not just the
  `Puffer-Mischer-Spreizung 7 Tage Mittel`.
- Short-cycling threshold: currently average cycle < 15 min with > 10
  starts/day. Re-evaluate against a full season of cycle lengths.
- Defrost threshold: currently > 8 defrosts/day. Re-evaluate against the
  coldest weeks.
- If the mobile app/phone changes, update the notify service name in
  `ha/homeassistant-alerts.yaml` (`notify.mobile_app_...`).
