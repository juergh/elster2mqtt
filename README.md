# elster2mqtt

`elster2mqtt` is a lightweight bridge service that queries Stiebel-Eltron / Elster heating and heat pump controllers over a SocketCAN interface and publishes telemetry metrics to an MQTT broker.

It supports periodic batch publishing (formatted JSON topic), single register polling, and offline simulation for local development.

---

## Features

- **SocketCAN Integration:** Reads and decodes standard 11-bit CAN bus frames using the Elster/Kromschröder protocol at 20 kbit/s.
- **Multi-Node Architecture:** Queries master managers (WPM at `180`), circuit controllers (`601`/`602`), and outdoor inverter boards (`500`).
- **Resilient Polling:** Per-register timeout guards ensure that an unavailable or uninstalled sensor does not abort batch publishing.
- **Safe MQTT Delivery:** Waits for publication acknowledgment before tearing down network threads, avoiding dropped packets on one-shot timer runs.
- **Systemd Integration:** Includes systemd service and timer units for automated 5-minute telemetry intervals.
- **Simulation Mode:** Test CAN parsing and MQTT publishing offline without active CAN hardware.

---

## Hardware & CAN Bus Setup

### Physical Connection
Stiebel-Eltron controllers (such as the WPM 3, WPMsystem, and WPL inverter units) communicate using standard 11-bit CAN identifiers at **20 kbit/s**.

Connect a SocketCAN-compatible adapter (e.g. Raspberry Pi with PiCAN hat, CANable, or USB2CAN) to the CAN bus terminals:
- `CAN_H` -> Controller CAN High
- `CAN_L` -> Controller CAN Low
- `GND` -> Controller Ground / Shield

Ensure the bus is terminated with a 120 Ω resistor at both physical ends of the line.

### SocketCAN Configuration (`systemd-networkd`)
Copy `etc/80-can.network` to `/etc/systemd/network/80-can.network`:

```ini
[Match]
Name=can0

[CAN]
BitRate=20K
RestartSec=100ms
```

Restart systemd-networkd to initialize the interface:
```bash
sudo systemctl restart systemd-networkd
```

---

## CAN Bus Addressing & Schema Topology

On Stiebel-Eltron systems, metrics are distributed across distinct nodes:

| CAN Node | Typical Role | Typical Metrics |
| :--- | :--- | :--- |
| **`180`** | **WPM Master Controller** | Outdoor temp, buffer temp, DHW temp, room setpoint (`180.0005`), flow setpoints, pump status bitmask (`180.01d2`) |
| **`500`** | **IWS (Outdoor Inverter Board)** | Compressor frequency (`500.063e`), hot gas temp (`500.0265`), defrost cycles (`500.0806`), compressor run hours, heat yield & electrical intake |
| **`601`** | **Heating Circuit 1 (HK 1)** | Heating curve slope HK 1 (`601.010e`) |
| **`602`** | **Heating Circuit 2 (HK 2)** | Heating curve slope HK 2 (`602.010e`) |

---

## Configuration (`/etc/elster2mqtt.yaml`)

Configuration is declared in YAML:

```yaml
mqtt:
  server: 192.168.1.100
  port: 1883
  topic_prefix: wpms3/

can:
  interface: can0
  sender: 680
  timeout: 5

data:
  # Temperatures
  - name: AUSSENTEMPERATUR
    index: 180.000c
    format: dec_val
  - name: ISTTEMPERATUR_HK_1
    index: 180.02ca
    format: dec_val
  - name: SOLLTEMPERATUR_HK_1
    index: 180.01d7
    format: dec_val
  - name: ISTTEMPERATUR_HK_2
    index: 180.000f
    format: dec_val
  - name: SOLLTEMPERATUR_HK_2
    index: 180.0004
    format: dec_val
  - name: VORLAUFISTTEMPERATUR
    index: 180.01d6
    format: dec_val
  - name: RUECKLAUFISTTEMPERATUR
    index: 180.0016
    format: dec_val
  - name: PUFFERISTTEMPERATUR
    index: 180.0078
    format: dec_val
  - name: PUFFERSOLLTEMPERATUR
    index: 180.01d5
    format: dec_val
  - name: ANLAGENFROST
    index: 180.0a00
    format: dec_val

  # Domestic Hot Water (DHW)
  - name: WARMWASSER_ISTTEMPERATUR
    index: 180.000e
    format: dec_val
  - name: WARMWASSER_SOLLTEMPERATUR
    index: 180.0003
    format: dec_val

  # Misc
  - name: FEHLER
    index: 180.0001
  - name: PUMPENSTATUS
    index: 180.01d2

  # Heating Curves & Setpoints
  - name: HEIZKURVE_HK_1
    index: 601.010e
    format: cent_val
  - name: HEIZKURVE_HK_2
    index: 602.010e
    format: cent_val
  - name: RAUMSOLLTEMP_I
    index: 180.0005
    format: dec_val

  # Status & Inverter Diagnostics (WPL 15 AS)
  - name: ABTAUUNGAKTIV
    index: 500.0061
  - name: ISTDREHZAHL_VERDICHTER
    index: 500.063e
  - name: HEISSGAS_TEMP
    index: 500.0265
    format: dec_val
  - name: STARTS_ABTAUUNG
    index: 500.0806
  - name: LAUFZEIT_VERDICHTER_HEIZEN
    index: 500.07fc
  - name: LAUFZEIT_VERDICHTER_WW
    index: 500.0802

  # Energy & Intake (for COP / JAZ calculation)
  - name: WAERMEERTRAG_HEIZ_TAG_KWH
    index: 500.092f
  - name: EL_AUFNAHMELEISTUNG_HEIZ_TAG_KWH
    index: 500.091f
  - name: WAERMEERTRAG_WW_TAG_KWH
    index: 500.092b
  - name: EL_AUFNAHMELEISTUNG_WW_TAG_KWH
    index: 500.091b
  - name: WAERMEERTRAG_2WE_HEIZ_TAG_KWH
    index: 500.0927
  - name: WAERMEERTRAG_HEIZ_SUM_MWH
    index: 500.0931
  - name: WAERMEERTRAG_HEIZ_SUM_KWH
    index: 500.0930
  - name: EL_AUFNAHMELEISTUNG_HEIZ_SUM_MWH
    index: 500.0921
  - name: EL_AUFNAHMELEISTUNG_HEIZ_SUM_KWH
    index: 500.0920
  - name: WAERMEERTRAG_WW_SUM_MWH
    index: 500.092d
  - name: WAERMEERTRAG_WW_SUM_KWH
    index: 500.092c
  - name: EL_AUFNAHMELEISTUNG_WW_SUM_MWH
    index: 500.091d
  - name: EL_AUFNAHMELEISTUNG_WW_SUM_KWH
    index: 500.091c
```

### Supported Formatters

| Formatter | Operation | Typical Use Case |
| :--- | :--- | :--- |
| `dec_val` | Value divided by `10.0` (with two's complement) | Temperatures in °C (e.g. `215` -> `21.5 °C`) |
| `cent_val` | Value divided by `100.0` (with two's complement) | Heating curve slopes (e.g. `105` -> `1.05`) |
| `mil_val` | Value divided by `1000.0` (with two's complement) | High-resolution meters |
| `little_endian` | Byte-swapped 16-bit unsigned integer | Status bitmasks (`PUMPENSTATUS`) |
| *(omitted)* | Raw integer | Counters, frequency in Hz, status codes |

---

## Usage

### Command Line Interface

```bash
# Full batch poll (reads all configured parameters, synthesizes state, and publishes to <topic_prefix>info and <topic_prefix>status)
elster2mqtt /etc/elster2mqtt.yaml

# Poll with custom read timeout (in seconds, overriding config)
elster2mqtt /etc/elster2mqtt.yaml --timeout 10

# Poll single named register from config
elster2mqtt /etc/elster2mqtt.yaml --name AUSSENTEMPERATUR

# Poll arbitrary CAN node and register (hex format: <node>.<register>)
elster2mqtt /etc/elster2mqtt.yaml --index 500.063e

# Simulate operations offline
elster2mqtt /etc/elster2mqtt.yaml --simulate-can --simulate-mqtt
```

### Systemd Automated Execution

`elster2mqtt` installs a systemd timer that executes the oneshot service every 5 minutes:

```bash
# Enable and start the timer
sudo systemctl enable --now elster2mqtt.timer

# Check timer status
systemctl list-timers elster2mqtt.timer

# View execution logs
journalctl -u elster2mqtt.service -e
```

---

## Heat Pump Performance, Efficiency & Tuning Guide

### 1. Key Formulas & Calculations

`elster2mqtt` automatically computes the following efficiency and energy metrics on every poll:

#### A. Daily Heating Coefficient of Performance (cop_daily_heating)
```
cop_daily_heating = waermeertrag_heiz_tag_kwh / el_aufnahmeleistung_heiz_tag_kwh
```

#### B. Daily Domestic Hot Water COP (cop_daily_ww)
```
cop_daily_ww = waermeertrag_ww_tag_kwh / el_aufnahmeleistung_ww_tag_kwh
```
*(Note: Daily COP fields are populated once today's electrical consumption reaches at least 1 kWh to guard against division by zero early in the morning).*

#### C. All-Time Seasonal Performance Factor (JAZ / SCOP)
Stiebel-Eltron controllers store cumulative lifetime energy across two registers: MWh (megawatt-hours) and remaining kWh (0–999). `elster2mqtt` combines them into monotonic total kWh values:

```
waermeertrag_total_kwh = (waermeertrag_heiz_sum_mwh * 1000 + waermeertrag_heiz_sum_kwh) +
                         (waermeertrag_ww_sum_mwh * 1000 + waermeertrag_ww_sum_kwh)

el_aufnahmeleistung_total_kwh = (el_aufnahmeleistung_heiz_sum_mwh * 1000 + el_aufnahmeleistung_heiz_sum_kwh) +
                                (el_aufnahmeleistung_ww_sum_mwh * 1000 + el_aufnahmeleistung_ww_sum_kwh)

jaz_total = waermeertrag_total_kwh / el_aufnahmeleistung_total_kwh
```

#### D. Parasitic Buffer Tank Mixing Loss (ΔT_buffer_loss)
In hydraulic schemas with a combi/buffer tank and heating circuit mixing valve (e.g. Schema AM 11212):

```
ΔT_buffer_loss = vorlaufisttemperatur - isttemperatur_hk_2
```
*Each 1 K excess temperature generated by the heat pump and mixed down by the 3-way valve incurs roughly a 2.5% compressor efficiency penalty.*

---

### 2. Operational States & Component Decoding

`elster2mqtt` decodes the 16-bit status word (`PUMPENSTATUS` on `180.01d2`) and the active defrost status into human-readable states published to `<topic_prefix>status` and included in the `<topic_prefix>info` JSON payload:

| Operating State (`betriebszustand`) | Meaning |
| :--- | :--- |
| **`abtauen`** | Reverse-cycle defrosting active (melting outdoor evaporator coil) |
| **`evu_sperre`** | Electrical utility grid ripple-control lock active |
| **`zusatzheizung`** | Pure electric immersion heater running (emergency / legionella protection) |
| **`heizen_booster`** | Bivalent heating: Heat pump compressor + electric booster running together |
| **`warmwasser`** | Domestic hot water heating active (charging drinking water tank) |
| **`heizen`** | Space heating active (radiator / buffer heating) |
| **`standby`** | Compressor idle, no active heating demand |

The bit map below was verified empirically on a WPM 3 with firmware 390-08 by correlating the raw status word with hot gas temperature and utility lock events:

| Bit | Hex Mask | Component / Meaning |
| :--- | :--- | :--- |
| **Bit 0** | `0x0001` | **Compressor running** — verified (`verdichter_laeuft`) |
| **Bit 1** | `0x0002` | Booster heater stage 1 — unverified (`zusatzheizung_stufe_1`) |
| **Bit 2** | `0x0004` | Booster heater stage 2 — unverified (`zusatzheizung_stufe_2`) |
| **Bit 3** | `0x0008` | Buffer charging pump — unverified |
| **Bit 4** | `0x0010` | Constant configuration mask — not a live state |
| **Bit 5** | `0x0020` | Constant configuration mask — not a live state |
| **Bit 7** | `0x0080` | Mixing valve driving open — verified (`mischer_auf`) |
| **Bit 8** | `0x0100` | Utility lock active — verified (`evu_sperre`) |
| **Bit 9** | `0x0200` | Constant configuration mask — not a live state |

Because no live domestic hot water pump bit exists on this firmware, the hot water versus heating phase is derived from the tank temperatures (the controller prioritizes DHW over space heating).

Firmware caveats (390-08):
- `ISTDREHZAHL_VERDICHTER` (`500.063e`) always reports 0, even while the compressor runs; compressor detection relies on the status word.
- `LAUFZEIT_VERDICHTER_HEIZEN` (`500.07fc`) saturates at 9999; the hot water hours counter (`500.0802`) appears valid.
- Heating circuit registers on the virtual circuit nodes answer on a slow internal cycle and can take several seconds.

---

### 3. Efficiency Optimization Rules

1. **Lower the Heating Curve (Thermischer Abgleich):**
   - Open thermostatic radiator valves completely in main living rooms.
   - Gradually lower curve slope (`HEIZKURVE_HK_1` / `HEIZKURVE_HK_2`) until living spaces maintain the desired temperature.
   - Every 1 K lower flow temperature increases COP by 2.5% to 3%.
2. **Minimize Buffer Elevation (*Pufferüberhöhung*):**
   - In installations with mixing valves, ensure the buffer setpoint (`SOLLTEMPERATUR_HK_1`) is no more than 1–2 K above the mixer requirement (`SOLLTEMPERATUR_HK_2`).
3. **Restrict the Electrical Booster Heater (DHC):**
   - Ensure the bivalence point in the WPM menu (*Bivalenzpunkt*) is set low (e.g. -7 °C to -10 °C). Electric immersion elements operate at COP = 1.0 and should only assist in extreme weather.
4. **Optimize Hot Water Charging:**
   - Keep DHW target temperatures at 48–50 °C rather than 55–60 °C.
   - Schedule DHW production during early afternoon hours when ambient air temperature is highest, maximizing air-source COP.
5. **Prevent Short-Cycling (Takten):**
   - Inverter heat pumps should run continuous modulation cycles lasting 45–90+ minutes. Monitor compressor cycles via the operating state (`betriebszustand`) and hot gas temperature (`HEISSGAS_TEMP`); note that `ISTDREHZAHL_VERDICHTER` reports 0 on firmware 390-08.

---

## Home Assistant Integration

Add the following to your Home Assistant `configuration.yaml` to ingest the operational status, temperatures, and total energy entities:

```yaml
mqtt:
  sensor:
    # Operational State
    - name: "Heat Pump Operating State"
      state_topic: "wpms3/status"
      icon: "mdi:heat-pump-outline"

    # Temperatures
    - name: "Heat Pump Outside Temperature"
      state_topic: "wpms3/info"
      value_template: "{{ value_json.aussentemperatur }}"
      unit_of_measurement: "°C"
      device_class: temperature
      state_class: measurement

    - name: "Radiator Flow Temperature (HK 2)"
      state_topic: "wpms3/info"
      value_template: "{{ value_json.isttemperatur_hk_2 }}"
      unit_of_measurement: "°C"
      device_class: temperature
      state_class: measurement

    - name: "Buffer Temperature"
      state_topic: "wpms3/info"
      value_template: "{{ value_json.pufferisttemperatur }}"
      unit_of_measurement: "°C"
      device_class: temperature
      state_class: measurement

    # Energy Dashboard Entity (Cumulative Electricity Consumption)
    - name: "Heat Pump Total Electrical Intake"
      state_topic: "wpms3/info"
      value_template: "{{ value_json.el_aufnahmeleistung_total_kwh }}"
      unit_of_measurement: "kWh"
      device_class: energy
      state_class: total_increasing

    # Energy Dashboard Entity (Cumulative Heat Output)
    - name: "Heat Pump Total Heat Output"
      state_topic: "wpms3/info"
      value_template: "{{ value_json.waermeertrag_total_kwh }}"
      unit_of_measurement: "kWh"
      device_class: energy
      state_class: total_increasing

    # Seasonal Performance Factor
    - name: "Heat Pump JAZ Total"
      state_topic: "wpms3/info"
      value_template: "{{ value_json.jaz_total }}"
      icon: "mdi:gauge"

    # Daily Efficiency (COP)
    - name: "Heat Pump Daily Heating COP"
      state_topic: "wpms3/info"
      value_template: "{{ value_json.cop_daily_heating | default('unavailable') }}"
      icon: "mdi:chart-line"

    - name: "Heat Pump Daily DHW COP"
      state_topic: "wpms3/info"
      value_template: "{{ value_json.cop_daily_ww | default('unavailable') }}"
      icon: "mdi:chart-line"

  binary_sensor:
    - name: "Heat Pump Compressor Active"
      state_topic: "wpms3/info"
      value_template: "{{ 'on' if value_json.verdichter_laeuft else 'off' }}"
      payload_on: "on"
      payload_off: "off"
      device_class: running

    - name: "Heat Pump Defrost Active"
      state_topic: "wpms3/info"
      value_template: "{{ 'on' if value_json.abtauung_aktiv else 'off' }}"
      payload_on: "on"
      payload_off: "off"
      device_class: cold

    - name: "Heat Pump Booster Active"
      state_topic: "wpms3/info"
      value_template: "{{ 'on' if value_json.zusatzheizung_aktiv else 'off' }}"
      payload_on: "on"
      payload_off: "off"
      device_class: heat
```

### Daily COP History & Rolling Averages

`cop_daily_heating` and `cop_daily_ww` evolve throughout the day and reset at midnight. To track seasonal performance trends without clock or timezone fragility, `etc/homeassistant-cop-history.yaml` provides event-driven trigger-based template sensors that detect when the controller rolls over its daily counters (resetting to 0) to capture the previous day's final converged COP and compute a 7-day rolling average.

To install:
1. Merge the `template:` and `sensor:` blocks from `etc/homeassistant-cop-history.yaml` into your `configuration.yaml` as top-level sections (not under `mqtt:`).
2. Go to **Developer Tools** -> **YAML** -> reload **Template entities**.
3. Restart Home Assistant to initialize the `statistics` sensors.
4. *(Optional)* To change the averaging window from 7 to 30 days, change `days: 7` to `days: 30` in the two statistics sensor blocks.

### Dashboard Setup

A ready-to-use Lovelace dashboard configuration is provided in `etc/homeassistant-dashboard.yaml`. To import it:
1. In Home Assistant, go to **Settings** -> **Dashboards** -> **Add Dashboard** -> select **New dashboard from scratch**.
2. Open the new dashboard, click the three dots (`...`) top-right -> **Edit Dashboard**.
3. Click the three dots (`...`) again -> **Raw configuration editor**.
4. Paste the content of `etc/homeassistant-dashboard.yaml` and click **Save**.

---

## Development & Building

### Running Tests
```bash
pytest -v
```

### Linting
```bash
ruff check .
```

### Building Debian Package
```bash
dpkg-buildpackage -us -uc -b
```

---

## License & Credits

Distributed under the terms of the **GNU General Public License v2.0** (`LICENSE`).

Reverse-engineering information and Elster CAN mapping credits:
- [fknipp/canpicker](https://github.com/fknipp/canpicker)
- [hanno2003/stiebel-eltron-can-bus-to-mqtt-reader](https://github.com/hanno2003/stiebel-eltron-can-bus-to-mqtt-reader)
- [Jürg Müller (juerg5524.ch)](https://juerg5524.ch)
