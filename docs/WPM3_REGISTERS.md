# WPM 3 / WPL 15 AS — Elster CAN Bus Register Reference

Empirically derived register and protocol reference for the Stiebel-Eltron
**WPM 3** heat pump manager (firmware 390-08) driving a **WPL 15 AS**
inverter air-to-water heat pump.

Everything below was established on live hardware through three methods:

1. **RELAISTEST ANLAGE / WÄRMEPUMPE** — activating each output relay
   individually from the service menu while sampling `180.01d2` every
   1–2 s via `cansend`, and passively capturing the WPM's own relay
   command writes.
2. **Display-navigation correlation** — navigating `INFO`/`DIAGNOSE`
   menus on the WPM display while recording the bus; the display polls
   exactly the registers behind each screen item, in screen order.
3. **Natural-cycle observation** — long `candump` captures across
   compressor start/stop, DHW charging and defrost events, correlated
   with `elster2mqtt` telemetry logs.

Confidence is marked per item: **verified** (direct experiment),
**observed** (consistent across captures, semantics inferred),
**tentative** (single observation, interpretation uncertain).

---

## 1. Protocol Essentials

- Physical layer: 20 kbit/s, standard 11-bit CAN identifiers,
  7-byte data payload, big-endian 16-bit register values.

### 1.1 Command byte derivation

The first data byte encodes both the command type *and* the node
address it refers to:

- **Request:**  `cmd = ((receiver >> 3) & 0xF0) | 0x01`
- **Response:** `cmd = ((requester >> 3) & 0xF0) | 0x02`
- **Write:**    `cmd = ((receiver >> 3) & 0xF0) | 0x00`

| Example frame | Explanation |
|---|---|
| `680#3100FA01D2` | 680 requests register `01D2` from node `0x180` (`0x30|1 = 0x31`) |
| `180#D200FA01D20230` | 180 answers to requester `0x680` (`0xD0|2 = 0xD2`), value `0230` |
| `480#A100FA01990000` | 480 requests register `0199` from node `0x500` (`0xA0|1 = 0xA1`) |
| `500#9200FA01990093` | 500 answers to requester `0x480` (`0x90|2 = 0x92`), value `0x93` |
| `100#A114FA0A370000` | 100 requests register `0A37` from node `0x514` (`byte1 = 0x514 & 0x7F = 0x14`) |
| `514#2200FA0A370137` | 514 answers to requester `0x100` (`0x20|2 = 0x22`) |

### 1.2 Frame layout

Extended indices (>= 0x100):

    [cmd] [receiver & 0x7F] [0xFA] [reg_hi] [reg_lo] [val_hi] [val_lo]

Short indices (< 0x100):

    [cmd] [receiver & 0x7F] [reg]   [val_hi] [val_lo] [0x00]  [0x00]

**Common pitfall:** the command byte and byte 1 *select the target
node*. `31 00 FA 063E` queries node **180** (which answers `0x8000 =
not available` for heat pump registers); node 500 must be addressed
with `A1 00 FA ...`.

### 1.3 Value sentinel

`0x8000` (32768, interpreted signed: −32768) means *"not available on
this node/firmware"*. Registers reporting it are dead on this
configuration, not necessarily on other models.

### 1.4 Other frame types observed

| Command byte pattern | Meaning |
|---|---|
| `20 00 FA <reg> <val>` | Write command (e.g. relay commands, parameter changes) |
| `70/71 00 FA <reg> <val>` | Node-500 self-announcement of changed values (seen during compressor ramp) |
| `90/91/30/31` pairs | Bus synchronisation / FE handshake bursts (~every 3 min, normal) |
| `66/76/A6/C6/26/97 xx FE 01` | FE synchronisation sequence |

---

## 2. Bus Node Map

| CAN ID | Role |
|---|---|
| `0x100` | Bedienteil / display and ambient source; broadcasts outdoor temperature (`067A`, `000C`), polls the `PROZESSDATEN` screen block when the display shows it |
| `0x180` | WPM system node: heating circuits, buffer, DHW, system status `01d2` |
| `0x480` | WPM manager: bus master; cyclic 10 s polling (`0199`, `02CA`, `0917`, `07A8`, `0A36`, `01D6`, `0016`); writes relay command bitmaps |
| `0x500` | WPL 15 AS heat pump controller: refrigerant circuit, inverter, energy counters |
| `0x514` | Auxiliary inverter/IWS controller; mirrors most `500` process-data registers; writes `500.4E7A` |
| `0x601` | Heating circuit 1 (direct/buffer circuit): `010e` heating curve |
| `0x602` | Heating circuit 2 (mixer circuit): `010e` heating curve, `000f`/`0004` flow Ist/Soll |
| `0x680` | Free sender ID used by diagnostic tools (`elster2mqtt`, `cansend`) |

---

## 3. System Status `180.01d2` (`PUMPENSTATUS` / STATUS ANLAGE)

**Verified bit map** (each bit individually activated via RELAISTEST
ANLAGE and observed in `01d2` responses):

| Bit | Mask | Component |
|---|---|---|
| 0 | `0x0001` | Pufferladepumpe 1 (buffer charging pump 1) |
| 1 | `0x0002` | Pufferladepumpe 2 |
| 2 | `0x0004` | Quellenpumpe (source pump / brine) |
| 3 | `0x0008` | Solarpumpe |
| 4 | `0x0010` | Heizkreispumpe (heating circuit pump) |
| 5 | `0x0020` | Mischerpumpe (mixer circuit pump) |
| 6 | `0x0040` | Warmwasserladepumpe (DHW loading pump) |
| 7 | `0x0080` | Mischer AUF (mixer driving open) |
| 8 | `0x0100` | Mischer ZU (mixer driving closed) |
| 9 | `0x0200` | Zirkulationspumpe (DHW circulation) |
| 10 | `0x0400` | 2. Wärmeerzeuger (electric booster heater / NHZ) |

Notes:

- There is **no compressor bit and no EVU-lock bit** in `01d2`.
  Earlier community interpretations (bit 0 = compressor, bits 1/2 =
  booster stages, bit 8 = EVU lock) are wrong for the WPM 3; they
  originate from the ISG Modbus register 2501 spec and the older
  `BETRIEBS_STATUS` (`0x0176`) used on other models.
- Bit 0 (Pufferladepumpe 1) runs whenever the heat pump delivers heat,
  so it is a good *proxy* for compressor activity — which is why the
  mistake persisted.
- EVU lock has its own registers: `0x0074` (`EVU_SPERRE_AKTIV`),
  `0x01D3` (`EVU`), `0xFDAC` (`WP_EVU`).

Commonly observed composite values on this installation:

| Value | Hex | Bits | Situation |
|---|---|---|---|
| 560 | `0x0230` | 4, 5, 9 | Idle circulation (HK + mixer + DHW circulation pumps) |
| 513 | `0x0201` | 0, 9 | Transition: buffer pump + circulation (heat pump starting) |
| 561 | `0x0231` | 0, 4, 5, 9 | Space heating with compressor |
| 577 | `0x0241` | 0, 6, 9 | DHW charging |
| 688 | `0x02B0` | 4, 5, 7, 9 | Circulation while mixer opens |
| 816 | `0x0330` | 4, 5, 8, 9 | Circulation while mixer closes |

---

## 4. Relay Command Bitmaps (written by 480 to 500)

The manager continuously mirrors its output relay state onto the heat
pump controller. Refresh burst every ~60 s plus immediate writes on
change. **Not present in any public community table** (discovered via
bus sniffing + relay test).

### `500.4E5E` — system relay command bitmask

| Mask | Relay |
|---|---|
| `0x0001` | Heizkreispumpe |
| `0x0002` | Mischerpumpe |
| `0x0004` | Mischer AUF |
| `0x0008` | Mischer ZU |
| `0x0040` | Quellenpumpe |
| `0x0100` | Warmwasserladepumpe |
| `0x0400` | 2. Wärmeerzeuger |
| `0x0800` | Zirkulationspumpe |
| `0x1000` | Solarpumpe |

Normal idle/heating value on this installation: `0x0803` (Zirkulation +
Heizkreispumpe + Mischerpumpe), `0x080B` while the mixer closes.

### `500.4E7E` — buffer pump command bitmask

`0x0001` = Pufferladepumpe 1, `0x0002` = Pufferladepumpe 2.

### Other periodic writes observed from 480

| Register | Observed values | Interpretation |
|---|---|---|
| `500.4E6A` | `0x0000`, `0x0240` | Output group bitmap (semantics open) |
| `500.0692` | `0x0000`, `0x0001` | Output group bitmap (semantics open) |
| `500.4E30` | polled, value `0` | Output group (semantics open) |
| `500.4E7A` | written by **514**: `0x040A`/`0x022A` running, `0x000A`/`0x002A` idle | Bit `0x0200` correlated with compressor running (tentative) |
| `500.0126` | +`0x100` per minute (`0x2D00`, `0x2E00`, ...) | Minute accumulator (`MINUTE` in community table) |

---

## 5. Energy Counters — Split 32-Bit Architecture

All energy counters are stored as two consecutive 16-bit registers
forming a 32-bit value:

- **Daily counters (`TAG`):** low word = **Wh** (even address),
  high word = **kWh** (odd address)
  → `tag_kwh = kWh + Wh / 1000`
- **All-time counters (`SUMME`):** low word = **kWh** (even address),
  high word = **MWh** (odd address)
  → `sum_kwh = MWh * 1000 + kWh`

Empirical proof: immediately after a DHW cycle started,
`500.092A = 85`, `500.092B = 0` → 0.085 kWh charged so far; the display
shows the same value with three decimals (e.g. `3.866 kWh`).

| Display item (INFO → WÄRMEPUMPE) | Wh reg | kWh reg | MWh reg |
|---|---|---|---|
| WÄRMEMENGE VD HEIZEN TAG | `500.092E` | `500.092F` | — |
| WÄRMEMENGE VD HEIZEN SUMME | — | `500.0930` | `500.0931` |
| WÄRMEMENGE VD WW TAG | `500.092A` | `500.092B` | — |
| WÄRMEMENGE VD WW SUMME | — | `500.092C` | `500.092D` |
| WÄRMEMENGE NHZ HEIZEN TAG | `500.0926` | `500.0927` | — |
| WÄRMEMENGE NHZ HEIZEN SUMME | — | `500.0928` | `500.0929` |
| WÄRMEMENGE NHZ WW TAG | `500.0922` | `500.0923` | — |
| WÄRMEMENGE NHZ WW SUMME | — | `500.0924` | `500.0925` |
| LEISTUNGSAUFNAHME VD HEIZEN TAG | `500.091E` | `500.091F` | — |
| LEISTUNGSAUFNAHME VD HEIZEN SUMME | — | `500.0920` | `500.0921` |
| LEISTUNGSAUFNAHME VD WW TAG | `500.091A` | `500.091B` | — |
| LEISTUNGSAUFNAHME VD WW SUMME | — | `500.091C` | `500.091D` |

Important caveat from the WPM 3 manual (section 5.1.2):
*LEISTUNGSAUFNAHME is calculated from refrigerant circuit pressures —
it is not suitable for billing, only for approximate energy balancing
together with WÄRMEMENGE.*

Derived efficiency metrics:

    cop_daily_heating = waermeertrag_heiz_tag   / el_aufnahmeleistung_heiz_tag
    cop_daily_ww      = waermeertrag_ww_tag     / el_aufnahmeleistung_ww_tag
    jaz_heizung       = waermeertrag_heiz_summe / el_aufnahmeleistung_heiz_summe
    jaz_ww            = waermeertrag_ww_summe   / el_aufnahmeleistung_ww_summe

Reference values on this installation: JAZ heating 3.90, DHW 4.22,
total 3.92 (device-internal estimates).

---

## 6. Live Process Data (INFO → WÄRMEPUMPE → PROZESSDATEN)

Verified by navigating the display screen while capturing; the display
polls each shown item in screen order.

| Display item | Register | Format | Observed live values | Confidence |
|---|---|---|---|---|
| VERDAMPFERTEMPERATUR | `500.07A9` | dec_val °C | 2.8 → 1.5 °C while running | verified |
| HEISSGASTEMPERATUR | `500.0265` | dec_val °C | 54.1 → 59.4 °C running; ~26 °C idle | verified |
| ÖLSUMPFTEMPERATUR | `500.0A37` | dec_val °C | 30.7–31.6 °C | observed |
| VERFLÜSSIGERTEMPERATUR | `500.0A39` | dec_val °C | 34.9 → 29.8 °C | observed |
| DRUCK NIEDERDRUCK | `500.07A7` | cent_val bar | 8.20 → 7.33 bar running; 10.52 idle | verified |
| DRUCK HOCHDRUCK | `500.07A6` | cent_val bar | 21.56 → 22.54 bar running; 20.87 idle | verified |
| WP WASSERVOLUMENSTROM | `500.02E2` | dec_val l/min | 28.2–28.8 l/min while running | verified |
| STROM INVERTER | `500.06B2` | dec_val A | 5.2–5.6 A running | verified |
| SPANNUNG INVERTER | `500.06B1` | dec_val V | 237.6–238.2 V running | verified |
| ISTDREHZAHL VERDICHTER | `500.063B` | integer Hz | ramps 38 → 71 Hz on start, steady 71 | verified |
| SOLLDREHZAHL VERDICHTER | `500.063D` | integer Hz | 71 Hz steady; 0 during early ramp | observed |
| LUEFTERLEISTUNG REL | `500.0833` | integer % | 47–48 % while running | observed |

Compressor start sequence captured end-to-end (natural DHW start):

1. `500.0917` (heating integral, see section 8) snaps from `1000` to
   `0` — start permission granted.
2. Relay commands: buffer pump on (`4E7E 0x0001`), heating circuit
   valves close; `01d2` walks `560 → 513 → 577`.
3. Compressor ignites at 38 Hz and ramps in ~4 Hz steps every ~2 s to
   71 Hz (`063B`).
4. Once at full speed, `500.07A8` changes `0x0008 → 0x0218`.
5. Rough output estimate with the confirmed registers (validations
   still open, see section 11):
   - `Q_th = flow[l/min]/60 * 4.18 * (T_vorlauf - T_ruecklauf)`
   - `P_el ≈ sqrt(3) * U * I` (three-phase inverter output) or
     `U * I` (single-phase) — factor still to be validated against a
     meter; the three-phase reading gives physically plausible results.

Live but unlabeled candidates (answered on the bus, semantics open):

| Register | Observed behaviour |
|---|---|
| `500.063F` / `500.06D9` | identical values, 50 → 15 during compressor run (decreasing; candidate: fan or ND-stage speed) |
| `500.05D7` | ~31.4–31.6 °C, nearly constant (temperature candidate) |
| `500.0832` | 1189 → 1060 during run (modulating value, scaling unknown) |
| `500.06EB` / `500.06EC` | 47 on node 514 (equals `0833` fan %; Ist/Soll speed pair on other models) |
| `500.0A36` | temperature polled by 480 every 10 s; 27–42 range, drifts with operating state |
| `500.0A38` | 46 → 17 during run |

---

## 7. Runtime & Defrost Counters

| Register | Meaning | Observed | Confidence |
|---|---|---|---|
| `500.0802` | Compressor runtime DHW [h] | 781, increments | verified |
| `500.07FC` | Compressor runtime heating [h] | saturated at 9999 | verified dead |
| `500.0805` | NHZ 1/2 runtime [h] | 124 | observed (live!) |
| `500.0259` | NHZ 1 runtime [h] | 15 | observed |
| `500.025A` | NHZ 2 runtime [h] | 16 | observed |
| `500.0806` | Defrost start counter | 3048 → 3049 | verified |
| `500.0807` | Duration of last defrost [min] | 2 | verified |
| `500.0808` | Defrost compressor runtime VD1 [s] | 181 | verified |
| `500.0061` | Defrost active flag | 0; read 512 once during DHW (semantics unclear) | tentative |
| `500.07FD`–`0801` | Further compressor runtime registers | 0 | observed |

---

## 8. Regulation & Diagnostics

| Register | Meaning | Observed behaviour | Confidence |
|---|---|---|---|
| `500.0917` | `INTEGRAL_REGELABWEICHUNG_RELATIV` — heating control integral (community name; matches behaviour) | Holds `1000` while idle-with-demand; snaps to `0` at the instant the WPM commands a start; WPM polls it every 10 s | observed (strong) |
| `500.07A8` | `VERDICHTER` — compressor state word | `0x0008` idle; `0x0218` while compressor at full speed; `0x0408` seen for exactly 20 min once (unresolved) | observed |
| `500.0199` | `SOFTWARE_NUMMER` | constant `0x0093` (147) | verified |
| `500.019A` | `SOFTWARE_VERSION` | constant `6` | verified |
| `500.0359` | `HARDWARE_NUMMER` | `0x8000` sentinel | observed |
| `500.0961` | unknown | constant `0x4F` (79) | observed |
| `500.06FF` | `TEST_OBJEKT_190` | constant `0x1FD1` (8145) | observed |
| `500.0639` | `K_OS_RMX_RESERVE_INFO3` | constant `8` | observed |
| `180.0001` | `FEHLERMELDUNG` | constant `69`, not a live fault code | observed |
| `180.0074` | `EVU_SPERRE_AKTIV` (community) | not yet observed live on this bus | tentative |
| `180.01D3` | `EVU` | not yet observed | tentative |

---

## 9. Pressure Sensor Calibration (node 500 self-broadcast)

Broadcast periodically by node 500 (`90 00 FA ...`):

| Register | Name | Value |
|---|---|---|
| `500.079F` | `STUETZSTELLE_ND1` | 100 |
| `500.07A0` | `STUETZSTELLE_ND2` | 1700 |
| `500.07A1` | `STUETZSTELLE_HD1` | 100 |
| `500.07A2` | `STUETZSTELLE_HD2` | 4300 |

---

## 10. Dead / Unavailable Registers (firmware 390-08, WPL 15 AS)

These answer `0x8000`, constant 0, or saturated values on this
configuration. Listed so nobody re-probes them blindly:

| Register | Community name | Observed |
|---|---|---|
| `500.063E` / `500.063C` | `IST/SOLLDREHZAHL_VERDICHTER_1` (old mapping) | 0 — the live speed pair is `063B`/`063D` |
| `180.063E` | ISTDREHZAHL on system node | `0x8000` sentinel |
| `500.0014` | `VERDAMPFERTEMP` (old mapping) | `0x8000` — live evaporator temp is `07A9` |
| `180.01DA` / `500.01DA` | `VOLUMENSTROM` (old mapping) | 0 / `0x8000` — live flow is `02E2` |
| `500.05A8` / `500.05A9` | `LAUFZEIT_NHZ_WW/HEIZEN` | `0x8000` — live NHZ runtime is `0805` |
| `500.090A` | `STROMAUFNAHME_WE1_STUFE_1` (old mapping for inverter current) | `0x8000` — live current is `06B2` |
| `500.0929` / `500.0925` | NHZ 2WE sums (MWh word) | 0 unless the booster has run |
| `500.C24C/C24D/C252/C255/C2C0/C2C1` | unknown block | 70/70/7/3/`0x8000`/`0x8000` |
| `500.1388` / `500.138A` | unknown | 0 |

---

## 11. Open Questions

- `063D` (Soll-Drehzahl) broadcast `0` during the compressor ramp while
  `063B` tracked 38 → 71 Hz; IST/SOLL assignment needs one more
  observation at partial load (values differ).
- `500.07A8` semantics: `0x0008` idle, `0x0218` running, one unexplained
  20-minute `0x0408` window.
- Whether `06B1 × 06B2` needs the √3 three-phase factor for a correct
  electrical power reading (validate against a utility meter).
- `500.0832`, `500.063F`, `500.05D7`, `500.0A38` semantics.
- `500.0061` (`ABTAUUNGAKTIV`) reported `512` once during a DHW cycle —
  defrost flag semantics need a capture during an actual defrost.
- Location of the live EVU-lock state on this bus (`0074`/`01D3`/`FDAC`
  are candidates, none observed active yet).

---

## 12. Community Cross-Reference

Register naming follows the community `ElsterTable` (juerg5524.ch,
fknipp/canpicker, hanno2003/stiebel-eltron-can-bus-to-mqtt-reader).
Corrections established by the experiments above:

- `01d2` is the *system relay status* (STATUS ANLAGE), not the ISG
  operating-state bitfield; see section 3.
- `063B`/`063D`, not `063E`/`063C`, carry the live compressor speed on
  this firmware.
- Flow, inverter voltage/current live at `02E2`/`06B1`/`06B2`, not at
  `01DA`/`090A`.
- `4E5E`/`4E7E` relay command bitmaps are not in any public table.
- `0917` is the heating control integral — it explains "standby despite
  demand" periods (start permission is granted by the integral, not by
  a simple Ist/Soll comparison).
