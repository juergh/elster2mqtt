#
# Read from an Elster CAN bus and publish to MQTT
#

import argparse
import json
import select
import socket
import struct
import time

import yaml

TYPE_REQUEST = 0x01
TYPE_RESPONSE = 0x02

CAN_FRAME_SIZE = 16
CAN_MAX_DLEN = 8
CAN_EFF_FLAG = 0x80000000
CAN_RTR_FLAG = 0x40000000
CAN_ERR_FLAG = 0x20000000


class ElsterError(Exception):
    pass


class ElsterDataSizeError(ElsterError):
    pass


class ElsterDataByte2Error(ElsterError):
    pass


class ElsterInvalidName(ElsterError):
    pass


class ElsterReadTimedOut(ElsterError):
    pass


def encode_elster_data(receiver, register, value=None):
    """Encode Elster request/respone frame data"""
    if value is None:
        msg_type = TYPE_REQUEST
        data = [0] * 5
    else:
        msg_type = TYPE_RESPONSE
        data = [0] * 7

    data[0] = ((receiver >> 3) & 0xf0) | (msg_type & 0x0f)
    data[1] = receiver & 0x7f
    data[2] = 0xfa
    data[3] = (register >> 8) & 0xff
    data[4] = register & 0xff

    if value is not None:
        data[5] = (value >> 8) & 0xff
        data[6] = value & 0xff

    return data


def decode_elster_data(data, size=None):
    """Decode Elster request/response frame data"""
    if size is not None:
        if len(data) != size:
            raise ElsterDataSizeError(f"Invalid data frame size: {len(data)} (expected {size})")
    elif len(data) not in (5, 7):
        raise ElsterDataSizeError(f"Invalid data frame size: {len(data)} (expected 5 or 7)")
    if data[2] != 0xfa:
        raise ElsterDataByte2Error(f"Unsupported data frame byte 2: {data}")

    msg_type = data[0] & 0x0f
    receiver = ((data[0] & 0xf0) << 3) | (data[1] & 0x7f)
    register = ((data[3] & 0xff) << 8) | (data[4] & 0xff)

    if len(data) == 7:
        value = ((data[5] & 0xff) << 8) | (data[6] & 0xff)
    else:
        value = None

    return msg_type, receiver, register, value


def twos_complement(value):
    """Compute the 2's complement"""
    if value & 0x8000:
        # Sign bit is set
        value = value - 0x10000
    return value


class CanFrame:
    """Minimal CAN frame: arbitration id plus payload"""
    def __init__(self, arbitration_id, data):
        self.arbitration_id = arbitration_id
        self.data = data


class SocketCanBus:
    """Minimal raw SocketCAN transport"""
    def __init__(self, channel):
        self.socket = socket.socket(socket.PF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
        self.socket.bind((channel,))

    def shutdown(self):
        self.socket.close()

    def send(self, frame):
        can_id = frame.arbitration_id
        data = bytes(frame.data)[:CAN_MAX_DLEN]
        if can_id > 0x7FF:
            can_id |= CAN_EFF_FLAG
        packet = struct.pack("=IB3x", can_id, len(data)) + data
        packet += b"\x00" * (CAN_FRAME_SIZE - len(packet))
        self.socket.send(packet)

    def recv(self, timeout=None):
        ready, _, _ = select.select([self.socket], [], [], timeout)
        if not ready:
            return None
        raw = self.socket.recv(CAN_FRAME_SIZE)
        can_id, = struct.unpack_from("=I", raw)
        if can_id & (CAN_RTR_FLAG | CAN_ERR_FLAG):
            return None
        dlc = raw[4]
        data = raw[8:8 + dlc]
        if can_id & CAN_EFF_FLAG:
            arbitration_id = can_id & 0x1FFFFFFF
        else:
            arbitration_id = can_id & 0x7FF
        return CanFrame(arbitration_id, data)


class ElsterMessage:
    """Elster message class"""
    def __init__(self, sender=None, receiver=None, register=None, msg=None, fmt=None):
        if msg:
            self.fmt = fmt
            self.sender = msg.arbitration_id
            self.type, self.receiver, self.register, self.value = decode_elster_data(msg.data)
            self.data = msg.data
        else:
            self.sender = sender
            self.receiver = receiver
            self.register = register
            self.fmt = fmt
            self.type = TYPE_REQUEST
            self.value = None
            self.data = encode_elster_data(self.receiver, self.register)

    @property
    def arbitration_id(self):
        return self.sender

    @property
    def formatted_value(self):
        if not self.fmt:
            return self.value
        return {
            "dec_val": twos_complement(self.value) / 10.0,
            "cent_val": twos_complement(self.value) / 100.0,
            "mil_val": twos_complement(self.value) / 1000.0,
            "little_endian": ((self.value & 0xff) << 8) | (self.value >> 8),
        }[self.fmt]


class ElsterBus:
    """Elster bus class"""
    def __init__(self, config, simulate=False, timeout=None):
        self.channel = config["can"]["interface"]
        self.sender = int(config["can"]["sender"], 16)
        config_timeout = float(config.get("can", {}).get("timeout", 5))
        self.timeout = float(timeout) if timeout is not None else config_timeout
        self.config = {x["name"]: x for x in config["data"]}
        self.simulate = simulate
        self.bus = None

    def __enter__(self):
        if not self.simulate:
            self.bus = SocketCanBus(self.channel)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.bus:
            self.bus.shutdown()

    def config_lookup(self, name):
        c = self.config.get(name)
        if not c:
            raise ElsterInvalidName(f"Invalid config name: {name}")
        rec, reg = c["index"].split(".")
        return int(rec, 16), int(reg, 16), c.get("format")

    def read(self, name=None, receiver=None, register=None, fmt=None, timeout=None):
        if timeout is None:
            timeout = self.timeout
        # Construct the read request
        if name:
            receiver, register, fmt = self.config_lookup(name)
        msg = ElsterMessage(sender=self.sender, receiver=receiver, register=register, fmt=fmt)

        if self.simulate:
            msg.type = TYPE_RESPONSE
            msg.sender = receiver
            msg.receiver = self.sender
            msg.value = 12345
            return msg

        # Send the read request
        self.bus.send(msg)
        try:
            return self._recv_response(receiver, register, fmt, timeout)
        except ElsterReadTimedOut:
            pass

        # Resend the request once: the bus has been quiet since the first
        # attempt, so controllers that ignored the query while busy usually
        # answer the resend.
        self.bus.send(msg)
        return self._recv_response(receiver, register, fmt, timeout)

    def _recv_response(self, receiver, register, fmt, timeout):
        # Wait for the response
        start_time = time.time()
        while True:
            remaining = (start_time + timeout) - time.time()
            if remaining <= 0:
                break
            m = self.bus.recv(timeout=remaining)
            if m is None:
                time.sleep(0.01)
                continue
            try:
                msg = ElsterMessage(msg=m, fmt=fmt)
                if (
                    msg.type == TYPE_RESPONSE
                    and msg.sender == receiver
                    and msg.receiver == self.sender
                    and msg.register == register
                ):
                    return msg
            except ElsterError:
                pass
        raise ElsterReadTimedOut("Timed out waiting for a response")


class MqttClient:
    """MQTT client class"""
    def __init__(self, config, simulate=False):
        self.server = config["mqtt"]["server"]
        self.port = int(config["mqtt"]["port"])
        self.topic_prefix = config["mqtt"]["topic_prefix"]
        self.simulate = simulate
        self.client = None

    def __enter__(self):
        if not self.simulate:
            import paho.mqtt.client as mqtt

            if hasattr(mqtt, "CallbackAPIVersion"):
                self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            else:
                self.client = mqtt.Client()
            self.client.connect(self.server, self.port)
            self.client.loop_start()  # runs in the background
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client:
            self.client.disconnect()
            self.client.loop_stop()

    def publish(self, topic, value, timeout=10):
        if self.topic_prefix:
            topic = f"{self.topic_prefix}{topic}"
        print(topic, value)
        if not self.simulate:
            res = self.client.publish(topic, value)
            res.wait_for_publish(timeout=timeout)


def derive_operational_state(info):
    """Derive operational state and active components from telemetry.

    Bit map of PUMPENSTATUS (180.01d2), verified by activating every
    relay individually via RELAISTEST ANLAGE on WPM 3 firmware 390-08
    (see docs/WPM3_REGISTERS.md): the word mirrors the system's physical
    pump and valve outputs. It carries no compressor and no utility-lock
    bit. Bit 0 (buffer charging pump 1) runs whenever the heat pump
    delivers heat, so it serves as the compressor proxy; a live speed
    reading is accepted as an additional signal.
    """
    pumpenstatus = int(info.get("pumpenstatus") or 0)
    verdichter_speed = float(info.get("istdrehzahl_verdichter") or 0)
    abtauung = int(info.get("abtauungaktiv") or 0)

    components = (
        ("pufferladepumpe_1", 0x0001),
        ("pufferladepumpe_2", 0x0002),
        ("quellenpumpe", 0x0004),
        ("solarpumpe", 0x0008),
        ("heizkreispumpe", 0x0010),
        ("mischerpumpe", 0x0020),
        ("warmwasserladepumpe", 0x0040),
        ("mischer_auf", 0x0080),
        ("mischer_zu", 0x0100),
        ("zirkulationspumpe", 0x0200),
        ("zusatzheizung", 0x0400),
    )
    active_components = [name for name, mask in components if pumpenstatus & mask]
    booster_active = bool(pumpenstatus & 0x0400)
    compressor_running = bool(pumpenstatus & 0x0001) or (verdichter_speed > 0)

    if abtauung == 1:
        operating_state = "abtauen"
    elif pumpenstatus & 0x0040:
        operating_state = "warmwasser"
    elif compressor_running:
        operating_state = "heizen"
    else:
        operating_state = "standby"

    return operating_state, compressor_running, active_components, booster_active, bool(abtauung == 1)


def synthesize_metrics(info):
    """Enrich telemetry with derived state, cumulative energies, and efficiencies."""
    state, comp_running, components, booster, defrosting = derive_operational_state(info)
    info["betriebszustand"] = state
    info["verdichter_laeuft"] = comp_running
    info["abtauung_aktiv"] = defrosting
    info["zusatzheizung_aktiv"] = booster
    info["aktive_komponenten"] = components

    # Energy counters split a 32-bit value across two registers whose
    # units differ by a factor of 1000 (MWh + kWh for lifetime totals,
    # kWh + Wh for daily totals). Combining them restores the display's
    # resolution, which the bare integer high register loses to
    # truncation. The low register is optional: without it the high
    # value is still 99.9% accurate.
    for target_key, high_key, low_key, factor, decimals in (
        ("waermeertrag_heiz_total_kwh", "waermeertrag_heiz_sum_mwh", "waermeertrag_heiz_sum_kwh", 1, 1),
        ("el_aufnahmeleistung_heiz_total_kwh", "el_aufnahmeleistung_heiz_sum_mwh", "el_aufnahmeleistung_heiz_sum_kwh", 1, 1),
        ("waermeertrag_ww_total_kwh", "waermeertrag_ww_sum_mwh", "waermeertrag_ww_sum_kwh", 1, 1),
        ("el_aufnahmeleistung_ww_total_kwh", "el_aufnahmeleistung_ww_sum_mwh", "el_aufnahmeleistung_ww_sum_kwh", 1, 1),
        ("waermeertrag_heiz_tag_kwh", "waermeertrag_heiz_tag_kwh", "waermeertrag_heiz_tag_wh", 1000, 3),
        ("waermeertrag_ww_tag_kwh", "waermeertrag_ww_tag_kwh", "waermeertrag_ww_tag_wh", 1000, 3),
        ("el_aufnahmeleistung_heiz_tag_kwh", "el_aufnahmeleistung_heiz_tag_kwh", "el_aufnahmeleistung_heiz_tag_wh", 1000, 3),
        ("el_aufnahmeleistung_ww_tag_kwh", "el_aufnahmeleistung_ww_tag_kwh", "el_aufnahmeleistung_ww_tag_wh", 1000, 3),
    ):
        if high_key not in info:
            continue
        try:
            high = float(info[high_key])
            low = float(info.get(low_key) or 0)
            info[target_key] = round((high * 1000.0 + low) / factor, decimals)
        except (ValueError, TypeError):
            pass

    heiz_heat = info.get("waermeertrag_heiz_total_kwh")
    heiz_el = info.get("el_aufnahmeleistung_heiz_total_kwh")
    ww_heat = info.get("waermeertrag_ww_total_kwh")
    ww_el = info.get("el_aufnahmeleistung_ww_total_kwh")

    if heiz_heat is not None and ww_heat is not None:
        info["waermeertrag_total_kwh"] = heiz_heat + ww_heat

    if heiz_el is not None and ww_el is not None:
        info["el_aufnahmeleistung_total_kwh"] = heiz_el + ww_el

    if heiz_heat is not None and heiz_el is not None and heiz_el > 0:
        info["jaz_heizung"] = round(heiz_heat / heiz_el, 2)
    if ww_heat is not None and ww_el is not None and ww_el > 0:
        info["jaz_warmwasser"] = round(ww_heat / ww_el, 2)
    if "waermeertrag_total_kwh" in info and "el_aufnahmeleistung_total_kwh" in info:
        if info["el_aufnahmeleistung_total_kwh"] > 0:
            info["jaz_total"] = round(info["waermeertrag_total_kwh"] / info["el_aufnahmeleistung_total_kwh"], 2)

    try:
        el_tag_hzg = float(info.get("el_aufnahmeleistung_heiz_tag_kwh") or 0)
        heat_tag_hzg = float(info.get("waermeertrag_heiz_tag_kwh") or 0)
        if el_tag_hzg > 0:
            info["cop_daily_heating"] = round(heat_tag_hzg / el_tag_hzg, 2)
    except (ValueError, TypeError):
        pass

    try:
        el_tag_ww = float(info.get("el_aufnahmeleistung_ww_tag_kwh") or 0)
        heat_tag_ww = float(info.get("waermeertrag_ww_tag_kwh") or 0)
        if el_tag_ww > 0:
            info["cop_daily_ww"] = round(heat_tag_ww / el_tag_ww, 2)
    except (ValueError, TypeError):
        pass

    return state, components


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="Configuration file in YAML format")
    parser.add_argument("--simulate-can", action="store_true", default=False, help="Simulate CAN bus read")
    parser.add_argument("--simulate-mqtt", action="store_true", default=False, help="Simulate MQTT publish")
    parser.add_argument("--timeout", type=float, default=None, help="CAN read timeout in seconds")
    parser.add_argument("--name", help="Read the given name")
    parser.add_argument("--index", help="Read the given index")
    args = parser.parse_args()

    with open(args.config, "r") as fh:
        config = yaml.load(fh, Loader=yaml.BaseLoader)

    with ElsterBus(config, simulate=args.simulate_can, timeout=args.timeout) as elster, MqttClient(config, simulate=args.simulate_mqtt) as mqttc:
        if args.name:
            msg = elster.read(args.name)
            mqttc.publish(args.name.lower(), msg.formatted_value)
            return

        if args.index:
            rec, reg = args.index.split(".")
            msg = elster.read(receiver=int(rec, 16), register=int(reg, 16))
            mqttc.publish(args.index, msg.value)
            return

        info = {}
        for name in elster.config:
            try:
                msg = elster.read(name)
                print(f"{name} ({msg.sender:03x}.{msg.register:04x}): {msg.formatted_value}")
                info[name.lower()] = msg.formatted_value
            except ElsterReadTimedOut as e:
                print(f"{name}: {e}")
            if not elster.simulate:
                time.sleep(0.05)
        if info:
            state, components = synthesize_metrics(info)
            comp_str = ", ".join(components) if components else "keine"
            print(f"BETRIEBSZUSTAND: {state.upper()} (Aktive Komponenten: {comp_str})")
            mqttc.publish("info", json.dumps(info))
            mqttc.publish("status", state)


if __name__ == "__main__":
    main()
