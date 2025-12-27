#
# Read from an Elster CAN bus and publish to MQTT
#

import argparse
import json
import time

import can
import paho.mqtt.client as mqtt
import yaml

TYPE_REQUEST = 0x01
TYPE_RESPONSE = 0x02


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


def decode_elster_data(data, size=7):
    """Decode Elster request/response frame data"""
    if len(data) != size:
        raise ElsterDataSizeError(f"Invalid data frame size: {len(data)} (expected {size})")
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


class ElsterMessage(can.Message):
    """Elster message class"""
    def __init__(self, sender=None, receiver=None, register=None, msg=None, fmt=None):
        if msg:
            self.fmt = fmt
            self.sender = msg.arbitration_id
            self.type, self.receiver, self.register, self.value = decode_elster_data(msg.data)
            data = msg.data
        else:
            self.sender = sender
            self.receiver = receiver
            self.register = register
            self.fmt = fmt
            self.type = TYPE_REQUEST
            self.value = None
            data = encode_elster_data(self.receiver, self.register)
        super().__init__(arbitration_id=self.sender, data=data, is_extended_id=False)

    @property
    def formatted_value(self):
        if not self.fmt:
            return self.value
        return {
            "dec_val": f"{(self.value / 10.0):.1f}",
            "mil_val": f"{(self.value / 1000.0):.3f}",
            "little_endian": f"{(((self.value & 0xff) << 8) | (self.value >> 8)):d}",
        }[self.fmt]


class ElsterBus:
    """Elster bus class"""
    def __init__(self, config, simulate=False):
        self.channel = config["can"]["interface"]
        self.sender = int(config["can"]["sender"], 16)
        self.config = {x["name"]: x for x in config["data"]}
        self.simulate = simulate
        self.bus = None

    def __enter__(self):
        if not self.simulate:
            self.bus = can.Bus(channel=self.channel, interface="socketcan")
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

    def read(self, name=None, receiver=None, register=None, fmt=None, timeout=30):
        # Construct the read request
        if name:
            receiver, register, fmt = self.config_lookup(name)
        msg = ElsterMessage(sender=self.sender, receiver=receiver, register=register, fmt=fmt)

        if self.simulate:
            msg.value = 12345
            return msg

        # Send the read request
        self.bus.send(msg)

        # Wait for the response
        start_time = time.time()
        while time.time() < (start_time + timeout):
            m = self.bus.recv(timeout=timeout)
            try:
                msg = ElsterMessage(msg=m, fmt=fmt)
                if msg.type == TYPE_RESPONSE and msg.sender == receiver and msg.receiver == self.sender:
                    return msg
            except ElsterError as e:
                print(e)
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
            self.client = mqtt.Client()
            self.client.connect(self.server, self.port)
            self.client.loop_start()  # runs in the background
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.client:
            self.client.loop_stop()

    def publish(self, topic, value):
        if self.topic_prefix:
            topic = f"{self.topic_prefix}{topic}"
        print(f"Publish: {topic}: {value}")
        if not self.simulate:
            self.client.publish(topic, value)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="Configuration file in YAML format")
    parser.add_argument("--simulate-can", action="store_true", default=False, help="Simulate CAN bus read")
    parser.add_argument("--simulate-mqtt", action="store_true", default=False, help="Simulate MQTT publish")
    parser.add_argument("--name", help="Read the given name")
    parser.add_argument("--index", help="Read the given index")
    args = parser.parse_args()

    with open(args.config, "r") as fh:
        config = yaml.load(fh, Loader=yaml.BaseLoader)

    with ElsterBus(config, simulate=args.simulate_can) as elster, MqttClient(config, simulate=args.simulate_mqtt) as mqttc:
        if args.name:
            msg = elster.read(args.name)
            print(f"{args.name} ({msg.sender:03x}.{msg.register:04x}): {msg.formatted_value}")
            return

        if args.index:
            rec, reg = args.index.split(".")
            msg = elster.read(receiver=int(rec, 16), register=int(reg, 16))
            print(f"UNBEKANNT ({args.index}): {msg.value}")
            return

        info = {}
        for name in elster.config:
            msg = elster.read(name)
            print(f"{name} ({msg.sender:03x}.{msg.register:04x}): {msg.formatted_value}")
            info[name.lower()] = msg.formatted_value
        mqttc.publish("info", json.dumps(info))


if __name__ == "__main__":
    main()
