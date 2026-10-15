import can
import pytest

from elster2mqtt.elster2mqtt import (ElsterBus,
                                     ElsterReadTimedOut,
                                     TYPE_REQUEST,
                                     TYPE_RESPONSE,
                                     decode_elster_data,
                                     encode_elster_data)


class DummyCanBus:
    def __init__(self, sender, receiver, register, value):
        self.sender = sender
        self.receiver = receiver
        self.register = register
        self.value = value

    def recv(self, timeout=None):
        data = encode_elster_data(self.sender, self.register, self.value)
        return can.Message(arbitration_id=self.receiver, data=data)

    def send(self, msg):
        msg_type, receiver, register, value = decode_elster_data(msg.data, size=5)
        assert msg_type == TYPE_REQUEST
        assert receiver == self.receiver
        assert register == self.register
        assert value is None


def test_read():
    config = {
        "can": {
            "interface": "dummy",
            "sender": "123",
        },
        "data": [
            {"name": "FOO", "index": "180.deaf", "format": None},
        ],
    }
    elster = ElsterBus(config)
    elster.bus = DummyCanBus(sender=0x123, receiver=0x180, register=0xdeaf, value=0xdead)
    msg = elster.read("FOO")
    assert msg.type == TYPE_RESPONSE
    assert msg.sender == 0x180
    assert msg.receiver == 0x123
    assert msg.register == 0xdeaf
    assert msg.value == 0xdead


def test_read_timeout():
    class TimeoutCanBus:
        def send(self, msg):
            pass

        def recv(self, timeout=None):
            return None

    config = {
        "can": {"interface": "dummy", "sender": "123"},
        "data": [{"name": "FOO", "index": "180.deaf", "format": None}],
    }
    elster = ElsterBus(config)
    elster.bus = TimeoutCanBus()
    with pytest.raises(ElsterReadTimedOut):
        elster.read("FOO", timeout=0.05)


def test_read_ignores_mismatched_register():
    class MultiMsgCanBus:
        def __init__(self):
            self.messages = [
                can.Message(arbitration_id=0x180, data=encode_elster_data(0x123, 0x0001, 999)),
                can.Message(arbitration_id=0x180, data=encode_elster_data(0x123, 0xdeaf, 0xdead)),
            ]

        def send(self, msg):
            pass

        def recv(self, timeout=None):
            if self.messages:
                return self.messages.pop(0)
            return None

    config = {
        "can": {"interface": "dummy", "sender": "123"},
        "data": [{"name": "FOO", "index": "180.deaf", "format": None}],
    }
    elster = ElsterBus(config)
    elster.bus = MultiMsgCanBus()
    msg = elster.read("FOO", timeout=1)
    assert msg.register == 0xdeaf
    assert msg.value == 0xdead


def test_read_simulate():
    config = {
        "can": {"interface": "dummy", "sender": "123"},
        "data": [{"name": "FOO", "index": "180.deaf", "format": None}],
    }
    elster = ElsterBus(config, simulate=True)
    msg = elster.read("FOO")
    assert msg.type == TYPE_RESPONSE
    assert msg.sender == 0x180
    assert msg.receiver == 0x123
    assert msg.register == 0xdeaf
    assert msg.value == 12345
