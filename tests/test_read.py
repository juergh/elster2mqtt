import can

from src.elster2mqtt.elster2mqtt import (ElsterBus,
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

    def __iter__(self):
        return self

    def __next__(self):
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
