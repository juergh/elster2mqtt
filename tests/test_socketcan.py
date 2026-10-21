import socket
import struct

from elster2mqtt.elster2mqtt import (CAN_EFF_FLAG,
                                     CAN_FRAME_SIZE,
                                     CAN_RTR_FLAG,
                                     CanFrame,
                                     ElsterMessage,
                                     SocketCanBus,
                                     encode_elster_data)


class FakeSocket:
    def __init__(self, incoming=None):
        self.incoming = incoming
        self.sent = []

    def bind(self, addr):
        pass

    def send(self, data):
        self.sent.append(data)

    def recv(self, n):
        return self.incoming


def make_bus(monkeypatch, incoming=None):
    fake = FakeSocket(incoming)
    monkeypatch.setattr(socket, "socket", lambda *a, **k: fake)
    monkeypatch.setattr("elster2mqtt.elster2mqtt.select.select",
                        lambda *a, **k: ([fake], [], []) if incoming else ([], [], []))
    return SocketCanBus("can0"), fake


def test_socket_can_send_packs_request(monkeypatch):
    bus, fake = make_bus(monkeypatch)
    msg = ElsterMessage(sender=0x680, receiver=0x180, register=0x01D2)
    bus.send(msg)

    assert len(fake.sent) == 1
    frame = fake.sent[0]
    assert len(frame) == CAN_FRAME_SIZE
    can_id, dlc = struct.unpack_from("=IB", frame)
    assert can_id == 0x680
    assert dlc == 5
    assert frame[8:13] == bytes(encode_elster_data(0x180, 0x01D2))
    assert frame[13:] == b"\x00" * 3


def test_socket_can_send_sets_eff_flag(monkeypatch):
    bus, fake = make_bus(monkeypatch)
    msg = ElsterMessage(sender=0x1800, receiver=0x180, register=0x0001)
    bus.send(msg)

    can_id, = struct.unpack_from("=I", fake.sent[0])
    assert can_id == 0x1800 | CAN_EFF_FLAG


def test_socket_can_recv_parses_response(monkeypatch):
    payload = bytes([0xD2, 0x00, 0xFA, 0x01, 0xD2, 0x02, 0x31])
    incoming = struct.pack("=IB3x", 0x180, 7) + payload
    bus, _ = make_bus(monkeypatch, incoming=incoming)

    frame = bus.recv(timeout=1)
    assert isinstance(frame, CanFrame)
    assert frame.arbitration_id == 0x180
    assert frame.data == payload


def test_socket_can_recv_masks_extended_id(monkeypatch):
    incoming = struct.pack("=IB3x", 0x180 | CAN_EFF_FLAG, 7) + bytes(7)
    bus, _ = make_bus(monkeypatch, incoming=incoming)

    frame = bus.recv(timeout=1)
    assert frame.arbitration_id == 0x180


def test_socket_can_recv_empty_select(monkeypatch):
    bus, _ = make_bus(monkeypatch)
    assert bus.recv(timeout=0.01) is None


def test_socket_can_recv_skips_rtr(monkeypatch):
    incoming = struct.pack("=IB3x", 0x180 | CAN_RTR_FLAG, 0) + b"\x00" * 8
    bus, _ = make_bus(monkeypatch, incoming=incoming)
    assert bus.recv(timeout=1) is None