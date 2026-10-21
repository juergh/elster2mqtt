import pytest

from elster2mqtt.elster2mqtt import (CanFrame,
                                     ElsterBus,
                                     ElsterReadTimedOut,
                                     TYPE_REQUEST,
                                     TYPE_RESPONSE,
                                     decode_elster_data,
                                     derive_operational_state,
                                     encode_elster_data,
                                     synthesize_metrics)


class DummyCanBus:
    def __init__(self, sender, receiver, register, value):
        self.sender = sender
        self.receiver = receiver
        self.register = register
        self.value = value

    def recv(self, timeout=None):
        data = encode_elster_data(self.sender, self.register, self.value)
        return CanFrame(arbitration_id=self.receiver, data=data)

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


def test_read_retries_once_after_timeout():
    class RetryCanBus:
        def __init__(self):
            self.sends = 0

        def send(self, msg):
            self.sends += 1

        def recv(self, timeout=None):
            if self.sends < 2:
                return None
            data = encode_elster_data(0x123, 0xdeaf, 0xdead)
            return CanFrame(arbitration_id=0x180, data=data)

    config = {
        "can": {"interface": "dummy", "sender": "123"},
        "data": [{"name": "FOO", "index": "180.deaf", "format": None}],
    }
    elster = ElsterBus(config)
    bus = RetryCanBus()
    elster.bus = bus
    msg = elster.read("FOO", timeout=0.05)
    assert bus.sends == 2
    assert msg.register == 0xdeaf
    assert msg.value == 0xdead


def test_read_ignores_mismatched_register():
    class MultiMsgCanBus:
        def __init__(self):
            self.messages = [
                CanFrame(arbitration_id=0x180, data=encode_elster_data(0x123, 0x0001, 999)),
                CanFrame(arbitration_id=0x180, data=encode_elster_data(0x123, 0xdeaf, 0xdead)),
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


def test_derive_operational_state():
    state, comp_running, comps, booster, defrost = derive_operational_state(
        {"pumpenstatus": 560, "istdrehzahl_verdichter": 0}
    )
    assert state == "standby"
    assert not comp_running
    assert not booster
    assert not defrost
    assert comps == []

    state, comp_running, comps, _, _ = derive_operational_state(
        {"pumpenstatus": 561,
         "warmwasser_isttemperatur": 56.5,
         "warmwasser_solltemperatur": 52.0}
    )
    assert state == "heizen"
    assert comp_running
    assert comps == ["verdichter"]

    state, comp_running, comps, _, _ = derive_operational_state(
        {"pumpenstatus": 561,
         "warmwasser_isttemperatur": 45.0,
         "warmwasser_solltemperatur": 52.0}
    )
    assert state == "warmwasser"
    assert comp_running
    assert comps == ["verdichter"]

    state, comp_running, comps, _, _ = derive_operational_state(
        {"pumpenstatus": 0, "istdrehzahl_verdichter": 45,
         "warmwasser_isttemperatur": 56.5,
         "warmwasser_solltemperatur": 52.0}
    )
    assert state == "heizen"
    assert comp_running

    state, _, _, _, defrost = derive_operational_state(
        {"abtauungaktiv": 1, "pumpenstatus": 561}
    )
    assert state == "abtauen"
    assert defrost

    state, comp_running, comps, booster, _ = derive_operational_state(
        {"pumpenstatus": 0x0232}
    )
    assert state == "zusatzheizung"
    assert not comp_running
    assert booster
    assert "zusatzheizung_stufe_1" in comps

    state, comp_running, comps, booster, _ = derive_operational_state(
        {"pumpenstatus": 0x0233}
    )
    assert state == "heizen_booster"
    assert comp_running
    assert booster

    state, _, comps, _, _ = derive_operational_state({"pumpenstatus": 816})
    assert state == "evu_sperre"
    assert comps == ["evu_sperre"]

    state, _, comps, _, _ = derive_operational_state({"pumpenstatus": 688})
    assert state == "standby"
    assert comps == ["mischer_auf"]


def test_synthesize_metrics():
    info = {
        "pumpenstatus": 561,
        "waermeertrag_heiz_sum_mwh": 74,
        "waermeertrag_heiz_sum_kwh": 128,
        "el_aufnahmeleistung_heiz_sum_mwh": 19,
        "el_aufnahmeleistung_heiz_sum_kwh": 5,
        "waermeertrag_ww_sum_mwh": 4,
        "waermeertrag_ww_sum_kwh": 411,
        "el_aufnahmeleistung_ww_sum_mwh": 1,
        "el_aufnahmeleistung_ww_sum_kwh": 44,
        "waermeertrag_heiz_tag_kwh": 20,
        "el_aufnahmeleistung_heiz_tag_kwh": 5,
        "waermeertrag_ww_tag_kwh": 8,
        "el_aufnahmeleistung_ww_tag_kwh": 2,
    }
    state, comps = synthesize_metrics(info)
    assert state == "heizen"
    assert info["betriebszustand"] == "heizen"
    assert info["verdichter_laeuft"] is True
    assert info["waermeertrag_heiz_total_kwh"] == 74128.0
    assert info["el_aufnahmeleistung_heiz_total_kwh"] == 19005.0
    assert info["waermeertrag_ww_total_kwh"] == 4411.0
    assert info["el_aufnahmeleistung_ww_total_kwh"] == 1044.0
    assert info["waermeertrag_total_kwh"] == 78539.0
    assert info["el_aufnahmeleistung_total_kwh"] == 20049.0
    assert info["jaz_heizung"] == 3.90
    assert info["jaz_warmwasser"] == 4.23
    assert info["jaz_total"] == 3.92
    assert info["cop_daily_heating"] == 4.0
    assert info["cop_daily_ww"] == 4.0


def test_elster_bus_timeout_config():
    config = {
        "can": {"interface": "dummy", "sender": "123", "timeout": 10},
        "data": [],
    }
    bus = ElsterBus(config, simulate=True)
    assert bus.timeout == 10.0

    config_default = {
        "can": {"interface": "dummy", "sender": "123"},
        "data": [],
    }
    bus_default = ElsterBus(config_default, simulate=True)
    assert bus_default.timeout == 5.0

    bus_override = ElsterBus(config, simulate=True, timeout=2.5)
    assert bus_override.timeout == 2.5
