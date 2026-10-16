import pytest

from elster2mqtt.elster2mqtt import (ElsterDataByte2Error,
                                     ElsterDataSizeError,
                                     ElsterMessage,
                                     decode_elster_data,
                                     encode_elster_data,
                                     twos_complement)


def test_encode_request_ok():
    receiver = 0x0123
    register = 0xdeaf
    d = encode_elster_data(receiver, register)
    assert d == [0x21, 0x23, 0xfa, 0xde, 0xaf]


def test_encode_response_ok():
    receiver = 0x0123
    register = 0xdeaf
    value = 0xbabe
    d = encode_elster_data(receiver, register, value)
    assert d == [0x22, 0x23, 0xfa, 0xde, 0xaf, 0xba, 0xbe]


def test_decode_request_ok():
    d = decode_elster_data([0x21, 0x23, 0xfa, 0xde, 0xaf], size=5)
    assert d == (0x1, 0x0123, 0xdeaf, None)


def test_decode_request_default_size_ok():
    d = decode_elster_data([0x21, 0x23, 0xfa, 0xde, 0xaf])
    assert d == (0x1, 0x0123, 0xdeaf, None)


def test_decode_response_ok():
    d = decode_elster_data([0x21, 0x23, 0xfa, 0xde, 0xaf, 0xba, 0xbe], size=7)
    assert d == (0x1, 0x0123, 0xdeaf, 0xbabe)


def test_decode_response_default_size_ok():
    d = decode_elster_data([0x21, 0x23, 0xfa, 0xde, 0xaf, 0xba, 0xbe])
    assert d == (0x1, 0x0123, 0xdeaf, 0xbabe)


def test_decode_size_error():
    with pytest.raises(ElsterDataSizeError):
        decode_elster_data([])


def test_decode_byte2_error():
    with pytest.raises(ElsterDataByte2Error):
        decode_elster_data([0, 0, 0, 0, 0, 0, 0])


def test_twos_complement_positive():
    assert twos_complement(0x0000) == 0
    assert twos_complement(0x00c8) == 200
    assert twos_complement(0x7fff) == 32767


def test_twos_complement_negative():
    assert twos_complement(0xffff) == -1
    assert twos_complement(0xfff6) == -10
    assert twos_complement(0x8000) == -32768


def test_formatted_value():
    msg = ElsterMessage(sender=0x180, receiver=0x680, register=0x000c, fmt="dec_val")
    msg.value = 0x00c8
    assert msg.formatted_value == 20.0

    msg.value = 0xfff6
    assert msg.formatted_value == -1.0

    msg.fmt = "cent_val"
    msg.value = 40
    assert msg.formatted_value == 0.40

    msg.fmt = "mil_val"
    msg.value = 0x07d0
    assert msg.formatted_value == 2.0

    msg.fmt = "little_endian"
    msg.value = 0x1234
    assert msg.formatted_value == 0x3412

    msg.fmt = None
    msg.value = 42
    assert msg.formatted_value == 42
