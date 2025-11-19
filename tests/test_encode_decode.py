import pytest

from src.elster2mqtt.elster2mqtt import (ElsterDataByte2Error,
                                         ElsterDataSizeError,
                                         decode_elster_data,
                                         encode_elster_data)


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


def test_decode_response_ok():
    d = decode_elster_data([0x21, 0x23, 0xfa, 0xde, 0xaf, 0xba, 0xbe], size=7)
    assert d == (0x1, 0x0123, 0xdeaf, 0xbabe)


def test_decode_size_error():
    with pytest.raises(ElsterDataSizeError):
        decode_elster_data([])


def test_decode_byte2_error():
    with pytest.raises(ElsterDataByte2Error):
        decode_elster_data([0, 0, 0, 0, 0, 0, 0])
