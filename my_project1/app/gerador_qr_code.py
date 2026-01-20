import crcmod
from decimal import Decimal


def _emv(id_, value):
    tamanho = f"{len(value):02d}"
    return f"{id_}{tamanho}{value}"


def _crc16(payload: str) -> str:
    crc16 = crcmod.mkCrcFun(0x11021, initCrc=0xFFFF, xorOut=0x0000)
    crc = crc16(payload.encode('utf-8'))
    return f"{crc:04X}"


def gerar_qr_pix(
        chave_pix: str,
        valor: Decimal,
        txid: str,
        nome: str,
        cidade: str
) -> str:
    '''
    Gera payload PIX Cópia e Cola conforme padrão BACEN (EMV-Co)
    '''
    valor = f"{valor:.2f}"

    payload = (
        _emv("00", "01") +
        _emv(
            "26",
            _emv("00", "BR.GOV.BCB.PIX") +
            _emv("01", chave_pix) +
            _emv("05", txid)
        ) + 
        _emv("52", "0000") +
        _emv("53", "986") + 
        _emv("54", valor) +
        _emv("58", "BR") +
        _emv("59", nome[:25]) +
        _emv("60", cidade[:15]) +
        _emv("62", _emv("05", txid))
    )

    payload_crc = payload + "6304"
    crc = _crc16(payload_crc)

    return payload_crc + crc
