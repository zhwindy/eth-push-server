from cashaddress.crypto import *
from cashaddress.base58 import b58decode_check, b58encode_check
import sys


class InvalidAddress(Exception):
    pass


class Address:
    VERSION_MAP = {
        'legacy': [
            ('P2SH', 5, False),
            ('P2PKH', 0, False),
            ('P2SH-TESTNET', 196, True),
            ('P2PKH-TESTNET', 111, True)
        ],
        'cash': [
            ('P2SH', 8, False),
            ('P2PKH', 0, False),
            ('P2SH-TESTNET', 8, True),
            ('P2PKH-TESTNET', 0, True)
        ]
    }
    MAINNET_PREFIX = 'bitcoincash'
    TESTNET_PREFIX = 'bchtest'

    def __init__(self, version, payload, prefix=None):
        self.version = version
        self.payload = payload
        if prefix:
            self.prefix = prefix
        else:
            if Address._address_type('cash', self.version)[2]:
                self.prefix = self.TESTNET_PREFIX
            else:
                self.prefix = self.MAINNET_PREFIX

    def __str__(self):
        return 'version: {}\npayload: {}\nprefix: {}'.format(self.version, self.payload, self.prefix)

    def legacy_address(self):
        version_int = Address._address_type('legacy', self.version)[1]
        return b58encode_check(Address.code_list_to_string([version_int] + self.payload))

    def cash_address(self):
        version_int = Address._address_type('cash', self.version)[1]
        payload = [version_int] + self.payload
        payload = convertbits(payload, 8, 5)
        checksum = calculate_checksum(self.prefix, payload)
        return self.prefix + ':' + b32encode(payload + checksum)

    @staticmethod
    def code_list_to_string(code_list):
        if sys.version_info > (3, 0):
            output = bytes()
            for code in code_list:
                output += bytes([code])
        else:
            output = ''
            for code in code_list:
                output += chr(code)
        return output

    @staticmethod
    def _address_type(address_type, version):
        for mapping in Address.VERSION_MAP[address_type]:
            if mapping[0] == version or mapping[1] == version:
                return mapping
        raise InvalidAddress('Could not determine address version')

    @staticmethod
    def from_string(address_string):
        try:
            address_string = str(address_string)
        except Exception:
            raise InvalidAddress('Expected string as input')
        if ':' not in address_string:
            return Address._legacy_string(address_string)
        else:
            return Address._cash_string(address_string)

    @staticmethod
    def _legacy_string(address_string):
        try:
            decoded = bytearray(b58decode_check(address_string))
        except ValueError:
            raise InvalidAddress('Could not decode legacy address')
        version = Address._address_type('legacy', decoded[0])[0]
        payload = list()
        for letter in decoded[1:]:
            payload.append(letter)
        return Address(version, payload)

    @staticmethod
    def _cash_string(address_string):
        if address_string.upper() != address_string and address_string.lower() != address_string:
            raise InvalidAddress('Cash address contains uppercase and lowercase characters')
        address_string = address_string.lower()
        if ':' not in address_string:
            address_string = Address.MAINNET_PREFIX + ':' + address_string
        prefix, base32string = address_string.split(':')
        decoded = b32decode(base32string)
        if not verify_checksum(prefix, decoded):
            raise InvalidAddress('Bad cash address checksum')
        converted = convertbits(decoded, 5, 8)
        version = Address._address_type('cash', converted[0])[0]
        if prefix == Address.TESTNET_PREFIX:
            version += '-TESTNET'
        payload = converted[1:-6]
        return Address(version, payload, prefix)


def to_cash_address(address):
    return Address.from_string(address).cash_address()


def to_legacy_address(address):
    return Address.from_string(address).legacy_address()


def is_valid(address):
    try:
        Address.from_string(address)
        return True
    except InvalidAddress:
        return False


if __name__ == '__main__':
    old_address = '1BpEi6DfDAUFd7GtittLSdBeYJvcoaVggu'
    new_address = 'bitcoincash:qpm2qsznhks23z7629mms6s4cwef74vcwvy22gdx6a'

    old_address_list = ['1AyEgvE2XNM65EkdisywrZZghHuMv1ngf8', '1GTdYMTz13X1TrnvKfBEWLaUrHrPMvfU1a', '1GDfi2b3QD4SZNrbNoFyfx8SBtzBqpCeRm']
    new_address_list = ['qpk4hk3wuxe2uqtqc97n8atzrrr6r5mleczf9sur4h', 'qz5exdzv0yye2ygry545g0fd5v3w8qfkj52g0punj9',
                        'qznw7vygwg02elf7fehecvemsl3fgym7k5lwrtan7z']

    assert to_cash_address(old_address) == new_address
    assert to_legacy_address(new_address) == old_address

    for i in range(len(old_address_list)):
        assert to_cash_address(old_address_list[i]) == ('bitcoincash:' + new_address_list[i])
        assert to_legacy_address(('bitcoincash:' + new_address_list[i])) == old_address_list[i]

    print('转换全部成功!!')

    print(to_legacy_address(new_address))
    print('*' * 20)
    print(to_cash_address(old_address))

