from web3 import Web3


def to_hex(b):
    return "0x" + b.hex()


def to_checksum_address(b):
    return Web3.to_checksum_address(b)
