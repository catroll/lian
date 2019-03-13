ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
ALPHABET_SIZE = len(ALPHABET)


# -----------------------------------------------------------------------------
def bytes_to_int(bytes):
    # int.from_bytes(bytes, byteorder, *, signed=False)
    result = 0
    for b in bytes:
        result = result * 256 + int(b)
    return result


def int_to_bytes(value, length):
    # int.to_bytes(length, byteorder, *, signed=False)
    result = []
    for i in range(0, length):
        result.append(value >> (i * 8) & 0xff)
    result.reverse()
    return result
# -----------------------------------------------------------------------------


def encode_int(number):
    if number < ALPHABET_SIZE:
        return ALPHABET[number]
    s = list()
    while number > 0:
        number, n = divmod(number, ALPHABET_SIZE)
        s.append(ALPHABET[n])
    s.reverse()
    return ''.join(s)


def decode_int(encoded):
    result = 0
    encoded_len = len(encoded)
    for i, char in enumerate(encoded):
        result += ALPHABET.index(char) * ALPHABET_SIZE ** (encoded_len - (i + 1))
    return result


def encode_bytes(b):
    _int = int.from_bytes(b, 'big')
    return encode_int(_int)


def decode_bytes(encoded):
    _int = decode_int(encoded)
    _bytes = bytearray()
    while _int > 0:
        _bytes.append(_int & 0xff)
        _int //= 256
    _bytes.reverse()
    return bytes(_bytes)


def encode(u):
    _bytes = u.encode('utf-8')
    return encode_bytes(_bytes)


def decode(encoded):
    _bytes = decode_bytes(encoded)
    return _bytes.decode('utf-8')


def __test():
    s = '我是中国人，我爱中国'
    print('original: ' + s)
    encoded = encode(s)
    print(' encoded: ' + encoded)
    decoded = decode(encoded)
    print(' decoded: ' + decoded)
    assert decoded == s, '---> Error!!!'
    print('---> OK')


if __name__ == "__main__":
    __test()
