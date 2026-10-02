def encode_unsigned_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("Unsigned varint cannot encode negative values")

    encoded = bytearray()

    while True:
        byte = value & 0x7F  # take the lowest 7 bits
        value >>= 7

        if value:
            encoded.append(byte | 0x80)  # set the continuation bit
        else:
            encoded.append(byte)
            break

    return bytes(encoded)

def encode_varint(value: int) -> bytes:
    """
    Encodes a signed integer as a Kafka varint
    (ZigZag + unsigned varint).
    """
    # ZigZag encoding
    encoded = (value << 1) ^ (value >> 31)

    return encode_unsigned_varint(encoded)

def read_unsigned_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    
    while True:
        byte = data[offset]
        offset+=1
        
        value |= (byte & 0x7F) << shift
        
        if (byte & 0x80) == 0:
            break
        
        shift+=7
        
        if shift > 28:
            raise ValueError("Malformed varint")
        
    return value, offset


def read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value, offset = read_unsigned_varint(data, offset)
    
    # zigzag decoding
    decoded = (value >> 1) ^ -(value & 1)
    
    return decoded, offset    