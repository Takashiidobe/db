pub fn bytes_to_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap())
}

pub fn bytes_to_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().unwrap())
}

pub fn bytes_to_u8(bytes: &[u8]) -> u8 {
    u8::from_le_bytes(bytes.try_into().unwrap())
}
