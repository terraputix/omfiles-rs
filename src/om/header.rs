use crate::compression::CompressionType;

#[repr(C)]
pub struct OmHeader {
    pub magic_number1: u8,
    pub magic_number2: u8,
    pub version: u8,
    pub compression: CompressionType,
    pub scale_factor: f32,
    pub dim0: u64,
    pub dim1: u64,
    pub chunk0: u64,
    pub chunk1: u64,
}

impl OmHeader {
    pub const MAGIC_NUMBER1: u8 = 79;
    pub const MAGIC_NUMBER2: u8 = 77;
    pub const VERSION: u8 = 2;
    pub const LENGTH: usize = 40;

    pub fn as_bytes(self) -> [u8; Self::LENGTH] {
        let mut bytes = [0u8; Self::LENGTH];
        bytes[0] = self.magic_number1;
        bytes[1] = self.magic_number2;
        bytes[2] = self.version;
        bytes[3] = self.compression as u8;

        bytes[4..8].copy_from_slice(&self.scale_factor.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.dim0.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.dim1.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.chunk0.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.chunk1.to_le_bytes());

        bytes
    }
}
