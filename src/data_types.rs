use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum DataType {
    Int8 = 0,
    Uint8 = 1,
    Int16 = 2,
    Uint16 = 3,
    Int32 = 4,
    Uint32 = 5,
    Int64 = 6,
    Uint64 = 7,
    Float = 8,
    Double = 9,
}

impl DataType {
    pub fn bytes_per_element(&self) -> usize {
        match self {
            DataType::Int8 | DataType::Uint8 => 1,
            DataType::Int16 | DataType::Uint16 => 2,
            DataType::Int32 | DataType::Uint32 | DataType::Float => 4,
            DataType::Int64 | DataType::Uint64 | DataType::Double => 8,
        }
    }

    pub fn to_c(&self) -> u32 {
        *self as u32
    }
}
