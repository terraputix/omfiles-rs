use omfileformatc_rs::OmDataType_t;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
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

    pub fn to_c(&self) -> OmDataType_t {
        *self as OmDataType_t
    }
}
pub trait OmFileDataType {
    const DATA_TYPE: DataType;
}

impl OmFileDataType for i8 {
    const DATA_TYPE: DataType = DataType::Int8;
}

impl OmFileDataType for u8 {
    const DATA_TYPE: DataType = DataType::Uint8;
}

impl OmFileDataType for i16 {
    const DATA_TYPE: DataType = DataType::Int16;
}

impl OmFileDataType for u16 {
    const DATA_TYPE: DataType = DataType::Uint16;
}

impl OmFileDataType for i32 {
    const DATA_TYPE: DataType = DataType::Int32;
}

impl OmFileDataType for u32 {
    const DATA_TYPE: DataType = DataType::Uint32;
}

impl OmFileDataType for i64 {
    const DATA_TYPE: DataType = DataType::Int64;
}

impl OmFileDataType for u64 {
    const DATA_TYPE: DataType = DataType::Uint64;
}

impl OmFileDataType for f32 {
    const DATA_TYPE: DataType = DataType::Float;
}

impl OmFileDataType for f64 {
    const DATA_TYPE: DataType = DataType::Double;
}
