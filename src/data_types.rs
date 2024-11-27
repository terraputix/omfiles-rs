use omfileformatc_rs::OmDataType_t;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DataType {
    None = 0,
    Int8 = 1,
    Uint8 = 2,
    Int16 = 3,
    Uint16 = 4,
    Int32 = 5,
    Uint32 = 6,
    Int64 = 7,
    Uint64 = 8,
    Float = 9,
    Double = 10,
    String = 11,
    Int8Array = 12,
    Uint8Array = 13,
    Int16Array = 14,
    Uint16Array = 15,
    Int32Array = 16,
    Uint32Array = 17,
    Int64Array = 18,
    Uint64Array = 19,
    FloatArray = 20,
    DoubleArray = 21,
    StringArray = 22,
}

impl DataType {
    pub fn to_c(&self) -> OmDataType_t {
        *self as OmDataType_t
    }
}

impl TryFrom<u8> for DataType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(DataType::None),
            1 => Ok(DataType::Int8),
            2 => Ok(DataType::Uint8),
            3 => Ok(DataType::Int16),
            4 => Ok(DataType::Uint16),
            5 => Ok(DataType::Int32),
            6 => Ok(DataType::Uint32),
            7 => Ok(DataType::Int64),
            8 => Ok(DataType::Uint64),
            9 => Ok(DataType::Float),
            10 => Ok(DataType::Double),
            11 => Ok(DataType::String),
            12 => Ok(DataType::Int8Array),
            13 => Ok(DataType::Uint8Array),
            14 => Ok(DataType::Int16Array),
            15 => Ok(DataType::Uint16Array),
            16 => Ok(DataType::Int32Array),
            17 => Ok(DataType::Uint32Array),
            18 => Ok(DataType::Int64Array),
            19 => Ok(DataType::Uint64Array),
            20 => Ok(DataType::FloatArray),
            21 => Ok(DataType::DoubleArray),
            22 => Ok(DataType::StringArray),
            _ => Err("Invalid data type value"),
        }
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

/// Trait for types that can be stored as arrays in OmFiles
pub trait OmFileArrayDataType {
    const DATA_TYPE_ARRAY: DataType;
}

/// Trait for types that can be stored as scalars in OmFiles
pub trait OmFileScalarDataType: Default {
    const DATA_TYPE_SCALAR: DataType;
}

// Implement both traits for all supported numeric types
impl OmFileArrayDataType for i8 {
    const DATA_TYPE_ARRAY: DataType = DataType::Int8Array;
}
impl OmFileScalarDataType for i8 {
    const DATA_TYPE_SCALAR: DataType = DataType::Int8;
}

impl OmFileArrayDataType for u8 {
    const DATA_TYPE_ARRAY: DataType = DataType::Uint8Array;
}
impl OmFileScalarDataType for u8 {
    const DATA_TYPE_SCALAR: DataType = DataType::Uint8;
}

impl OmFileArrayDataType for i16 {
    const DATA_TYPE_ARRAY: DataType = DataType::Int16Array;
}
impl OmFileScalarDataType for i16 {
    const DATA_TYPE_SCALAR: DataType = DataType::Int16;
}

impl OmFileArrayDataType for u16 {
    const DATA_TYPE_ARRAY: DataType = DataType::Uint16Array;
}
impl OmFileScalarDataType for u16 {
    const DATA_TYPE_SCALAR: DataType = DataType::Uint16;
}

impl OmFileArrayDataType for i32 {
    const DATA_TYPE_ARRAY: DataType = DataType::Int32Array;
}
impl OmFileScalarDataType for i32 {
    const DATA_TYPE_SCALAR: DataType = DataType::Int32;
}

impl OmFileArrayDataType for u32 {
    const DATA_TYPE_ARRAY: DataType = DataType::Uint32Array;
}
impl OmFileScalarDataType for u32 {
    const DATA_TYPE_SCALAR: DataType = DataType::Uint32;
}

impl OmFileArrayDataType for i64 {
    const DATA_TYPE_ARRAY: DataType = DataType::Int64Array;
}
impl OmFileScalarDataType for i64 {
    const DATA_TYPE_SCALAR: DataType = DataType::Int64;
}

impl OmFileArrayDataType for u64 {
    const DATA_TYPE_ARRAY: DataType = DataType::Uint64Array;
}
impl OmFileScalarDataType for u64 {
    const DATA_TYPE_SCALAR: DataType = DataType::Uint64;
}

impl OmFileArrayDataType for f32 {
    const DATA_TYPE_ARRAY: DataType = DataType::FloatArray;
}
impl OmFileScalarDataType for f32 {
    const DATA_TYPE_SCALAR: DataType = DataType::Float;
}

impl OmFileArrayDataType for f64 {
    const DATA_TYPE_ARRAY: DataType = DataType::DoubleArray;
}
impl OmFileScalarDataType for f64 {
    const DATA_TYPE_SCALAR: DataType = DataType::Double;
}
