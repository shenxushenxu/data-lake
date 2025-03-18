use core::fmt::{Debug, Display};
use std::io::Error;
use std::num::{ParseFloatError, ParseIntError};
use std::str::{ParseBoolError, Utf8Error};
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum DataLakeError {
    IoError(std::io::Error),
    CustomError(String),
    BincodeError(bincode::Error),
    SerderErr(serde_json::Error),
    ParseBoolError(ParseBoolError),
    ParseI32Error(ParseIntError),
    ParseFloatError(ParseFloatError),
    FromUtf8Error(FromUtf8Error),
    Utf8Error(Utf8Error),
    SnapError(snap::Error)
}

impl Display for DataLakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataLakeError::IoError(e) => write!(f, "Io 错误: {}", e),
            DataLakeError::CustomError(e) => write!(f, "{}", e),
            DataLakeError::BincodeError(e) => write!(f, "bincode序列化数据错误: {}", e),
            DataLakeError::SerderErr(e) => write!(f, "serder 序列化数据错误: {}", e),
            DataLakeError::ParseBoolError(e) => write!(f, "bool 类型转换错误: {}", e),
            DataLakeError::ParseI32Error(e) => write!(f, "int 类型转换错误: {}", e),
            DataLakeError::ParseFloatError(e) => write!(f, "float 类型转换错误: {}", e),
            DataLakeError::FromUtf8Error(e) => write!(f, "string 类型转换错误: {}", e),
            DataLakeError::Utf8Error(e) => write!(f, "string 类型转换错误: {}", e),
            DataLakeError::SnapError(e) => write!(f, "snappy 压缩算法出错: {}", e),
        }
    }
}

impl From<snap::Error> for DataLakeError {
    fn from(value: snap::Error) -> Self {
        DataLakeError::SnapError(value)
    }
}

impl std::error::Error for DataLakeError {}

impl From<std::io::Error> for DataLakeError {
    fn from(value: Error) -> Self {
        DataLakeError::IoError(value)
    }
}
impl From<bincode::Error> for DataLakeError {
    fn from(value: bincode::Error) -> Self {
        DataLakeError::BincodeError(value)
    }
}
impl From<serde_json::Error> for DataLakeError {
    fn from(value: serde_json::Error) -> Self {
        DataLakeError::SerderErr(value)
    }
}
impl From<ParseBoolError> for DataLakeError {
    fn from(value: ParseBoolError) -> Self {
        DataLakeError::ParseBoolError(value)
    }
}
impl From<ParseIntError> for DataLakeError {
    fn from(value: ParseIntError) -> Self {
        DataLakeError::ParseI32Error(value)
    }
}
impl From<ParseFloatError> for DataLakeError {
    fn from(value: ParseFloatError) -> Self {
        DataLakeError::ParseFloatError(value)
    }
}
impl From<FromUtf8Error> for DataLakeError {
    fn from(value: FromUtf8Error) -> Self {
        DataLakeError::FromUtf8Error(value)
    }
}
impl From<Utf8Error> for DataLakeError {
    fn from(value: Utf8Error) -> Self {
        DataLakeError::Utf8Error(value)
    }
}
