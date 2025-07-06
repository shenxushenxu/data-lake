use core::fmt::{Debug, Display};
use std::io::Error;
use std::num::{ParseFloatError, ParseIntError};
use std::panic::Location;
use std::str::{ParseBoolError, Utf8Error};
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum DataLakeError {
    IoError {
        source: std::io::Error,
        location: &'static Location<'static>,
    },
    CustomError {
        source: String,
        location: &'static Location<'static>,
    },
    BincodeError {
        source: bincode::Error,
        location: &'static Location<'static>,
    },
    SerderErr {
        source: serde_json::Error,
        location: &'static Location<'static>,
    },
    ParseBoolError {
        source: ParseBoolError,
        location: &'static Location<'static>,
    },
    ParseI32Error {
        source: ParseIntError,
        location: &'static Location<'static>,
    },
    ParseFloatError {
        source: ParseFloatError,
        location: &'static Location<'static>,
    },
    FromUtf8Error {
        source: FromUtf8Error,
        location: &'static Location<'static>,
    },
    Utf8Error {
        source: Utf8Error,
        location: &'static Location<'static>,
    },
    SnapError {
        source: snap::Error,
        location: &'static Location<'static>,
    },
}

impl Display for DataLakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataLakeError::IoError { source, location } => write!(
                f,
                "Io 错误: {}:{}:{}",
                location.file(),
                location.line(),
                source
            ),
            DataLakeError::CustomError { source, location } => {
                write!(f, "{}:{}:{}", location.file(), location.line(), source)
            }
            DataLakeError::BincodeError { source, location } => write!(
                f,
                "{}:{}:bincode序列化数据错误:{}",
                location.file(),
                location.line(),
                source
            ),
            DataLakeError::SerderErr { source, location } => write!(
                f,
                "{}:{}:serder 序列化数据错误: {}",
                location.file(),
                location.line(),
                source
            ),
            DataLakeError::ParseBoolError { source, location } => write!(
                f,
                "{}:{}: bool 类型转换错误: {}",
                location.file(),
                location.line(),
                source
            ),
            DataLakeError::ParseI32Error { source, location } => write!(
                f,
                "{}:{}: int 类型转换错误: {}",
                location.file(),
                location.line(),
                source
            ),
            DataLakeError::ParseFloatError { source, location } => write!(
                f,
                "{}:{}: float 类型转换错误: {}",
                location.file(),
                location.line(),
                source
            ),
            DataLakeError::FromUtf8Error { source, location } => write!(
                f,
                "{}:{}: string 类型转换错误: {}",
                location.file(),
                location.line(),
                source
            ),
            DataLakeError::Utf8Error { source, location } => write!(
                f,
                "{}:{}: string 类型转换错误: {}",
                location.file(),
                location.line(),
                source
            ),
            DataLakeError::SnapError { source, location } => write!(
                f,
                "{}:{}: snappy 压缩算法出错: {}",
                location.file(),
                location.line(),
                source
            ),
        }
    }
}


impl DataLakeError{
    #[track_caller]
    pub fn custom(value: String) -> DataLakeError{
        DataLakeError::CustomError {
            source: value,
            location: Location::caller()
        }
    }
}


impl From<snap::Error> for DataLakeError {
    #[track_caller]
    fn from(value: snap::Error) -> Self {
        DataLakeError::SnapError { 
            source: value,
            location: Location::caller()
        }
    }
}

impl std::error::Error for DataLakeError {}

impl From<std::io::Error> for DataLakeError {
    #[track_caller]
    fn from(value: Error) -> Self {
        DataLakeError::IoError{
            source: value,
            location: Location::caller()
        }
    }
}
impl From<bincode::Error> for DataLakeError {
    #[track_caller]
    fn from(value: bincode::Error) -> Self {
        DataLakeError::BincodeError{
            source: value,
            location: Location::caller()
        }
    }
}
impl From<serde_json::Error> for DataLakeError {
    #[track_caller]
    fn from(value: serde_json::Error) -> Self {
        DataLakeError::SerderErr{
            source: value,
            location: Location::caller()
        }
    }
}
impl From<ParseBoolError> for DataLakeError {
    #[track_caller]
    fn from(value: ParseBoolError) -> Self {
        DataLakeError::ParseBoolError{
            source: value,
            location: Location::caller()
        }
    }
}
impl From<ParseIntError> for DataLakeError {
    #[track_caller]
    fn from(value: ParseIntError) -> Self {
        DataLakeError::ParseI32Error{
            source: value,
            location: Location::caller()
        }
    }
}
impl From<ParseFloatError> for DataLakeError {
    #[track_caller]
    fn from(value: ParseFloatError) -> Self {
        DataLakeError::ParseFloatError{
            source: value,
            location: Location::caller()
        }
    }
}
impl From<FromUtf8Error> for DataLakeError {
    #[track_caller]
    fn from(value: FromUtf8Error) -> Self {
        DataLakeError::FromUtf8Error{
            source: value,
            location: Location::caller()
        }
    }
}
impl From<Utf8Error> for DataLakeError {
    #[track_caller]
    fn from(value: Utf8Error) -> Self {
        DataLakeError::Utf8Error{
            source: value,
            location: Location::caller()
        }
    }
}
