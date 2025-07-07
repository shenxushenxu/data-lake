use std::mem;
use crate::entity::SlaveEntity::IndexStruct;

// 分区个数
pub const PARTITION_NUMBER:&str = "partition.number";

// 副本个数
pub const REPLICAS_NUMBER:&str = "replicas.number";

pub const CRUD_TYPE:&str = "_crud_type";
pub const CURD_INSERT:&str = "insert";
pub const CURD_DELETE:&str = "delete";
// 索引大小
pub const INDEX_SIZE:usize = mem::size_of::<IndexStruct>();

// 合并压缩数据的 文件夹名称
pub const COMPRESS_FILE:&str = "compress";

// 未合并文件的文件夹名称
pub const LOG_FILE:&str = "log";

//数据文件的后缀名
pub const DATA_FILE_EXTENSION:&str = ".snappy";

// 索引文件的后缀名
pub const INDEX_FILE_EXTENSION:&str = ".index";

// 明文文件的后缀名称
pub const PLAINTEXT_FILE_EXTENSION:&str = ".log";

// 存储最大offset的文件
pub const METADATA_LOG:&str = "metadata.log";


// i32 的字节长度
pub const I32_BYTE_LEN:usize = mem::size_of::<i32>();