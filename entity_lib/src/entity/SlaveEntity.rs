use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use crate::entity::MasterEntity::{Insert, QueryItem, SlaveInsert};

/**
创建 表的 slave 分区的 struct
**/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlaveCreate {
    pub tablename: String,
}





#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryMessage{
    pub tablename: String,
    pub cloums: Vec<String>,
    pub conditions: Option<Vec<(String, String, String)>>,
}
/**
发送给从节点的消息体
**/
#[derive(Serialize, Deserialize, Debug)]
pub enum SlaveMessage{
    create(SlaveCreate),
    query(QueryMessage),
    insert(Insert),
    compress_table(String),
    stream_read(StreamReadStruct),
    batch_insert(SlaveInsert),
}




/**
slave 缓存池 存储的 缓存结构
**/
//#[derive(Serialize, Deserialize, Debug)]
pub struct SlaveCacheStruct{
    pub data_file: File,
    pub index_file: File,
    pub metadata_file: File,
    pub metadata_mmap: MmapMut,
}


/**
index 的结构体
**/
#[derive(Serialize, Deserialize, Debug)]
pub struct IndexStruct{
    pub offset:i64,
    pub start_seek: u64,
    pub end_seek: u64,
}

/**
数据落文件的  结构体
**/
#[derive(Serialize, Deserialize, Debug)]
pub struct DataStructure {
    pub table_name: String,
    pub major_key: String,
    pub data: String,
    pub crud: String,
    pub partition_code: String,
    pub offset: i64
}




/**
流式读取的 struct
**/
#[derive(Serialize, Deserialize, Debug)]
pub struct StreamReadStruct{
    pub table_name: String,
    pub partition_code: usize,
    pub offset: i64,
    pub read_count: usize,
}