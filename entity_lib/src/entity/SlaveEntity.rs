use std::collections::HashMap;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use crate::entity::MasterEntity::{ColumnConfigJudgment, DataType, Insert, QueryItem, SlaveInsert};

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
    batch_insert,
    drop_table(String),
    follower_replicas_sync(ReplicasSyncStruct),
    leader_replicas_sync(SyncMessage),
    max_offset(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncMessage{
    pub offset: i64,
    pub partition_code: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReplicasSyncStruct{
    pub slave_parti_name: String,
    pub leader_address: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReplicaseSyncData{
    pub offset_set: Vec<u8>,
    pub data_set: Vec<u8>,
    pub index_code: String,
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

impl SlaveCacheStruct{
    pub fn get_data_file<'a>(&'a mut self) -> &'a mut File{
        return &mut self.data_file;
    }
    pub fn get_index_file<'a>(&'a mut self) -> &'a mut File{
        return &mut self.index_file;
    }
    pub fn get_metadata_mmap<'a>(&'a mut self) -> &'a mut MmapMut{
        return &mut self.metadata_mmap;
    }
}


/**
index 的结构体
**/

#[derive(Serialize,Deserialize, Debug)]
pub struct IndexStruct{
    pub offset: i64,
    pub start_seek: u64,
    pub end_seek: u64,
}

#[derive(Serialize, Debug)]
pub struct IndexStructSerialize<'a>{
    pub offset: &'a i64,
    pub start_seek: u64,
    pub end_seek: u64,
}

/**
数据落文件的  结构体
**/
#[derive(Serialize, Debug)]
pub struct DataStructureSerialize<'a> {
    pub table_name: &'a str,
    pub major_value: &'a str,
    pub data: &'a HashMap<&'a str, &'a str>,
    pub _crud_type: &'a str,
    pub partition_code: &'a str,
    pub offset: &'a i64
}


#[derive(Serialize, Deserialize, Debug)]
pub struct DataStructure<'a> {
    pub table_name: &'a str,
    pub major_value: &'a str,
    pub data: HashMap<&'a str, &'a str>,
    pub _crud_type: &'a str,
    pub partition_code: &'a str,
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
    pub table_col_type: HashMap<String, (DataType, ColumnConfigJudgment, Option<String>)>,
}