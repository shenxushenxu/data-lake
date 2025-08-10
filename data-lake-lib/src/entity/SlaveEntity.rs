use crate::entity::Error::DataLakeError;
use crate::entity::MasterEntity::{ColumnConfigJudgment, DataType, Insert, TableStructure};
use crate::function::vec_trait::VecPutVec;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::fs::File;
use tokio::io::BufWriter;
use crate::entity::bytes_reader::ArrayBytesReader;
use crate::entity::const_property::{I32_BYTE_LEN, I64_BYTE_LEN};

/**
创建 表的 slave 分区的 struct
**/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlaveCreate {
    pub tablename: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryMessage {
    pub tablename: String,
    pub cloums: Vec<String>,
    pub conditions: Option<Vec<(String, String, String)>>,
}
/**
发送给从节点的消息体
**/
#[derive(Serialize, Deserialize, Debug)]
pub enum SlaveMessage {
    create(SlaveCreate),
    query(QueryMessage),
    insert(Insert),
    compress_table(String),
    stream_read(StreamReadStruct),
    batch_insert(TableStructure),
    drop_table(String),
    follower_replicas_sync(ReplicasSyncStruct),
    leader_replicas_sync(SyncMessage),
    max_offset(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncMessage {
    pub offset: i64,
    pub partition_code: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReplicasSyncStruct {
    pub slave_parti_name: String,
    pub leader_address: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReplicaseSyncData {
    pub offset_set: Vec<u8>,
    pub data_set: Vec<u8>,
    pub index_code: String,
}

/**
slave 缓存池 存储的 缓存结构
**/
//#[derive(Serialize, Deserialize, Debug)]
pub struct SlaveCacheStruct {
    pub data_file: File,
    pub index_file: File,
    pub metadata_file: File,
    pub metadata_mmap: MmapMut,
}

/**
index 的结构体
**/

#[derive(Serialize, Deserialize, Debug)]
#[repr(C)]
pub struct IndexStruct {
    pub offset: i64,
    pub start_seek: u64,
    pub end_seek: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataStructure<'a> {
    pub table_name: &'a str,
    pub major_value: &'a str,
    pub data: HashMap<&'a str, &'a str>,
    pub _crud_type: &'a str,
    pub partition_code: i32,
    pub offset: i64,
}

impl<'a> DataStructure<'a> {
    pub fn serialize(&self) -> Result<Vec<u8>, DataLakeError> {
        
        let data_len = self.calculate_serialized_size();
        let mut data = Vec::<u8>::with_capacity(data_len);

        // table_name
        let table_name_bytes = self.table_name.as_bytes();
        let table_name_len = &(table_name_bytes.len() as i32).to_le_bytes();
        data.put_array(table_name_len);
        data.put_array(table_name_bytes);
        // major_value
        let major_value_bytes = self.major_value.as_bytes();
        let major_value_len = &(major_value_bytes.len() as i32).to_le_bytes();
        data.put_array(major_value_len);
        data.put_array(major_value_bytes);
        // data
        let hash_map = &self.data;
        let hash_map_len = &(hash_map.len() as i32).to_le_bytes();
        data.put_array(hash_map_len);
        for (key, value) in hash_map.iter() {
            // key
            let key_bytes = key.as_bytes();
            let key_bytes_len = &(key_bytes.len() as i32).to_le_bytes();
            data.put_array(key_bytes_len);
            data.put_array(key_bytes);
            //value
            let value_bytes = value.as_bytes();
            let value_bytes_len = &(value_bytes.len() as i32).to_le_bytes();
            data.put_array(value_bytes_len);
            data.put_array(value_bytes);
        }

        // _crud_type
        let crud_type_bytes = self._crud_type.as_bytes();
        let crud_type_len = &(crud_type_bytes.len() as i32).to_le_bytes();
        data.put_array(crud_type_len);
        data.put_array(crud_type_bytes);
        // partition_code
        let major_value_bytes = &self.partition_code.to_le_bytes();
        data.put_array(major_value_bytes);
        // offset
        let crud_type_bytes = &self.offset.to_le_bytes();
        data.put_array(crud_type_bytes);

        return Ok(data);
    }
    
    pub fn deserialize(bytes: &[u8]) -> DataStructure{

        let mut arraybytesreader = ArrayBytesReader::new(bytes);
        // table_name
        let table_name_len = arraybytesreader.read_usize();
        let table_name = arraybytesreader.read_str(table_name_len);
        
        //major_value
        let major_value_len = arraybytesreader.read_usize();
        let major_value = arraybytesreader.read_str(major_value_len);
        
        // data
        let data_len = arraybytesreader.read_usize();
        let mut data = HashMap::<&str, &str>::with_capacity(data_len);
        for _ in 0..data_len {
            let key_len = arraybytesreader.read_usize();
            let key = arraybytesreader.read_str(key_len);

            let value_len = arraybytesreader.read_usize();
            let value = arraybytesreader.read_str(value_len);
            data.insert(key, value);
        }
        
        // _crud_type
        let crud_type_len = arraybytesreader.read_usize();
        let crud_type = arraybytesreader.read_str(crud_type_len);
        
        //partition_code
        let partition_code = arraybytesreader.read_i32();
        
        //offset
        let offset = arraybytesreader.read_i64();

        let data_structure =  DataStructure{
            table_name: table_name,
            major_value: major_value,
            data: data,
            _crud_type: crud_type,
            partition_code: partition_code,
            offset: offset
        };
        
        return data_structure;
    }
    
    
    
    

   pub fn calculate_serialized_size(&self) -> usize {
        let mut total = 0;

        // table_name 的数量
        total += I32_BYTE_LEN;
        total += self.table_name.as_bytes().len();

        // major_value
        total += I32_BYTE_LEN;
        total += self.major_value.as_bytes().len();

        // data
        // 元素的行数
        total += I32_BYTE_LEN;
        // 所有行数据
        for (key, value) in self.data.iter() {
            total += I32_BYTE_LEN;
            total += key.as_bytes().len();
            total += I32_BYTE_LEN;
            total += value.as_bytes().len();
        }

        // _crud_type
        total += I32_BYTE_LEN;
        total += self._crud_type.as_bytes().len();
        // pub partition_code: i32,
        total += I32_BYTE_LEN;
        // offset
        total += I64_BYTE_LEN;
        
        
        return total;
    }
    
    
    
    
    
    
    
}

/**
流式读取的 struct
**/
#[derive(Serialize, Deserialize, Debug)]
pub struct StreamReadStruct {
    pub table_name: String,
    pub partition_code: usize,
    pub offset: i64,
    pub read_count: usize,
    pub table_col_type: HashMap<String, (DataType, ColumnConfigJudgment, Option<String>)>,
}
