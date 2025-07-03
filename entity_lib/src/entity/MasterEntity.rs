use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataType {
    string,
    int,
    float,
    boolean,
    long,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ColumnConfigJudgment {
    PRIMARY_KEY,
    NOT_NULL,
    DEFAULT,
    NOT
}


/*
create 语句的 struct 类
*/
#[derive(Serialize, Deserialize, Debug)]
pub struct Create {
    // 表名
    pub table_name: String,
    // 表的列名和对应的类型
    pub col_type: HashMap<String, DataType>,
    // 分区数
    pub partition_number: usize,
    // 主键的名字
    pub major_key: String,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MesterInsert{
    pub table_name: String,
    pub _crud_type: String,
    pub data: HashMap<String, String>,

}


/**
insert 语句的 struct 类
**/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Insert {
    pub table_name: String,
    pub major_key: String,
    pub data: String,
    pub _crud_type: String,
    pub address: String,
    pub partition_code: String,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlaveBatchData{
    pub major_key: String,
    pub data: String,
    pub _crud_type: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlaveInsert {
    pub table_name: String,
    pub data: Vec<HashMap<String, String>>,
    pub partition_code: String,
    pub table_structure: TableStructure
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BatchInsertTruth {
    pub table_name: String,
    pub data: Vec<HashMap<String, String>>,
    pub partition_code: Option<String>,
}

/**
批量插入 的 struct
**/
#[derive(Serialize, Deserialize, Debug)]
pub struct BatchInsert{
    pub table_name: String,
    pub data: Vec<u8>,
    // 批量插入数据指定的分区号可以为None
    pub partition_code: Option<String>,
}

/**
语句的 枚举类
**/
#[derive(Serialize, Deserialize, Debug)]
pub enum Statement {
    // create(Create),
    // insert(MesterInsert),
    // metadata(String),
    // compress_table(String),
    sql(String),
    stream_read(MasterStreamRead),
    batch_insert(BatchInsert)
}

/**
流式读取的struct类
**/
#[derive(Serialize, Deserialize, Debug)]
pub struct MasterStreamRead {
    pub table_name: String,
    pub patition_mess: Vec<Parti>,
    pub read_count: usize,
}
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct Parti {
    pub patition_code: usize,
    pub offset: i64,
}


/**
区分分区的副本的活跃状态和等待状态
**/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Info{
    Leader,
    Follower
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PartitionInfo{
    pub address: String,
    pub info: Info,
}


/**
表结构的 struct
**/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableStructure {
    // 表名
    pub table_name: String,
    // 表的列名和对应的类型
    pub col_type: HashMap::<String, (DataType, ColumnConfigJudgment, Option<String>)>,
    // 分区编号对应的slave 地址
    pub partition_address: HashMap<usize, Vec<PartitionInfo>>,
    // 分区个数
    pub partition_number: usize,
    // 主键的名字
    pub major_key: String,
}

/**
查询的结构
**/
#[derive(Debug, Serialize, Deserialize)]
pub enum QueryItem {
    table(String),
    column(Vec<String>),
    term(HashMap<String, serde_json::Value>),
    gt(HashMap<String, serde_json::Value>),
    lt(HashMap<String, serde_json::Value>),
}
