use std::mem;
use crate::entity::SlaveEntity::IndexStruct;

// 分区个数
pub const PARTITION_NUMBER:&str = "partition.number";

// 副本个数
pub const REPLICAS_NUMBER:&str = "replicas.number";


// 索引大小
pub const INDEX_SIZE:usize = mem::size_of::<IndexStruct>();