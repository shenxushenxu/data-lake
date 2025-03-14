use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use entity_lib::entity::MasterEntity::{BatchInsert, Create, MasterStreamRead, MesterInsert};


#[derive(Serialize, Deserialize, Debug)]
pub enum ClientStatement {
    create(Create),
    query(String),
    insert(MesterInsert),
    metadata(String),
    compress_table(String),
    stream_read(MasterStreamRead),
    batch_insert(BatchInsert),
}