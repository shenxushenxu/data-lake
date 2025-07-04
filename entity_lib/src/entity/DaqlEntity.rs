use serde::{Deserialize, Serialize};
use crate::entity::MasterEntity::TableStructure;
use crate::entity::SlaveEntity::QueryMessage;

#[derive(Serialize, Deserialize, Debug)]
pub enum DaqlType{
    CREATE_TABLE(TableStructure),
    SELECT_TABLE(QueryMessage),
    ALTER_OROP((String, String)),
    ALTER_ADD((String, String, String, String, String)),
    SHOW_TABLE(String),
    COMPRESS_TABLE(String),
    DROP_TABLE(String),
    MAX_OFFSET(String)
}