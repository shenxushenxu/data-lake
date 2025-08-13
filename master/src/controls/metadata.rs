use std::collections::HashMap;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::TableStructure;
use memmap2::Mmap;
use std::path::Path;
use tokio::fs::OpenOptions;
use entity_lib::function::MASTER_CONFIG;
use entity_lib::function::table_structure::get_table_structure;

/**
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

**/


pub async fn get_table_metadata(table_name: &str) -> Result<HashMap<String, String>, DataLakeError> {

    let table_structure = get_table_structure(table_name).await?;
    
    
    let mut res_map = HashMap::<String, String>::new();

    let table_name = table_structure.table_name;
    let col_type = table_structure.col_type;
    let partition_address = table_structure.partition_address;
    let partition_number = table_structure.partition_number;
    let major_key = table_structure.major_key;

    for (column, property_all) in col_type {

        let mut property = HashMap::<String,String>::new();

        let datatype = property_all.0;
        let column_config = property_all.1;
        let default_value = property_all.2;

        property.insert(String::from("datatype"), datatype.to_string());
        property.insert(String::from("column_config"), column_config.to_string());

        if let Some(value) = default_value{
            property.insert(String::from("default_value"), value.to_string());
        }

        let column_value = serde_json::to_string(&property)?;

        res_map.insert(column, column_value);
    }
    

    let partition_address = serde_json::to_string(&partition_address)?;

    res_map.insert(String::from("partition_address"), partition_address);
    res_map.insert(String::from("table_name"), table_name);
    res_map.insert(String::from("partition_number"), partition_number.to_string());
    res_map.insert(String::from("major_key"), major_key);
    
    return Ok(res_map);
}



