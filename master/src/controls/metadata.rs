use std::collections::HashMap;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::TableStructure;
use memmap2::Mmap;
use std::path::Path;
use tokio::fs::OpenOptions;
use entity_lib::function::MASTER_CONFIG;

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

    let table_structure = get_metadata(table_name).await?;
    
    
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


pub async fn get_metadata(table_name: &str) -> Result<TableStructure, DataLakeError> {


    let table_path = get_table_path(table_name).await?;
    let file = OpenOptions::new().read(true).open(&table_path).await?;

    let mut mmap = unsafe { Mmap::map(&file)? };

    let metadata_message = &mmap[..];
    let table_structure = bincode::deserialize::<TableStructure>(metadata_message)?;
    
   
    
    
    return Ok(table_structure);
    
}

pub async fn get_table_path(table_name: &str) -> Result<String, DataLakeError> {
    let master_config = MASTER_CONFIG.lock().await;
    let data_path_vec = &master_config.master_data_path;

    for data_path in data_path_vec {
        let file_path = format!("{}/{}", data_path, table_name);

        let path = Path::new(&file_path);
        if path.exists() {
            return Ok(file_path);
        }
    }

    return Err(DataLakeError::custom(format!(
        "{} The table does not exist.",
        table_name
    )));
}
