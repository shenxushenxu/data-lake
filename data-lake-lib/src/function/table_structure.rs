use std::path::Path;
use memmap2::Mmap;
use tokio::fs::OpenOptions;
use crate::entity::Error::DataLakeError;
use crate::entity::MasterEntity::TableStructure;
use crate::function::{check_file_exists, MASTER_CONFIG};

pub async fn get_table_structure(table_name: &str) -> Result<TableStructure, DataLakeError> {


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

        if check_file_exists(&file_path) {
            return Ok(file_path);
        }
    }

    return Err(DataLakeError::custom(format!(
        "{} The table does not exist.",
        table_name
    )));
}