use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::TableStructure;
use memmap2::Mmap;
use public_function::MASTER_CONFIG;
use std::path::Path;
use tokio::fs::OpenOptions;

pub async fn get_metadata(table_name: &str) -> Result<TableStructure, DataLakeError> {


    let table_path = get_table_path(table_name).await?;
    let mut file = OpenOptions::new().read(true).open(&table_path).await?;

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

    return Err(DataLakeError::CustomError(format!(
        "{} The table does not exist.",
        table_name
    )));
}
