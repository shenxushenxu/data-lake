use memmap2::Mmap;
use tokio::fs::OpenOptions;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::TableStructure;
use public_function::MASTER_CONFIG;

pub async fn get_metadata(table_name: &str) -> Result<TableStructure, DataLakeError> {
    let file_path = format!(
        "{}\\{}",
        MASTER_CONFIG.get("master.data.path").unwrap(),
        table_name);
    match std::fs::metadata(&file_path) {
        Ok(_) => {
            let mut file = OpenOptions::new()
                .read(true)
                .open(file_path)
                .await?;

            let mut mmap = unsafe{Mmap::map(&file)?};

            let metadata_message = &mmap[..];
            let table_structure = bincode::deserialize::<TableStructure>(metadata_message)?;
            return Ok(table_structure);
        }
        Err(_) => {
            return Err(DataLakeError::CustomError(format!("{} 表不存在", table_name)))
        }
    }
}