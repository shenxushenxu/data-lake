use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::SlaveCreate;
use entity_lib::function::{SLAVE_CONFIG, check_file_exists};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

pub async fn create_table_controls(create_message: SlaveCreate) -> Result<(), DataLakeError> {
    let slave_data = {
        let mut slave_config = SLAVE_CONFIG.lock().await;
        slave_config.get_slave_data().await
    };

    let log_path = format!("{}/{}/{}", &slave_data, &create_message.tablename, "log");
    if check_file_exists(&log_path) {
        return Err(DataLakeError::custom(format!("{} 已经存在", log_path)));
    }
    tokio::fs::create_dir_all(log_path).await?; // 确保文件夹路径存在

    let compress_path = format!("{}/{}/{}",  &slave_data, &create_message.tablename, "compress");
    if check_file_exists(&compress_path) {
        return Err(DataLakeError::custom(format!("{} 已经存在", compress_path)));
    }
    tokio::fs::create_dir_all(compress_path).await?; // 确保文件夹路径存在

    let partition_metadata = format!(
        "{}/{}/{}",
        &slave_data, &create_message.tablename, "metadata.log"
    );

    let mut metadata_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(partition_metadata)
        .await?;
    let max_offset: i64 = 0;
    let max_offset_bytes = max_offset.to_le_bytes().to_vec();
    metadata_file.write_all(&max_offset_bytes).await?;

    Ok(())
}
