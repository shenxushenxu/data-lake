use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::SlaveCreate;
use entity_lib::function::SLAVE_CONFIG;

pub async fn create_table_controls(create_message: SlaveCreate) -> Result<(), DataLakeError> {
    
    let slave_data = {
        let mut slave_config = SLAVE_CONFIG.lock().await;
        slave_config.get_slave_data().await
    };
    
    
    let log_path = format!(
        "{}/{}/{}",
        &slave_data,
        &create_message.tablename,
        "log"
    );

    tokio::fs::create_dir_all(log_path).await?; // 确保文件夹路径存在

    let compress_path = format!(
        "{}/{}/{}",
        &slave_data,
        &create_message.tablename,
        "compress"
    );

    tokio::fs::create_dir_all(compress_path).await?; // 确保文件夹路径存在

    let partition_metadata = format!(
        "{}/{}/{}",
        &slave_data,
        &create_message.tablename,
        "metadata.log"
    );

    let mut metadata_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(partition_metadata)
        .await?;

    metadata_file.write_i64(0).await?;

    Ok(())
}