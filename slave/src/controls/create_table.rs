use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use entity_lib::entity::SlaveEntity::SlaveCreate;
use public_function::SLAVE_CONFIG;

pub async fn create_table_controls(create_message: SlaveCreate) -> Result<(), std::io::Error> {
    let log_path = format!(
        "{}\\{}\\{}",
        SLAVE_CONFIG.get("slave.data").unwrap(),
        &create_message.tablename,
        "log"
    );

    tokio::fs::create_dir_all(log_path).await?; // 确保文件夹路径存在

    let compress_path = format!(
        "{}\\{}\\{}",
        SLAVE_CONFIG.get("slave.data").unwrap(),
        &create_message.tablename,
        "compress"
    );

    tokio::fs::create_dir_all(compress_path).await?; // 确保文件夹路径存在

    let partition_metadata = format!(
        "{}\\{}\\{}",
        SLAVE_CONFIG.get("slave.data").unwrap(),
        &create_message.tablename,
        "metadata.log"
    );

    let mut log_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(partition_metadata)
        .await?;

    log_file.write_i64(0).await?;

    Ok(())
}