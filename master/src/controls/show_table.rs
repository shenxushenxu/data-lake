use entity_lib::entity::Error::DataLakeError;
use entity_lib::function::MASTER_CONFIG;

pub async fn show_table() -> Result<Vec<String>, DataLakeError> {
    let master_config = MASTER_CONFIG.lock().await;
    let data_path_vec = &master_config.master_data_path;
    
    let mut vec_table_name = Vec::<String>::new();
    for data_path in data_path_vec {
        let mut folder_entries = tokio::fs::read_dir(data_path).await?;
        // 遍历条目并打印文件名
        while let Some(folder_entry) = folder_entries.next_entry().await? {
            let folder_entry_path = folder_entry.path(); // 获取条目路径
            if !folder_entry_path.is_file() {
                let table_name = folder_entry_path.file_name().unwrap().to_str().unwrap().to_owned();
                vec_table_name.push(table_name);
            }
        }
    }
    
    return Ok(vec_table_name);
}