use entity_lib::entity::Error::DataLakeError;
use public_function::SLAVE_CONFIG;

pub async fn drop_table_operation(table_name: &String) -> Result<(), DataLakeError>{
    let slave_data = SLAVE_CONFIG.get("slave.data").unwrap();
    let path = format!("{}\\{}",slave_data, table_name);
    tokio::fs::remove_dir_all(path).await?;
    return Ok(());
}