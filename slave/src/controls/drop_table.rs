use entity_lib::entity::Error::DataLakeError;
use public_function::read_function::get_slave_path;

pub async fn drop_table_operation(table_name: &String) -> Result<(), DataLakeError>{
    let data_path = get_slave_path(table_name).await?;
    
    tokio::fs::remove_dir_all(data_path).await?;
    return Ok(());
}