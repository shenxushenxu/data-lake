use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::TableStructure;
use entity_lib::entity::SlaveEntity::{SlaveCreate, SlaveMessage};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use entity_lib::function::MASTER_CONFIG;

pub async fn create_table(table_structure: TableStructure) -> Result<(), DataLakeError> {
    let mut master_config = MASTER_CONFIG.lock().await;
    let path_vec = &master_config.master_data_path;

    for data_path in path_vec {
        let file_path = format!(
            "{}/{}",
            data_path,
            &table_structure.table_name
        );

        if entity_lib::function::check_file_exists(&file_path) {
            return Err(DataLakeError::custom(format!(
                "{} The table already exists.",
                table_structure.table_name
            )));
        }
    }

    let data_path = master_config.get_master_data_path().await;
    let file_path = format!(
        "{}/{}",
        data_path,
        &table_structure.table_name
    );
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&file_path)
        .await?;
    let bincode_table_structure = bincode::serialize(&table_structure)?;
    file.write_all(&bincode_table_structure).await?;
    
    
    let partition_address = &table_structure.partition_address;
    for (code, partition_info_vec) in partition_address.iter() {
        let slavecreate = SlaveCreate {
            tablename: format!("{}-{}", &table_structure.table_name, &code),
        };
        let slave_message_create = SlaveMessage::create(slavecreate);
        
        for partition_info in partition_info_vec.iter() {
            
            let address = &partition_info.address;
            
            let mut stream = TcpStream::connect(address).await?;

            let slave = bincode::serialize(&slave_message_create)?;
            stream.write_i32(slave.len() as i32).await?;
            stream.write_all(slave.as_slice()).await?;
            
            let massage_len = stream.read_i32().await?;
            if massage_len == -2 {
                let mut massage = vec![0u8; massage_len as usize];
                stream.read_exact(massage.as_mut_slice()).await?;
                return Err(DataLakeError::custom(String::from_utf8(massage)?));
            }
        }
    }

    

    
    return Ok(());
}
