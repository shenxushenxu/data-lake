use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{Info, PartitionInfo};
use entity_lib::entity::SlaveEntity::SlaveMessage;
use crate::controls::metadata::get_metadata;

pub async fn compress_table(table_name: &String) -> Result<(), DataLakeError> {

    let table_structure =
        get_metadata(&table_name).await?;

    let address_map = &table_structure.partition_address;

    for key in address_map.keys() {

        let partition_info_vec = address_map.get(key).unwrap();
        
        for partition_info in partition_info_vec.iter() {
            
            let address = &partition_info.address;
            
            let mut stream = TcpStream::connect(address).await?;

            let partition_code = format!("{}-{}", table_name, key);
            let message = SlaveMessage::compress_table(partition_code.clone());


            let bytes = bincode::serialize(&message)?;
            let bytes_len = bytes.len();


            stream.write_i32(bytes_len as i32).await?;
            stream.write_all(&bytes).await?;
            
            entity_lib::function::read_error(&mut stream).await?
        }
    }

    return Ok(());
}