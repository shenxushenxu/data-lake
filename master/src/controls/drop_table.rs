use crate::controls::metadata::{get_metadata, get_table_path};
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::SlaveMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn drop_table_operation(table_name: &String) -> Result<(), DataLakeError> {
    let tablestructure = get_metadata(table_name).await?;

    

    let path = get_table_path(table_name).await?;
    
    tokio::fs::remove_file(path).await?;


    let partition_address = tablestructure.partition_address;
    
    for (code, partition_info_vec) in partition_address {
        let slave_table_name = format!("{}-{}", table_name, code);

        for partition_info in partition_info_vec {
            let address = &partition_info.address;

            let mut stream = TcpStream::connect(address).await?;

            let slave_message = SlaveMessage::drop_table(slave_table_name.clone());

            let slave_byes = bincode::serialize(&slave_message)?;

            stream.write_i32(slave_byes.len() as i32).await?;
            stream.write_all(slave_byes.as_slice()).await?;
            
            loop {
                match stream.read_i32().await {
                    Ok(len) => {
                        if len == -1 {
                            break;
                        } else if len == -2 {
                            let bytes_len = stream.read_i32().await?;
                            let mut mess = vec![0u8; bytes_len as usize];
                            stream.read_exact(&mut mess).await?;
                            return Err(DataLakeError::custom(String::from_utf8(mess)?));
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }
       
    }

    

    return Ok(());
}
