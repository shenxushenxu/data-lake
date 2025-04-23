use crate::controls::metadata::get_metadata;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::SlaveMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use public_function::{MASTER_CONFIG};

pub async fn drop_table_operation(table_name: &String) -> Result<(), DataLakeError> {
    let tablestructure = get_metadata(table_name).await?;

    let partition_address = &tablestructure.partition_address;

    for (code, address) in partition_address.iter() {

        let slave_table_name = format!("{}-{}", table_name, code);

        tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await?;

            let slave_message = SlaveMessage::drop_table(slave_table_name);

            let slave_byes = bincode::serialize(&slave_message)?;

            stream.write_i32(slave_byes.len() as i32).await?;
            stream.write_all(slave_byes.as_slice()).await?;

            loop {
                match stream.read_i32().await{
                    Ok(len) => {
                        if len == -1 {
                            break;
                        }else if len == -2 {
                            let bytes_len = stream.read_i32().await?;
                            let mut mess = vec![0u8; bytes_len as usize];
                            stream.read_exact(&mut mess).await?;
                            return Err(DataLakeError::CustomError(String::from_utf8(mess)?));
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
            Ok(())
        });
    }

    let master_data = MASTER_CONFIG.get("master.data.path").unwrap();
    let path = format!("{}\\{}",master_data, table_name);
    tokio::fs::remove_dir_all(path).await?;

    Ok(())
}
