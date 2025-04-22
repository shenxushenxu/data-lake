use crate::controls::metadata::get_metadata;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{QueryMessage, SlaveMessage};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn query_daql(query_message: QueryMessage) -> Result<Option<Vec<String>>, DataLakeError> {
    let table_name = &query_message.tablename;
    let table_structure = get_metadata(table_name).await?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<String>>(100000);

    let address_map = &table_structure.partition_address;
    for key in address_map.keys() {
        let querymessage = QueryMessage {
            tablename: format!("{}-{}", table_name, key),
            cloums: query_message.cloums.clone(),
            conditions: query_message.conditions.clone(),
        };

        let slave_message = SlaveMessage::query(querymessage);

        let bytes = Arc::new(bincode::serialize(&slave_message)?);
        let bytes_len = bytes.len();

        let address = address_map.get(key).unwrap().clone();

        let se = sender.clone();

        tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await?;

            stream.write_i32(bytes_len.clone() as i32).await?;
            stream.write_all(&bytes).await?;

            loop {
                match stream.read_i32().await {
                    Ok(mess_len) => {
                        if mess_len == -1 {
                            break;
                        } else if mess_len == -2 {
                            let len = stream.read_i32().await?;

                            let mut mess = vec![0u8; len as usize];
                            stream.read_exact(&mut mess).await?;
                            let dd = String::from_utf8(mess)?;
                            return Err(DataLakeError::CustomError(dd));
                        } else {
                            let mut mess = vec![0u8; mess_len as usize];
                            stream.read_exact(&mut mess).await?;
                            let data_map = bincode::deserialize::<Option<Vec<String>>>(&mess)?;

                            if let Some(data) = data_map {
                                if let Err(_) = se.send(data).await {
                                    println!("receiver dropped");
                                    break;
                                }
                            }
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
    drop(sender);
    let mut res_vec = Vec::<String>::new();

    while let Some(mut data_map) = receiver.recv().await {
        res_vec.append(&mut data_map);
    }

    if res_vec.len() > 0 {
        return Ok(Some(res_vec));
    } else {
        return Ok(None);
    }
}
