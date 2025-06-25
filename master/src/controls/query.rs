use std::collections::HashMap;
use crate::controls::metadata::get_metadata;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{QueryMessage, SlaveMessage};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use entity_lib::entity::MasterEntity::{Info, PartitionInfo};
use crate::controls::stream_read::data_complete;

pub async fn query_daql(query_message: QueryMessage) -> Result<Option<Vec<String>>, DataLakeError> {
    let table_name = &query_message.tablename;
    let table_structure = get_metadata(table_name).await?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<String>>(100000);

    let arc_table_structure = Arc::new(table_structure);

    let address_map = &arc_table_structure.partition_address;
    for key in address_map.keys() {
        let querymessage = QueryMessage {
            tablename: format!("{}-{}", table_name, key),
            cloums: query_message.cloums.clone(),
            conditions: query_message.conditions.clone(),
        };

        let slave_message = SlaveMessage::query(querymessage);

        let bytes = Arc::new(bincode::serialize(&slave_message)?);
        let bytes_len = bytes.len();

        let partition_info_vec = address_map.get(key).unwrap().clone();

        let se = sender.clone();
        
        let clone_arc_table_structure = Arc::clone(&arc_table_structure);
        tokio::spawn(async move {

            let partition_info = partition_info_vec.iter().filter(|x| {
                match x.info {
                    Info::Leader => {
                        true
                    }
                    Info::Follower => {
                        false
                    }
                }
            }).collect::<Vec<&PartitionInfo>>()[0];
            
            let address = &partition_info.address;
            
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

                            let mut data_vec = Vec::<String>::new();
                            if let Some(data) = data_map {
                                
                                for st in data{
                                    let mut data_map = serde_json::from_str::<HashMap<String, String>>(st.as_str())?;
                                    let col_type = &clone_arc_table_structure.col_type;
                                    let complete_map = data_complete(col_type, &mut data_map).await;
                                    let json_str = serde_json::to_string(complete_map)?;

                                    data_vec.push(json_str);
                                }



                                if let Err(_) = se.send(data_vec).await {
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
