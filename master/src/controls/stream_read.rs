use crate::controls::metadata::get_metadata;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{Info, MasterStreamRead, Parti, PartitionInfo, TableStructure};
use entity_lib::entity::SlaveEntity::{SlaveMessage, StreamReadStruct};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinError;
use public_function::BufferObject::STREAM_TCP_TABLESTRUCTURE;

pub async fn stream_read_data(
    masterstreamread: MasterStreamRead,
    uuid: &String,
) -> Result<Option<Vec<Vec<u8>>>, DataLakeError> {
    
    let table_name = masterstreamread.table_name;
    let read_count = masterstreamread.read_count;
    let table_structure = get_metadata(&table_name).await?;

    let address_map = &table_structure.partition_address;
    
    let mut patiti_vec = masterstreamread.patition_mess;

        for key in address_map.keys() {
            let mut is = true;
            
            for p_v in patiti_vec.iter() {
                if p_v.patition_code == *key{
                     is = false;        
                }
            }
            if is {
                let par = Parti {
                    patition_code: key.clone(),
                    offset: 0,
                };
                patiti_vec.push(par);
            }
        }
    

    let arc_table_name = Arc::new(table_name);
    
    let mut join_handle_set = tokio::task::JoinSet::new();
    
    for parti in patiti_vec.iter() {
        let partition_code = parti.patition_code;
        let offset = parti.offset;

        let partition_info_vec = address_map.get(&partition_code).unwrap();
        
        let partition_info = partition_info_vec.iter().filter(|x| {
            match x.info {
                Info::Leader => {
                    true
                }
                Info::Follower => {
                    false
                }
            }
        }).collect::<Vec<&PartitionInfo>>()[0].clone();

        let table_name_clone = table_structure.table_name.clone();

        let arc_ta_name = Arc::clone(&arc_table_name);

        let map_key = format!("{}_{}_{}", uuid, arc_ta_name.as_ref(), partition_code);

        let stream_tcp_tablestructure = Arc::clone(&STREAM_TCP_TABLESTRUCTURE);
        join_handle_set.spawn(async move {
            
            
            let ref_mutex = match stream_tcp_tablestructure.contains_key(&map_key) {
                true => {
                    Arc::clone(stream_tcp_tablestructure.get(&map_key).unwrap().value())
                },
                false => {
                    let address = &partition_info.address;
                    let mut stream = TcpStream::connect(address).await?;
                    
                    let tablestructure = get_metadata(arc_ta_name.as_ref()).await?;

                    let ref_value = stream_tcp_tablestructure.entry(map_key.clone()).or_insert_with(|| {
                        Arc::new(Mutex::new((stream, tablestructure)))
                    });
                    
                    Arc::clone(ref_value.value())
                }
            };

            // let mutex = ref_mutex.value();
            let mut mutex_guard = ref_mutex.lock().await;
            
            let col_type = {
                let tablestructure = &mutex_guard.1;
                tablestructure.col_type.clone()
            };
            
            let stream = &mut mutex_guard.0;


            let stream_read = StreamReadStruct {
                table_name: table_name_clone,
                partition_code: partition_code.clone(),
                offset: offset,
                read_count: read_count.clone(),
                table_col_type: col_type.clone(),
            };
            
            let salve_message = SlaveMessage::stream_read(stream_read);

            let message_bytes = bincode::serialize(&salve_message)?;

            stream.write_i32(message_bytes.len() as i32).await?;
            stream.write(&message_bytes).await?;

            match stream.read_i32().await {
                Ok(read_count) => {
                    if read_count == -1 {
                        return Ok(None);
                    } else if read_count == -2 {
                        let len = stream.read_i32().await?;
                        let mut mess = vec![0u8; len as usize];
                        stream.read_exact(&mut mess).await?;
                        let dd = String::from_utf8(mess)?;
                        return Err(DataLakeError::custom(dd));
                    } else {
                        let mut message = vec![0u8; read_count as usize];
                        stream.read_exact(&mut message).await?;

                        return Ok(Some(message));
                    }
                }
                Err(e) => {
                    return Err(DataLakeError::custom(format!("流读 失败了: {:?}", e)));
                }
            }

            
        });
    }
    let mut res_vec = Vec::<Vec<u8>>::new();

    while let Some(res) = join_handle_set.join_next().await {
        match res {
            Ok(res_data) => {
                match res_data {
                    Ok(data) => {
                        if let Some(da) = data {
                            res_vec.push(da);
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                return Err(DataLakeError::custom(format!("流读的 协程队列报错 : {:?}", e)));
            }
        }
    }
    
    
    if res_vec.len() > 0 {
        return Ok(Some(res_vec));
    }else { 
        return Ok(None);
    }

    
}

