use crate::controls::metadata::get_metadata;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{Info, MasterStreamRead, Parti, PartitionInfo, TableStructure};
use entity_lib::entity::SlaveEntity::{SlaveMessage, StreamReadStruct};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Receiver;

pub static STREAM_TCP_TABLESTRUCTURE: LazyLock<
    Mutex<HashMap<String, (TcpStream, TableStructure)>>,
> = LazyLock::new(|| Mutex::new(HashMap::<String, (TcpStream, TableStructure)>::new()));

pub async fn stream_read_data(
    masterstreamread: MasterStreamRead,
    uuid: &String,
) -> Result<Receiver<Option<Vec<u8>>>, DataLakeError> {
    let table_name = masterstreamread.table_name;
    let read_count = masterstreamread.read_count;
    let table_structure = get_metadata(&table_name).await?;

    let address_map = &table_structure.partition_address;

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Option<Vec<u8>>>(10000);

    let mut patiti_vec = masterstreamread.patition_mess;

    if patiti_vec.len() == 0 {
        for key in address_map.keys() {
            let par = Parti {
                patition_code: key.clone(),
                offset: 0,
            };
            patiti_vec.push(par);
        }
    }

    let ta_na = Arc::new(table_name);
    
    for parti in patiti_vec.iter() {
        let partition_code = parti.patition_code;
        let offset = parti.offset;

        let partition_info_vec = address_map.get(&partition_code).unwrap();

     

        let sen = sender.clone();


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

        let arc_ta_name = Arc::clone(&ta_na);

        let map_key = format!("{}_{}", uuid, arc_ta_name.as_ref());
        
        tokio::spawn(async move {
            
            let mut stream_map = STREAM_TCP_TABLESTRUCTURE.lock().await;
            let (stream, tablestructure) = match stream_map.get_mut(&map_key) {
                Some(stream) => stream,
                None => {
                    let address = &partition_info.address;
                    let mut stream = TcpStream::connect(address).await?;

                    
                    let tablestructure = get_metadata(arc_ta_name.as_ref()).await?;
                    stream_map.insert(map_key.clone(), (stream, tablestructure));

                    stream_map.get_mut(&map_key).unwrap()
                }
            };
            let col_type = tablestructure.col_type.clone();

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
                        if let Err(e) = sen.send(None).await {
                            println!("Error sending message: {:?}", e);
                        }
                    } else if read_count == -2 {
                        let len = stream.read_i32().await?;
                        let mut mess = vec![0u8; len as usize];
                        stream.read_exact(&mut mess).await?;
                        let dd = String::from_utf8(mess)?;
                        return Err(DataLakeError::CustomError(dd));
                    } else {
                        let mut message = vec![0u8; read_count as usize];
                        stream.read_exact(&mut message).await?;

                        if let Err(e) = sen.send(Some(message)).await {
                            println!("Error sending message: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    return Err(DataLakeError::CustomError(format!("流读 失败了: {:?}", e)));
                }
            }

            Ok(())
        });
    }

    return Ok(receiver);
}

