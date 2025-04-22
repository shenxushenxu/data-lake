use crate::controls::metadata::get_metadata;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{
    ColumnConfigJudgment, DataType, MasterStreamRead, Parti, TableStructure,
};
use entity_lib::entity::SlaveEntity::{DataStructure, SlaveMessage, StreamReadStruct};
use public_function::read_function::ArrayBytesReader;
use snap::raw::{Decoder, Encoder};
use std::array;
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
    let read_count = &masterstreamread.read_count;
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

    for parti in patiti_vec.iter() {
        let partition_code = &parti.patition_code;
        let offset = parti.offset;

        let address = address_map.get(&partition_code).unwrap();

        let stream_read = StreamReadStruct {
            table_name: table_name.clone(),
            partition_code: partition_code.clone(),
            offset: offset,
            read_count: read_count.clone(),
        };

        let sen = sender.clone();
        let addre = address.clone();

        let map_key = format!("{}_{}_{}", uuid, addre, &table_name);

        let ta_na = Arc::new(table_name.clone());
        tokio::spawn(async move {
            let mut stream_map = STREAM_TCP_TABLESTRUCTURE.lock().await;
            let (stream, tablestructure) = match stream_map.get_mut(&map_key[..]) {
                Some(stream) => stream,
                None => {
                    let adre = &addre[..];
                    let mut stream = TcpStream::connect(adre).await?;
                    let bb = ta_na.clone();

                    let tablestructure = get_metadata(bb.as_ref()).await?;
                    stream_map.insert(map_key.clone(), (stream, tablestructure));

                    stream_map.get_mut(&map_key).unwrap()
                }
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
                        let mut me = vec![0u8; read_count as usize];
                        stream.read_exact(&mut me).await?;

                        let mut vec_datastructure = Vec::<DataStructure>::new();

                        let mut arraybytesreader = ArrayBytesReader::new(me.as_slice());
                        loop {
                            if arraybytesreader.is_stop() {
                                break;
                            }

                            let mess_len = arraybytesreader.read_i32();
                            let mess = arraybytesreader.read_exact(mess_len as usize);

                            let mut decoder = Decoder::new();
                            let message_bytes = decoder
                                .decompress_vec(mess)
                                .unwrap_or_else(|e| panic!("解压失败: {}", e));
                            let message = String::from_utf8(message_bytes)?;
                            let mut datastructure = serde_json::from_str::<DataStructure>(&message)?;
                            let mut data = serde_json::from_str::<HashMap<String, String>>(
                                &datastructure.data,
                            )?;

                            let col_type = &tablestructure.col_type;

                            let mut remove_key = Vec::<String>::new();
                            let mut insert_map = HashMap::<String, String>::new();

                            for (key, value) in data.iter() {
                                match col_type.get(key) {
                                    None => {
                                        remove_key.push(key.clone());
                                    }
                                    Some(attribute) => {
                                        let column_configh_judgment = &attribute.1;

                                        if value.is_empty() {
                                            if let ColumnConfigJudgment::DEFAULT =
                                                column_configh_judgment
                                            {
                                                let default_option = &attribute.2;
                                                if let Some(default_value) = default_option {
                                                    insert_map
                                                        .insert(key.clone(), default_value.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            for r_k in remove_key.into_iter() {
                                data.remove(&r_k);
                            }
                            for (k, v) in insert_map.into_iter() {
                                data.insert(k, v);
                            }

                            datastructure.data = serde_json::to_string(&data)?;
                            vec_datastructure.push(datastructure);
                        }

                        let vec_string = serde_json::to_string(&vec_datastructure)?;

                        let mut encoder = Encoder::new();
                        let compressed_data = encoder.compress_vec(vec_string.as_bytes())?;

                        if let Err(e) = sen.send(Some(compressed_data)).await {
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
