use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{DataStructure, QueryMessage};
use memmap2::Mmap;
use public_function::SLAVE_CONFIG;
use snap::raw::Decoder;
use std::collections::HashMap;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;

pub async fn query(querymessage: QueryMessage) -> Result<Option<Vec<String>>, DataLakeError> {
    let log_path = format!(
        "{}\\{}\\log",
        SLAVE_CONFIG.get("slave.data").unwrap(),
        &querymessage.tablename
    );
    let compress_path = format!(
        "{}\\{}\\compress",
        SLAVE_CONFIG.get("slave.data").unwrap(),
        &querymessage.tablename
    );

    let mut log_file = public_function::get_list_filename(&log_path[..]).await;
    let mut compress_files = public_function::get_list_filename(&compress_path[..]).await;

    log_file.append(&mut compress_files);

    let mut file_vec = log_file
        .iter()
        .filter(|x1| {
            let file_name = &x1.0;
            if file_name.contains(".log") || file_name.contains(".snappy") {
                return true;
            }
            return false;
        })
        .collect::<Vec<&(String, String)>>();

    // let mut res_map:HashMap<String, DataStructure> = HashMap::new();
    //
    //
    // for file_key in file_vec.iter() {
    //
    //     let file_path = &file_key.1;
    //
    //     let mut file = OpenOptions::new().read(true).open(file_path).await?;
    //
    //     loop {
    //         match file.read_i32().await {
    //             Ok(message_len) => {
    //                 let mut message = vec![0u8; message_len as usize];
    //
    //                 file.read_exact(&mut message).await?;
    //
    //                 let data_structure;
    //
    //                 if file_path.contains(".snappy") {
    //                     let mut decoder = Decoder::new();
    //                     let message_bytes = decoder.decompress_vec(&message)?;
    //
    //                     let message_str = std::str::from_utf8(&message_bytes)?;
    //                     data_structure = serde_json::from_str::<DataStructure>(message_str)?;
    //                 }else {
    //                     data_structure = bincode::deserialize::<DataStructure>(&message)?;
    //                 }
    //
    //                 let major_key = &data_structure.major_key;
    //                 let crud = &data_structure.crud;
    //                 let offset = &data_structure.offset;
    //
    //                 match res_map.get(major_key) {
    //                     Some(data) => match crud.as_str() {
    //                         "insert" => {
    //                             let map_data_offset = &data.offset;
    //                             if offset > map_data_offset {
    //                                 res_map.insert(major_key.clone(), data_structure);
    //                             } else {
    //                                 return Err(DataLakeError::CustomError("奶奶的，offset出毛病了".to_string()));
    //                             }
    //                         }
    //                         "delete" => {
    //                             res_map.remove(major_key);
    //                         }
    //                         _ => return Err(DataLakeError::CustomError(format!("存在没有被定义的  crud 操作: {:?}", data_structure))),
    //                     },
    //                     None => match crud.as_str() {
    //                         "insert" => {
    //                             res_map.insert(major_key.clone(), data_structure);
    //                         }
    //                         "delete" => {
    //                             res_map.remove(major_key);
    //                         }
    //                         _ => return Err(DataLakeError::CustomError(format!("存在没有被定义的  crud 操作: {:?}", data_structure))),
    //                     },
    //                 }
    //             }
    //
    //             Err(_) => {
    //                 break;
    //             }
    //         }
    //     }
    // }

    let f_v = file_vec
        .iter()
        .map(|x2| x2.1.clone())
        .collect::<Vec<String>>();

    let (res_map, temp_file) = public_function::read_function::data_duplicate_removal(f_v).await?;
    let temp_mmap = unsafe { Mmap::map(&temp_file) }?;

    let res_vec = res_map
        .into_iter()
        .map(|(k, v)| {
            let data = &temp_mmap[v.0..(v.0 + v.1)];
            let data_structure = bincode::deserialize::<DataStructure>(data).unwrap();

            data_structure.data
        })
        .filter(|x| {
            let map = serde_json::from_str::<HashMap<String, String>>(x).unwrap();

            let mut i = 0;
            if let Some(condition) = &querymessage.conditions {
                for con in condition.iter() {
                    if let Some(value) = map.get(&con.0) {
                        if con.1.eq("=") {
                            if con.2.eq(value) {
                                i += 1;
                            }
                        } else if con.1.eq("<") {
                            if con.2.parse::<i64>().unwrap() < value.parse::<i64>().unwrap() {
                                i += 1;
                            }
                        } else if con.1.eq(">") {
                            if con.2.parse::<i64>().unwrap() > value.parse::<i64>().unwrap() {
                                i += 1;
                            }
                        }
                    }
                }

                if i == condition.len() {
                    return true;
                }
            } else {
                return true;
            }
            return false;
        })
        .collect::<Vec<String>>();

    if res_vec.len() > 0 {
        return Ok(Some(res_vec));
    } else {
        return Ok(None);
    }
}
