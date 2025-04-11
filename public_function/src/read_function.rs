use crate::SLAVE_CONFIG;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::DataStructure;
use snap::raw::Decoder;
use std::collections::HashMap;
use std::fmt::format;
use std::io::SeekFrom;
use std::path::Path;
use memmap2::Mmap;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;




pub async fn data_duplicate_removal(file_vec: Vec<String>) -> Result<(HashMap<String, (usize, usize)>, File), DataLakeError> {
    let mut res_map = HashMap::<String, (usize, usize)>::new();

    let uuid = Uuid::new_v4().to_string();
    let temp_path = format!(
        "{}\\{}",
        SLAVE_CONFIG.get("slave.data").unwrap(),
        "temp"
    );
    let path = Path::new(&temp_path);
    if !path.exists() {
        tokio::fs::create_dir(&temp_path).await?;
    }

    let mut temp_file = OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(format!("{}\\{}",&temp_path, uuid))
        .await?;

    for file_path in file_vec.iter() {
        let mut file = OpenOptions::new().read(true).open(file_path).await?;

        loop {
            match file.read_i32().await {
                Ok(message_len) => {
                    let mut message = vec![0u8; message_len as usize];

                    file.read_exact(&mut message).await?;


                    let mut decoder = Decoder::new();
                    let message_bytes = decoder
                        .decompress_vec(&message)
                        .unwrap_or_else(|e| panic!("解压失败: {}", e));

                    let message_str = std::str::from_utf8(&message_bytes)?;
                    let data_structure = serde_json::from_str::<DataStructure>(message_str)?;

                    let major_key = &data_structure.major_key;
                    let crud = &data_structure.crud;
                    let offset = &data_structure.offset;

                    match res_map.get(major_key) {
                        Some((position, len)) => match crud.as_str() {
                            "insert" => {

                                let temp_mmap = unsafe {Mmap::map(&temp_file)}?;

                                let this_data = &temp_mmap[position.clone()..(position+len)];
                                let data = bincode::deserialize::<DataStructure>(this_data)?;


                                let map_data_offset = &data.offset;
                                if offset > map_data_offset {
                                    let data_position = temp_file.seek(SeekFrom::End(0)).await?;
                                    let data_bytes = bincode::serialize(&data_structure)?;
                                    let data_len = data_bytes.len();

                                    temp_file.write_all(data_bytes.as_slice()).await?;

                                    res_map.insert(major_key.clone(), (data_position as usize, data_len));
                                } else {
                                    return Err(DataLakeError::CustomError(
                                        "奶奶的，offset出毛病了".to_string(),
                                    ));
                                }
                            }
                            "delete" => {
                                res_map.remove(major_key);
                            }
                            _ => {
                                return Err(DataLakeError::CustomError(format!(
                                    "存在没有被定义的  crud 操作: {:?}",
                                    data_structure
                                )));
                            }
                        },
                        None => match crud.as_str() {
                            "insert" => {

                                let data_position = temp_file.seek(SeekFrom::End(0)).await?;
                                let data_bytes = bincode::serialize(&data_structure)?;
                                let data_len = data_bytes.len();
                                temp_file.write_all(data_bytes.as_slice()).await?;

                                res_map.insert(major_key.clone(), (data_position as usize, data_len));
                            }
                            "delete" => {
                                res_map.remove(major_key);
                            }
                            _ => {
                                return Err(DataLakeError::CustomError(format!(
                                    "存在没有被定义的  crud 操作: {:?}",
                                    data_structure
                                )));
                            }
                        },
                    }
                }

                Err(_) => {
                    break;
                }
            }
        }
    }

    return Ok((res_map, temp_file));
}
