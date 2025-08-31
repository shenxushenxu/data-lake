use snap::raw::Decoder;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use memmap2::Mmap;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
use crate::entity::const_property;
use crate::entity::const_property::I32_BYTE_LEN;
use crate::entity::Error::DataLakeError;
use crate::entity::SlaveEntity::DataStructure;
use crate::function::{check_file_exists, SLAVE_CONFIG};

pub async fn data_duplicate_removal(file_vec: Vec<String>, uuid:&String) -> Result<(HashMap<String, (usize, usize)>, File), DataLakeError> {
    
    
    
    let mut res_map = HashMap::<String, (usize, usize)>::new();


    let slave_data = {
        let mut slave_config = SLAVE_CONFIG.lock().await;
        slave_config.get_slave_data().await
    };
    
    
    let temp_path = format!(
        "{}/{}",
        slave_data,
        "temp"
    );
    if !check_file_exists(&temp_path) {
        tokio::fs::create_dir(&temp_path).await?;
    }

    let mut temp_file = OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(format!("{}/{}",&temp_path, uuid))
        .await?;


    let mut decoder = Decoder::new();
    
    for file_path in file_vec.iter() {
        let mut file = OpenOptions::new().read(true).open(file_path).await?;

        loop {
            let mut message_len_bytes = vec![0u8; I32_BYTE_LEN];
            match file.read_exact(message_len_bytes.as_mut_slice()).await {
                Ok(_) => {

                    let message_len = i32::from_le_bytes(message_len_bytes.try_into().unwrap());
                    
                    let mut message = vec![0u8; message_len as usize];

                    file.read_exact(&mut message).await?;
                    
                    let message_bytes = decoder
                        .decompress_vec(&message)?;

                    let data_structure = DataStructure::deserialize(message_bytes.as_slice());

                    let major_value = data_structure.major_value;
                    let crud_type = data_structure._crud_type;
                    let offset = data_structure.offset;

                    match res_map.get(major_value) {
                        Some((position, len)) => match crud_type {
                            const_property::CURD_INSERT => {

                                let temp_mmap = unsafe {Mmap::map(&temp_file)}?;

                                let this_data = &temp_mmap[position.clone()..(position+len)];
                                let data = DataStructure::deserialize(this_data);


                                let map_data_offset = data.offset;
                                if offset > map_data_offset {
                                    let data_position = temp_file.stream_position().await?;;
                                    let data_bytes = data_structure.serialize()?;
                                    let data_len = data_bytes.len();

                                    temp_file.write_all(data_bytes.as_slice()).await?;

                                    res_map.insert(major_value.to_string(), (data_position as usize, data_len));
                                } else {
                                    return Err(DataLakeError::custom(
                                        format!("{} : 奶奶的，offset出毛病了  本身的offset {}   map里面存储的offset{}", file_path, offset, map_data_offset)
                                    ));
                                }
                            }
                            const_property::CURD_DELETE => {
                                res_map.remove(major_value);
                            }
                            _ => {
                                return Err(DataLakeError::custom(format!(
                                    "存在没有被定义的  crud 操作: {:?}",
                                    data_structure
                                )));
                            }
                        },
                        None => match crud_type {
                            const_property::CURD_INSERT => {

                                let data_position = temp_file.stream_position().await?;;
                                let data_bytes = data_structure.serialize()?;
                                let data_len = data_bytes.len();
                                temp_file.write_all(data_bytes.as_slice()).await?;

                                res_map.insert(major_value.to_string(), (data_position as usize, data_len));
                            }
                            const_property::CURD_DELETE => {
                                res_map.remove(major_value);
                            }
                            _ => {
                                return Err(DataLakeError::custom(format!(
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



pub async fn get_slave_path(table_name: &String) -> Result<String, DataLakeError> {
    

    let slave_data = {
        let slave_config = SLAVE_CONFIG.lock().await;
        let slave_data = &slave_config.slave_data;
        slave_data.clone()
    };
    
    for data_path in slave_data {
        
        let file_path = format!("{}/{}", data_path, table_name);
        

        if check_file_exists(&file_path) {
            return Ok(file_path);
        }
    }
    
    return Err(DataLakeError::custom(format!("{} This partition does not exist.",table_name)));
    
}


