use crate::{SlaveConfig, SLAVE_CONFIG};
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
use entity_lib::entity::const_property;

pub async fn data_duplicate_removal(file_vec: Vec<String>, uuid:&String) -> Result<(HashMap<String, (usize, usize)>, File), DataLakeError> {
    
    
    
    let mut res_map = HashMap::<String, (usize, usize)>::new();


    let slave_data = {
        let mut slave_config = SLAVE_CONFIG.lock().await;
        slave_config.get_slave_data().await
    };
    
    
    // let uuid = Uuid::new_v4().to_string();
    let temp_path = format!(
        "{}/{}",
        slave_data,
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
        .open(format!("{}/{}",&temp_path, uuid))
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
                        .decompress_vec(&message)?;

                    let data_structure = bincode::deserialize::<DataStructure>(&message_bytes)?;

                    let major_value = data_structure.major_value;
                    let crud_type = data_structure._crud_type;
                    let offset = data_structure.offset;

                    match res_map.get(major_value) {
                        Some((position, len)) => match crud_type {
                            const_property::CURD_INSERT => {

                                let temp_mmap = unsafe {Mmap::map(&temp_file)}?;

                                let this_data = &temp_mmap[position.clone()..(position+len)];
                                let data = bincode::deserialize::<DataStructure>(this_data)?;


                                let map_data_offset = data.offset;
                                if offset > map_data_offset {
                                    let data_position = temp_file.seek(SeekFrom::End(0)).await?;
                                    let data_bytes = bincode::serialize(&data_structure)?;
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

                                let data_position = temp_file.seek(SeekFrom::End(0)).await?;
                                let data_bytes = bincode::serialize(&data_structure)?;
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
        
        let path = Path::new(&file_path);
        if path.exists() {
            return Ok(file_path);
        }
    }
    
    return Err(DataLakeError::custom(format!("{} This partition does not exist.",table_name)));
    
}


pub struct ArrayBytesReader<'a> {
    data: &'a [u8],
    array_pointer: usize,
}
impl<'a> ArrayBytesReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        return ArrayBytesReader {
            data: data,
            array_pointer: 0,
        };
    }

    pub fn read_i32(&mut self) -> Result<i32, DataLakeError> {
        let size = size_of::<i32>();

        let bytes = self.data[self.array_pointer..self.array_pointer + size].try_into()?;
        let len = i32::from_be_bytes(bytes);

        self.array_pointer += size;

        return Ok(len);
    }

    pub fn read_exact(&mut self, len: usize) -> &'a [u8] {
        let uuu = &self.data[self.array_pointer..self.array_pointer + len];

        self.array_pointer += len;

        return uuu;
    }

    pub fn is_stop(&self) -> bool {
        if self.array_pointer == self.data.len() {
            return true;
        } else {
            return false;
        }
    }
}