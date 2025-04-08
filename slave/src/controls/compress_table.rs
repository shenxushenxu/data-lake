use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{DataStructure, IndexStruct};
use memmap2::Mmap;
use public_function::SLAVE_CONFIG;
use snap::raw::{Decoder, Encoder};
use std::collections::HashMap;
use std::io::SeekFrom;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

pub async fn compress_table(table_name: &String) -> Result<(), DataLakeError> {
    let data_path = SLAVE_CONFIG.get("slave.data").unwrap();
    let log_path = format!("{}/{}/log", data_path, table_name);
    let compress_path = format!("{}/{}/compress", data_path, table_name);

    let mut log_files = public_function::get_list_filename(&log_path[..]).await;
    let mut compress_files = public_function::get_list_filename(&compress_path[..]).await;

    log_files.append(&mut compress_files);

    let mut file_vec = log_files
        .iter()
        .filter(|x1| {
            let file_name = &x1.0;

            if file_name.contains(".log") || file_name.contains(".snappy") {
                return true;
            }
            return false;
        })
        .collect::<Vec<&(String, String)>>();

    file_vec.sort_by_key(|x| {
        let file_name = &x.0;

        let offset_n = file_name.replace(".log", "").replace(".snappy", "");

        let file_code = offset_n.parse::<i64>().unwrap();

        return file_code;
    });

    file_vec.remove(file_vec.len() - 1);
    file_vec.remove(file_vec.len() - 1);

    // let mut res_map = HashMap::<String, DataStructure>::new();
    //
    // for file_value in file_vec.iter() {
    //     let file_path = &file_value.1;
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
    //                     let message_bytes = decoder
    //                         .decompress_vec(&message)
    //                         .unwrap_or_else(|e| panic!("解压失败: {}", e));
    //
    //                     let message_str = std::str::from_utf8(&message_bytes)?;
    //                     data_structure = serde_json::from_str::<DataStructure>(message_str)?;
    //                 } else {
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
    //                                 return Err(DataLakeError::CustomError("奶奶的，offset出毛病了".to_string()))
    //                             }
    //                         }
    //                         "delete" => {
    //                             res_map.remove(major_key);
    //                         }
    //                         _ => {
    //                             return Err(DataLakeError::CustomError(format!("存在没有被定义的  crud 操作: {:?}", data_structure)))
    //                         }
    //                     },
    //                     None => match crud.as_str() {
    //                         "insert" => {
    //                             res_map.insert(major_key.clone(), data_structure);
    //                         }
    //                         "delete" => {
    //                             res_map.remove(major_key);
    //                         }
    //                         _ => {
    //                             return Err(DataLakeError::CustomError(format!("存在没有被定义的  crud 操作: {:?}", data_structure)))
    //                         }
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

    let mut tmpfile_offsetCode = HashMap::<String, i64>::new();

    let tmp_file_name = Uuid::new_v4().to_string();
    tmpfile_offsetCode.insert(tmp_file_name.clone(), 0);

    let mut compress_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(format!("{}\\{}.snappy", &compress_path, tmp_file_name))
        .await?;

    let mut index_file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(format!("{}\\{}.index", &compress_path, tmp_file_name))
        .await?;

    let mut offset: i64 = 0;

    let file_max_capacity = SLAVE_CONFIG
        .get("slave.file.segment.bytes")
        .unwrap()
        .parse::<u64>()?;

    for (key, (position, len)) in res_map.into_iter() {
        let data_bytes = &temp_mmap[position..(position + len)];
        let mut value = bincode::deserialize::<DataStructure>(&data_bytes)?;

        value.offset = offset;
        let json_value = serde_json::to_string_pretty(&value)?;

        let mut encoder = Encoder::new();
        let compressed_data = encoder.compress_vec(json_value.as_bytes())?;

        let data_len = compressed_data.len() as i32;

        let start_seek = compress_file.seek(SeekFrom::End(0)).await?;

        compress_file.write_i32(data_len).await?;
        compress_file.write_all(&compressed_data).await?;
        let end_seek = compress_file.seek(SeekFrom::End(0)).await?;

        let index_struct = IndexStruct {
            offset: offset,
            start_seek: start_seek,
            end_seek: end_seek,
        };

        let offset_struct = bincode::serialize(&index_struct)?;
        index_file.write_all(&offset_struct).await?;

        offset += 1;

        if end_seek > file_max_capacity {
            let tmp_file_name = Uuid::new_v4().to_string();

            compress_file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(format!("{}\\{}.snappy", &compress_path, tmp_file_name))
                .await?;

            index_file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(format!("{}\\{}.index", &compress_path, tmp_file_name))
                .await?;

            tmpfile_offsetCode.insert(tmp_file_name.clone(), offset);
        }
    }

    // 删除原本的无用文件
    for file_key in file_vec.iter() {
        let file_path = &file_key.1;

        std::fs::remove_file(file_path)?;
        let index_file_path = file_path
            .replace(".snappy", ".index")
            .replace(".log", ".index");
        std::fs::remove_file(index_file_path)?;
    }

    // 将临时文件的名字，改为正经数据文件的名字
    for key in tmpfile_offsetCode.keys() {
        let old_snappy = format!("{}\\{}.snappy", &compress_path, key);
        let old_index = format!("{}\\{}.index", &compress_path, key);

        let offset_name = tmpfile_offsetCode.get(key).unwrap();
        let new_snappy = format!("{}\\{}.snappy", &compress_path, offset_name);
        let new_index = format!("{}\\{}.index", &compress_path, offset_name);

        std::fs::rename(old_snappy, new_snappy)?;
        std::fs::rename(old_index, new_index)?;
    }

    return Ok(());
}
