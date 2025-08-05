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
use public_function::read_function::get_slave_path;

pub async fn compress_table(table_name: &String, uuid: &String) -> Result<(), DataLakeError> {
    
    
    let data_path = get_slave_path(table_name).await?;
    let compress_path = format!("{}/compress", data_path);

    let mut log_files = public_function::get_list_filename(&table_name).await;
  

    let mut file_vec = log_files
        .iter()
        .filter(|x1| {
            let file_name = &x1.0;

            if file_name.contains(".snappy") {
                return true;
            }
            return false;
        })
        .collect::<Vec<&(String, String)>>();

    file_vec.sort_by_key(|x| {
        let file_name = &x.0;

        let offset_n = file_name.replace(".snappy", "");

        let file_code = offset_n.parse::<i64>().unwrap();

        return file_code;
    });
    
    let file_vec_len = file_vec.len();
    
    if file_vec_len > 2 {
        file_vec.remove(file_vec.len() - 1);
        file_vec.remove(file_vec.len() - 1);
    }else { 
        return Ok(());
    }

    



    let f_v = file_vec
        .iter()
        .map(|x2| x2.1.clone())
        .collect::<Vec<String>>();
    
    let (res_map, temp_file) = public_function::read_function::data_duplicate_removal(f_v, uuid).await?;

    
    let temp_mmap = unsafe { Mmap::map(&temp_file) }?;

    let mut tmpfile_offsetCode = HashMap::<String, i64>::new();

    let tmp_file_name = get_tmp_file_name();
    tmpfile_offsetCode.insert(tmp_file_name.clone(), 0);

    let mut compress_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{}/{}.snappy", &compress_path, tmp_file_name))
        .await?;

    let mut index_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{}/{}.index", &compress_path, tmp_file_name))
        .await?;

    let mut offset: i64 = 0;

    let file_max_capacity = {
        let slave_config = SLAVE_CONFIG.lock().await;
        slave_config.slave_file_segment_bytes as u64
    };
    

    for (_, (position, len)) in res_map.into_iter() {
        let data_bytes = &temp_mmap[position..(position + len)];
        let mut value = bincode::deserialize::<DataStructure>(&data_bytes)?;

        value.offset = offset;
        let bincode_value = bincode::serialize(&value)?;

        let data_len = bincode_value.len() as i32;

        let start_seek = compress_file.seek(SeekFrom::End(0)).await?;

        compress_file.write_i32(data_len).await?;
        compress_file.write_all(&bincode_value).await?;
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

            compress_file.flush().await?;
            index_file.flush().await?;
            
            
            let tmp_file_name = get_tmp_file_name();

            compress_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("{}/{}.snappy", &compress_path, tmp_file_name))
                .await?;

            index_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("{}/{}.index", &compress_path, tmp_file_name))
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
        let old_snappy = format!("{}/{}.snappy", &compress_path, key);
        let old_index = format!("{}/{}.index", &compress_path, key);

        let offset_name = tmpfile_offsetCode.get(key).unwrap();
        let new_snappy = format!("{}/{}.snappy", &compress_path, offset_name);
        let new_index = format!("{}/{}.index", &compress_path, offset_name);

        std::fs::rename(old_snappy, new_snappy)?;
        std::fs::rename(old_index, new_index)?;
    }

    return Ok(());
}



fn get_tmp_file_name() -> String {
    
    let uuid = Uuid::new_v4().to_string();
    let tmp_file_name = format!("temp_{}", uuid);
    return tmp_file_name;
}