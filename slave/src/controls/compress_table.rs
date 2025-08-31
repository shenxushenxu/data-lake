use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{DataStructure, IndexStruct};
use memmap2::Mmap;
use snap::raw::Encoder;
use std::collections::HashMap;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use uuid::Uuid;
use entity_lib::entity::const_property::I32_BYTE_LEN;
use entity_lib::function::{get_list_filename, SLAVE_CONFIG};
use entity_lib::function::read_function::get_slave_path;
use log::log;

pub async fn compress_table(table_name: &String, uuid: &String) -> Result<(), DataLakeError> {


    let slave_compaction_log_retain_number = {
        let slave_config = SLAVE_CONFIG.lock().await;
        slave_config.slave_compaction_log_retain_number.clone()
    };



    let data_path = get_slave_path(table_name).await?;

    let log_path = format!("{}/log", &data_path);
    // 验证 log 下有几个文件，要是 <= 2 就不压缩了
    let mut log_count = 0;
    let mut entries = tokio::fs::read_dir(log_path).await?;
    while let Some(folder_entry) = entries.next_entry().await? {
        let folder_entry_path = folder_entry.path(); // 获取条目路径
        if folder_entry_path.is_file() {
            let file_name = folder_entry_path
                .file_name()
                .unwrap()
                .to_str().unwrap();
            if file_name.contains(entity_lib::entity::const_property::DATA_FILE_EXTENSION) {
                log_count += 1;
            }
        }
    }

    if log_count <= slave_compaction_log_retain_number {
        return Ok(());
    }




    let mut log_files = get_list_filename(&table_name).await;
  

    let mut file_list = log_files
        .iter()
        .filter(|x1| {
            let file_name = &x1.0;

            if file_name.contains(".snappy") {
                return true;
            }
            return false;
        })
        .collect::<Vec<&(String, String)>>();

    file_list.sort_by_key(|x| {
        let file_name = &x.0;

        let offset_n = file_name.replace(".snappy", "");

        let file_code = offset_n.parse::<i64>().unwrap();

        return file_code;
    });


    let file_vec = &file_list[..(file_list.len() - 1 - (slave_compaction_log_retain_number as usize))];


    let f_v = file_vec
        .iter()
        .map(|x2| x2.1.clone())
        .collect::<Vec<String>>();
    
    let (res_map, temp_file) = entity_lib::function::read_function::data_duplicate_removal(f_v, uuid).await?;

    let mut sort_vec = Vec::<(usize, usize)>::with_capacity(res_map.len());

    res_map.into_iter().for_each(|x3| {
        let coordinate = x3.1;
        sort_vec.push(coordinate);
    });

    sort_vec.sort_by_key(|x4| {
       x4.0
    });


    let compress_path = format!("{}/compress", data_path);

    let temp_mmap = unsafe { Mmap::map(&temp_file) }?;

    let mut tmpfile_offsetCode = HashMap::<String, i64>::new();

    let tmp_file_name = get_tmp_file_name();
    tmpfile_offsetCode.insert(tmp_file_name.clone(), 0);

    let compressf = OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{}/{}.snappy", &compress_path, tmp_file_name))
        .await?;

    let indexf = OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{}/{}.index", &compress_path, tmp_file_name))
        .await?;
    
    // 定义 5MB 的缓冲区
    let buf_size = 5000000;
    let mut compress_writer = BufWriter::with_capacity(buf_size, compressf);
    let mut index_writer = BufWriter::with_capacity(buf_size, indexf);

    let mut offset: i64 = 0;

    let file_max_capacity = {
        let slave_config = SLAVE_CONFIG.lock().await;
        slave_config.slave_file_segment_bytes as u64
    };

    let mut encoder = Encoder::new();
    let mut start_seek:u64 = 0;
    
    for (position, len) in sort_vec.into_iter() {
        let data_bytes = &temp_mmap[position..(position + len)];
        let mut value = DataStructure::deserialize(data_bytes);

        value.offset = offset;
        let value_serialize = value.serialize()?;
        let compressed_data = encoder.compress_vec(&value_serialize)?;

        let data_len = compressed_data.len() as i32;
        let data_len_bytes = &data_len.to_le_bytes();

        compress_writer.write_all(data_len_bytes).await?;
        compress_writer.write_all(&compressed_data).await?;
        
        let end_seek = start_seek + (I32_BYTE_LEN as u64) + (data_len as u64);
        let index_struct = IndexStruct {
            offset: offset,
            start_seek: start_seek,
            end_seek: end_seek,
        };

        let offset_struct = bincode::serialize(&index_struct)?;
        index_writer.write_all(&offset_struct).await?;

        offset += 1;
        start_seek = end_seek;

        if end_seek > file_max_capacity {

            compress_writer.flush().await?;
            index_writer.flush().await?;
            
            
            let tmp_file_name = get_tmp_file_name();

            let compressf = OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("{}/{}.snappy", &compress_path, tmp_file_name))
                .await?;
            compress_writer = BufWriter::with_capacity(buf_size, compressf);

            let indexf = OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("{}/{}.index", &compress_path, tmp_file_name))
                .await?;
            index_writer = BufWriter::with_capacity(buf_size, indexf);

            tmpfile_offsetCode.insert(tmp_file_name.clone(), offset);
            start_seek = 0;
        }
    }

    compress_writer.flush().await?;
    index_writer.flush().await?;
    
    
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