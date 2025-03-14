use std::io::SeekFrom;
use memmap2::MmapMut;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use entity_lib::entity::MasterEntity::{BatchInsert, Insert, SlaveInsert};
use entity_lib::entity::SlaveEntity::{DataStructure, IndexStruct, SlaveCacheStruct};
use public_function::SLAVE_CONFIG;
use crate::controls::insert_data::FILE_CACHE_POOL;

pub async  fn batch_insert_data(batch_insert: SlaveInsert) {
    insert_operation(&batch_insert).await;
}


pub async fn insert_operation(batch_insert: &SlaveInsert) {

    let mut mutex_map= FILE_CACHE_POOL.lock().await;

    let mut offset_init = None;

    let table_name = &batch_insert.table_name;
    let partition_code = &batch_insert.partition_code;
    let batch_insert = &batch_insert.data;
    let file_key = format!("{}-{}", table_name, partition_code);


    for insert in batch_insert.iter() {


        let crud = &insert.crud;
        let data = &insert.data;
        let major_key = &insert.major_key;

        let slave_cache_struct = match mutex_map.get_mut(&file_key) {
            Some(slave_cache_struct) => {
                slave_cache_struct
            }
            None => {
                // 插入数据的缓存中，没有插入的文件对象

                let metadata_file_path = format!(
                    "{}\\{}-{}\\{}",
                    SLAVE_CONFIG.get("slave.data").unwrap(),
                    table_name,
                    partition_code,
                    "metadata.log"
                );

                let mut metadata_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(metadata_file_path)
                    .await
                    .unwrap();
                let mut metadata_mmap = unsafe {MmapMut::map_mut(&metadata_file).unwrap()};

                // let offset_file_name = i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap());

                let offset_file_name = match offset_init{
                    Some(offset) => {
                        offset
                    }
                    None => {
                        i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap())
                    }
                };


                let log_file_path = format!(
                    "{}\\{}-{}\\{}\\{}.log",
                    SLAVE_CONFIG.get("slave.data").unwrap(),
                    table_name,
                    partition_code,
                    "log",
                    offset_file_name
                );
                let index_file_path = format!(
                    "{}\\{}-{}\\{}\\{}.index",
                    SLAVE_CONFIG.get("slave.data").unwrap(),
                    table_name,
                    partition_code,
                    "log",
                    offset_file_name
                );

                let mut log_file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(log_file_path)
                    .await
                    .unwrap();

                let mut index_file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(index_file_path)
                    .await
                    .unwrap();
                let slave_cache_struct = SlaveCacheStruct {
                    data_file: log_file,
                    index_file: index_file,
                    metadata_file: metadata_file,
                    metadata_mmap: metadata_mmap,
                };

                mutex_map.insert(file_key.clone(), slave_cache_struct);

                mutex_map.get_mut(&file_key).unwrap()
            }
        };




        let mut metadata_mmap = &mut slave_cache_struct.metadata_mmap;
        let data_file = &mut slave_cache_struct.data_file;
        let index_file = &mut slave_cache_struct.index_file;

        let offset = match offset_init{
            Some(offset) => {
                offset
            }
            None => {
                i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap())
            }
        };


        // let offset = i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap());

        let start_seek = data_file.seek(SeekFrom::End(0)).await.unwrap();


        let data = DataStructure {
            table_name: table_name.clone(),
            major_key: major_key.clone(),
            data: data.clone(),
            crud: crud.clone(),
            partition_code: partition_code.clone(),
            offset: offset,
        };

        let bincode_data = bincode::serialize(&data).unwrap();
        let data_len = bincode_data.len() as i32;

        data_file.write_i32(data_len).await.unwrap();
        data_file.write_all(&bincode_data).await.unwrap();

        let end_seek = data_file.seek(SeekFrom::End(0)).await.unwrap();

        let index_struct = IndexStruct {
            offset: offset,
            start_seek: start_seek,
            end_seek: end_seek,
        };

        let index_data = bincode::serialize(&index_struct).unwrap();
        index_file.write_all(&index_data).await.unwrap();






        offset_init = Some(offset + 1);

        if end_seek > (SLAVE_CONFIG.get("slave.file.segment.bytes").unwrap().parse::<u64>().unwrap()){

            data_file.flush().await.unwrap();
            index_file.flush().await.unwrap();

            mutex_map.remove(&file_key);
        }

    }

    let mut metadata_mmap = &mut mutex_map.get_mut(&file_key).unwrap().metadata_mmap;
    let offset = match offset_init{
        Some(offset) => {
            offset
        }
        None => {
            i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap())
        }
    };
    unsafe {
        let dst_ptr = metadata_mmap.as_mut_ptr();
        let slice = (offset + 1).to_be_bytes();
        let src_ptr = slice.as_ptr();

        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, slice.len());
    }

}