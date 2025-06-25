use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::LazyLock;
use memmap2::MmapMut;
use snap::raw::Encoder;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::Insert;
use entity_lib::entity::SlaveEntity::{DataStructure, IndexStruct, SlaveCacheStruct};
use public_function::read_function::get_slave_path;
use public_function::SLAVE_CONFIG;

pub static FILE_CACHE_POOL: LazyLock<Mutex<HashMap<String, SlaveCacheStruct>>> =
    LazyLock::new(|| {
        let file_cache_pool = HashMap::<String, SlaveCacheStruct>::new();

        Mutex::new(file_cache_pool)
    });

pub async fn insert_operation(insert: Insert) -> Result<(), DataLakeError>{


    let table_name = insert.table_name;
    let partition_code = insert.partition_code;
    let crud_type = insert._crud_type;
    let data = insert.data;
    let major_key = insert.major_key;

    let file_key = format!("{}-{}", table_name, partition_code);

    let mut mutex_map= FILE_CACHE_POOL.lock().await;

    let slave_cache_struct = match mutex_map.get_mut(&file_key) {
        Some(slave_cache_struct) => {
            // 插入数据的缓存中，有插入的文件对象

            slave_cache_struct
        }
        None => {
            
            let partition_name = format!("{}-{}", table_name, partition_code);
            let data_path = get_slave_path(&partition_name).await?;

            let metadata_file_path = format!(
                "{}/{}",
                data_path,
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


            let offset = i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap());


            let log_file_path = format!(
                "{}/{}/{}.snappy",
                data_path,
                "log",
                offset
            );
            let index_file_path = format!(
                "{}/{}/{}.index",
                data_path,
                "log",
                offset
            );

            let mut log_file = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(log_file_path)
                .await?;

            let mut index_file = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(index_file_path)
                .await?;

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


    let offset = i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap());

    let start_seek = data_file.seek(SeekFrom::End(0)).await?;


    let data = DataStructure {
        table_name: table_name,
        major_key: major_key,
        data: data,
        _crud_type: crud_type,
        partition_code: partition_code,
        offset: offset,
    };

    let json_value = serde_json::to_string(&data)?;
    let mut encoder = Encoder::new();
    let compressed_data = encoder.compress_vec(json_value.as_bytes())?;

    let data_len = compressed_data.len() as i32;

    data_file.write_i32(data_len).await?;
    data_file.write_all(&compressed_data).await?;

    let end_seek = data_file.seek(SeekFrom::End(0)).await?;

    let index_struct = IndexStruct {
        offset: offset,
        start_seek: start_seek,
        end_seek: end_seek,
    };

    let index_data = bincode::serialize(&index_struct)?;
    index_file.write_all(&index_data).await?;



    unsafe {
        let dst_ptr = metadata_mmap.as_mut_ptr();
        let slice = (offset + 1).to_be_bytes();
        let src_ptr = slice.as_ptr();

        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, slice.len());

    }


    let slave_file_segment_bytes = {
        let slave_config = SLAVE_CONFIG.lock().await;
        slave_config.slave_file_segment_bytes as u64
    };
    
    if end_seek > slave_file_segment_bytes{

        data_file.flush().await?;
        index_file.flush().await?;

        mutex_map.remove(&file_key);
    }


    return Ok(());
}