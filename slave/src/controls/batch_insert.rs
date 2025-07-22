use std::collections::HashMap;
use std::io::SeekFrom;
use std::mem;
use std::time::Instant;
use memmap2::MmapMut;
use snap::raw::Encoder;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use entity_lib::entity::const_property::{CRUD_TYPE, DATA_FILE_EXTENSION, I32_BYTE_LEN, INDEX_FILE_EXTENSION, LOG_FILE, METADATA_LOG};
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{ColumnConfigJudgment, DataType, SlaveInsert, TableStructure};
use entity_lib::entity::SlaveEntity::{DataStructure, IndexStruct, SlaveCacheStruct};
use public_function::read_function::get_slave_path;
use public_function::SLAVE_CONFIG;
use public_function::vec_trait::VecPutVec;
use crate::controls::insert_data::FILE_CACHE_POOL;








pub async  fn batch_insert_data(batch_insert: SlaveInsert) -> Result<(), DataLakeError>{

    insert_operation(&batch_insert).await?;
    
    
    
    return Ok(());
}


pub async fn insert_operation(batch_insert: &SlaveInsert) -> Result<(), DataLakeError> {

    
    let table_name = &batch_insert.table_name;
    let partition_code = &batch_insert.partition_code;
    let batch_insert_data = bincode::deserialize::<Vec<HashMap<String, String>>>(&batch_insert.data)?;
    let table_structure = &batch_insert.table_structure;
    let major_key = &table_structure.major_key;
    
    let file_key = format!("{}-{}", table_name, partition_code);
    let mut mutex_map= FILE_CACHE_POOL.lock().await;
    
    // 定义 offset 变量
    let mut offset_init = get_offset(None, &file_key).await?;
    
    let slave_cache_struct = {
        match mutex_map.get_mut(&file_key) {
            Some(slave_cache_struct) => {
                slave_cache_struct
            }
            None => {
                // 插入数据的缓存中，没有插入的文件对象
                let partition_name = format!("{}-{}", table_name, &partition_code);
                let partition_path = get_slave_path(&partition_name).await?;
    
                let metadata_file_path = format!(
                    "{}/{}",
                    partition_path,
                    METADATA_LOG
                );
    
                let mut metadata_file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(metadata_file_path)
                    .await?;
                let mut metadata_mmap = unsafe {MmapMut::map_mut(&metadata_file)?};
    
    
                let offset_file_name = &offset_init;
    
    
                let log_file_path = format!(
                    "{}/{}/{}{}",
                    partition_path,
                    LOG_FILE,
                    offset_file_name,
                    DATA_FILE_EXTENSION
                );
                let index_file_path = format!(
                    "{}/{}/{}{}",
                    partition_path,
                    LOG_FILE,
                    offset_file_name,
                    INDEX_FILE_EXTENSION
                );
    
                let mut log_file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_file_path)
                    .await?;
    
                let mut index_file = OpenOptions::new()
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
        }
    };

    let data_file = &mut slave_cache_struct.data_file;
    let index_file = &mut slave_cache_struct.index_file;
    let mut start_seek = data_file.seek(SeekFrom::End(0)).await?;
    

    let mut data_vec = Vec::<u8>::new();
    let mut index_vec = Vec::<u8>::new();

    for insert_single in batch_insert_data.iter() {
        
        batch_format_matching(insert_single, table_structure).await?;
        
        let offset = offset_init.clone();
        
        let major_value = insert_single.get(major_key).unwrap();
        let crud_type = insert_single.get(CRUD_TYPE).unwrap();
        
        let data = DataStructure {
            table_name: table_name.clone(),
            major_value: major_value.clone(),
            data: insert_single.clone(),
            _crud_type: crud_type.clone(),
            partition_code: partition_code.clone(),
            offset: offset,
        };
        
        let json_value = serde_json::to_string(&data)?;
        let mut encoder = Encoder::new();
        let mut compressed_data = encoder.compress_vec(json_value.as_bytes())?;
        
        let data_len = compressed_data.len() as i32;
        
        data_vec.put_i32_vec(data_len);
        data_vec.put_vec(&mut compressed_data);
        
        let end_seek = start_seek + (data_len as u64) + (I32_BYTE_LEN as u64);

        let index_struct = IndexStruct {
            offset: offset,
            start_seek: start_seek,
            end_seek: end_seek,
        };

        let mut index_data = bincode::serialize(&index_struct)?;
        
        index_vec.put_vec(&mut index_data);

        offset_init = offset_init + 1;
        start_seek = end_seek;
        
    }



    data_file.write_all(data_vec.as_slice()).await?;
    index_file.write_all(index_vec.as_slice()).await?;
    
    

    let mut metadata_mmap = &mut slave_cache_struct.metadata_mmap;
    
    unsafe {
        let dst_ptr = metadata_mmap.as_mut_ptr();
        let slice = offset_init.to_be_bytes();
        let src_ptr = slice.as_ptr();

        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, slice.len());
    }

    
    let slave_file_segment_bytes = {
        let slave_config = SLAVE_CONFIG.lock().await;
        slave_config.slave_file_segment_bytes as u64
    };
    let data_file_seek = data_file.seek(SeekFrom::End(0)).await?;
    
    if data_file_seek > slave_file_segment_bytes{

        data_file.flush().await?;
        index_file.flush().await?;

        mutex_map.remove(&file_key);
    }

    return Ok(());
}




/**
获得 当前数据的offset
**/
async fn get_offset(offset_init: Option<i64>, partition_code: &String) -> Result<i64, DataLakeError> {
    let offset_file_name = match offset_init{
        Some(offset) => {
            offset
        }
        None => {

            let partition_path = get_slave_path(partition_code).await?;

            let metadata_file_path = format!(
                "{}/{}",
                partition_path,
                METADATA_LOG
            );

            let mut metadata_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(metadata_file_path)
                .await?;
            let mut metadata_mmap = unsafe {MmapMut::map_mut(&metadata_file)?};
            
            
            
            i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap())
        }
    };
    
    return Ok(offset_file_name);
}


/**
数据验证，查看数据是否符合 元数据的格式
**/
pub async fn batch_format_matching(
    insert_single: &HashMap<String, String>,
    table_structure: &TableStructure
) -> Result<(), DataLakeError> {

    

    let structure_col_type = &table_structure.col_type;
    let table_name = &table_structure.table_name;
    
    type_verification(structure_col_type, insert_single, table_name).await?;


        
    

    return Ok(());
}

pub async fn type_verification(
    metadata_col_type: &HashMap<String, (DataType, ColumnConfigJudgment, Option<String>)>,
    insert_data: &HashMap<String, String>,
    table_name: &String,
) -> Result<(), DataLakeError> {
    for (col_name, value) in insert_data.iter() {
        match metadata_col_type.get(col_name) {
            Some((data_type, column_conf_judg, _)) => {
                // 验证类型是否匹配
                verification_type(col_name, value, data_type)?;
                //验证属性配置是否匹配
                conf_verification(col_name, value, column_conf_judg)?;
            }
            None => {
                
                if col_name != CRUD_TYPE {
                    return Err(DataLakeError::custom(format!(
                        "{} 表内不存在 {} 列",
                        table_name, col_name
                    )));
                }
            }
        }
    }

    return Ok(());
}

fn conf_verification(
    col_name: &String,
    col_value: &String,
    column_conf_judg: &ColumnConfigJudgment,
) -> Result<(), DataLakeError> {
    match column_conf_judg {
        ColumnConfigJudgment::PRIMARY_KEY => {
            if col_value.trim().is_empty() {
                return Err(DataLakeError::custom(format!("{} 列为 空", col_name)));
            }
        }
        ColumnConfigJudgment::NOT_NULL => {
            if col_value.trim().is_empty() {
                return Err(DataLakeError::custom(format!("{} 列为 空", col_name)));
            }
        }
        _ => {}
    }

    return Ok(());
}
/**
检查插入数据的 类型是否和 表元数据匹配
**/
fn verification_type(
    col_name: &String,
    col_value: &String,
    data_type: &DataType,
) -> Result<(), DataLakeError> {
    match data_type {
        DataType::string => {
            col_value.to_string();
        }
        DataType::int => match col_value.parse::<i32>() {
            Ok(_) => {}
            Err(e) => {
                return Err(DataLakeError::custom(format!(
                    "{} 列转换为 int 失败，检查插入的数据: {}",
                    col_name, col_value
                )));
            }
        },
        DataType::float => match col_value.parse::<f32>() {
            Ok(_) => {}
            Err(_) => {
                return Err(DataLakeError::custom(format!(
                    "{} 列转换为 float 失败，检查插入的数据: {}",
                    col_name, col_value
                )));
            }
        },
        DataType::boolean => match col_value.parse::<bool>() {
            Ok(_) => {}
            Err(_) => {
                return Err(DataLakeError::custom(format!(
                    "{} 列转换为 bool 失败，检查插入的数据: {}",
                    col_name, col_value
                )));
            }
        },
        DataType::long => match col_value.parse::<i64>() {
            Ok(_) => {}
            Err(_) => {
                return Err(DataLakeError::custom(format!(
                    "{} 列转换为 long 失败，检查插入的数据: {}",
                    col_name, col_value
                )));
            }
        },
        _ => {
            return Err(DataLakeError::custom(format!(
                "{}  不符合任何数据类型",
                col_name
            )));
        }
    }

    return Ok(());
}
