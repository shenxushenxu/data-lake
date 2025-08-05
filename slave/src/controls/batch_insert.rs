use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{
    ColumnConfigJudgment, DataType, SlaveInsert, TableStructure,
};
use entity_lib::entity::SlaveEntity::{DataStructure, IndexStruct, SlaveCacheStruct};
use entity_lib::entity::const_property::{CRUD_TYPE, DATA_FILE_EXTENSION, I32_BYTE_LEN, INDEX_FILE_EXTENSION, INDEX_SIZE, LOG_FILE, METADATA_LOG};
use memmap2::MmapMut;
use public_function::BufferObject::FILE_CACHE_POOL;
use public_function::SLAVE_CONFIG;
use public_function::read_function::get_slave_path;
use public_function::vec_trait::VecPutVec;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use snap::raw::Encoder;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use entity_lib::entity::const_property;

pub async fn batch_insert_data<'a>(batch_insert: SlaveInsert<'a>) -> Result<(), DataLakeError> {
    match insert_operation(&batch_insert).await {
        Ok(is) => {
            if let Some(file_key) = is {
                let file_cache_pool = Arc::clone(&FILE_CACHE_POOL);
                file_cache_pool.remove(&file_key);
            }
            return Ok(());
        }
        Err(e) => {
            return Err(e);
        }
    }
}

pub async fn insert_operation<'a>(
    batch_insert: &SlaveInsert<'a>,
) -> Result<Option<String>, DataLakeError> {
    let table_name = &batch_insert.table_name;
    let partition_code = batch_insert.partition_code;
    let batch_data = &batch_insert.data;

    let table_structure = &batch_insert.table_structure;
    let major_key = table_structure.major_key.as_str();
    let file_key = format!("{}-{}", table_name, partition_code);

    let mutex_slave_cache_struct = get_cache_file_object(&file_key).await?;

    let mut slave_cache_struct = mutex_slave_cache_struct.lock().await;
    
    let batch_insert_data_size = batch_data.get_data_size();
    
    // 定义 offset 变量
    let mut offset_init = get_offset(None, &file_key).await?;
    
    let batch_index = batch_insert_data_size - 1;
    
    let mut vec_data_structure = (0..batch_insert_data_size)
        .collect::<Vec<usize>>()
        .into_par_iter()
        .map(|index| {
            let mut insert_single = batch_data.get_line_map(index);
            batch_format_matching(&insert_single, table_structure)?;
            let major_value = insert_single.remove(major_key).unwrap();
            let crud_type = insert_single.remove(CRUD_TYPE).unwrap();
            
            let offset = offset_init + (( batch_index - index) as i64);
            
            let data = DataStructure {
                table_name: table_name,
                major_value: major_value,
                data: insert_single,
                _crud_type: crud_type,
                partition_code: partition_code,
                offset: offset,
            };

            let vec_mess = bincode::serialize(&data)?;

            let mut encoder = Encoder::new();
            let compressed_data = encoder.compress_vec(&vec_mess)?;
            

            Ok((offset, compressed_data))
        }).collect::<Result<Vec<(i64, Vec<u8>)>, DataLakeError>>()?;


    vec_data_structure.sort_by_key(|x| x.0);

    
    let mut start_seek = slave_cache_struct.data_file.metadata().await?.len();
    
    let mut data_vec = Vec::<u8>::new();
    let mut index_vec = Vec::<u8>::with_capacity(vec_data_structure.len() * INDEX_SIZE);
    
    for (_, data_structre_vecu8) in vec_data_structure.iter_mut() {
            let data_len = data_structre_vecu8.len() as i32;
        
            data_vec.put_i32_vec(data_len);
            data_vec.put_vec(data_structre_vecu8);
        
            let end_seek = start_seek + (I32_BYTE_LEN as u64) + (data_len as u64) ;
        
            let index_struct = IndexStruct {
                offset: offset_init,
                start_seek: start_seek,
                end_seek: end_seek,
            };
        
            let mut index_data = bincode::serialize(&index_struct)?;
        
            index_vec.put_vec(&mut index_data);
        
            offset_init = offset_init + 1;
            start_seek = end_seek;
    }


    slave_cache_struct
        .data_file
        .write_all(data_vec.as_slice())
        .await?;
    slave_cache_struct
        .index_file
        .write_all(index_vec.as_slice())
        .await?;


    unsafe {
        let dst_ptr = slave_cache_struct.metadata_mmap.as_mut_ptr();
        let slice = offset_init.to_be_bytes();
        let src_ptr = slice.as_ptr();

        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, slice.len());
    }

    let slave_file_segment_bytes = {
        let slave_config = SLAVE_CONFIG.lock().await;
        slave_config.slave_file_segment_bytes as u64
    };
    let data_file_len = slave_cache_struct.data_file.metadata().await?.len();

    if data_file_len > slave_file_segment_bytes {
        slave_cache_struct.data_file.flush().await?;
        slave_cache_struct.index_file.flush().await?;

        return Ok(Some(file_key.clone()));
    }

    return Ok(None);
}

/**
获得 缓存的文件对象
**/
async fn get_cache_file_object(
    file_key: &String,
) -> Result<Arc<Mutex<SlaveCacheStruct>>, DataLakeError> {
    let file_cache_pool = Arc::clone(&FILE_CACHE_POOL);

    let is = { file_cache_pool.contains_key(file_key) };

    let mutex_slave_cache_struct = match is {
        false => {
            let partition_path = get_slave_path(file_key).await?;

            let metadata_file_path = format!("{}/{}", partition_path, METADATA_LOG);

            let mut metadata_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(metadata_file_path)
                .await?;
            let mut metadata_mmap = unsafe { MmapMut::map_mut(&metadata_file)? };

            // 定义 offset 变量
            let offset_file_name = get_offset(None, file_key).await?;

            let log_file_path = format!(
                "{}/{}/{}{}",
                partition_path, LOG_FILE, offset_file_name, DATA_FILE_EXTENSION
            );
            let index_file_path = format!(
                "{}/{}/{}{}",
                partition_path, LOG_FILE, offset_file_name, INDEX_FILE_EXTENSION
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

            let ref_value = file_cache_pool
                .entry(file_key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(slave_cache_struct)));
            Arc::clone(ref_value.value())
        }
        true => {
            let ref_value = file_cache_pool.get(file_key).unwrap();
            Arc::clone(ref_value.value())
        }
    };

    return Ok(mutex_slave_cache_struct);
}

/**
获得 当前数据的offset
**/
async fn get_offset(
    offset_init: Option<i64>,
    partition_code: &String,
) -> Result<i64, DataLakeError> {
    let offset_file_name = match offset_init {
        Some(offset) => offset,
        None => {
            let partition_path = get_slave_path(partition_code).await?;

            let metadata_file_path = format!("{}/{}", partition_path, METADATA_LOG);

            let mut metadata_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(metadata_file_path)
                .await?;
            let mut metadata_mmap = unsafe { MmapMut::map_mut(&metadata_file)? };

            i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap())
        }
    };

    return Ok(offset_file_name);
}

/**
数据验证，查看数据是否符合 元数据的格式
**/
pub fn batch_format_matching(
    insert_single: &HashMap<&str, &str>,
    table_structure: &TableStructure,
) -> Result<(), DataLakeError> {
    let structure_col_type = &table_structure.col_type;
    let table_name = &table_structure.table_name;

    type_verification(structure_col_type, insert_single, table_name)?;

    return Ok(());
}

pub fn type_verification(
    metadata_col_type: &HashMap<String, (DataType, ColumnConfigJudgment, Option<String>)>,
    insert_data: &HashMap<&str, &str>,
    table_name: &String,
) -> Result<(), DataLakeError> {
    for (col_name, value) in insert_data.iter() {
        match metadata_col_type.get(*col_name) {
            Some((data_type, column_conf_judg, _)) => {
                //验证属性配置是否匹配
                conf_verification(*col_name, *value, column_conf_judg)?;
                // 验证类型是否匹配
                verification_type(*col_name, *value, data_type)?;
            }
            None => {
                if *col_name != CRUD_TYPE {
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
    col_name: &str,
    col_value: &str,
    column_conf_judg: &ColumnConfigJudgment,
) -> Result<(), DataLakeError> {
    match column_conf_judg {
        ColumnConfigJudgment::PRIMARY_KEY => {
            if col_value == const_property::NULL_STR {
                return Err(DataLakeError::custom(format!("{} 为主键 不允许为空", col_name)));
            }
        }
        ColumnConfigJudgment::NOT_NULL => {
            if col_value == const_property::NULL_STR {
                return Err(DataLakeError::custom(format!("{} 不允许为空", col_name)));
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
    col_name: &str,
    col_value: &str,
    data_type: &DataType,
) -> Result<(), DataLakeError> {
    
    if col_value != const_property::NULL_STR {
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
    }
    return Ok(());
}
