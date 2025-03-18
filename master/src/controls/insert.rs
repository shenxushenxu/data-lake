use crate::controls::metadata::get_metadata;
use entity_lib::entity::MasterEntity::{DataType, Insert, MesterInsert, TableStructure};
use entity_lib::entity::SlaveEntity::SlaveMessage;
use public_function::hashcode;
use std::collections::HashMap;
use std::num::{ParseFloatError, ParseIntError};
use std::str::ParseBoolError;
use std::sync::LazyLock;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use entity_lib::entity::Error::DataLakeError;

pub static INSERT_TCPSTREAM_CACHE_POOL: LazyLock<Mutex<HashMap<String, TcpStream>>> =
    LazyLock::new(|| Mutex::new(HashMap::<String, TcpStream>::new()));


pub async fn type_verification(metadata_col_type: &HashMap<String, DataType>, insert_data: &HashMap<String, String>, table_name:&String) -> Result<(), DataLakeError>{
    for (col_name, value) in insert_data.iter() {
        match metadata_col_type.get(col_name) {
            Some(data_type) => match data_type {
                DataType::string => {
                    value.to_string();
                }
                DataType::int => {
                    match value.parse::<i32>() {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(DataLakeError::CustomError(format!("{} 列转换为 int 失败，检查插入的数据: {}", col_name, value)))
                        }
                    }
                }
                DataType::float => {
                    match value.parse::<f32>(){
                        Ok(_) => {}
                        Err(_) => {
                            return Err(DataLakeError::CustomError(format!("{} 列转换为 float 失败，检查插入的数据: {}", col_name, value)))
                        }
                    }
                }
                DataType::boolean => {
                    match value.parse::<bool>(){
                        Ok(_) => {}
                        Err(_) => {
                            return Err(DataLakeError::CustomError(format!("{} 列转换为 bool 失败，检查插入的数据: {}", col_name, value)))
                        }
                    }
                }
                DataType::long => {
                    match value.parse::<i64>(){
                        Ok(_) => {}
                        Err(_) => {
                            return Err(DataLakeError::CustomError(format!("{} 列转换为 bool 失败，检查插入的数据: {}", col_name, value)))
                        }
                    }
                }
                _ => {

                    return Err( DataLakeError::CustomError(format!("{}  不符合任何数据类型", col_name)));
                }
            },
            None => {
                return Err(DataLakeError::CustomError(format!("{} 内没有 {}  列", table_name, col_name)));
            }
        }
    }

    return Ok(());
}


pub async fn format_matching(
    mester_insert: &MesterInsert,
    table_structure: &TableStructure,
) -> Result<Insert, DataLakeError> {

    let insert_data_yu = &mester_insert.data;
    let table_name = &mester_insert.table_name;

    let structure_col_type = &table_structure.col_type;

    // 验证插入的数据 与表的类型是否匹配
    type_verification(structure_col_type, insert_data_yu, table_name).await?;

    let partition_address = &table_structure.partition_address;
    let major_key = &table_structure.major_key;
    let crud = &mester_insert.crud;
    let data_major_key = match insert_data_yu.get(major_key){
        None => {return Err(DataLakeError::CustomError(format!("插入的数据内不包含 {} 主键", major_key)))},
        Some(value) => value
    };

    let partition_code = data_major_key.hash_code() as usize % table_structure.partition_number;
    let par_addres = partition_address.get(&partition_code).unwrap();

    let data_json = serde_json::to_string(insert_data_yu)?;
    let ins = Insert {
        table_name: table_name.clone(),
        major_key: data_major_key.clone(),
        data: data_json,
        crud: crud.clone(),
        address: par_addres.clone(),
        partition_code: partition_code.to_string(),
    };

    return Ok(ins);
}

pub async fn insert_data(mester_insert: MesterInsert, uuid: &String) -> Result<(), DataLakeError> {
    let table_name = &mester_insert.table_name;
    let table_structure = get_metadata(&table_name).await?;

    let insert_struct = format_matching(&mester_insert, &table_structure).await?;

    let address = &insert_struct.address.clone();
    let table_name = &insert_struct.table_name;
    let partition_code = &insert_struct.partition_code;

    let tcp_key = format!("{}_{}_{}_{}", uuid, address, table_name, partition_code);
    let slave_message = SlaveMessage::insert(insert_struct);

    let insert_byte_array = bincode::serialize(&slave_message)?;

    let mut map_guard = INSERT_TCPSTREAM_CACHE_POOL.lock().await;

    let tcp_stream = match map_guard.get_mut(&tcp_key) {
        Some(tcp_stream) => {
            tcp_stream
        }
        None => {
            let mut stream = TcpStream::connect(address).await?;
            map_guard.insert(tcp_key.clone(), stream);
            map_guard.get_mut(&tcp_key).unwrap()
        }
    };

    tcp_stream.write_i32(insert_byte_array.len() as i32).await?;
    tcp_stream.write_all(&insert_byte_array).await?;

    public_function::read_error(tcp_stream).await?;


    return Ok(());
    // 表结构
}

