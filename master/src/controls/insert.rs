use crate::controls::metadata::get_metadata;
use entity_lib::entity::MasterEntity::{DataType, Insert, MesterInsert, TableStructure};
use entity_lib::entity::SlaveEntity::SlaveMessage;
use public_function::hashcode;
use std::collections::HashMap;
use std::sync::LazyLock;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub static INSERT_TCPSTREAM_CACHE_POOL: LazyLock<Mutex<HashMap<String, TcpStream>>> =
    LazyLock::new(|| Mutex::new(HashMap::<String, TcpStream>::new()));

pub async fn format_matching(
    mester_insert: &MesterInsert,
    table_structure: &TableStructure,
) -> Result<Insert, String> {
    let insert_data_yu = &mester_insert.data;
    let table_name = &mester_insert.table_name;

    let structure_col_type = &table_structure.col_type;

    for (col_name, value) in insert_data_yu.iter() {
        match structure_col_type.get(col_name) {
            Some(data_type) => match data_type {
                DataType::string => {
                    value.to_string();
                }
                DataType::int => {
                    value.parse::<i32>().unwrap();
                }
                DataType::float => {
                    value.parse::<f32>().unwrap();
                }
                DataType::boolean => {
                    value.parse::<bool>().unwrap();
                }
                DataType::long => {
                    value.parse::<i64>().unwrap();
                }
                _ => {
                    return Err(format!("{}  不符合任何数据类型", col_name));
                }
            },
            None => {
                return Err(format!("{} 内没有 {}  列", table_name, col_name));
            }
        }
    }

    let partition_address = &table_structure.partition_address;
    let major_key = &table_structure.major_key;
    let crud = &mester_insert.crud;
    let data_major_key = insert_data_yu.get(major_key).unwrap();

    let partition_code = data_major_key.hash_code() as usize % table_structure.partition_number;
    let par_addres = partition_address.get(&partition_code).unwrap();

    let data_json = serde_json::to_string(insert_data_yu).unwrap();
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

pub async fn insert_data(mester_insert: MesterInsert, uuid: &String) {
    let table_name = &mester_insert.table_name;
    let table_structure = get_metadata(&table_name).await;

    let insert_struct = format_matching(&mester_insert, &table_structure)
        .await
        .unwrap();
    let address = &insert_struct.address.clone();
    let table_name = &insert_struct.table_name;
    let partition_code = &insert_struct.partition_code;

    let tcp_key = format!("{}_{}_{}_{}", uuid, address, table_name, partition_code);
    let slave_message = SlaveMessage::insert(insert_struct);

    let insert_byte_array = bincode::serialize(&slave_message).unwrap();

    let mut map_guard = INSERT_TCPSTREAM_CACHE_POOL.lock().await;

    match map_guard.get_mut(&tcp_key) {
        Some(tcp_stream) => {
            tcp_stream
                .write_i32(insert_byte_array.len() as i32)
                .await
                .unwrap();
            tcp_stream.write_all(&insert_byte_array).await.unwrap();
        }
        None => {
            let mut stream = TcpStream::connect(address).await.unwrap();

            stream
                .write_i32(insert_byte_array.len() as i32)
                .await
                .unwrap();
            stream.write_all(&insert_byte_array).await.unwrap();

            map_guard.insert(tcp_key, stream);
        }
    }
    // 表结构
}

