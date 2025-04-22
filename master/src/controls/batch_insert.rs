use crate::controls::metadata::get_metadata;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{
    BatchInsertTruth, ColumnConfigJudgment, DataType, SlaveBatchData, SlaveInsert, TableStructure,
};
use entity_lib::entity::SlaveEntity::SlaveMessage;
use public_function::string_trait::StringFunction;
use std::collections::HashMap;
use std::sync::LazyLock;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub static INSERT_TCPSTREAM_CACHE_POOL: LazyLock<Mutex<HashMap<String, TcpStream>>> =
    LazyLock::new(|| Mutex::new(HashMap::<String, TcpStream>::new()));

pub async fn batch_insert_data(
    mut batch_insert: BatchInsertTruth,
    uuid: &String,
) -> Result<(), DataLakeError> {
    let table_name = &batch_insert.table_name;
    let table_structure = get_metadata(&table_name).await?;

    let map_insert = batch_format_matching(&mut batch_insert, &table_structure, uuid).await?;

    for (key, list) in map_insert {
        let array = key.split("|=_=|").collect::<Vec<&str>>();
        let address = &array[1];
        let table_name = &array[2];
        let partition_code = &array[3];

        let slave_insert = SlaveInsert {
            table_name: table_name.to_string(),
            data: list,
            partition_code: partition_code.to_string(),
        };

        let slave_message = SlaveMessage::batch_insert(slave_insert);

        let bytes = bincode::serialize(&slave_message)?;
        let bytes_len = bytes.len() as i32;

        let mut map_guard = INSERT_TCPSTREAM_CACHE_POOL.lock().await;
        let tcp_stream = match map_guard.get_mut(&key) {
            Some(value) => value,
            None => {
                let mut stream = TcpStream::connect(address).await?;
                map_guard.insert(key.clone(), stream);
                let value = map_guard.get_mut(&key).unwrap();
                value
            }
        };

        tcp_stream.write_i32(bytes_len).await?;
        tcp_stream.write(&bytes).await?;
    }

    return Ok(());
}

pub async fn batch_format_matching(
    batch_insert: &mut BatchInsertTruth,
    table_structure: &TableStructure,
    uuid: &String,
) -> Result<HashMap<String, Vec<SlaveBatchData>>, DataLakeError> {
    let mut res_map = HashMap::<String, Vec<SlaveBatchData>>::new();

    let batch_data = &mut batch_insert.data;
    let table_name = &batch_insert.table_name;

    let structure_col_type = &table_structure.col_type;

    for batch_datum in batch_data.iter_mut() {
        // 验证 插入的数据  与表的元数据是否匹配
        let crud_type = batch_datum.get("_crud_type").unwrap().clone();
        batch_datum.remove("_crud_type");

        type_verification(structure_col_type, batch_datum, table_name).await?;

        let partition_address = &table_structure.partition_address;
        let major_key = &table_structure.major_key;

        let data_major_key = batch_datum.get(major_key).unwrap().clone();

        let partition_code = data_major_key.hash_code() as usize % table_structure.partition_number;
        let par_addres = partition_address.get(&partition_code).unwrap();

        let data_json = serde_json::to_string(batch_datum)?;
        let ins = SlaveBatchData {
            major_key: data_major_key,
            data: data_json,
            _crud_type: crud_type,
        };

        let tcp_key = format!(
            "{}|=_=|{}|=_=|{}|=_=|{}",
            uuid, &par_addres, &table_name, &partition_code
        );

        res_map.entry(tcp_key).or_insert(Vec::new()).push(ins);
    }

    return Ok(res_map);
}

pub async fn type_verification(
    metadata_col_type: &HashMap<String, (DataType, ColumnConfigJudgment, Option<String>)>,
    insert_data: &HashMap<String, String>,
    table_name: &String,
) -> Result<(), DataLakeError> {
    for (col_name, value) in insert_data.iter() {
        match metadata_col_type.get(col_name) {
            Some((data_type, column_conf_judg, default_value)) => {
                // 验证类型是否匹配
                verification_type(col_name, value, data_type)?;
                //验证属性配置是否匹配
                conf_verification(col_name, value, column_conf_judg)?;
            }
            None => {
                return Err(DataLakeError::CustomError(format!(
                    "{} 表内不存在 {} 数据",
                    table_name, col_name
                )));
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
                return Err(DataLakeError::CustomError(format!("{} 列为 空", col_name)));
            }
        }
        ColumnConfigJudgment::NOT_NULL => {
            if col_value.trim().is_empty() {
                return Err(DataLakeError::CustomError(format!("{} 列为 空", col_name)));
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
                return Err(DataLakeError::CustomError(format!(
                    "{} 列转换为 int 失败，检查插入的数据: {}",
                    col_name, col_value
                )));
            }
        },
        DataType::float => match col_value.parse::<f32>() {
            Ok(_) => {}
            Err(_) => {
                return Err(DataLakeError::CustomError(format!(
                    "{} 列转换为 float 失败，检查插入的数据: {}",
                    col_name, col_value
                )));
            }
        },
        DataType::boolean => match col_value.parse::<bool>() {
            Ok(_) => {}
            Err(_) => {
                return Err(DataLakeError::CustomError(format!(
                    "{} 列转换为 bool 失败，检查插入的数据: {}",
                    col_name, col_value
                )));
            }
        },
        DataType::long => match col_value.parse::<i64>() {
            Ok(_) => {}
            Err(_) => {
                return Err(DataLakeError::CustomError(format!(
                    "{} 列转换为 bool 失败，检查插入的数据: {}",
                    col_name, col_value
                )));
            }
        },
        _ => {
            return Err(DataLakeError::CustomError(format!(
                "{}  不符合任何数据类型",
                col_name
            )));
        }
    }

    return Ok(());
}
