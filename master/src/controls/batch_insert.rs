use crate::controls::metadata::get_metadata;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{
    BatchInsertTruth, ColumnConfigJudgment, DataType, Info, PartitionInfo, SlaveBatchData,
    SlaveInsert, TableStructure,
};
use entity_lib::entity::SlaveEntity::SlaveMessage;
use public_function::PosttingTcpStream::DataLakeTcpStream;
use public_function::string_trait::StringFunction;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinError;

pub static INSERT_TCPSTREAM_CACHE_POOL: LazyLock<Mutex<HashMap<String, DataLakeTcpStream>>> =
    LazyLock::new(|| Mutex::new(HashMap::<String, DataLakeTcpStream>::new()));

pub async fn batch_insert_data(
    batch_insert: BatchInsertTruth,
    uuid: Arc<String>,
) -> Result<(), DataLakeError> {

    let vec_data_map = batch_insert.data;
    
    if vec_data_map.len() == 0 {
        return Ok(());
    }
    
    
    let table_name = &batch_insert.table_name;
    let mut table_structure = get_metadata(&table_name).await?;

    let mut res_map = HashMap::<i32, Vec<HashMap<String, String>>>::new();
    
    match &batch_insert.partition_code {
        None => {
            let major_key = &table_structure.major_key;
            let partition_number = table_structure.partition_number as i32;
            for data_map in vec_data_map {
                match data_map.get(major_key) {
                    None => {
                        return Err(DataLakeError::custom(format!(
                            "{:?}  这行数据没有主键列 {}",
                            data_map, major_key
                        )));
                    }
                    Some(major_value) => {
                        let partition_code = major_value.hash_code() % partition_number;
                        res_map
                            .entry(partition_code)
                            .or_insert(Vec::<HashMap<String, String>>::new())
                            .push(data_map);
                    }
                }
            }
        }
        Some(specified_partition_code) => {
            let partition_code = specified_partition_code.parse::<i32>()?;
            res_map.entry(partition_code).or_insert(vec_data_map);
        }
    }

    let mut join_handle_set = tokio::task::JoinSet::new();
    for (partition_code, vec_map) in res_map {
        
        let table_name_clone = table_name.clone();
        
        let table_structure_clone = table_structure.clone();
        
        let uuid_clone = Arc::clone(&uuid);
        
        
        join_handle_set.spawn(async move {
            
            let mut partition_address = table_structure_clone.partition_address.clone();
            
            let slave_insert = SlaveInsert {
                table_name: table_name_clone,
                data: vec_map,
                partition_code: partition_code.to_string(),
                table_structure: table_structure_clone,
            };

            let slave_message = SlaveMessage::batch_insert(slave_insert);

            let bytes = bincode::serialize(&slave_message)?;
            let bytes_len = bytes.len() as i32;

            
            
            let mut map_guard = INSERT_TCPSTREAM_CACHE_POOL.lock().await;
            
            let tcp_key = format!("{}-{}", uuid_clone.as_ref(), partition_code);
            
            let tcp_stream = match map_guard.get_mut(&tcp_key) {
                Some(value) => value,
                None => {
                    let partition_address = &mut partition_address;
                    let partition_info_vec = partition_address
                        .get_mut(&(partition_code as usize))
                        .unwrap();

                    let stream = DataLakeTcpStream::connect(partition_info_vec).await?;
                    map_guard.insert(tcp_key.clone(), stream);
                    let value = map_guard.get_mut(&tcp_key).unwrap();
                    value
                }
            };
            tcp_stream.write_i32(bytes_len).await?;
            tcp_stream.write_all(&bytes).await?;

            let bytes_len = tcp_stream.read_i32().await?;
            if bytes_len == -2 {
                let meass_len = tcp_stream.read_i32().await?;
                let mut message = vec![0; meass_len as usize];
                tcp_stream.read_exact(message.as_mut_slice()).await?;

                let message_str = String::from_utf8(message)?;
                return Err(DataLakeError::custom(message_str));

            }
          return Ok(());
        });
        
    }

    while let Some(res) = join_handle_set.join_next().await {
        match res {
            Ok(ee) => {
                if let Err(e) = ee {
                    return Err(e);
                }
            }
            Err(e) => {
                eprintln!("Task failed: {:?}", e);
                return Err(DataLakeError::custom("插入数据的协程报错了。".to_string()));

            }
        }
    }


    return Ok(());
}
