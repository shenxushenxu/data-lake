use entity_lib::entity::DataLakeEntity::{BatchData, SlaveBatchData, SlaveInsert};
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::SlaveMessage;
use rayon::prelude::*;
use std::collections::HashMap;
use std::panic::Location;
use std::sync::Arc;
use std::time::Instant;
use chrono::{DateTime, Utc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex};
use entity_lib::entity::const_property;
use entity_lib::function::BufferObject::INSERT_TCPSTREAM_CACHE_POOL;
use entity_lib::function::PosttingTcpStream::DataLakeTcpStream;
use entity_lib::function::string_trait::StringFunction;
use entity_lib::function::table_structure::get_table_structure;

pub async fn batch_insert_data<'a>(
    message_bytes: Vec<u8>,
    uuid: Arc<String>,
) -> Result<(), DataLakeError> {

    let message_leak = Box::leak(Box::new(message_bytes));

 
    
    let batch_data_result = {
        let raw_ptr = message_leak as *mut Vec<u8>;
        // 临时借用仅在此作用域内存在
        let slice = unsafe { &*raw_ptr };
        BatchData::entity_decode(slice)
    };
    
    
    
    match batch_data_result {
        Ok(batch_data) => {

            let batch_data_leak = Box::leak(Box::new(batch_data));

            let vec_data_map = &batch_data_leak.data;

            if vec_data_map.len() == 0 {
                unsafe {
                    Box::from_raw(message_leak);
                    Box::from_raw(batch_data_leak);
                }

                return Ok(());
            }

            let table_name = batch_data_leak.table_name;
            let mut table_structure = match get_table_structure(&table_name).await {
                Ok(table_structure) => table_structure,
                Err(e) => {
                    unsafe {
                        Box::from_raw(message_leak);
                        Box::from_raw(batch_data_leak);
                    }
                    return Err(e);
                }
            };

            let mut res_map = HashMap::<i32, SlaveBatchData>::new();

            let major_key = &table_structure.major_key;
            let partition_number = table_structure.partition_number as i32;

            let major_index = match batch_data_leak.get_column_index(major_key) {
                Ok(major_index) => major_index,
                Err(e) => {
                    unsafe {
                        Box::from_raw(message_leak);
                        Box::from_raw(batch_data_leak);
                    }
                    return Err(e);
                }
            };

            let column = { batch_data_leak.column.clone() };

            let mut index = batch_data_leak.get_data_size();
            loop {
                if index != 0 {
                    index = index - 1;
                    let line_data = batch_data_leak.get_line(index);
                    let major_value = &line_data[major_index];
                    if const_property::NULL_STR == *major_value {
                        return Err(DataLakeError::custom(String::from("主键不能为空")));
                    }
                    let partition_code = major_value.hash_code() % partition_number;

                    res_map
                        .entry(partition_code)
                        .or_insert(SlaveBatchData::new(column.clone()))
                        .push_data(line_data);
                } else {
                    break;
                }
            }
            
            let mut join_handle_set = tokio::task::JoinSet::new();
            for (partition_code, slave_batch_data) in res_map {

                
                let table_name_clone = table_name;

                let table_structure_clone = table_structure.clone();

                let uuid_clone = Arc::clone(&uuid);

                let insert_tcpstream_cache_pool = Arc::clone(&INSERT_TCPSTREAM_CACHE_POOL);
                join_handle_set.spawn(async move {


                    
                    let mut partition_address = table_structure_clone.partition_address.clone();
                    let tcp_key = format!("{}-{}", uuid_clone.as_ref(), partition_code);

                    let is = { insert_tcpstream_cache_pool.contains_key(&tcp_key) };

                    let tcp_stream = match is {
                        true => {
                            Arc::clone(insert_tcpstream_cache_pool.get(&tcp_key).unwrap().value())
                        }
                        false => {
                            let partition_address = &mut partition_address;
                            let partition_info_vec = partition_address
                                .get(&(partition_code as usize))
                                .unwrap();

                            let stream = DataLakeTcpStream::connect(partition_info_vec.clone(),
                                                                    table_name.to_string(),
                                                                    partition_code as usize).await?;

                            let ref_tcp = insert_tcpstream_cache_pool
                                .entry(tcp_key.clone())
                                .or_insert_with(|| Arc::new(Mutex::new(stream)));

                            Arc::clone(ref_tcp.value())
                        }
                    };

                    let mut stream = tcp_stream.lock().await;
                    
                    let slave_message = SlaveMessage::batch_insert(table_structure_clone);
                    let bytes = bincode::serialize(&slave_message)?;
                    let bytes_len = bytes.len() as i32;
                    stream.write_i32(bytes_len).await?;
                    stream.write_all(&bytes).await?;

                    let slave_insert = SlaveInsert {
                        table_name: table_name_clone,
                        data: slave_batch_data,
                        partition_code: partition_code,
                    };
                    let slave_insert_bytes = slave_insert.serialize()?;
                    stream.write_i32(slave_insert_bytes.len() as i32).await?;
                    stream.write_all(&slave_insert_bytes).await?;

                    
                    
                    let bytes_len = stream.read_i32().await?;
                    if bytes_len == -2 {
                        let meass_len = stream.read_i32().await?;
                        let mut message = vec![0; meass_len as usize];
                        stream.read_exact(message.as_mut_slice()).await?;

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
                            unsafe {
                                Box::from_raw(message_leak);
                                Box::from_raw(batch_data_leak);
                            }
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Task failed: {:?}", e);
                        unsafe {
                            Box::from_raw(message_leak);
                            Box::from_raw(batch_data_leak);
                        }
                        return Err(DataLakeError::custom("插入数据的协程报错了。".to_string()));
                    }
                }
            }
            unsafe {
                Box::from_raw(message_leak);
                Box::from_raw(batch_data_leak);
            }
            
            return Ok(());
        }
        Err(e) => {
            unsafe {
                Box::from_raw(message_leak);
            }
            return Err(e);
        }
    };
}
