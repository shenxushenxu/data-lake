use crate::controls::metadata::get_metadata;
use chrono::{Datelike, Local, Timelike};
use dashmap::DashMap;
use entity_lib::entity::DataLakeEntity::{BatchData, SlaveBatchData};
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::SlaveInsert;
use entity_lib::entity::SlaveEntity::SlaveMessage;
use public_function::PosttingTcpStream::DataLakeTcpStream;
use public_function::string_trait::StringFunction;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};

pub static INSERT_TCPSTREAM_CACHE_POOL: LazyLock<Arc<DashMap<String, Mutex<DataLakeTcpStream>>>> =
    LazyLock::new(|| Arc::new(DashMap::<String, Mutex<DataLakeTcpStream>>::new()));

pub async fn batch_insert_data(
    mut batch_data: BatchData,
    uuid: Arc<String>,
) -> Result<(), DataLakeError> {
    let vec_data_map = &batch_data.data;
    if vec_data_map.len() == 0 {
        return Ok(());
    }

    let table_name = batch_data.table_name.clone();
    let mut table_structure = get_metadata(&table_name).await?;

    let mut res_map = HashMap::<i32, SlaveBatchData>::new();

    let start_now = Instant::now();

    let major_key = &table_structure.major_key;
    let partition_number = table_structure.partition_number as i32;

    let major_index = batch_data.get_column_index(major_key)?;

    loop {
        if batch_data.get_data_size() != 0 {
            let line_data = batch_data.get_line();
            let major_value = &line_data[major_index];
            let partition_code = major_value.hash_code() % partition_number;
            res_map
                .entry(partition_code)
                .or_insert(SlaveBatchData::new(batch_data.column.clone()))
                .push_data(line_data);
        } else {
            break;
        }
    }

    println!(
        " match &batch_data.partition_code:  {:?}",
        start_now.elapsed()
    );

    let mut join_handle_set = tokio::task::JoinSet::new();
    for (partition_code, slave_batch_data) in res_map {
        let table_name_clone = table_name.clone();

        let table_structure_clone = table_structure.clone();

        let uuid_clone = Arc::clone(&uuid);

        let insert_tcpstream_cache_pool = Arc::clone(&INSERT_TCPSTREAM_CACHE_POOL);
        join_handle_set.spawn(async move {
            let mut partition_address = table_structure_clone.partition_address.clone();

            let slave_insert = SlaveInsert {
                table_name: table_name_clone,
                data: slave_batch_data,
                partition_code: partition_code.to_string(),
                table_structure: table_structure_clone,
            };

            let slave_message = SlaveMessage::batch_insert(slave_insert);

            let bytes = bincode::serialize(&slave_message)?;
            let bytes_len = bytes.len() as i32;

            let tcp_key = format!("{}-{}", uuid_clone.as_ref(), partition_code);

            let tcp_stream = match insert_tcpstream_cache_pool.contains_key(&tcp_key) {
                true => insert_tcpstream_cache_pool.get(&tcp_key).unwrap(),
                false => {
                    let partition_address = &mut partition_address;
                    let partition_info_vec = partition_address
                        .get_mut(&(partition_code as usize))
                        .unwrap();

                    let stream = DataLakeTcpStream::connect(partition_info_vec).await?;
                    insert_tcpstream_cache_pool.insert(tcp_key.clone(), Mutex::new(stream));
                    insert_tcpstream_cache_pool.get(&tcp_key).unwrap()
                }
            };

            let tcp_stream = tcp_stream.value();

            let mut stream = tcp_stream.lock().await;
            stream.write_i32(bytes_len).await?;
            stream.write_all(&bytes).await?;

            let now = Local::now();
            println!(
                "master向slave {}发送完成数据的时间： {}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
                partition_code,
                now.year(),
                now.month(),
                now.day(),
                now.hour(),
                now.minute(),
                now.second(),
                now.nanosecond() / 1_000_000
            );

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
