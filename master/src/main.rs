mod controls;
mod mechanism;

use crate::controls::alter::{alter_add, alter_orop};
use crate::controls::batch_insert::INSERT_TCPSTREAM_CACHE_POOL;
use crate::controls::compress_table::compress_table;
use crate::controls::create::create_table;
use crate::controls::drop_table::drop_table_operation;
use crate::controls::metadata::get_metadata;
use crate::controls::query::query_daql;
use crate::controls::stream_read::{STREAM_TCP_TABLESTRUCTURE, stream_read_data};
use crate::mechanism::replicas::copy_sync_notif;
use daql_analysis::daql_analysis_function;
use entity_lib::entity::DaqlEntity::DaqlType;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{BatchInsertTruth, Statement};
use log::{error, info};
use public_function::{MASTER_CONFIG, MasterConfig, load_properties, write_error};
use serde_json::json;
use snap::raw::{Decoder, Encoder};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::controls::max_offset::get_max_offset;



/**
-1 是停止
-2 是异常
**/
#[tokio::main]
async fn main() {
    {
        let args: Vec<String> = std::env::args().collect();
        println!("{:?}", args);
        let file_path = args.get(1).unwrap();

        let map = load_properties(file_path);

        let master_data_port = map.get("master.data.port").unwrap().clone();
        let master_path = map.get("master.data.path").unwrap().clone();
        let nodes = map.get("slave.nodes").unwrap().clone();

        let master_data_path = master_path
            .split(",")
            .map(|x| x.trim().to_string())
            .collect::<Vec<String>>();
        let slave_nodes = nodes
            .split(",")
            .map(|x| x.trim().to_string())
            .collect::<Vec<String>>();

        let mut master_config = MASTER_CONFIG.lock().await;
        unsafe {
            *master_config = MasterConfig::new(master_data_port, master_data_path, slave_nodes);
        }
    }

    // 初始化日志系统
    env_logger::init();

    let master_main = data_interface();
    let replicas_sync = copy_sync_notif();
    
    tokio::join!(master_main, replicas_sync);
}

/**
数据操作的接收端
**/
fn data_interface() -> JoinHandle<()> {
    let joinhandle = tokio::spawn(async move {
        let master_config = MASTER_CONFIG.lock().await;
        let master_data_port = &master_config.master_data_port;

        let listener = TcpListener::bind(master_data_port).await.unwrap();

        drop(master_config);

        loop {
            let (mut tcp_stream, _) = listener.accept().await.unwrap();

            tokio::spawn(async move {
                let uuid = Uuid::new_v4().to_string();
                let uuid_arc = Arc::new(uuid);

                let (mut read, mut write) = tcp_stream.split();

                loop {
                    match read.read_i32().await {
                        Ok(message_len) => {
                            let mut message = vec![0; message_len as usize];

                            read.read_exact(message.as_mut_slice()).await.unwrap();

                            let message_str = String::from_utf8(message).unwrap();
                            
                            let statement = serde_json::from_str::<Statement>(&message_str).unwrap();

                            match statement {
                                Statement::sql(daql) => match daql_analysis_function(&daql).await {

                                    Ok(daqltype) => match daqltype {
                                        DaqlType::CREATE_TABLE(tablestructure) => {
                                            match create_table(tablestructure).await {
                                                Ok(_) => {
                                                    write.write_i32(-1).await.unwrap();
                                                }
                                                Err(e) => {
                                                    public_function::write_error(e, &mut write)
                                                        .await;
                                                }
                                            }
                                        }
                                        DaqlType::SELECT_TABLE(querymessage) => {
                                            match query_daql(querymessage).await {
                                                Ok(option_vec) => {
                                                    if let Some(vec) = option_vec {
                                                        for ve in vec {
                                                            let byt = ve.as_bytes();
                                                            let write_len = byt.len();
                                                            write
                                                                .write_i32(write_len as i32)
                                                                .await
                                                                .unwrap();
                                                            write.write_all(byt).await.unwrap();
                                                        }
                                                    }
                                                    write.write_i32(-1).await.unwrap();
                                                }
                                                Err(e) => {
                                                    public_function::write_error(e, &mut write)
                                                        .await;
                                                }
                                            }
                                        }
                                        DaqlType::ALTER_OROP(alterorop) => {
                                            match alter_orop(alterorop).await {
                                                Ok(_) => {
                                                    write.write_i32(-1).await.unwrap();
                                                }
                                                Err(e) => {
                                                    public_function::write_error(e, &mut write)
                                                        .await;
                                                }
                                            }
                                        }
                                        DaqlType::ALTER_ADD(alteradd) => {
                                            match alter_add(alteradd).await {
                                                Ok(_) => {
                                                    write.write_i32(-1).await.unwrap();
                                                }
                                                Err(e) => {
                                                    public_function::write_error(e, &mut write)
                                                        .await;
                                                }
                                            }
                                        }
                                        DaqlType::SHOW_TABLE(table_name) => {
                                            let metadata_return = get_metadata(&table_name).await;
                                            match metadata_return {
                                                Ok(table_structure) => {
                                                    let metadtat_message =
                                                        serde_json::to_string(&table_structure).unwrap();
                                                    let byt = metadtat_message.as_bytes();

                                                    let write_len = byt.len();
                                                    let write_message = byt;

                                                    write.write_i32(-3).await.unwrap();

                                                    write.write_i32(write_len as i32)
                                                        .await
                                                        .unwrap();
                                                    write.write_all(write_message).await.unwrap();
                                                    write.write_i32(-1).await.unwrap();
                                                }
                                                Err(e) => {
                                                    public_function::write_error(e, &mut write)
                                                        .await;
                                                }
                                            }
                                        }
                                        DaqlType::COMPRESS_TABLE(table_name) => {
                                            let compress_return = compress_table(&table_name).await;

                                            match compress_return {
                                                Ok(_) => {
                                                    write.write_i32(-1).await.unwrap();
                                                }
                                                Err(e) => {
                                                    public_function::write_error(e, &mut write)
                                                        .await;
                                                }
                                            }
                                        }
                                        DaqlType::DROP_TABLE(table_name) => {
                                            match drop_table_operation(&table_name).await {
                                                Ok(_) => {
                                                    write.write_i32(-1).await.unwrap();
                                                }
                                                Err(e) => {
                                                    public_function::write_error(e, &mut write)
                                                        .await;
                                                }
                                            }
                                        },
                                        DaqlType::MAX_OFFSET(table_name) => {
                                            match get_max_offset(&table_name).await{
                                                Ok(offset_map) => {
                                                    write.write_i32(-4).await.unwrap();
                                                    
                                                    let json = serde_json::to_string(&offset_map).unwrap();
                                                    let bytes = json.as_bytes();
                                                    let bytes_len = bytes.len() as i32;
                                                    write.write_i32(bytes_len).await.unwrap();
                                                    write.write_all(bytes).await.unwrap();
                                                    
                                                    write.write_i32(-1).await.unwrap();
                                                }
                                                Err(e) => {
                                                    public_function::write_error(e, &mut write)
                                                        .await;
                                                }
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        public_function::write_error(e, &mut write).await;
                                    }
                                },
                                Statement::stream_read(stream_read) => {
                                    let stream_return = stream_read_data(stream_read, uuid_arc.as_ref()).await;

                                    match stream_return {
                                        Ok(mut vec_data) => {
                                            
                                            if let Some(data) = vec_data {
                                                for datum in data.iter() {
                                                    write.write_i32(datum.len() as i32).await.unwrap();
                                                    write.write_all(&datum).await.unwrap();
                                                }
                                            }
                                            
                                            // while let Some(message) = receiver.recv().await {
                                            //     if let Some(message_bytes) = message {
                                            //         write
                                            //             .write_i32(message_bytes.len() as i32)
                                            //             .await
                                            //             .unwrap();
                                            //         write.write_all(&message_bytes).await.unwrap();
                                            //     }
                                            // }
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }

                                Statement::batch_insert(batch_insert) => {

                                    
                                    
                                    let data = &batch_insert.data;
                                    let table_name = batch_insert.table_name;
                                    let partition_code = batch_insert.partition_code;

                                    let mut decoder = Decoder::new();
                                    let message_bytes = decoder
                                        .decompress_vec(data)
                                        .unwrap_or_else(|e| panic!("解压失败: {}", e));
                                    let message_str = std::str::from_utf8(&message_bytes).unwrap();
                                    let hashmap = serde_json::from_str::<Vec<HashMap<String, String>>>(message_str).unwrap();

                                    let batch = BatchInsertTruth {
                                        table_name: table_name,
                                        data: hashmap,
                                        partition_code: partition_code,
                                    };


                                    let arc_uuid_clone = Arc::clone(&uuid_arc);
                                    let batch_return =
                                        controls::batch_insert::batch_insert_data(batch, arc_uuid_clone)
                                            .await;

                                    match batch_return {
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                            }
                        }

                        Err(e) => {
                            info!("{:?}", e);
                            // 输入插入连接断开，清理缓存中的连接
                            let mut remove_vec = Vec::<String>::new();
                            let mut mutex_guard = INSERT_TCPSTREAM_CACHE_POOL.lock().await;

                            mutex_guard.keys().for_each(|k| {
                                if k.contains(uuid_arc.as_ref()) {
                                    remove_vec.push(k.clone());
                                }
                            });

                            remove_vec.iter().for_each(|k| {
                                mutex_guard.remove(k).unwrap();
                            });

                            // ----------  流消费连接断开，清理缓存中的连接
                            let mut remove_vec = Vec::<String>::new();
                            let mut mutex_guard = STREAM_TCP_TABLESTRUCTURE.lock().await;

                            mutex_guard.keys().for_each(|k| {
                                if k.contains(uuid_arc.as_ref()) {
                                    remove_vec.push(k.clone());
                                }
                            });

                            remove_vec.iter().for_each(|k| {
                                mutex_guard.remove(k).unwrap();
                            });
                            break;
                        }
                    }
                }
            });
        }
    });

    return joinhandle;
}
