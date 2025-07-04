mod controls;
mod mechanism;

use crate::controls::compress_table::compress_table;
use crate::controls::create_table::create_table_controls;
use crate::controls::drop_table::drop_table_operation;
use crate::controls::insert_data::insert_operation;
use crate::controls::query_table::query;
use crate::controls::stream_read::stream_read;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::QueryItem;
use entity_lib::entity::SlaveEntity::SlaveMessage::drop_table;
use entity_lib::entity::SlaveEntity::{QueryMessage, ReplicaseSyncData, SlaveMessage};
use log::{error, info};
use public_function::{load_properties, SlaveConfig, SLAVE_CONFIG};
use std::path;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::controls::max_offset::get_max_offset;
use crate::mechanism::replicas::{follower_replicas_sync, Leader_replicas_sync};

/**
消息返回 
   -1 是消息完结
   -2 是错误
**/
#[tokio::main]
async fn main() {


    {

        let args: Vec<String> = std::env::args().collect();
        println!("{:?}", args);

        let file_path = args.get(1).unwrap();
        let map = load_properties(file_path);

        let slave_node = map.get("slave.node").unwrap().clone();
        let slave_data = map.get("slave.data").unwrap().clone();
        let slave_file_segment_bytes = map.get("slave.file.segment.bytes").unwrap().clone();

        let slave_data_vec = slave_data
            .split(",")
            .map(|x| x.trim().to_string())
            .collect::<Vec<String>>();

        let slave_file_segment_bytes = slave_file_segment_bytes.parse::<usize>().unwrap();
        
        let mut slave_config = SLAVE_CONFIG.lock().await;
        unsafe {
            *slave_config = SlaveConfig::new(slave_node, slave_data_vec, slave_file_segment_bytes);
        }
    }
    
    
    
    
    // 初始化日志系统
    env_logger::init();

    let slave = data_read_write();
    println!("slave  启动成功.......");
    tokio::join!(slave);
}

/**
请求分类
**/
fn data_read_write() -> JoinHandle<()> {
    let joinhandle = tokio::spawn(async move {

        let listener = {
            let slave_config = SLAVE_CONFIG.lock().await;
            let slave_node = &slave_config.slave_node;

            let listener = TcpListener::bind(slave_node).await.unwrap();
            listener
        };
        

        loop {
            let (mut tcp_stream, _) = listener.accept().await.unwrap();

            tokio::spawn(async move {
                let uuid = Uuid::new_v4().to_string();

                let (mut read, mut write) = tcp_stream.split();

                loop {
                    match read.read_i32().await {
                        Ok(mess_len) => {
                            let mut message = vec![0u8; mess_len as usize];

                            read.read_exact(&mut message).await.unwrap();

                            let slave_message =
                                bincode::deserialize::<SlaveMessage>(&message).unwrap();

                            match slave_message {
                                SlaveMessage::create(create_message) => {
                                    let create_return = create_table_controls(create_message).await;

                                    match create_return {
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                SlaveMessage::insert(insert) => {
                                    let insert_data = insert_operation(insert).await;
                                    match insert_data {
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                SlaveMessage::compress_table(table_name) => {
                                    let compress_return = compress_table(&table_name, &uuid).await;
                                    match compress_return {
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                SlaveMessage::query(querymessage) => {
                                    let query_return = query(querymessage, &uuid).await;
                                    match query_return {
                                        Ok(vec) => {
                                            let bincode_bytes = bincode::serialize(&vec).unwrap();

                                            write
                                                .write_i32(bincode_bytes.len() as i32)
                                                .await
                                                .unwrap();
                                            write.write_all(&bincode_bytes).await.unwrap();
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                SlaveMessage::stream_read(stream_read_message) => {
                                    let stream_return = stream_read(&stream_read_message).await;

                                    match stream_return {
                                        Ok(stream_message) => match stream_message {
                                            None => {
                                                write.write_i32(-1).await.unwrap();
                                            }

                                            Some(stream_me) => {
                                                write
                                                    .write_i32(stream_me.len() as i32)
                                                    .await
                                                    .unwrap();
                                                write.write_all(&stream_me).await.unwrap();
                                            }
                                        },
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                SlaveMessage::batch_insert(batch) => {
                                    let batch_return =
                                        controls::batch_insert::batch_insert_data(batch).await;

                                    match batch_return {
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                SlaveMessage::drop_table(table_name) => {
                                    match drop_table_operation(&table_name).await {
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                SlaveMessage::follower_replicas_sync(replicas_sync_struct) => {
                                    match follower_replicas_sync(&replicas_sync_struct).await{
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                SlaveMessage::leader_replicas_sync(sync_message) => {

                                    let replicase_sync_data = Leader_replicas_sync(&sync_message).await;
                                    
                                    match replicase_sync_data {
                                        Ok(sync_data) => {
                                            if let Some(replicase_sync) = sync_data {
                                                let mess_bytes = bincode::serialize(&replicase_sync).unwrap();
                                                let bytes_len = mess_bytes.len() as i32;

                                                write.write_i32(bytes_len).await.unwrap();
                                                write.write_all(&mess_bytes).await.unwrap();
                                            }else {
                                                write.write_i32(-1).await.unwrap();
                                            }
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                    
                                    
                                }
                                SlaveMessage::max_offset(partition_code) => {
                                    match get_max_offset(&partition_code).await{
                                        Ok(offset) => {
                                            write.write_i32(8).await.unwrap();
                                            write.write_i64(offset).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                            }
                        }

                        Err(e) => {
                            let slave_data = {
                                let slave_config = SLAVE_CONFIG.lock().await;
                                let slave_data = &slave_config.slave_data;
                                slave_data.clone()
                            };
                            

                            for slave_path in slave_data {
                                // 如果有临时文件的话 就删除临时文件
                                let temp_path = format!("{}/{}/{}", slave_path, "temp", &uuid);

                                match tokio::fs::metadata(&temp_path).await {
                                    Ok(_) => {
                                        tokio::fs::remove_dir_all(temp_path).await.unwrap();
                                    }
                                    Err(_) => {}
                                }
                            }

                            info!("master 与 slave 的连接断开了:  {}", e);
                            break;
                        }
                    }
                }
            });
        }
    });

    return joinhandle;
}
