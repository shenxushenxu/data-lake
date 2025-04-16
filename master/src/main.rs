
mod controls;
mod master_operation;

use std::any::Any;
use std::collections::HashMap;
use log::{error, info};
use serde_json::json;
use snap::raw::Decoder;
use public_function::{MASTER_CONFIG, hashcode, write_error};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use uuid::Uuid;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{BatchInsertTruth, Statement};
use crate::controls::compress_table::{compress_table};
use crate::controls::create::{create_table};
use crate::controls::insert::{insert_data, INSERT_TCPSTREAM_CACHE_POOL};
use crate::controls::metadata::{get_metadata};
use crate::controls::query::{query_sql};
use crate::controls::stream_read::{stream_read_data, STREAM_TCP};
use crate::master_operation::tcp_encapsulation::TcpStream;

/**
-1 是停止
-2 是异常
**/
#[tokio::main]
async fn main() {
    // 初始化日志系统
    env_logger::init();

    let master_main = data_interface();

    tokio::join!(master_main);
}

/**
数据操作的接收端
**/
fn data_interface() -> JoinHandle<()> {
    let joinhandle = tokio::spawn(async move {
        let listener = TcpListener::bind(MASTER_CONFIG.get("master.data.port").unwrap())
            .await
            .unwrap();

        loop {
            let (mut socket, _) = listener.accept().await.unwrap();

            let mut tcp_stream = TcpStream::new(socket);


            tokio::spawn(async move {
                let uuid = Uuid::new_v4().to_string();

                let (mut read, mut write) = tcp_stream.split();

                loop {
                    match read.read_i32().await {
                        Ok(message_len) => {



                            let mut message = vec![0; message_len as usize];

                            read.read_exact(message.as_mut_slice()).await.unwrap();

                            let message_str = String::from_utf8(message).unwrap();

                            let statement: Statement = serde_json::from_str(&message_str[..]).unwrap();

                            match statement {
                                Statement::create(create_struct) => {
                                    let create_return = create_table(create_struct).await;

                                    match create_return {
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                Statement::insert(insert) => {
                                    let insert_return = insert_data(insert, &uuid).await;
                                    match insert_return {
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    };
                                }
                                Statement::metadata(table_name) => {
                                    let metadata_return = get_metadata(&table_name).await;
                                    match metadata_return {
                                        Ok(table_structure) => {
                                            let metadtat_message = serde_json::to_string(&table_structure).unwrap();
                                            let byt = metadtat_message.as_bytes();

                                            let write_len = byt.len();
                                            let write_message = byt;

                                            write.write_i32(write_len as i32).await.unwrap();
                                            write.write_all(write_message).await.unwrap();
                                        }
                                       Err(e) => {
                                           public_function::write_error(e, &mut write).await;
                                       }
                                    }
                                }
                                Statement::compress_table(table_name) => {
                                    let compress_return = compress_table(&table_name).await;

                                    match compress_return {
                                        Ok(_) => {
                                            write.write_i32(-1).await.unwrap();
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }

                                }
                                Statement::query(sql) => {

                                    let query_return = query_sql(sql).await;
                                    println!("-----------    {:?}", query_return);
                                    match query_return {
                                        Ok(res_vec) => {
                                            if let Some(vec) = res_vec {
                                                for ve in vec {
                                                    let byt = ve.as_bytes();
                                                    let write_len = byt.len();
                                                    write.write_i32(write_len as i32).await.unwrap();
                                                    write.write_all(byt).await.unwrap();
                                                }
                                            }
                                                write.write_i32(-1).await.unwrap();

                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }



                                }
                                Statement::stream_read(stream_read) => {


                                    let stream_return = stream_read_data(stream_read, &uuid).await;

                                    match stream_return {
                                        Ok(mut receiver) => {
                                            while let Some(message) = receiver.recv().await {
                                                if let Some(message_bytes) = message {
                                                    write.write_all(&message_bytes).await.unwrap();
                                                }
                                            }
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

                                    let mut decoder = Decoder::new();
                                    let message_bytes = decoder
                                        .decompress_vec(data)
                                        .unwrap_or_else(|e| panic!("解压失败: {}", e));
                                    let message_str = std::str::from_utf8(&message_bytes).unwrap();
                                    let hashmap = serde_json::from_str::<Vec<HashMap<String, String>>>(message_str).unwrap();

                                    let batch = BatchInsertTruth{
                                        table_name: table_name,
                                        data: hashmap
                                    };


                                    let batch_return = controls::batch_insert::batch_insert_data(batch, &uuid).await;

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


                            break;
                        }
                    }
                }
            });
        }
    });

    return joinhandle;
}
