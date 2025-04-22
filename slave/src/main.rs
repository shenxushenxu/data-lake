
mod controls;
use entity_lib::entity::MasterEntity::QueryItem;
use entity_lib::entity::SlaveEntity::{QueryMessage, SlaveMessage};
use log::{error, info};
use public_function::SLAVE_CONFIG;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use entity_lib::entity::Error::DataLakeError;
use crate::controls::compress_table::compress_table;
use crate::controls::create_table::create_table_controls;
use crate::controls::insert_data::insert_operation;
use crate::controls::query_table::query;
use crate::controls::stream_read::stream_read;

#[tokio::main]
async fn main() {
    // 初始化日志系统
    env_logger::init();

    let slave = data_read_write();

    tokio::join!(slave);
}

/**
请求分类
**/
fn data_read_write() -> JoinHandle<()> {
    let joinhandle = tokio::spawn(async move {
        let listener = TcpListener::bind(SLAVE_CONFIG.get("slave.node").unwrap())
            .await
            .unwrap();

        loop {
            let (mut tcp_stream, _) = listener.accept().await.unwrap();


            tokio::spawn(async move {
                let (mut read, mut write) = tcp_stream.split();

                loop {
                    match read.read_i32().await {
                        Ok(mess_len) => {
                            let mut message = vec![0u8; mess_len as usize];

                            read.read_exact(&mut message).await.unwrap();

                            let slave_message = bincode::deserialize::<SlaveMessage>(&message).unwrap();

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
                                SlaveMessage::query(querymessage) => {

                                    let query_return = query(querymessage).await;
                                    match query_return {
                                        Ok(vec) => {
                                            let bincode_bytes = bincode::serialize(&vec).unwrap();

                                            write.write_i32(bincode_bytes.len() as i32).await.unwrap();
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
                                        Ok(stream_message) => {

                                            match stream_message{
                                                None => {write.write_i32(-1).await.unwrap();}

                                                Some(stream_me) => {
                                                    write.write_i32(stream_me.len() as i32).await.unwrap();
                                                    write.write_all(&stream_me).await.unwrap();
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            public_function::write_error(e, &mut write).await;
                                        }
                                    }
                                }
                                SlaveMessage::batch_insert(batch) => {

                                    let batch_return = controls::batch_insert::batch_insert_data(batch).await;

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
