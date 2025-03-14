
mod controls;

use entity_lib::entity::MasterEntity::QueryItem;
use entity_lib::entity::SlaveEntity::{QueryMessage, SlaveMessage};
use log::{error, info};
use public_function::SLAVE_CONFIG;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
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
            let (mut socket, _) = listener.accept().await.unwrap();

            tokio::spawn(async move {
                let (mut read, mut write) = socket.split();

                loop {
                    match read.read_i32().await {
                        Ok(mess_len) => {
                            let mut message = vec![0u8; mess_len as usize];

                            read.read_exact(&mut message).await.unwrap();

                            let slave_message =
                                bincode::deserialize::<SlaveMessage>(&message).unwrap();

                            match slave_message {
                                SlaveMessage::create(create_message) => {
                                    if let Err(e) =
                                        create_table_controls(create_message).await
                                    {
                                        error!("Error creating table: {:?}", e);
                                    }
                                }
                                SlaveMessage::insert(insert) => {
                                    insert_operation(insert).await;
                                }
                                SlaveMessage::compress_table(table_name) => {
                                    compress_table(&table_name).await;
                                }
                                SlaveMessage::query(querymessage) => {
                                    let vec = query(querymessage).await;
                                    let bincode_bytes = bincode::serialize(&vec).unwrap();

                                    write.write_i32(bincode_bytes.len() as i32).await.unwrap();
                                    write.write_all(&bincode_bytes).await.unwrap();
                                }
                                SlaveMessage::stream_read(stream_read_message) => {
                                    let message = stream_read(&stream_read_message).await;
                                    if message.len() == 0{
                                        write.write_i32(-1).await.unwrap();

                                    }else {
                                        write.write_i32(message.len() as i32).await.unwrap();
                                        write.write_all(&message).await.unwrap();
                                    }

                                }
                                SlaveMessage::batch_insert(batch) => {
                                    // println!("Batch insert {:?}", batch);
                                    controls::batch_insert::batch_insert_data(batch).await;



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
