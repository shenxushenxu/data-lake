
mod controls;
use log::{error, info};
use serde_json::json;
use public_function::{MASTER_CONFIG, hashcode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use uuid::Uuid;
use entity_lib::entity::MasterEntity::Statement;
use crate::controls::compress_table::{compress_table};
use crate::controls::create::{create_table};
use crate::controls::insert::{insert_data, INSERT_TCPSTREAM_CACHE_POOL};
use crate::controls::metadata::{get_metadata};
use crate::controls::query::{query_sql};
use crate::controls::stream_read::{stream_read_data, STREAM_TCP};

#[tokio::main]
async fn main() {
    // 初始化日志系统
    env_logger::init();

    let cc = data_interface();

    tokio::join!(cc);
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

            tokio::spawn(async move {
                let uuid = Uuid::new_v4().to_string();

                let (mut read, mut write) = socket.split();

                loop {
                    match read.read_i32().await {
                        Ok(message_len) => {

                            let mut message = vec![0; message_len as usize];

                            read.read_exact(message.as_mut_slice()).await.unwrap();

                            let message_str = String::from_utf8(message).unwrap();

                            let statement: Statement = serde_json::from_str(&message_str[..]).unwrap();

                            match statement {
                                Statement::create(create_struct) => {
                                    if let Err(e) = create_table(create_struct).await {
                                        error!("create failed {}", e);
                                    }
                                }
                                Statement::insert(insert) => {
                                    insert_data(insert, &uuid).await;
                                }
                                Statement::metadata(table_name) => {
                                    let table_structure =
                                        get_metadata(&table_name).await;

                                    let metadtat_message =
                                        serde_json::to_string(&table_structure).unwrap();
                                    let byt = metadtat_message.as_bytes();

                                    let write_len = byt.len();
                                    let write_message = byt;

                                    write.write_i32(write_len as i32).await.unwrap();
                                    write.write_all(write_message).await.unwrap();
                                }
                                Statement::compress_table(table_name) => {
                                    compress_table(&table_name).await;
                                }
                                Statement::query(sql) => {

                                    let res_vec = query_sql(sql).await;

                                    if let Some(vec) = res_vec {

                                        // let json = r#"{"code":1}"#;
                                        //
                                        // let byt = json.as_bytes();
                                        // let write_len = byt.len();
                                        // write.write_i32(write_len as i32).await.unwrap();
                                        // write.write_all(byt).await.unwrap();

                                        for ve in vec {

                                            let byt = ve.as_bytes();
                                            let write_len = byt.len();

                                            write.write_i32(write_len as i32).await.unwrap();
                                            write.write_all(byt).await.unwrap();
                                        }
                                        write.write_i32(-1).await.unwrap();
                                    }else {
                                        write.write_i32(-1).await.unwrap();
                                    }
                                }
                                Statement::stream_read(stream_read) => {


                                    let mut receiver = stream_read_data(stream_read, &uuid).await;

                                    while let Some(message) = receiver.recv().await {

                                        if let Some(message_bytes) = message {
                                            write.write_all(&message_bytes).await.unwrap();
                                        }
                                    }

                                    write.write_i32(-1).await.unwrap();


                                }

                                Statement::batch_insert(batch) => {

                                    controls::batch_insert::batch_insert_data(batch, &uuid).await;

                                }

                            }
                        }

                        Err(e) => {
                            info!("{:?}", e);
                            // 输入插入连接断开，清理缓存中的连接
                            let mut remove_vec = Vec::<String>::new();
                            let mut mutex_guard = INSERT_TCPSTREAM_CACHE_POOL.lock().await;

                            mutex_guard.keys().for_each(|k| {
                                if k.contains(uuid.as_str()) {
                                    remove_vec.push(k.clone());
                                }
                            });

                            remove_vec.iter().for_each(|k| {
                                mutex_guard.remove(k).unwrap();
                            });

                            // ----------  流消费连接断开，清理缓存中的连接
                            let mut remove_vec = Vec::<String>::new();
                            let mut mutex_guard = STREAM_TCP.lock().await;

                            mutex_guard.keys().for_each(|k| {
                                if k.contains(uuid.as_str()) {
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
