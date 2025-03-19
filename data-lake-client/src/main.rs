mod controls;
mod entity;
use std::io::{Write};
use std::{env, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use entity_lib::entity::MasterEntity::TableStructure;
use crate::controls::stream_read_logic::Consumer;
use crate::entity::ClientStatement;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);
    let mut stream = TcpStream::connect(&args[1]).await.unwrap();


    loop {
        print!(">>> ");
        io::stdout().flush().unwrap();
        // 创建一个可变的 String 存储输入
        let mut input = String::new();
        // 读取输入

        io::stdin()
            .read_line(&mut input)
            .expect("读取输入失败");

        let input_trim = input.trim();

        if input_trim == "exit" {
            break;
        }else if input_trim == "" {
            continue;
        }

        let client_statement: ClientStatement = serde_json::from_str(&input_trim[..]).unwrap();

        let bytes = input_trim.as_bytes();
        let bytes_len = bytes.len();

        match client_statement {
            ClientStatement::create(create_struct) => {
                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();

                entity::pub_function::read_error(&mut stream).await;
            }
            ClientStatement::insert(master_insert) => {

                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();

                entity::pub_function::read_error(&mut stream).await;

            }
            ClientStatement::metadata(table_name) => {
                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();

                if let Ok(len) = stream.read_i32().await {

                    if len == -2 {

                        entity::pub_function::read_error(&mut stream).await;

                    }else {
                        let mut return_mess = vec![0u8; len as usize];
                        stream.read(return_mess.as_mut_slice()).await.unwrap();

                        let table_structure: TableStructure =
                            serde_json::from_str(std::str::from_utf8(return_mess.as_slice()).unwrap()).unwrap();
                        let json_str = serde_json::to_string(&table_structure).unwrap();
                        println!("{}", json_str);
                        io::stdout().flush().unwrap();
                    }
                }
            }
            ClientStatement::compress_table(table_name) => {
                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();

                entity::pub_function::read_error(&mut stream).await;

            }
            ClientStatement::query(sql) => {
                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();

                loop {
                    let mess_len = stream.read_i32().await.unwrap();
                    if mess_len == -1 {
                        break;
                    }else if mess_len == -2 {
                        let len = stream.read_i32().await.unwrap();

                        let mut mess = vec![0u8;len as usize];
                        stream.read_exact(&mut mess).await.unwrap();

                        println!("{}", String::from_utf8(mess).unwrap());
                    }else {
                        let mut mess = vec![0; mess_len as usize];
                        stream.read_exact(&mut mess).await.unwrap();

                        let nn = String::from_utf8(mess).unwrap();
                        println!("||  {}",nn);

                    }
                }
            }
            ClientStatement::stream_read(stream_read) => {

                println!("{:?}", stream_read);
                let mut sumer = Consumer::new(&mut stream, stream_read.read_count, stream_read.table_name);
                sumer.set_offset(stream_read.patition_mess);
                loop {


                    let vec = sumer.load().await;

                    for v in vec.iter() {
                        println!("> {}", v);
                    }
                    io::stdout().flush().unwrap();
                    // tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }

            }
            ClientStatement::batch_insert(batch_insert) => {
                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();

                entity::pub_function::read_error(&mut stream).await;

            }
            _ =>{
                println!("没有匹配的语句")
            }
        }


        io::stdout().flush().unwrap();


    }






}
