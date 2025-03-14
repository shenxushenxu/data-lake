mod controls;
mod entity;

use std::{env, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::controls::stream_read_logic::Consumer;
use crate::entity::ClientStatement;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);
    let mut stream = TcpStream::connect(&args[1]).await.unwrap();


    loop {
        // 创建一个可变的 String 存储输入
        let mut input = String::new();
        // 读取输入
        print!(">>> ");
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
            }
            ClientStatement::insert(master_insert) => {

                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();

            }
            ClientStatement::metadata(table_name) => {
              let table_structure = controls::metadata_logic::get_metadata(bytes, bytes_len, &mut stream).await;

                match table_structure {
                    Some(structure) => {
                        let json_str = serde_json::to_string(&structure).unwrap();
                        println!("{}", json_str);
                    }
                    None => {
                        println!("{}  没有这个表", table_name);
                    }
                }
            }
            ClientStatement::compress_table(table_name) => {
                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();
            }
            ClientStatement::query(sql) => {
                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();

                loop {
                    let mess_len = stream.read_i32().await.unwrap();
                    if mess_len == -1 {
                        break;
                    }

                    let mut mess = vec![0; mess_len as usize];
                    stream.read_exact(&mut mess).await.unwrap();

                    let nn = String::from_utf8(mess).unwrap();
                    println!(">> {}",nn);
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
                }

            }
            ClientStatement::batch_insert(batch_insert) => {
                stream.write_i32(bytes_len as i32).await.unwrap();
                stream.write_all(bytes).await.unwrap();
            }
            _ =>{
                println!("没有匹配的语句")
            }
        }





    }






}
