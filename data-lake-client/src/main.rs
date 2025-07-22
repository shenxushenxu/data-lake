mod controls;
mod entity;
use std::io::{Write};
use std::{env, io};
use std::collections::HashMap;
use snap::raw::{Decoder, Encoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use entity_lib::entity::MasterEntity::{Statement, TableStructure};
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
            ClientStatement::sql(sql) => {
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
                    }else if mess_len == -3{
                        let table_structure_len = stream.read_i32().await.unwrap();
                        let mut table_structure = vec![0; table_structure_len as usize];
                        stream.read_exact(&mut table_structure).await.unwrap();
                        let ts_str = String::from_utf8(table_structure).unwrap();
                        
                        println!("{}", ts_str)
                        
                    }else if mess_len == -4 {
                        let offset_map_len = stream.read_i32().await.unwrap();
                        let mut offset_map_bytes = vec![0; offset_map_len as usize];
                        stream.read_exact(offset_map_bytes.as_mut_slice()).await.unwrap();
                        let offset_map = serde_json::from_slice::<HashMap<usize, i64>>(&offset_map_bytes).unwrap();

                        println!("{:?}", offset_map)
                        
                    } else {
                        let mut mess = vec![0; mess_len as usize];
                        stream.read_exact(&mut mess).await.unwrap();

                        let mut decoder = Decoder::new();
                        let message_bytes = decoder
                            .decompress_vec(&mess)
                            .unwrap_or_else(|e| panic!("解压失败: {}", e));

                        let data_vec = serde_json::from_slice::<Vec<String>>(&message_bytes).unwrap();
                        for data in data_vec.iter() {
                            println!("||  {}",data);
                        }


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
                }

            }
            ClientStatement::batch_insert(batch_insert) => {

                let table_name = batch_insert.table_name;
                let hash_data = batch_insert.data;
                let partition_code = batch_insert.partition_code;

                let data_str = serde_json::to_string(&hash_data).unwrap();

                let mut encoder = Encoder::new();
                let compressed_data = encoder.compress_vec(data_str.as_bytes()).unwrap();

                // let batchinsert = BatchInsert{
                //     table_name: table_name,
                //     data: compressed_data,
                //     partition_code: partition_code
                // };

                let state = Statement::batch_insert;

                let write_data = serde_json::to_string(&state).unwrap();
                let data_bytes = write_data.as_bytes();
                let data_len = data_bytes.len();

                stream.write_i32(data_len as i32).await.unwrap();
                stream.write_all(data_bytes).await.unwrap();

                entity::pub_function::read_error(&mut stream).await;

            }
            _ =>{
                println!("没有匹配的语句")
            }
        }


        io::stdout().flush().unwrap();


    }






}
