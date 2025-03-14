use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use snap::raw::Decoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use entity_lib::entity::MasterEntity::{MasterStreamRead, Parti, Statement};

#[derive(Serialize, Deserialize, Debug)]
struct StreamRead{
    table_name: String,
    read_count: usize,
    patition_mess: Vec<Parti>
}


pub struct Consumer<'a>{
    socket: &'a mut TcpStream,
    read_count: usize,
    table_name: String,
    offset: Vec<Parti>,

}

impl<'a> Consumer<'a>{

    pub fn new(socket: &'a mut TcpStream, read_count:usize, table_name: String) -> Self{

        let vec = Vec::<Parti>::new();

        return Consumer{socket: socket,
            read_count: read_count,
            table_name: table_name,
            offset: vec};
    }

    pub fn set_offset(&mut self, offset:Vec<Parti>){
        self.offset = offset;
    }

    pub async fn load(&mut self) -> Vec<String>{

        let stream_read = MasterStreamRead{
            table_name: self.table_name.clone(),
            read_count: self.read_count,
            patition_mess: self.offset.clone(),
        };

        let re = Statement::stream_read(stream_read);

        let json = serde_json::to_string(&re).unwrap();

        let bytes = json.as_bytes();
        let bytes_len = bytes.len();
        self.socket.write_i32(bytes_len as i32).await.unwrap();
        self.socket.write_all(bytes).await.unwrap();

        let mut vec = Vec::<String>::new();

        loop {
            let mess_len = self.socket.read_i32().await.unwrap();
            if mess_len == -1 {
                return vec;
            }

            let mut mess = vec![0u8; mess_len as usize];
            self.socket.read_exact(mess.as_mut_slice()).await.unwrap();

            let mut decoder = Decoder::new();
            let message_bytes = decoder
                .decompress_vec(&mess)
                .unwrap_or_else(|e| panic!("解压失败: {}", e));

            let messss = String::from_utf8(message_bytes).unwrap();
            vec.push(messss);
        }


    }



}
