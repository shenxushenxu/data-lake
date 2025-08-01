use entity_lib::entity::MasterEntity::{MasterStreamRead, Parti, Statement};
use entity_lib::entity::SlaveEntity::DataStructure;
use serde::{Deserialize, Serialize};
use snap::raw::Decoder;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use entity_lib::entity::MasterEntity::Statement::batch_insert;

#[derive(Serialize, Deserialize, Debug)]
struct StreamRead {
    table_name: String,
    read_count: usize,
    patition_mess: Vec<Parti>,
}

pub struct Consumer<'a> {
    socket: &'a mut TcpStream,
    read_count: usize,
    table_name: String,
    offset: Vec<Parti>,
    hash_map: HashMap<String, i64>,
}

impl<'a> Consumer<'a> {
    pub fn new(socket: &'a mut TcpStream, read_count: usize, table_name: String) -> Self {
        let vec = Vec::<Parti>::new();
        let mut hash_map = HashMap::<String, i64>::new();

        return Consumer {
            socket: socket,
            read_count: read_count,
            table_name: table_name,
            offset: vec,
            hash_map: hash_map,
        };
    }

    pub fn set_offset(&mut self, offset: Vec<Parti>) {
        self.offset = offset;
    }

    pub async fn load(&mut self) -> Vec<String> {
        let mut write_vec_offset = Vec::<Parti>::new();

        for par in self.offset.iter_mut() {
            let par_partition_code = par.patition_code;
            let par_offset = par.offset;

            let ppp = Parti {
                patition_code: par_partition_code,
                offset: par_offset + 1,
            };
            write_vec_offset.push(ppp);
        }

        let stream_read = MasterStreamRead {
            table_name: self.table_name.clone(),
            read_count: self.read_count,
            patition_mess: write_vec_offset,
        };

        let re = Statement::stream_read(stream_read);

        let json = serde_json::to_string(&re).unwrap();

        let bytes = json.as_bytes();
        let bytes_len = bytes.len();
        self.socket.write_i32(bytes_len as i32).await.unwrap();
        self.socket.write_all(bytes).await.unwrap();

        let mut res_vec = Vec::<String>::new();
        
        loop {
            let mess_len = self.socket.read_i32().await.unwrap();
            if mess_len == -1 {
                let mut offset_vec = Vec::<Parti>::new();
                let mut hash_map = &self.hash_map;

                for (partition_code, offset) in hash_map.into_iter() {
                    let pa = Parti {
                        patition_code: partition_code.parse::<usize>().unwrap(),
                        offset: offset.clone(),
                    };
                    offset_vec.push(pa);
                }
                if offset_vec.len() > 0 {
                    self.set_offset(offset_vec);
                }

                return res_vec;
            } else if mess_len == -2 {
                let len = self.socket.read_i32().await.unwrap();

                let mut mess = vec![0u8; len as usize];
                self.socket.read_exact(&mut mess).await.unwrap();

                println!("{}",String::from_utf8(mess).unwrap())
            } else {
                let mut mess = vec![0u8; mess_len as usize];
                self.socket.read_exact(mess.as_mut_slice()).await.unwrap();

                let mut decoder = Decoder::new();
                let message_bytes = decoder
                    .decompress_vec(&mess)
                    .unwrap_or_else(|e| panic!("解压失败: {}", e));

                let data_structure_vec =
                    bincode::deserialize::<Vec<DataStructure>>(&message_bytes).unwrap();

                for data_structure in data_structure_vec.iter() {
                    let offset = data_structure.offset;
                    let partition_code = &data_structure.partition_code;

                    self.hash_map.insert(partition_code.to_string(), offset);

                    let messss = serde_json::to_string(data_structure).unwrap();
                    res_vec.push(messss);
                }



            }
        }
    }
}
