use std::collections::HashMap;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use entity_lib::entity::MasterEntity::{BatchInsert, DataType, SlaveBatchData, SlaveInsert, TableStructure};
use entity_lib::entity::SlaveEntity::SlaveMessage;
use public_function::hashcode;
use crate::controls::insert::INSERT_TCPSTREAM_CACHE_POOL;
use crate::controls::metadata::get_metadata;

pub async fn batch_insert_data(mut batch_insert: BatchInsert, uuid:&String){

    let table_name = &batch_insert.table_name;
    let table_structure = get_metadata(&table_name).await;

    let map_insert = batch_format_matching(&mut batch_insert, &table_structure, uuid)
        .await
        .unwrap();



    for (key, list) in map_insert {


        let array = key.split("|=_=|").collect::<Vec<&str>>();
        let uuid = &array[0];
        let address = &array[1];
        let table_name = &array[2];
        let partition_code = &array[3];

        let slave_insert = SlaveInsert{
            table_name: table_name.to_string(),
            data: list,
            partition_code: partition_code.to_string(),
        };


        let slave_message = SlaveMessage::batch_insert(slave_insert);



        let bytes = bincode::serialize(&slave_message).unwrap();
        let bytes_len = bytes.len() as i32;


        let mut map_guard = INSERT_TCPSTREAM_CACHE_POOL.lock().await;
        let tcp_stream = match map_guard.get_mut(&key){
            Some(value) => {
                value
            }
            None => {

                let mut stream = TcpStream::connect(address).await.unwrap();

                map_guard.insert(key.clone(), stream);

                let value = map_guard.get_mut(&key).unwrap();
                value
            }
        };

        tcp_stream.write_i32(bytes_len).await.unwrap();
        tcp_stream.write(&bytes).await.unwrap();
    }
}



pub async fn batch_format_matching(
    batch_insert: &mut BatchInsert,
    table_structure: &TableStructure,
    uuid: &String
) -> Result<HashMap<String, Vec<SlaveBatchData>>, String> {

    let mut res_map = HashMap::<String, Vec<SlaveBatchData>>::new();


    let batch_data = &mut batch_insert.data;
    let table_name = &batch_insert.table_name;

    let structure_col_type = &table_structure.col_type;

    for batch_datum in batch_data.iter_mut() {
        for (col_name, value) in batch_datum.iter() {
            match structure_col_type.get(col_name) {
                Some(data_type) => match data_type {
                    DataType::string => {
                        value.to_string();
                    }
                    DataType::int => {
                        value.parse::<i32>().unwrap();
                    }
                    DataType::float => {
                        value.parse::<f32>().unwrap();
                    }
                    DataType::boolean => {
                        value.parse::<bool>().unwrap();
                    }
                    DataType::long => {
                        value.parse::<i64>().unwrap();
                    }
                    _ => {
                        return Err(format!("{}  不符合任何数据类型", col_name));
                    }
                },
                None => {
                    if !col_name.eq("crud") {
                        return Err(format!("{} 内没有 {}  列", table_name, col_name));
                    }
                }
            }
        }



        let partition_address = &table_structure.partition_address;
        let major_key = &table_structure.major_key;
        let crud = batch_datum.get("crud").unwrap().clone();
        let data_major_key = batch_datum.get(major_key).unwrap().clone();

        let partition_code = data_major_key.hash_code() as usize % table_structure.partition_number;
        let par_addres = partition_address.get(&partition_code).unwrap();

        batch_datum.remove("crud");

        let data_json = serde_json::to_string(batch_datum).unwrap();
        let ins = SlaveBatchData {
            major_key: data_major_key,
            data: data_json,
            crud: crud,
        };

        let tcp_key = format!("{}|=_=|{}|=_=|{}|=_=|{}", uuid, &par_addres, &table_name, &partition_code);

        res_map.entry(tcp_key).or_insert(Vec::new()).push(ins);



    }


    return Ok(res_map);
}