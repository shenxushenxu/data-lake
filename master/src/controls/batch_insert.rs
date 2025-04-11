use std::collections::HashMap;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{BatchInsert, BatchInsertTruth, DataType, SlaveBatchData, SlaveInsert, TableStructure};
use entity_lib::entity::SlaveEntity::SlaveMessage;
use public_function::hashcode;
use crate::controls::insert;
use crate::controls::insert::INSERT_TCPSTREAM_CACHE_POOL;
use crate::controls::metadata::get_metadata;

pub async fn batch_insert_data(mut batch_insert: BatchInsertTruth, uuid:&String) -> Result<(), DataLakeError>{

    let table_name = &batch_insert.table_name;
    let table_structure = get_metadata(&table_name).await?;

    let map_insert = batch_format_matching(&mut batch_insert, &table_structure, uuid)
        .await?;

    for (key, list) in map_insert {

        let array = key.split("|=_=|").collect::<Vec<&str>>();
        let address = &array[1];
        let table_name = &array[2];
        let partition_code = &array[3];

        let slave_insert = SlaveInsert{
            table_name: table_name.to_string(),
            data: list,
            partition_code: partition_code.to_string(),
        };

        let slave_message = SlaveMessage::batch_insert(slave_insert);

        let bytes = bincode::serialize(&slave_message)?;
        let bytes_len = bytes.len() as i32;


        let mut map_guard = INSERT_TCPSTREAM_CACHE_POOL.lock().await;
        let tcp_stream = match map_guard.get_mut(&key){
            Some(value) => {
                value
            }
            None => {
                let mut stream = TcpStream::connect(address).await?;
                map_guard.insert(key.clone(), stream);
                let value = map_guard.get_mut(&key).unwrap();
                value
            }
        };

        tcp_stream.write_i32(bytes_len).await?;
        tcp_stream.write(&bytes).await?;
    }

    return Ok(());
}



pub async fn batch_format_matching(
    batch_insert: &mut BatchInsertTruth,
    table_structure: &TableStructure,
    uuid: &String
) -> Result<HashMap<String, Vec<SlaveBatchData>>, DataLakeError> {

    let mut res_map = HashMap::<String, Vec<SlaveBatchData>>::new();


    let batch_data = &mut batch_insert.data;
    let table_name = &batch_insert.table_name;

    let structure_col_type = &table_structure.col_type;

    for batch_datum in batch_data.iter_mut() {

        // 验证 插入的数据  与表的元数据是否匹配
        let crud = batch_datum.get("crud").unwrap().clone();
        batch_datum.remove("crud");

        insert::type_verification(structure_col_type, batch_datum, table_name).await?;


        let partition_address = &table_structure.partition_address;
        let major_key = &table_structure.major_key;

        let data_major_key = batch_datum.get(major_key).unwrap().clone();

        let partition_code = data_major_key.hash_code() as usize % table_structure.partition_number;
        let par_addres = partition_address.get(&partition_code).unwrap();



        let data_json = serde_json::to_string(batch_datum)?;
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