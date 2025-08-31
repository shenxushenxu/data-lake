use entity_lib::entity::Error::DataLakeError;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use entity_lib::entity::SlaveEntity::SlaveMessage;
use entity_lib::function::table_structure::get_table_structure;

pub async fn get_max_offset(table_name: &String) -> Result<HashMap<usize, i64>, DataLakeError> {
    let mut table_structure = get_table_structure(table_name).await?;

    let partition_address = &mut table_structure.partition_address;
    let table_name = &table_structure.table_name;

    
    let mut offset_map = HashMap::<usize, i64>::new();
    
    for (code, vec_partition_info) in partition_address.iter() {
        let partition_code = format!("{}-{}",table_name, code);

        let leader_address = entity_lib::function::get_leader_partition(table_name, vec_partition_info)?;
        let max_offset = partition_max_offset(leader_address, partition_code).await?;
        offset_map.insert(code.clone(), max_offset);
    }

    return Ok(offset_map);
}


pub async fn partition_max_offset(leader_address:&String, partition_code: String) -> Result<i64, DataLakeError> {
    let mut data_lake_tcpstream = TcpStream::connect(leader_address).await?;

    let slave_message = SlaveMessage::max_offset(partition_code);
    let mut slave_message_bytes = bincode::serialize(&slave_message)?;
    let byte_len = slave_message_bytes.len() as i32;

    data_lake_tcpstream.write_i32(byte_len).await?;
    data_lake_tcpstream.write_all(slave_message_bytes.as_mut_slice()).await?;

    let mes_len = data_lake_tcpstream.read_i32().await?;
    if mes_len == -2 {
        let mess_len = data_lake_tcpstream.read_i32().await?;
        let mut message = vec![0; mess_len as usize];
        data_lake_tcpstream.read_exact(message.as_mut_slice()).await?;

        let message_str = String::from_utf8(message)?;
        return Err(DataLakeError::custom(message_str));

    }else {
        let mut message = vec![0; mes_len as usize];
        data_lake_tcpstream.read_exact(message.as_mut_slice()).await?;

        let offset = i64::from_le_bytes(message.try_into().unwrap());

        return Ok(offset);
    }
} 
