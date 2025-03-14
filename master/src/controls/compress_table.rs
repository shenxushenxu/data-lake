use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use entity_lib::entity::SlaveEntity::SlaveMessage;
use crate::controls::metadata::get_metadata;

pub async fn compress_table(table_name: &String){

    let table_structure =
        get_metadata(&table_name).await;

    let address_map = &table_structure.partition_address;

    for key in address_map.keys() {

        let address = address_map.get(key).unwrap();

        let mut stream = TcpStream::connect(address).await.unwrap();

        let mut message = SlaveMessage::compress_table(format!("{}-{}", table_name, key));


        let bytes = bincode::serialize(&message).unwrap();
        let bytes_len = bytes.len();


        stream.write_i32(bytes_len as i32).await.unwrap();
        stream.write_all(&bytes).await.unwrap();


    }

}