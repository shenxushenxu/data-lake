use entity_lib::entity::MasterEntity::{Statement, TableStructure};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn get_metadata(
    bytes: &[u8],
    bytes_len: usize,
    stream: &mut TcpStream,
) -> Option<TableStructure> {
    stream.write_i32(bytes_len as i32).await.unwrap();
    stream.write_all(bytes).await.unwrap();

    if let Ok(len) = stream.read_i32().await {
        let mut return_mess = vec![0u8; len as usize];
        stream.read(return_mess.as_mut_slice()).await.unwrap();

        let table_structure: TableStructure =
            serde_json::from_str(std::str::from_utf8(return_mess.as_slice()).unwrap()).unwrap();

        return Some(table_structure);
    }

    return None;
}
