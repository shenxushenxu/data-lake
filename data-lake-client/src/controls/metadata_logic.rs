use entity_lib::entity::MasterEntity::{Statement, TableStructure};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn get_metadata(
    bytes: &[u8],
    bytes_len: usize,
    stream: &mut TcpStream,
) -> Option<TableStructure> {


    return None;
}
