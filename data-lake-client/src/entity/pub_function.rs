use std::io;
use std::io::Write;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

pub async fn read_error(stream: &mut TcpStream){
    let is = stream.read_i32().await.unwrap();
    if is == -2 {

        let len = stream.read_i32().await.unwrap();

        let mut mess = vec![0u8;len as usize];
        stream.read_exact(&mut mess).await.unwrap();

        println!("{}", String::from_utf8(mess).unwrap());
        io::stdout().flush().unwrap();
    }
}