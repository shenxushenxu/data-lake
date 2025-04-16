use tokio::net::tcp::{ReadHalf, WriteHalf};
use uuid::Uuid;
use crate::controls::insert::INSERT_TCPSTREAM_CACHE_POOL;
use crate::controls::stream_read::STREAM_TCP;

pub struct TcpStream{
    socket: tokio::net::TcpStream,
    uuid: String,
}

impl TcpStream {
    pub fn new(socket: tokio::net::TcpStream) -> Self {
        let uuid = Uuid::new_v4().to_string();
        return Self { socket, uuid };
    }

    pub fn split(&mut self) -> ((ReadHalf, WriteHalf)) {
        return self.socket.split();
    }
}


impl Drop for TcpStream{
    fn drop(&mut self) {

        let uuid = &self.uuid;

        // 输入插入连接断开，清理缓存中的连接
        let mut remove_vec = Vec::<String>::new();
        let mut mutex_guard = INSERT_TCPSTREAM_CACHE_POOL.lock().await;

        mutex_guard.keys().for_each(|k| {
            if k.contains(uuid.as_str()) {
                remove_vec.push(k.clone());
            }
        });

        remove_vec.iter().for_each(|k| {
            mutex_guard.remove(k).unwrap();
        });

        // ----------  流消费连接断开，清理缓存中的连接
        let mut remove_vec = Vec::<String>::new();
        let mut mutex_guard = STREAM_TCP.lock().await;

        mutex_guard.keys().for_each(|k| {
            if k.contains(uuid.as_str()) {
                remove_vec.push(k.clone());
            }
        });

        remove_vec.iter().for_each(|k| {
            mutex_guard.remove(k).unwrap();
        });
    }
}