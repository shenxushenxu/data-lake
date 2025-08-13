use std::sync::{Arc, LazyLock};
use dashmap::DashMap;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use crate::entity::MasterEntity::TableStructure;
use crate::entity::SlaveEntity::SlaveCacheStruct;
use crate::function::PosttingTcpStream::DataLakeTcpStream;

/**
master 端缓存的 插入slave 的 tcp 链接对象
**/
pub static INSERT_TCPSTREAM_CACHE_POOL: LazyLock<
    Arc<DashMap<String, Arc<Mutex<DataLakeTcpStream>>>>,
> = LazyLock::new(|| Arc::new(DashMap::<String, Arc<Mutex<DataLakeTcpStream>>>::new()));


/**
流读的情况下 master 端缓存的 slave tcp链接对象
**/
pub static STREAM_TCP_TABLESTRUCTURE: LazyLock<
    Arc<DashMap<String, Arc<Mutex<(DataLakeTcpStream, TableStructure)>>>>,
> = LazyLock::new(||{
    Arc::new(DashMap::<String, Arc<Mutex<(DataLakeTcpStream, TableStructure)>>>::new())
});


/**
slave 端 缓存文件对象的static
**/
pub static FILE_CACHE_POOL: LazyLock<Arc<DashMap<String, Arc<Mutex<SlaveCacheStruct>>>>> =
    LazyLock::new(|| {
        Arc::new(DashMap::<String, Arc<Mutex<SlaveCacheStruct>>>::new())
    });



// master 端 如果修改了表结构，清除表的缓存
pub fn remove_table_cache(table_name: &String){
    let stream_tcp_tablestructure = Arc::clone(&STREAM_TCP_TABLESTRUCTURE);
    let mut remove_vec = Vec::<String>::new();
    stream_tcp_tablestructure.iter().for_each(|x| {
        let key = x.key();
        if key.contains(table_name) {
            remove_vec.push(key.clone());
        }
    });
    remove_vec.iter().for_each(|k| {
        stream_tcp_tablestructure.remove(k).unwrap();
    });
}