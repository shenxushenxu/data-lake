use std::sync::{Arc, LazyLock};
use dashmap::DashMap;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use entity_lib::entity::MasterEntity::TableStructure;
use entity_lib::entity::SlaveEntity::SlaveCacheStruct;
use crate::PosttingTcpStream::DataLakeTcpStream;

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
    Arc<DashMap<String, Arc<Mutex<(TcpStream, TableStructure)>>>>,
> = LazyLock::new(||{
    Arc::new(DashMap::<String, Arc<Mutex<(TcpStream, TableStructure)>>>::new())
});


/**
slave 端 缓存文件对象的static
**/
pub static FILE_CACHE_POOL: LazyLock<Arc<DashMap<String, Arc<Mutex<SlaveCacheStruct>>>>> =
    LazyLock::new(|| {
        Arc::new(DashMap::<String, Arc<Mutex<SlaveCacheStruct>>>::new())
    });

