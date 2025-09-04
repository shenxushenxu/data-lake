use std::sync::Arc;
use log::{error, info};
use crate::controls::{max_offset, metadata};
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{Info, PartitionInfo, TableStructure};
use entity_lib::entity::SlaveEntity::{ReplicasSyncStruct, SlaveMessage};
use entity_lib::function::MASTER_CONFIG;
use memmap2::Mmap;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::{JoinError, JoinHandle};
use entity_lib::function::BufferObject::INSERT_TCPSTREAM_CACHE_POOL;
use entity_lib::function::RandomNumber::random_number;
use entity_lib::function::table_structure::get_table_path;

pub fn copy_sync_notif() -> JoinHandle<Result<(), DataLakeError>> {
    tokio::spawn(async move {
        loop {
            let table_sync = tokio::spawn(async move {
                let data_path_vec = {
                    let master_config = MASTER_CONFIG.lock().await;
                    master_config.master_data_path.clone()
                };

                let mut table_path_vec: Vec<String> = Vec::new();

                for data_path in data_path_vec.iter() {
                    // 异步读取目录中的条目
                    let mut entries = tokio::fs::read_dir(data_path).await?;
                    // 遍历条目并打印文件名
                    while let Some(entry) = entries.next_entry().await? {
                        let entry_path = entry.path(); // 获取条目路径
                        if entry_path.is_file() {
                            table_path_vec.push(entry_path.display().to_string());
                        }
                    }
                }
                let mut table_join_handle_set = tokio::task::JoinSet::new();

                for table_path in table_path_vec {
                    table_join_handle_set.spawn(async move {
                        let mut file = OpenOptions::new().read(true).open(&table_path).await?;
                        let file_len = file.metadata().await?.len();

                        let mut metadata_bytes = vec![0u8; file_len as usize];
                        file.read_exact(metadata_bytes.as_mut_slice()).await?;

                        let table_structure =
                            bincode::deserialize::<TableStructure>(metadata_bytes.as_mut_slice())?;

                        let par_address = table_structure.partition_address;
                        let table_name = table_structure.table_name;

                        for (code, partition_info_vec) in par_address {
                            let slave_parti_name = format!("{}-{}", table_name, code);

                            // 获得 活跃分区的 address
                            let leader_address = entity_lib::function::get_leader_partition(
                                &table_path,
                                &partition_info_vec,
                            )?;


                            let leader_partition_max_offset = get_max_offset(&leader_address, &table_name, code).await?;


                            for partition_info in partition_info_vec.iter() {
                                if let Info::Follower = partition_info.info {
                                    let partiti_name = slave_parti_name.clone();
                                    let leader_ress = leader_address.clone();

                                    let follower_address = &partition_info.address;

                                    let replicas_sync_struct = ReplicasSyncStruct {
                                        slave_parti_name: partiti_name.clone(),
                                        leader_address: leader_ress,
                                        leader_partition_max_offset: leader_partition_max_offset,
                                    };
                                    let slave_message =
                                        SlaveMessage::follower_replicas_sync(replicas_sync_struct);

                                    let mut follower_tcpstream =
                                        TcpStream::connect(&follower_address).await?;
                                    let mess_bytes = bincode::serialize(&slave_message)?;
                                    let bytes_len = mess_bytes.len();

                                    follower_tcpstream.write_i32(bytes_len as i32).await?;
                                    follower_tcpstream.write_all(mess_bytes.as_slice()).await?;

                                    let result_mass_len = follower_tcpstream.read_i32().await?;
                                    if result_mass_len == -2 {
                                        let mass_len = follower_tcpstream.read_i32().await?;
                                        let mut mass = vec![0u8; mass_len as usize];
                                        follower_tcpstream.read_exact(&mut mass[..]).await?;
                                        let massage = std::str::from_utf8(&mass)?;
                                        return Err(DataLakeError::custom(format!(
                                            "{}: {}",
                                            partiti_name.clone(),
                                            massage
                                        )));
                                    }
                                }
                            }
                        }

                        return Ok(());
                    });
                }

                while let Some(join_res) = table_join_handle_set.join_next().await {
                    match join_res {
                        Ok(res) => {
                            if let Err(e) = res {
                                return Err(e);
                            }
                        }
                        Err(e) => {
                            return Err(DataLakeError::custom(format!(
                                        "同步数据的协程  Error: {:?}",
                                        e
                                    )));
                        }
                    }
                }

                return Ok(());
            });

            if let Ok(sync_res) = table_sync.await {
                if let Err(e) = sync_res {
                    error!("replicase报错： {}", e);
                }
            }
        }
    })
}

// 通过 活跃分区 address 做一个类似的心跳机制
async fn get_max_offset(
    leader_address: &String,
    table_name: &String,
    code: usize,
) -> Result<i64, DataLakeError> {


    let partition_code = format!("{}-{}", table_name, code);

    match max_offset::partition_max_offset(leader_address, partition_code.clone()).await {
        Ok(partition_max_offset) => {
            return Ok(partition_max_offset);
        }
        Err(e) => {
            error!("replicase 出错： 开始切换活跃分区 {:?}", e);
            let mut table_structure = metadata::get_table_structure(table_name).await?;
            let partition_address = &mut table_structure.partition_address;

            let vec_par = partition_address.get_mut(&code).unwrap();

            // 获得所有的Follower分区
            let follower_vec = vec_par
                .iter_mut()
                .filter(|x| {
                    match x.info {
                        Info::Follower => true,
                        Info::Leader => false,
                    }

                }).map(|x2| {x2.clone()}).collect::<Vec<PartitionInfo>>();

            // 将活跃分区 设置成非活跃分区
            vec_par.iter_mut().for_each(|x1| {
                if let Info::Leader = x1.info {
                    x1.info = Info::Follower;
                }
            });

            // 从 Follower 分区中选举活跃分区
            let follower_vec_len = follower_vec.len();
            if follower_vec_len > 0{
                let address = match searchfac(&follower_vec).await{
                    Ok(address) => {
                        address
                    }
                    Err(e) => {
                        return Err(DataLakeError::custom(format!("{} {}", &partition_code, e)));
                    }
                };
                vec_par.iter_mut().for_each(|x1| {
                    if let Info::Follower = x1.info {
                        if address == x1.address{
                            x1.info = Info::Leader;
                        }
                    }
                });


                let leader_vec = vec_par.iter().filter(|x1| {
                    match x1.info {
                        Info::Leader => true,
                        _ => false,
                    }
                }).map(|x3| {1}).collect::<Vec<i32>>();

                if leader_vec.len() == 0 {
                    return Err(DataLakeError::custom(format!("{}  重置之后没有活跃分区 ", &partition_code)));
                }else if leader_vec.len() > 1 {
                    return Err(DataLakeError::custom(format!("{}  重置之后活跃分区 > 1 ", &partition_code)));
                }else if leader_vec.len() == 1 {
                    // 更新 表的元数据
                    save_table_structure(table_structure).await?;
                }else {
                    return Err(DataLakeError::custom(format!("{}  重置之后出现特殊情况: \n {:?}", &partition_code, table_structure)));
                }



            }else {
                return Err(DataLakeError::custom(format!("{} 没有其他分区", &partition_code)));
            }

        }
    }
    return Err(DataLakeError::custom(format!("{}  同步分区数据失败, 并且故障转移失败", &partition_code)))
}


async fn searchfac(
    partition_info_vec: &Vec<PartitionInfo>,
) -> Result<String, DataLakeError> {
    let mut partition_info_vec_clone = partition_info_vec.clone();

    let mut size = partition_info_vec_clone.len();
    loop {
        if size != 0 {

            let random_number = random_number(size);

            let partition_info = partition_info_vec_clone.get(random_number).unwrap();

            let address = &partition_info.address;

            if let Ok(_) = TcpStream::connect(address).await {
                return Ok(address.clone());
            } else {
                partition_info_vec_clone.remove(random_number);
                size = size - 1;
            }
        } else {
            return Err(DataLakeError::custom("没有可用的分区了".to_string()));
        }
    }
}


async fn save_table_structure(table_structure: TableStructure) -> Result<(), DataLakeError>{

    let table_name = &table_structure.table_name;

    let file_path = get_table_path(&table_name).await?;
    let data = bincode::serialize(&table_structure)?;
    let mut file = File::create(file_path).await?;
    file.write_all(data.as_slice()).await?;
    file.flush().await?;

    // 清空batch_insert tcp缓冲 和 stream_read tcp的缓冲
    entity_lib::function::BufferObject::remove_table_cache(&table_name);
    let insert_tcpstream_cache_pool = Arc::clone(&INSERT_TCPSTREAM_CACHE_POOL);
    let mut vec = Vec::<String>::new();
    insert_tcpstream_cache_pool.iter().for_each(|pool| {
        let key = pool.key();
        if key.contains(table_name) {
            vec.push(key.clone());
        }
    });
    vec.iter().for_each(|key| {
        insert_tcpstream_cache_pool.remove(key);
    });


    Ok(())
}