use entity_lib::entity::MasterEntity::{Info, PartitionInfo, TableStructure};
use entity_lib::entity::SlaveEntity::{ReplicasSyncStruct, SlaveMessage};
use memmap2::Mmap;
use public_function::MASTER_CONFIG;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

pub fn copy_sync_notif() -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let master_config = MASTER_CONFIG.lock().await;
            let data_path_vec = master_config.master_data_path.clone();
            drop(master_config);

            let mut table_path_vec: Vec<String> = Vec::new();

            for data_path in data_path_vec.iter() {
                // 异步读取目录中的条目
                let mut entries = tokio::fs::read_dir(data_path).await.unwrap();
                // 遍历条目并打印文件名
                while let Some(entry) = entries.next_entry().await.unwrap() {
                    let entry_path = entry.path(); // 获取条目路径
                    if entry_path.is_file() {
                        table_path_vec.push(entry_path.display().to_string());
                    }
                }
            }
            let mut join_handle_set = tokio::task::JoinSet::new();

            for table_path in table_path_vec {
                let mut file = OpenOptions::new()
                    .read(true)
                    .open(&table_path)
                    .await
                    .unwrap();

                let table_structure = tokio::task::spawn_blocking(move || {
                    let mmap = unsafe { Mmap::map(&file) }.unwrap();
                    let metadata_message = &mmap[..];
                    let table_structure = bincode::deserialize::<TableStructure>(metadata_message).unwrap();

                    table_structure
                })
                .await
                .unwrap();

                let par_address = table_structure.partition_address;
                let table_name = table_structure.table_name;

                for (code, partition_info_vec) in par_address {
                    let slave_parti_name = format!("{}-{}", table_name, code);

                    let leader_address = &partition_info_vec.iter()
                        .filter(|x| {
                            match x.info {
                                Info::Leader => true,
                                Info::Follower => false,
                            }
                        })
                        .map(|x1| x1.address.clone())
                        .collect::<Vec<String>>()[0];

                    for partition_info in partition_info_vec {
                        
                        if let Info::Follower = partition_info.info {

                            let partiti_name = slave_parti_name.clone();
                            let leader_ress = leader_address.clone();

                            join_handle_set.spawn(async move {
                                let follower_address = &partition_info.address;
                                
                                let replicas_sync_struct = ReplicasSyncStruct {
                                    slave_parti_name: partiti_name,
                                    leader_address: leader_ress,
                                };
                                let slave_message =
                                    SlaveMessage::follower_replicas_sync(replicas_sync_struct);

                                let mut follower_tcpstream =
                                    TcpStream::connect(&follower_address).await.unwrap();
                                let mess_bytes = bincode::serialize(&slave_message).unwrap();
                                let bytes_len = mess_bytes.len();

                                follower_tcpstream
                                    .write_i32(bytes_len as i32)
                                    .await
                                    .unwrap();
                                follower_tcpstream
                                    .write_all(mess_bytes.as_slice())
                                    .await
                                    .unwrap();

                                let result_mass_len = follower_tcpstream.read_i32().await.unwrap();
                                if result_mass_len == -2 {
                                    let mass_len = follower_tcpstream.read_i32().await.unwrap();
                                    let mut mass = vec![0u8; mass_len as usize];
                                    follower_tcpstream.read_exact(&mut mass[..]).await.unwrap();
                                    let massage = std::str::from_utf8(&mass).unwrap();
                                    eprintln!("ERROR:  {}", massage);
                                }
                            });
                        }
                    }
                }
            }

            while let Some(res) = join_handle_set.join_next().await {
                if let Err(e) = res {
                    eprintln!("Task failed: {:?}", e);
                }
            }
            
        }
    })
}
