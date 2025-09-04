use crate::controls;
use crate::controls::stream_read;
use crate::controls::stream_read::binary_search;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{
    IndexStruct, ReplicasSyncStruct, ReplicaseSyncData, SlaveMessage, SyncMessage,
};
use entity_lib::entity::const_property::{I64_BYTE_LEN, INDEX_SIZE};
use entity_lib::function::{SLAVE_CONFIG, get_partition_path};
use log::error;
use memmap2::{Mmap, MmapMut};
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/**
Follower副本 同步  Leader副本的数据
**/
pub async fn follower_replicas_sync(
    replicas_sync_struct: &ReplicasSyncStruct,
) -> Result<(), DataLakeError> {
    let leader_partition_max_offset = replicas_sync_struct.leader_partition_max_offset;
    let leader_address = &replicas_sync_struct.leader_address;
    let slave_parti_name = &replicas_sync_struct.slave_parti_name;

    let partition_path = get_partition_path(slave_parti_name).await?;
    let metadata_file_path = format!("{}/metadata.log", partition_path);
    let mut metadata_file = OpenOptions::new()
        .read(true)
        .open(&metadata_file_path)
        .await?;

    let mut follower_partition_max_offset_bytes = vec![0u8; I64_BYTE_LEN];
    metadata_file
        .read_exact(follower_partition_max_offset_bytes.as_mut_slice())
        .await?;
    let follower_partition_max_offset = i64::from_le_bytes(follower_partition_max_offset_bytes.as_slice().try_into()?);

    // 如果 follower 分区的最大offset 与 leader 分区的最大offset 相等就不要同步了, 因为获得的leader 分区的offset - 1 了 所以 这里 + 1
    if follower_partition_max_offset == (leader_partition_max_offset + 1) {
        return Ok(());
    }

    let sync_message = SyncMessage {
        offset: follower_partition_max_offset,
        partition_code: slave_parti_name.clone(),
    };
    let slave_message = SlaveMessage::leader_replicas_sync(sync_message);
    let mut tcp_stream = TcpStream::connect(leader_address).await?;

    let mess_bytes = bincode::serialize(&slave_message)?;
    let byte_len = mess_bytes.len();
    tcp_stream.write_i32(byte_len as i32).await?;
    tcp_stream.write_all(mess_bytes.as_slice()).await?;

    let return_mess_len = tcp_stream.read_i32().await?;
    if return_mess_len != -1 && return_mess_len != -2 {
        let mut return_mess_bytes = vec![0u8; return_mess_len as usize];
        tcp_stream
            .read_exact(return_mess_bytes.as_mut_slice())
            .await?;

        let replicase_sync_data = tokio::task::spawn_blocking(move || {
            bincode::deserialize::<ReplicaseSyncData>(return_mess_bytes.as_slice())
        }).await.unwrap()?;


        let offset_set = replicase_sync_data.offset_set;
        let data_set = replicase_sync_data.data_set;
        let index_code = replicase_sync_data.index_code;

        let new_index_file_path = format!("{}/log/{}.index", partition_path, index_code);
        let new_log_file_path = format!("{}/log/{}.snappy", partition_path, index_code);

        let mut index_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(new_index_file_path)
            .await?;
        let mut log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(new_log_file_path)
            .await?;

        log_file.write_all(data_set.as_slice()).await?;
        index_file.write_all(offset_set.as_slice()).await?;
        log_file.flush().await?;
        index_file.flush().await?;

        let offset_set_len = offset_set.len();
        let return_end_offset = &offset_set[(offset_set_len - INDEX_SIZE)..offset_set_len];
        let end_Index_struct = bincode::deserialize::<IndexStruct>(return_end_offset)?;
        let end_offset = end_Index_struct.offset + 1;
        let end_offset_bytes = &end_offset.to_le_bytes();

        let mut file = File::create(&metadata_file_path).await?;
        file.write_all(end_offset_bytes).await?;
        file.flush().await?;


    } else if return_mess_len == -2 {
        let len = tcp_stream.read_i32().await?;

        let mut mess = vec![0u8; len as usize];
        tcp_stream.read_exact(&mut mess).await?;

        error!("{}  {}  {}", file!(), line!(), String::from_utf8(mess)?);
    }

    return Ok(());
}

/**
Leader副本向Follower同步的数据
**/
pub async fn Leader_replicas_sync(
    sync_message: &SyncMessage,
) -> Result<Option<ReplicaseSyncData>, DataLakeError> {
    let offset = sync_message.offset;
    let partition_code = &sync_message.partition_code;

    let mut log_files = entity_lib::function::get_list_filename(&partition_code).await;

    if log_files.len() == 0 {
        return Ok(None);
    }

    let mut file_vec = log_files
        .iter()
        .filter(|x| {
            let file_name = &x.0;
            if file_name.contains(".index") {
                return true;
            }
            return false;
        })
        .collect::<Vec<&(String, String)>>();

    file_vec.sort_by_key(|x1| {
        let file_name = &x1.0;

        let file_code = file_name.replace(".index", "");

        return file_code.parse::<i64>().unwrap();
    });

    let option_this_offset_file = binary_search(&file_vec, offset, "replicas").await?;

    if let Some((index_name, this_offset_file_path)) = option_this_offset_file {
        let slave_replicas_sync_num = {
            let slave_config = SLAVE_CONFIG.lock().await;
            let slave_replicas_sync_num = slave_config.slave_replicas_sync_num;
            slave_replicas_sync_num
        };

        let (index_path, _, _, start_seek) = stream_read::find_data(
            index_name,
            this_offset_file_path,
            offset,
            slave_replicas_sync_num,
            "replicas",
        )
        .await?;

        let replicasesyncdata = load_data(index_path, start_seek, slave_replicas_sync_num).await?;
        return Ok(Some(replicasesyncdata));
    }

    return Ok(None);
}

async fn load_data(
    index_path: &String,
    start_seek: usize,
    slave_replicas_sync_num: usize,
) -> Result<ReplicaseSyncData, DataLakeError> {
    let mut index_file = OpenOptions::new().read(true).open(index_path).await?;

    let mut index_mmap = unsafe { Mmap::map(&index_file)? };
    let mut index_end_seek = start_seek + (INDEX_SIZE * slave_replicas_sync_num);
    let mut index_len = index_mmap.len();

    if index_len < index_end_seek {
        index_end_seek = loop {
            if index_len % INDEX_SIZE == 0 {
                break index_len;
            } else {
                index_file = OpenOptions::new().read(true).open(index_path).await?;
                index_mmap = unsafe { Mmap::map(&index_file)? };
                index_len = index_mmap.len();
            }
        };
    }

    // 获得索引
    let offset_set = &index_mmap[start_seek..index_end_seek];

    // 获得 数据
    let data_start_seek = {
        let start_offset = &offset_set[0..INDEX_SIZE];
        let start_index_struct = bincode::deserialize::<IndexStruct>(start_offset)?;
        start_index_struct.start_seek as usize
    };
    let data_end_seek = {
        let offset_set_len = offset_set.len();
        let start_offset = &offset_set[(offset_set_len - INDEX_SIZE)..offset_set_len];
        let start_index_struct = bincode::deserialize::<IndexStruct>(start_offset)?;
        start_index_struct.end_seek as usize
    };

    let snappy_file = OpenOptions::new()
        .read(true)
        .open(index_path.replace(".index", ".snappy"))
        .await?;
    let snappy_file_mmap = unsafe { Mmap::map(&snappy_file)? };

    let data_set = &snappy_file_mmap[data_start_seek..data_end_seek];

    let path = Path::new(index_path);

    let index_file_name = path.file_name().unwrap().to_str().unwrap();
    let index_code = index_file_name.replace(".index", "");

    let replicase_sync_data = ReplicaseSyncData {
        offset_set: offset_set.to_vec(),
        data_set: data_set.to_vec(),
        index_code: index_code,
    };

    Ok(replicase_sync_data)
}
