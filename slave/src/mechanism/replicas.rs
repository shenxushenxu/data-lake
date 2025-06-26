use crate::controls::stream_read::binary_search;
use entity_lib::entity::SlaveEntity::{
    IndexStruct, ReplicasSyncStruct, ReplicaseSyncData, SlaveMessage, SyncMessage,
};
use entity_lib::entity::const_property::INDEX_SIZE;
use memmap2::{Mmap, MmapMut};
use public_function::{SLAVE_CONFIG, get_partition_path};
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use entity_lib::entity::Error::DataLakeError;

/**
Follower副本 同步  Leader副本的数据
**/
pub async fn follower_replicas_sync(replicas_sync_struct: &ReplicasSyncStruct) -> Result<(), DataLakeError> {
    let leader_address = &replicas_sync_struct.leader_address;
    let slave_parti_name = &replicas_sync_struct.slave_parti_name;

    let partition_path = get_partition_path(slave_parti_name).await;
    let metadata_file_path = format!("{}/metadata.log", partition_path);
    let mut metadata_file = OpenOptions::new()
        .write(true)
        .read(true)
        .open(metadata_file_path)
        .await?;

    let mut metadata_mmap = unsafe {MmapMut::map_mut(&metadata_file)?};
    let file_end_offset = i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap());


    
    let sync_message = SyncMessage {
        offset: file_end_offset,
        partition_code: slave_parti_name.clone(),
    };
    let slave_message = SlaveMessage::leader_replicas_sync(sync_message);
    let mut tcp_stream = TcpStream::connect(leader_address).await?;

    let mess_bytes = bincode::serialize(&slave_message)?;
    let byte_len = mess_bytes.len();
    tcp_stream.write_i32(byte_len as i32).await?;
    tcp_stream.write_all(mess_bytes.as_slice()).await?;

    let return_mess_len = tcp_stream.read_i32().await?;
    if return_mess_len != -1 {
        let mut return_mess_bytes = vec![0u8; return_mess_len as usize];
        tcp_stream
            .read_exact(return_mess_bytes.as_mut_slice())
            .await
            .unwrap();

        let replicase_sync_data =
            bincode::deserialize::<ReplicaseSyncData>(&mut return_mess_bytes.as_slice())?;
        let offset_set = replicase_sync_data.offset_set;
        let data_set = replicase_sync_data.data_set;
        let index_code = replicase_sync_data.index_code;


        

        let new_index_file_path = format!("{}/log/{}.index", partition_path, index_code);
        let new_log_file_path = format!("{}/log/{}.snappy", partition_path, index_code);

        let mut index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(new_index_file_path)
            .await?;
        let mut log_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(new_log_file_path)
            .await?;

        index_file.write_all(offset_set.as_slice()).await?;
        log_file.write_all(data_set.as_slice()).await?;

        unsafe {
            let offset_set_len = offset_set.len();
            let return_end_offset = &offset_set[(offset_set_len - INDEX_SIZE)..offset_set_len ];
            let end_Index_struct = bincode::deserialize::<IndexStruct>(return_end_offset)?;
            
            let end_offset = end_Index_struct.offset + 1;
            let dst_ptr = metadata_mmap.as_mut_ptr();
            
            let slice = end_offset.to_be_bytes();
            let src_ptr = slice.as_ptr();

            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, slice.len());
        }
    }
    
    return Ok(());
}

/**
Leader副本向Follower同步的数据
**/
pub async fn Leader_replicas_sync(sync_message: &SyncMessage) -> Option<ReplicaseSyncData> {
    let offset = sync_message.offset;
    let partition_code = &sync_message.partition_code;

    let mut log_files = public_function::get_list_filename(&partition_code).await;

    if log_files.len() == 0 {
        return None;
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

    let option_this_offset_file = binary_search(&file_vec, offset).await.unwrap();

    if let Some(this_offset_file) = option_this_offset_file {
        if let Some(replicase_sync_data) = find_data(this_offset_file, offset).await {
            return Some(replicase_sync_data);
        }
    }

    return None;
}

pub async fn find_data(index_path: &String, offset: i64) -> Option<ReplicaseSyncData> {
    let mut index_file = OpenOptions::new()
        .read(true)
        .open(index_path)
        .await
        .unwrap();

    let index_mmap = unsafe { Mmap::map(&index_file).unwrap() };

    let index_file_len = index_mmap.len();
    let mut left = 0;
    let mut right = (index_file_len / INDEX_SIZE - 1);

    let mut start_index: Option<IndexStruct> = None;
    let mut start_seek: usize = 0;
    while left <= right {
        let mid = left + (right - left) / 2;
        start_seek = mid * INDEX_SIZE;
        let bytes_mid = &index_mmap[start_seek..start_seek + INDEX_SIZE];
        let data_mid = bincode::deserialize::<IndexStruct>(bytes_mid).unwrap();

        if data_mid.offset == offset {
            start_index = Some(data_mid);
            break;
        } else if data_mid.offset < offset {
            left = (mid + 1);
        } else {
            right = (mid - 1);
        }
    }

    if start_index.is_none() {
        return None;
    }

    // 获得索引
    let offset_set = &index_mmap[start_seek..];

    // 获得 数据
    let data_start_seek = start_index.unwrap().start_seek;
    let mut snappy_file = OpenOptions::new()
        .read(true)
        .open(index_path.replace(".index", ".snappy"))
        .await
        .unwrap();
    let snappy_file_mmap = unsafe { Mmap::map(&snappy_file).unwrap() };
    let data_set = &snappy_file_mmap[(data_start_seek as usize)..];

    let path = Path::new(index_path);

    let index_file_name = path.file_name().unwrap().to_str().unwrap();
    let index_code = index_file_name.replace(".index", "");

    let replicase_sync_data = ReplicaseSyncData {
        offset_set: offset_set.to_vec(),
        data_set: data_set.to_vec(),
        index_code: index_code,
    };

    return Some(replicase_sync_data);
}
