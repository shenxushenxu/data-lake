use crate::controls::stream_read;
use crate::controls::stream_read::binary_search;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{
    IndexStruct, ReplicasSyncStruct, ReplicaseSyncData, SlaveMessage, SyncMessage,
};
use entity_lib::entity::const_property::INDEX_SIZE;
use memmap2::{Mmap, MmapMut};
use public_function::vec_trait::VecPutVec;
use public_function::{SLAVE_CONFIG, get_partition_path};
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;

/**
Follower副本 同步  Leader副本的数据
**/
pub async fn follower_replicas_sync(
    replicas_sync_struct: &ReplicasSyncStruct,
) -> Result<(), DataLakeError> {
    let leader_address = &replicas_sync_struct.leader_address;
    let slave_parti_name = &replicas_sync_struct.slave_parti_name;

    let partition_path = get_partition_path(slave_parti_name).await;
    let metadata_file_path = format!("{}/metadata.log", partition_path);
    let metadata_file = OpenOptions::new()
        .write(true)
        .read(true)
        .open(metadata_file_path)
        .await?;

    let mut metadata_mmap = unsafe { MmapMut::map_mut(&metadata_file)? };
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
    if return_mess_len != -1 && return_mess_len != -2 {
        let mut return_mess_bytes = vec![0u8; return_mess_len as usize];
        tcp_stream
            .read_exact(return_mess_bytes.as_mut_slice())
            .await?;

        let replicase_sync_data =
            bincode::deserialize::<ReplicaseSyncData>(return_mess_bytes.as_slice())?;

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

        index_file.write_all(offset_set.as_slice()).await?;
        log_file.write_all(data_set.as_slice()).await?;
        index_file.flush().await?;
        log_file.flush().await?;
        unsafe {
            let offset_set_len = offset_set.len();
            let return_end_offset = &offset_set[(offset_set_len - INDEX_SIZE)..offset_set_len];
            let end_Index_struct = bincode::deserialize::<IndexStruct>(return_end_offset)?;

            let end_offset = end_Index_struct.offset + 1;
            let dst_ptr = metadata_mmap.as_mut_ptr();

            let slice = end_offset.to_be_bytes();
            let src_ptr = slice.as_ptr();

            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, slice.len());
        }
    } else if return_mess_len == -2 {
        let len = tcp_stream.read_i32().await?;

        let mut mess = vec![0u8; len as usize];
        tcp_stream.read_exact(&mut mess).await?;

        println!("{}", String::from_utf8(mess)?);
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

    let mut log_files = public_function::get_list_filename(&partition_code).await;

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

    let option_this_offset_file = binary_search(&file_vec, offset).await?;

    if let Some((index_name, this_offset_file)) = option_this_offset_file {
        let slave_replicas_sync_num = {
            let slave_config = SLAVE_CONFIG.lock().await;
            let slave_replicas_sync_num = slave_config.slave_replicas_sync_num;
            slave_replicas_sync_num
        };

        let (index_path, _, _, start_seek) =
            stream_read::find_data(index_name, this_offset_file, offset, slave_replicas_sync_num)
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
    let index_file = OpenOptions::new().read(true).open(index_path).await?;

    let index_mmap = unsafe { Mmap::map(&index_file)? };
    let mut index_end_seek = start_seek + (INDEX_SIZE * slave_replicas_sync_num);
    let index_len = index_mmap.len();

    if index_len < index_end_seek {
        index_end_seek = index_len;
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
