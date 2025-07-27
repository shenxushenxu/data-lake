use std::panic::Location;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{DataStructure, IndexStruct, StreamReadStruct};
use memmap2::Mmap;
use public_function::{data_complete};
use snap::raw::{Decoder, Encoder};
use tokio::fs::OpenOptions;
use entity_lib::entity::const_property::INDEX_SIZE;
use public_function::read_function::ArrayBytesReader;

pub async fn stream_read(streamreadstruct: &StreamReadStruct) -> Result<Option<Vec<u8>>, DataLakeError> {
    let stream_return = data_read(streamreadstruct).await;
    let col_type = &streamreadstruct.table_col_type;
    match stream_return {
        Ok(stream_message) => match stream_message {
            None => {
                return Ok(None);
            }
            Some(stream_me) => {

                let mut vec_datastructure = Vec::<DataStructure>::new();
                
                let mut arraybytesreader = ArrayBytesReader::new(stream_me.as_slice());
                
                let mut box_bytes_vec = Vec::<&mut Vec<u8>>::new();
                
                loop {
                    if arraybytesreader.is_stop() {
                        break;
                    }
                
                    let mess_len = match arraybytesreader.read_i32(){
                        Ok(len) => { len }
                        Err(e) => {
                            for box_leak in box_bytes_vec {
                                unsafe {Box::from_raw(box_leak)};
                            }
                            return Err(e);
                        }
                    };
                    let mess = arraybytesreader.read_exact(mess_len as usize);
                
                    let mut decoder = Decoder::new();
                    let message_bytes = match decoder.decompress_vec(mess){
                        Ok(bytes) => {
                            bytes
                        }
                        Err(e) => {
                            for box_leak in box_bytes_vec {
                                unsafe {Box::from_raw(box_leak)};
                            }
                            return Err(DataLakeError::custom(format!("解压报错: {:?}", e)));
                        }
                    };
                    
                    let box_bytes = Box::leak(Box::new(message_bytes));
                    
                    let mut datastructure = {
                        let prt_bytes = box_bytes as * mut Vec<u8>;
                        
                        let bytes = unsafe{&*prt_bytes};
                        
                        match bincode::deserialize::<DataStructure>(bytes){
                            Ok(datastruct) => datastruct,
                            Err(e) => {
                                for box_leak in box_bytes_vec {
                                    unsafe {Box::from_raw(box_leak)};
                                }
                                return Err(DataLakeError::BincodeError {
                                    source: e,
                                    location: Location::caller()
                                });
                            }
                        }
                    };

                    box_bytes_vec.push(box_bytes);
                    
                    
                    
                    let data = &mut datastructure.data;
                    
                    // 补全\验证  数据
                    data_complete(col_type, data).await;
                    
                    
                    vec_datastructure.push(datastructure);
                }
                
                let vec_mess = match bincode::serialize(&vec_datastructure){
                    Ok(vec_string) => {
                        for box_leak in box_bytes_vec {
                            unsafe {Box::from_raw(box_leak)};
                        }
                        
                        vec_string
                    },
                    Err(e) =>{
                        return Err(DataLakeError::BincodeError {
                            source: e,
                            location: Location::caller()
                        });
                    }
                };
                
                let mut encoder = Encoder::new();
                let compressed_data = encoder.compress_vec(&vec_mess)?;
                
                
                
                return Ok(Some(compressed_data));
            }
        },
        Err(e) => {
            return Err(e);
        }
    };
}


pub async fn data_read(
    streamreadstruct: &StreamReadStruct,
) -> Result<Option<Vec<u8>>, DataLakeError> {
    
    let partition_name = format!("{}-{}",&streamreadstruct.table_name, &streamreadstruct.partition_code);
    
  

    let mut log_files = public_function::get_list_filename(&partition_name).await;
   

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
        }).collect::<Vec<&(String, String)>>();

    file_vec.sort_by_key(|x1| {
        let file_name = &x1.0;

        let file_code = file_name.replace(".index", "");

        return file_code.parse::<i64>().unwrap();
    });

    let mut offset = streamreadstruct.offset;
    let mut read_count = streamreadstruct.read_count;
    let option_index_path = binary_search(&file_vec, offset).await?;

    match option_index_path {
        None => {
            return Ok(None);
        }
        Some(index_path) => {
            let data = find_data(index_path, offset, read_count).await?;
            return Ok(data);
        }
    }
}


/** 找到 offset 存在的索引文件 **/
pub async fn binary_search<'a>(file_vec: &'a Vec<&(String, String)>, offset: i64) -> Result<Option<&'a String>, DataLakeError> {

    for index in 0..file_vec.len() {
        let (_, this_index_path) = file_vec[index];

        let index_file = OpenOptions::new()
            .read(true)
            .open(this_index_path)
            .await?;
        
        let index_mmap = unsafe { Mmap::map(&index_file)?};

        let mmap_len = index_mmap.len();
        if mmap_len > INDEX_SIZE {
            let index_bytes = &index_mmap[(mmap_len - INDEX_SIZE)..mmap_len];

            let Index_struct = bincode::deserialize::<IndexStruct>(index_bytes)?;
            let file_end_offset = Index_struct.offset;

            if file_end_offset >= offset {
                return Ok(Some(this_index_path));
            }
        }
    }
    return Ok(None);
}

/**
在索引文件内 通过二分查找   查找到 对应的offset
**/
async fn find_data(index_path: &String, offset: i64, read_count: usize) -> Result<Option<Vec<u8>>, DataLakeError> {


    let mut index_file = OpenOptions::new()
        .read(true)
        .open(index_path)
        .await?;

    let index_mmap = unsafe { Mmap::map(&index_file)?};

    let index_file_len = index_mmap.len();
    let mut left = 0;
    let mut right = (index_file_len / INDEX_SIZE - 1);

    let mut start_index: Option<IndexStruct> = None;
    let mut start_seek: usize = 0;
    while left <= right {
        let mid = left + (right - left) / 2;
        start_seek = mid * INDEX_SIZE;
        let bytes_mid = &index_mmap[start_seek..start_seek + INDEX_SIZE];
        let data_mid = bincode::deserialize::<IndexStruct>(bytes_mid)?;

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

        let bytes_mid = &index_mmap[..start_seek + INDEX_SIZE];
        let data_mid = bincode::deserialize::<IndexStruct>(bytes_mid)?;
        start_index = Some(data_mid);
        start_seek = 0;
    }

    // 获得 结尾的offset 位置
    let mut end_seek = start_seek + (read_count * INDEX_SIZE);

    let data = if end_seek > index_file_len {
        // 如果结尾的offset位置超出索引文件的大小
        end_seek = index_file_len - INDEX_SIZE;

        // 获得结尾的 offset
        let bytes_end = &index_mmap[end_seek..end_seek + INDEX_SIZE];
        let end_index = bincode::deserialize::<IndexStruct>(bytes_end)?;

        let mut stream_data = load_data(index_path, &start_index, &end_index).await?;
        stream_data
    } else {
        // 结尾的offset位置 没有 超出索引文件的大小

        // 获得结尾的 offset
        let bytes_end = &index_mmap[end_seek..end_seek + INDEX_SIZE];
        let end_index = bincode::deserialize::<IndexStruct>(bytes_end)?;

        let mut stream_data = load_data(index_path, &start_index, &end_index).await?;
        stream_data
    };

    return Ok(Some(data));
}

async fn load_data(
    file_path: &String,
    start_index: &Option<IndexStruct>,
    end_index: &IndexStruct,
) -> Result<Vec<u8>, DataLakeError> {


    let start_file_seek = match start_index {
        Some(x) => x.start_seek,
        None => panic!("妈的二分查找没找到对应的offset"),
    } as usize;

    let data_path = file_path.replace(".index", ".snappy");
    let mut data_file = OpenOptions::new().read(true).open(data_path).await?;

    let data_mmap = unsafe { Mmap::map(&data_file)? };

    let end_file_seek = end_index.end_seek as usize;

    let read_data = &data_mmap[start_file_seek..end_file_seek];

    return Ok(read_data.to_vec());
}
