use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{DataStructure, IndexStruct, StreamReadStruct};
use entity_lib::entity::const_property::{I32_BYTE_LEN, INDEX_SIZE};
use memmap2::Mmap;
use rayon::prelude::*;
use snap::raw::{Decoder, Encoder};
use std::panic::Location;
use std::time::Instant;
use tokio::fs::OpenOptions;
use entity_lib::entity::bytes_reader::ArrayBytesReader;
use entity_lib::function::data_complete;
use crate::controls;

#[tokio::test]
pub async fn eeee() {}

pub async fn stream_read(
    streamreadstruct: &StreamReadStruct,
) -> Result<Option<Vec<u8>>, DataLakeError> {

    // 判断传输过来的offset 是否 小于最大offset
    let offset = &streamreadstruct.offset;
    let code = &streamreadstruct.partition_code;
    let table_name = &streamreadstruct.table_name;
    let partition_code = format!("{}-{}",table_name, code);
    let max_offset = controls::max_offset::get_max_offset(&partition_code).await?;

    if offset >= &max_offset {
        return Ok(None);
    }



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

                let mut box_bytes_vec = Vec::<&'static mut Vec<u8>>::new();

                let mut decoder = Decoder::new();

                loop {
                    if arraybytesreader.is_stop() {
                        break;
                    }

                    let mess_len = arraybytesreader.read_i32();
                    let bytes = arraybytesreader.read_exact(mess_len as usize);

                    let message_bytes = decoder.decompress_vec(&bytes)?;

                    let box_bytes = Box::leak(Box::new(message_bytes));

                    let datastructure = {
                        let prt_bytes = box_bytes as *mut Vec<u8>;

                        let bytes = unsafe { &*prt_bytes };

                        DataStructure::deserialize(bytes)
                    };
                    box_bytes_vec.push(box_bytes);

                    vec_datastructure.push(datastructure);
                }

                vec_datastructure.par_iter_mut().for_each(|datastructure| {
                    let data = &mut datastructure.data;
                    let major_value = datastructure.major_value;
                    // 补全\验证  数据
                    data_complete(col_type, data, major_value);
                });
                
                let vec_mess = match serialize_vec_dataStructure(vec_datastructure){
                    Ok(vec_string) => {
                        for box_leak in box_bytes_vec {
                            unsafe { Box::from_raw(box_leak) };
                        }

                        vec_string
                    }
                    Err(e) => {
                        return Err(DataLakeError::custom(format!("slave 序列化失败报错:{:?}", e)));
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


fn serialize_vec_dataStructure<'a>(vec: Vec<DataStructure<'a>>) -> Result<Vec<u8>, DataLakeError> {
    let mut data_len = I32_BYTE_LEN;
    for data_structure in vec.iter() {
        data_len += data_structure.calculate_serialized_size();
    }
    
    let mut data_vec = Vec::<u8>::with_capacity(data_len);
    let data_vec_ptr = data_vec.as_mut_ptr();
    let mut index = 0;
    
    unsafe {

        std::ptr::copy_nonoverlapping(
            (vec.len() as i32).to_le_bytes().as_ptr(),
            data_vec_ptr.add(index),
            I32_BYTE_LEN,
        );
        index += I32_BYTE_LEN;
        
        
        for data_structure in vec.iter() {
            let data_structure_bytes = data_structure.serialize()?;
            let data_structure_bytes_len = data_structure_bytes.len();
            std::ptr::copy_nonoverlapping(
                data_structure_bytes.as_ptr(),
                data_vec_ptr.add(index),
                data_structure_bytes_len,
            );
            
            index += data_structure_bytes_len;
        }

        data_vec.set_len(index)
    }
    
    if index == data_len {
        
        return Ok(data_vec)
    }else {
        return Err(DataLakeError::custom(format!(
            "serialize_vec_dataStructure  序列化失败  index:{}  data_len:{}",
            index, data_len
        )));
    }
}

pub async fn data_read(
    streamreadstruct: &StreamReadStruct,
) -> Result<Option<Vec<u8>>, DataLakeError> {
    let partition_name = format!(
        "{}-{}",
        &streamreadstruct.table_name, &streamreadstruct.partition_code
    );

    let mut log_files = entity_lib::function::get_list_filename(&partition_name).await;

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

    let offset = streamreadstruct.offset;
    let read_count = streamreadstruct.read_count;
    let option_index_path = binary_search(&file_vec, offset).await?;

    match option_index_path {
        None => {
            return Ok(None);
        }
        Some((index_name, index_path)) => {
            let (index_path, start_index, end_index,_) = find_data(index_name, index_path, offset, read_count).await?;
            let stream_data = load_data(index_path, &start_index, &end_index).await?;
            return Ok(Some(stream_data));
        }
    }
}

/** 找到 offset 存在的索引文件 **/
pub async fn binary_search<'a>(
    file_vec: &'a Vec<&(String, String)>,
    offset: i64,
) -> Result<Option<(&'a String, &'a String)>, DataLakeError> {
    for index in 0..file_vec.len() {
        let (index_name, this_index_path) = file_vec[index];

        let index_file = OpenOptions::new().read(true).open(this_index_path).await?;

        let index_mmap = unsafe { Mmap::map(&index_file)? };

        let mmap_len = index_mmap.len();
        if mmap_len > INDEX_SIZE {
            let index_bytes = &index_mmap[(mmap_len - INDEX_SIZE)..mmap_len];

            let Index_struct = bincode::deserialize::<IndexStruct>(index_bytes)?;
            let file_end_offset = Index_struct.offset;

            if file_end_offset >= offset {
                return Ok(Some((index_name, this_index_path)));
            }
        }
    }
    return Ok(None);
}

/**
在索引文件内 通过二分查找   查找到 对应的offset
**/
pub async fn find_data<'a>(
    index_name: &String,
    index_path: &'a String,
    offset: i64,
    read_count: usize,
) -> Result<(&'a String, Option<IndexStruct>, IndexStruct, usize), DataLakeError> {
    let code = index_name.replace(".index", "").parse::<i64>()?;

    let index_file = OpenOptions::new().read(true).open(index_path).await?;

    let index_mmap = unsafe { Mmap::map(&index_file)? };
    let mut start_index: Option<IndexStruct> = None;
    let mut start_seek: usize = 0;
    let index_file_len = index_mmap.len();

    if offset >= code {
        let mut left = 0;
        let mut right = (index_file_len / INDEX_SIZE) - 1;
        while left <= right {
            let mid = left + ((right - left) / 2);
            start_seek = mid * INDEX_SIZE;

            let bytes_mid = &index_mmap[start_seek..(start_seek + INDEX_SIZE)];
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
    } else {
        // 已经执行到这了，如果 offset =< 文件名的code 那么就取第一个的索引
        let bytes_mid = &index_mmap[..INDEX_SIZE];
        let data_mid = bincode::deserialize::<IndexStruct>(bytes_mid)?;
        start_index = Some(data_mid);
        start_seek = 0;
    }

    // 获得 结尾的offset 位置
    let mut end_seek = start_seek + (read_count * INDEX_SIZE);

    if end_seek > index_file_len {
        // 如果结尾的offset位置超出索引文件的大小
        end_seek = index_file_len - INDEX_SIZE;

        // 获得结尾的 offset
        let bytes_end = &index_mmap[end_seek..end_seek + INDEX_SIZE];
        let end_index = bincode::deserialize::<IndexStruct>(bytes_end)?;

        return Ok((index_path, start_index, end_index, start_seek));
        
    } else {
        // 结尾的offset位置 没有 超出索引文件的大小

        // 获得结尾的 offset
        let bytes_end = &index_mmap[(end_seek - INDEX_SIZE)..end_seek];
        let end_index = bincode::deserialize::<IndexStruct>(bytes_end)?;
        return Ok((index_path, start_index, end_index, start_seek));

    }

}

async fn load_data(
    file_path: &String,
    start_index: &Option<IndexStruct>,
    end_index: &IndexStruct,
) -> Result<Vec<u8>, DataLakeError> {
    let start_file_seek = match start_index {
        Some(x) => x.start_seek,
        None => {
            return Err(DataLakeError::custom(
                "妈的二分查找没找到对应的offset".to_string(),
            ));
        }
    } as usize;

    let data_path = file_path.replace(".index", ".snappy");
    let data_file = OpenOptions::new().read(true).open(data_path).await?;
    let data_mmap = unsafe { Mmap::map(&data_file)? };

    let end_file_seek = end_index.end_seek as usize;

    let read_data = &data_mmap[start_file_seek..end_file_seek];

    let bb = read_data[..4].try_into()?;
    let cc = i32::from_be_bytes(bb);

    return Ok(read_data.to_vec());
}
