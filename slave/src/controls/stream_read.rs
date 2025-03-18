use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{DataStructure, IndexStruct, StreamReadStruct};
use memmap2::Mmap;
use public_function::SLAVE_CONFIG;
use snap::raw::Encoder;
use std::mem;
use tokio::fs::OpenOptions;



const INDEX_SIZE:usize = mem::size_of::<IndexStruct>();

pub async fn stream_read(
    streamreadstruct: &StreamReadStruct,
) -> Result<Option<Vec<u8>>, DataLakeError> {
    let log_path = format!(
        "{}\\{}-{}\\log",
        SLAVE_CONFIG.get("slave.data").unwrap(),
        &streamreadstruct.table_name,
        &streamreadstruct.partition_code
    );
    let compress_path = format!(
        "{}\\{}-{}\\compress",
        SLAVE_CONFIG.get("slave.data").unwrap(),
        &streamreadstruct.table_name,
        &streamreadstruct.partition_code
    );

    let mut log_files = public_function::get_list_filename(&log_path[..]).await;
    let mut compress_files = public_function::get_list_filename(&compress_path[..]).await;

    log_files.append(&mut compress_files);

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

    let mut offset = streamreadstruct.offset;
    let mut read_count = streamreadstruct.read_count;
    let index_path = binary_search(&file_vec, offset).await?;
    println!("index_path:  {:?}", index_path);
    let data = find_data(index_path, offset, read_count).await?;
    println!("data:  {:?}", data);

    return Ok(data);
}

/*找到 offset 存在的索引文件*/
async fn binary_search<'a>(file_vec: &'a Vec<&(String, String)>, offset: i64) -> Result<&'a String, DataLakeError> {

    for index in 0..file_vec.len() {
        let (this_index_name, this_index_path) = file_vec[index];

        let mut index_file = OpenOptions::new()
            .read(true)
            .open(this_index_path)
            .await?;

        let index_mmap = unsafe { Mmap::map(&index_file)?};

        let mmap_len = index_mmap.len();

        let index_bytes = &index_mmap[(mmap_len - INDEX_SIZE)..mmap_len];
        let Index_struct = bincode::deserialize::<IndexStruct>(index_bytes)?;
        let file_end_offset = Index_struct.offset;


        if file_end_offset >= offset {
            return Ok(this_index_path);
        }
    }

    let index_path = &file_vec.last().unwrap().1;

    return Ok(index_path);
}


pub async fn find_data(index_path: &String, offset: i64, read_count: usize) -> Result<Option<Vec<u8>>, DataLakeError> {


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
        return Ok(None);
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

    let mut data_file;
    if file_path.contains("log") {
        let data_path = file_path.replace(".index", ".log");
        data_file = OpenOptions::new().read(true).open(data_path).await?;
    } else {
        let data_path = file_path.replace(".index", ".snappy");
        data_file = OpenOptions::new().read(true).open(data_path).await?;
    }

    let data_mmap = unsafe { Mmap::map(&data_file)? };

    let end_file_seek = end_index.end_seek as usize;

    let read_data = &data_mmap[start_file_seek..end_file_seek];

    let vec_u8 = if file_path.contains("log") {
        let mut array_bytes_reader = ArrayBytesReader::new(read_data);
        let mut compr_vec = Vec::<u8>::new();
        loop {
            if array_bytes_reader.is_stop() {
                break;
            }
            let len = array_bytes_reader.read_i32();
            let data = array_bytes_reader.read(len as usize);

            let data_structure = bincode::deserialize::<DataStructure>(data)?;
            let json_value = serde_json::to_string(&data_structure)?;
            let mut encoder = Encoder::new();
            let mut compressed_data = encoder.compress_vec(json_value.as_bytes())?;
            let compressed_data_len = compressed_data.len() as i32;
            compr_vec.append(compressed_data_len.to_be_bytes().to_vec().as_mut());
            compr_vec.append(&mut compressed_data);
        }

        compr_vec
    } else {
        read_data.to_vec()
    };

    return Ok(vec_u8);
}

struct ArrayBytesReader<'a> {
    data: &'a [u8],
    array_pointer: usize,
}
impl<'a> ArrayBytesReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        return ArrayBytesReader {
            data: data,
            array_pointer: 0,
        };
    }

    pub fn read_i32(&mut self) -> i32 {
        let size = size_of::<i32>();

        let len = i32::from_be_bytes(
            self.data[self.array_pointer..self.array_pointer + size]
                .try_into()
                .unwrap(),
        );

        self.array_pointer += size;

        return len;
    }

    pub fn read(&mut self, len: usize) -> &'a [u8] {
        let uuu = &self.data[self.array_pointer..self.array_pointer + len];

        self.array_pointer += len;

        return uuu;
    }

    pub fn is_stop(&self) -> bool {
        if self.array_pointer == self.data.len() {
            return true;
        } else {
            return false;
        }
    }
}
