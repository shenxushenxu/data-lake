use std::error::Error;
use memmap2::Mmap;
use tokio::fs::OpenOptions;
use entity_lib::entity::const_property::METADATA_LOG;
use entity_lib::entity::Error::DataLakeError;
use public_function::get_partition_path;

pub async fn get_max_offset(partition_code: &String) -> Result<i64, DataLakeError> {

    let path = get_partition_path(partition_code).await;
    let metadata_path = format!("{}/{}",path, METADATA_LOG);

    let metadata_file = OpenOptions::new()
        .read(true)
        .open(metadata_path)
        .await?;
    let metadata_mmap = unsafe {Mmap::map(&metadata_file)?};
    
    // 因为 metadata.log 文件存储的最大 offset 是 数据中最大offset + 1 所以这里 - 1
    let max_offset = i64::from_be_bytes((&metadata_mmap[..]).try_into().unwrap());
    let res_offset = max_offset - 1;
    return Ok(res_offset);
}