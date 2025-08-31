pub mod BufferObject;
pub mod RandomNumber;
pub mod read_function;
pub mod string_trait;
pub mod vec_trait;
pub mod fast_validate;
pub mod table_structure;

use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use crate::entity::const_property;
use crate::entity::Error::DataLakeError;
use crate::entity::MasterEntity::{ColumnConfigJudgment, DataType, Info, PartitionInfo};

pub struct MasterConfig {
    pub master_data_port: String,
    pub master_data_path: Vec<String>,
    pub slave_nodes: Vec<String>,
    pub slave_index: usize,
    pub data_path_index: usize,

}

impl MasterConfig {
    pub fn new(
        master_data_port: String,
        master_data_path: Vec<String>,
        slave_nodes: Vec<String>,
    ) -> Self {
        MasterConfig {
            master_data_port: master_data_port,
            master_data_path: master_data_path,
            slave_nodes: slave_nodes,
            slave_index: 0,
            data_path_index: 0,
        }
    }


    // master_data_path_index
    pub async fn get_master_data_path(&mut self) -> String {
        let master_data_path = &self.master_data_path;


        if self.data_path_index >= master_data_path.len() {
            self.data_path_index = 0;
        }

        let data_path = master_data_path[self.data_path_index].clone();

        self.data_path_index = self.data_path_index + 1;

        return data_path;
    }

    // master_slave_index
    pub async fn get_slave_nodes(&mut self) -> String {
        let slave_nodes = &self.slave_nodes;

        if self.data_path_index >= slave_nodes.len() {
            self.data_path_index = 0;
        }
        let address = slave_nodes[self.data_path_index].clone();

        self.data_path_index = self.data_path_index + 1;

        return address;
    }
}

pub struct SlaveConfig {
    pub slave_node: String,
    pub slave_data: Vec<String>,
    pub slave_file_segment_bytes: usize,
    pub slave_replicas_sync_num: usize,
    pub slave_insert_cache_time_second: u64,
    pub slave_compaction_log_retain_number: i32,
    pub data_path_index: usize,


}

impl SlaveConfig {
    pub fn new(
        slave_node: String,
        slave_data: Vec<String>,
        slave_file_segment_bytes: usize,
        slave_replicas_sync_num: usize,
        slave_insert_cache_time_second: u64,
        slave_compaction_log_retain_number: i32
    ) -> Self {
        SlaveConfig {
            slave_node: slave_node,
            slave_data: slave_data,
            slave_file_segment_bytes: slave_file_segment_bytes,
            slave_replicas_sync_num: slave_replicas_sync_num,
            slave_insert_cache_time_second: slave_insert_cache_time_second,
            slave_compaction_log_retain_number: slave_compaction_log_retain_number,
            data_path_index: 0
        }
    }

    // slave_data_path_index
    pub async fn get_slave_data(&mut self) -> String {
        let master_data_path = &self.slave_data;

        let mut path_index = self.data_path_index;

        let data_path = master_data_path[path_index].clone();

        if path_index == (master_data_path.len() - 1) {
            self.data_path_index = 0;
        } else {
            self.data_path_index = path_index + 1;
        }

        return data_path;
    }
}

pub static MASTER_CONFIG: LazyLock<Mutex<MasterConfig>> = LazyLock::new(|| {
    Mutex::new(MasterConfig::new(
        "".to_string(),
        vec!["".to_string()],
        vec!["".to_string()],
    ))
});

pub static SLAVE_CONFIG: LazyLock<Mutex<SlaveConfig>> =
    LazyLock::new(|| Mutex::new(SlaveConfig::new("".to_string(), vec!["".to_string()], 0,0, 0,0)));








pub fn load_properties(file_path: &str) -> HashMap<String, String> {
    let content = std::fs::read_to_string(file_path).unwrap();

    let mut map = HashMap::new();

    content
        .lines()
        .into_iter()
        .filter(|line| line.contains("="))
        .map(|x| x.split("=").collect::<Vec<&str>>())
        .for_each(|x1| {
            let key = x1[0].trim();
            let value = x1[1].trim();

            map.insert(key.to_string(), value.to_string());
        });

    return map;
}

/**
根据 分区名称 获得分区下的 索引文件 和数据文件的文件名和路径
**/
pub async fn get_list_filename(partition_name: &str) -> Vec<(String, String)> {

    let slave_data = {
        let mut slave_config = SLAVE_CONFIG.lock().await;
        let slave_data = &slave_config.slave_data;
        slave_data.clone()
    };


    let partition_path_vec = slave_data
        .iter()
        .filter(|x| {
            let partiti_path = format!("{}/{}", x, partition_name);
            check_file_exists(&partiti_path)
        })
        .map(|x1| {
            let partiti_path = format!("{}/{}", x1, partition_name);
            partiti_path
        }).collect::<Vec<String>>();

    let partition_path = &partition_path_vec[0];

    let mut file_name_vec = Vec::new();

    let mut folder_entries = tokio::fs::read_dir(partition_path).await.unwrap();
    // 遍历条目并打印文件名
    while let Some(folder_entry) = folder_entries.next_entry().await.unwrap() {
        let folder_entry_path = folder_entry.path(); // 获取条目路径
        if !folder_entry_path.is_file() {
            let mut file_entries = tokio::fs::read_dir(folder_entry_path.display().to_string())
                .await
                .unwrap();

            while let Some(file_entry) = file_entries.next_entry().await.unwrap() {
                let file_entry_path = file_entry.path(); // 获取条目路径
                if file_entry_path.is_file() {
                    let file_name = file_entry_path
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string();
                    let file_path = file_entry_path.display().to_string();

                    if !file_name.contains("temp_") {
                        file_name_vec.push((file_name, file_path));
                    }
                }
            }
        }
    }

    return file_name_vec;
}

/**
根据分区名称 获得分区在哪个路径下 并返回路径
**/
pub async fn get_partition_path(partition_code: &str) -> Result<String, DataLakeError> {
    let slave_data = {
        let slave_config = SLAVE_CONFIG.lock().await;
        let slave_data = &slave_config.slave_data;
        slave_data.clone()
    };


    for path in slave_data {
        let mut folder_entries = tokio::fs::read_dir(path).await.unwrap();
        while let Some(folder_entry) = folder_entries.next_entry().await.unwrap() {
            let folder_entry_path = folder_entry.path(); // 获取条目路径
            if !folder_entry_path.is_file() {
                let this_file_name = folder_entry_path.file_name().unwrap().to_str().unwrap();
                if this_file_name == partition_code {
                    return Ok(folder_entry_path.display().to_string());
                }
            }
        }
    }

   return Err(DataLakeError::custom(format!("Failed to get partition path: {}", partition_code)));
}

/**
发送异常
**/
pub async fn write_error(err: DataLakeError, write_half: &mut WriteHalf<'_>) {

    let addr = write_half.local_addr().unwrap().to_string();

    write_half.write_i32(-2).await.unwrap();

    let err_mess = format!("{} ERROR: {}", addr, err);
    let bytes = err_mess.as_bytes();
    let bytes_len = bytes.len();

    write_half.write_i32(bytes_len as i32).await.unwrap();
    write_half.write_all(bytes).await.unwrap();
}

/**
读取异常
**/
pub async fn read_error(stream: &mut TcpStream) -> Result<(), DataLakeError> {
    let is = stream.read_i32().await?;
    if is == -2 {
        let len = stream.read_i32().await?;

        let mut mess = vec![0u8; len as usize];
        stream.read_exact(&mut mess).await?;
        let dd = String::from_utf8(mess)?;
        return Err(DataLakeError::custom(dd));
    }

    return Ok(());
}



// 填充默认值
pub fn data_complete<'a>(
    col_type: &'a HashMap<String, (DataType, ColumnConfigJudgment, Option<String>)>,
    data: &mut HashMap<&'a str, &'a str>,
    major_value: &'a str
){

    let mut insert_map = HashMap::<&str, &str>::new();
    for (key, value) in col_type.iter() {
        match data.get(key.as_str()) {
            None => {
                if let ColumnConfigJudgment::PRIMARY_KEY =value.1{
                    insert_map.insert(key.as_str(), major_value);
                }else {
                    insert_map.insert(key.as_str(), const_property::NULL_STR);
                }
            }
            Some(data_value) => {
                if *data_value == const_property::NULL_STR {
                    let column_configh_judgment = &value.1;
                    if let ColumnConfigJudgment::DEFAULT = column_configh_judgment {
                        let default_option = &value.2;
                        if let Some(default_value) = default_option {
                            insert_map.insert(key.as_str(), default_value.as_str());
                        }
                    }
                }
            }
        }
    }

    for (k, v) in insert_map.into_iter() {
        data.insert(k, v);
    }

}



/**
根据 分区信息获得活跃分区
**/
pub fn get_leader_partition<'a>(table_path:&str, partition_info_vec: &'a Vec<PartitionInfo>) -> Result<&'a String, DataLakeError> {

    let leader_address_vec = partition_info_vec
        .iter()
        .filter(|x| match x.info {
            Info::Leader => true,
            Info::Follower => false,
        })
        .map(|x1| &x1.address)
        .collect::<Vec<&String>>();

    let leader_address_vec_len = leader_address_vec.len();


    let leader_address = if leader_address_vec_len == 1 {
        leader_address_vec[0]
    } else {
        if leader_address_vec_len == 0 {
            return Err(DataLakeError::custom(format!(
                "{}  没有Leader 的分区",
                table_path
            )));
        } else if leader_address_vec_len > 1 {
            return Err(DataLakeError::custom(format!(
                "{}  有多个Leader的分区",
                table_path
            )));
        } else {
            return Err(DataLakeError::custom(format!(
                "{}  的元数据出错，Leader 分区标记错乱",
                table_path
            )));
        }
    };

    return Ok(leader_address);

}



/**
查看路径是否存在
**/
pub fn check_file_exists(path: &str) -> bool {
    let path = Path::new(path);
    return path.exists();
}