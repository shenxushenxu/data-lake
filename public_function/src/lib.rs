pub mod PosttingTcpStream;
pub mod RandomNumber;
pub mod read_function;
pub mod string_trait;

use entity_lib::entity::Error::DataLakeError;
use std::collections::HashMap;
use std::iter::Map;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::sync::LazyLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::WriteHalf;
use tokio::sync::Mutex;
use entity_lib::entity::MasterEntity::{ColumnConfigJudgment, DataType};

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

        
        let mut data_path_index = self.data_path_index;
        
        let data_path = master_data_path[data_path_index].clone();

        if data_path_index == (master_data_path.len() - 1) {
            self.data_path_index = 0;
        } else {
            self.data_path_index = data_path_index + 1;
        }
        
        
        return data_path;
    }

    // master_slave_index
    pub async fn get_slave_nodes(&mut self) -> String {
        let slave_nodes = &self.slave_nodes;

        let mut master_slave_index = self.data_path_index;
        let address = slave_nodes[master_slave_index].clone();

        if master_slave_index == (slave_nodes.len() - 1) {
            self.data_path_index = 0;
        } else {
            self.data_path_index = master_slave_index + 1;
        }
        
        return address;
    }
}

pub struct SlaveConfig {
    pub slave_node: String,
    pub slave_data: Vec<String>,
    pub slave_file_segment_bytes: usize,
    pub slave_replicas_sync_num: usize,
    pub data_path_index: usize,
}

impl SlaveConfig {
    pub fn new(
        slave_node: String,
        slave_data: Vec<String>,
        slave_file_segment_bytes: usize,
        slave_replicas_sync_num: usize
    ) -> Self {
        SlaveConfig {
            slave_node: slave_node,
            slave_data: slave_data,
            slave_file_segment_bytes: slave_file_segment_bytes,
            slave_replicas_sync_num: slave_replicas_sync_num,
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
    LazyLock::new(|| Mutex::new(SlaveConfig::new("".to_string(), vec!["".to_string()], 0,0)));








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
pub async fn get_list_filename(partition_code: &str) -> Vec<(String, String)> {

    let slave_data = {
        let mut slave_config = SLAVE_CONFIG.lock().await;
        let slave_data = &slave_config.slave_data;
        slave_data.clone()
    };
    

    let partition_path_vec = slave_data
        .iter()
        .filter(|x| {
            let partiti_path = format!("{}/{}", x, partition_code);
            let path = Path::new(partiti_path.as_str());
            if path.exists() { true } else { false }
        })
        .map(|x1| {
            let partiti_path = format!("{}/{}", x1, partition_code);
            partiti_path
        })
        .collect::<Vec<String>>();

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
pub async fn get_partition_path(partition_code: &str) -> String {
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
                    return folder_entry_path.display().to_string();
                }
            }
        }
    }
    
    panic!("Failed to get partition path: {}", partition_code);
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
pub async fn data_complete<'a>(
    col_type: &HashMap<String, (DataType, ColumnConfigJudgment, Option<String>)>, data: &mut HashMap<String, String> ){

    let mut insert_map = HashMap::<String, String>::new();
    for (key, value) in col_type.iter() {
        match data.get(key) {
            None => {
                insert_map.insert(key.clone(), String::from(""));
            }
            Some(data_value) => {
                if data_value.is_empty() {
                    let column_configh_judgment = &value.1;
                    if let ColumnConfigJudgment::DEFAULT = column_configh_judgment {
                        let default_option = &value.2;
                        if let Some(default_value) = default_option {
                            insert_map.insert(key.clone(), default_value.clone());
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