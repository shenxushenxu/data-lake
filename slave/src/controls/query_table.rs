use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::{DataStructure, QueryMessage};
use memmap2::Mmap;
use public_function::SLAVE_CONFIG;
use snap::raw::Decoder;
use std::collections::HashMap;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use public_function::read_function::get_slave_path;

pub async fn query(querymessage: QueryMessage, uuid:&String) -> Result<Option<Vec<String>>, DataLakeError> {

    let partition_name = &querymessage.tablename;

    // let data_path = get_slave_path(partition_name).await?;
    // let log_path = format!(
    //     "{}\\log",
    //     data_path
    // );
    // let compress_path = format!(
    //     "{}\\compress",
    //     data_path
    // );

    let mut log_file = public_function::get_list_filename(&partition_name).await;
    // let mut compress_files = public_function::get_list_filename(&compress_path[..]).await;
    // log_file.append(&mut compress_files);

    let mut file_vec = log_file
        .iter()
        .filter(|x1| {
            let file_name = &x1.0;
            if file_name.contains(".snappy") {
                return true;
            }
            return false;
        })
        .collect::<Vec<&(String, String)>>();

    let f_v = file_vec
        .iter()
        .map(|x2| x2.1.clone())
        .collect::<Vec<String>>();

    let (res_map, temp_file) = public_function::read_function::data_duplicate_removal(f_v, uuid).await?;

    let temp_mmap = unsafe { Mmap::map(&temp_file) }?;

    let res_vec = res_map
        .into_iter()
        .map(|(k, v)| {
            let data = &temp_mmap[v.0..(v.0 + v.1)];
            let data_structure = bincode::deserialize::<DataStructure>(data).unwrap();

            data_structure.data
        })
        .filter(|x| {
            let map = serde_json::from_str::<HashMap<String, String>>(x).unwrap();

            let mut i = 0;
            if let Some(condition) = &querymessage.conditions {
                for con in condition.iter() {
                    if let Some(value) = map.get(&con.0) {
                        if con.1.eq("=") {
                            if con.2.eq(value) {
                                i += 1;
                            }
                        } else if con.1.eq("<") {
                            if con.2.parse::<i64>().unwrap() < value.parse::<i64>().unwrap() {
                                i += 1;
                            }
                        } else if con.1.eq(">") {
                            if con.2.parse::<i64>().unwrap() > value.parse::<i64>().unwrap() {
                                i += 1;
                            }
                        }
                    }
                }

                if i == condition.len() {
                    return true;
                }
            } else {
                return true;
            }
            return false;
        })
        .collect::<Vec<String>>();

    if res_vec.len() > 0 {
        return Ok(Some(res_vec));
    } else {
        return Ok(None);
    }
}
