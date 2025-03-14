use std::collections::HashMap;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use entity_lib::entity::MasterEntity::{Create, DataType, TableStructure};
use entity_lib::entity::SlaveEntity::{SlaveCreate, SlaveMessage};
use public_function::MASTER_CONFIG;

pub async fn create_table(create_struct: Create) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!(
            "{}\\{}",
            MASTER_CONFIG.get("master.data.path").unwrap(),
            &create_struct.table_name
        ))
        .await
        .unwrap();

    if file.metadata().await.unwrap().len() == 0 {
        let slave_nodes: Vec<&str> = serde_json::from_str(MASTER_CONFIG.get("slave.nodes").unwrap()).unwrap();

        let mut partition_address = HashMap::<usize, String>::new();

        let mut index = 0;
        for code in 0..create_struct.partition_number {
            if index == slave_nodes.len() {
                index = 0;
            }

            let node_address = slave_nodes[index];
            let mut stream = TcpStream::connect(node_address).await.unwrap();

            let slave_message = SlaveMessage::create(SlaveCreate {
                tablename: format!("{}-{}", create_struct.table_name.clone(), code),
            });

            let par_mess = bincode::serialize(&slave_message).unwrap();

            stream.write_i32(par_mess.len() as i32).await.unwrap();
            stream.write_all(&par_mess).await.unwrap();

            index += 1;

            partition_address.insert(code, node_address.to_string());

        }

        let mut col_type = HashMap::<String, DataType>::new();
        for (col_name, data_type) in create_struct.col_type.into_iter(){

            col_type.insert(col_name.to_lowercase(), data_type);
        }



        let table_structure = TableStructure{
            table_name: create_struct.table_name.to_lowercase(),
            col_type: col_type,
            partition_address: partition_address,
            partition_number: create_struct.partition_number,
            major_key: create_struct.major_key.to_lowercase()
        };



        let bincode_table_structure = bincode::serialize(&table_structure).unwrap();
        file.write_all(&bincode_table_structure).await.unwrap();

        return Ok(());
    } else {
        return Err(format!("{} 表已经存在", create_struct.table_name));
    }
}
