use std::collections::HashMap;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{Create, DataType, TableStructure};
use entity_lib::entity::SlaveEntity::{SlaveCreate, SlaveMessage};
use public_function::MASTER_CONFIG;

pub async fn create_table(create_struct: Create) -> Result<(), DataLakeError> {

    let file_path = format!(
        "{}\\{}",
        MASTER_CONFIG.get("master.data.path").unwrap(),
        &create_struct.table_name
    );




    match std::fs::metadata(&file_path){
        Ok(_) => {
            return Err(DataLakeError::CustomError(format!("{} 表已经存在", create_struct.table_name)));
        }
        Err(_) => {

            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file_path)
                .await?;

            let slave_nodes = MASTER_CONFIG.get("slave.nodes").unwrap();
            let slave_nodes: Vec<&str> = serde_json::from_str(slave_nodes)?;

            let mut partition_address = HashMap::<usize, String>::new();

            let mut index = 0;
            for code in 0..create_struct.partition_number {
                if index == slave_nodes.len() {
                    index = 0;
                }

                let node_address = slave_nodes[index];
                let mut stream = TcpStream::connect(node_address).await?;

                let slave_message = SlaveMessage::create(SlaveCreate {
                    tablename: format!("{}-{}", create_struct.table_name.clone(), code),
                });

                let par_mess = bincode::serialize(&slave_message)?;

                stream.write_i32(par_mess.len() as i32).await?;
                stream.write_all(&par_mess).await?;

                public_function::read_error(&mut stream).await?;


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



            let bincode_table_structure = bincode::serialize(&table_structure)?;
            file.write_all(&bincode_table_structure).await?;

            return Ok(());
        }
    }
}
