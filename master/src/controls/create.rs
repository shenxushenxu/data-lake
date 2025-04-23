use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{TableStructure};
use public_function::MASTER_CONFIG;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use entity_lib::entity::SlaveEntity::{SlaveCreate, SlaveMessage};

pub async fn create_table(table_structure: TableStructure) -> Result<(), DataLakeError> {
    let file_path = format!(
        "{}\\{}",
        MASTER_CONFIG.get("master.data.path").unwrap(),
        &table_structure.table_name
    );

    match tokio::fs::metadata(&file_path).await {
        Ok(_) => {
            return Err(DataLakeError::CustomError(format!(
                "{} 表已经存在",
                table_structure.table_name
            )));
        }
        Err(_) => {

            let partition_address = &table_structure.partition_address;
            for (code, address) in partition_address {
                let slavecreate = SlaveCreate {
                    tablename: format!("{}-{}", &table_structure.table_name, &code)
                };
                let slave_message_create = SlaveMessage::create(slavecreate);

                let mut stream = TcpStream::connect(&address).await?;

                let slave = bincode::serialize(&slave_message_create)?;
                stream.write_i32(slave.len() as i32).await?;
                stream.write_all(slave.as_slice()).await?;
            }


            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file_path)
                .await?;
            let bincode_table_structure = bincode::serialize(&table_structure)?;
            file.write_all(&bincode_table_structure).await?;


        }
    }

    return Ok(());
}
