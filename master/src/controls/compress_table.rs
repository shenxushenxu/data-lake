use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::SlaveMessage;
use entity_lib::function::table_structure::get_table_structure;

pub async fn compress_table(table_name: &String) -> Result<(), DataLakeError> {

    let table_structure =
        get_table_structure(&table_name).await?;

    let address_map = table_structure.partition_address.clone();

    let mut join_handle_set:JoinSet<Result<(), DataLakeError>> = tokio::task::JoinSet::new();
    
    for (key,partition_info_vec) in address_map {

        
        for partition_info in partition_info_vec.into_iter() {
            let partition_code = format!("{}-{}", table_name, key);

            join_handle_set.spawn(async move {
                let address = partition_info.address;
                let mut stream = TcpStream::connect(address).await?;

                let message = SlaveMessage::compress_table(partition_code.clone());


                let bytes = bincode::serialize(&message)?;
                let bytes_len = bytes.len();


                stream.write_i32(bytes_len as i32).await?;
                stream.write_all(&bytes).await?;

                entity_lib::function::read_error(&mut stream).await?;
                
                Ok(())
            });
            
        }
    }


    while let Some(res) = join_handle_set.join_next().await {
        match res {
            Ok(ee) => {
                if let Err(e) = ee {
                    
                    return Err(e);
                }
            }
            Err(e) => {
                eprintln!("Task failed: {:?}", e);
                return Err(DataLakeError::custom("压缩表数据的协程报错了。".to_string()));
            }
        }
    }
    
    
    return Ok(());
}