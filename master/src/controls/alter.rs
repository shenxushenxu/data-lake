use crate::controls::metadata::{get_metadata, get_table_path};
use entity_lib::entity::Error::DataLakeError;
use public_function::MASTER_CONFIG;
use tokio::fs::{File};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use entity_lib::entity::MasterEntity::{ColumnConfigJudgment, DataType, TableStructure};
use crate::controls::stream_read::STREAM_TCP_TABLESTRUCTURE;

pub async fn alter_orop(alteradd: (String, String)) -> Result<(), DataLakeError> {
    let table_name = alteradd.0;
    let col_name = alteradd.1;
    let mut tablestruct = get_metadata(&table_name).await?;

    let major_key = &tablestruct.major_key;

    if major_key.eq(col_name.as_str()) {
        return Err(DataLakeError::custom(format!(
            "{} 不能删除主键列",
            col_name
        )));
    }

    let mut col_type = tablestruct.col_type;
    if col_type.contains_key(&col_name) {
        col_type.remove(&col_name);
        tablestruct.col_type = col_type;

        let data = bincode::serialize(&tablestruct)?;

        {
            let mut vec_stream = Vec::<(String, TableStructure)>::new();

            let mut tcp_tablestructure = STREAM_TCP_TABLESTRUCTURE.lock().await;
            for key in tcp_tablestructure.keys() {
                if key.contains(table_name.as_str()) {
                    vec_stream.push((key.clone(), tablestruct.clone()));
                }
            }

            for (key, tabl) in vec_stream {
                let (stream, _) = tcp_tablestructure.remove(&key).unwrap();

                tcp_tablestructure.insert(key, (stream, tabl));
            }
        }

        
        
        let file_path = get_table_path(&table_name).await?;
        let mut file = File::create(file_path).await?;
        file.write_all(data.as_slice()).await?;

    } else {
        return Err(DataLakeError::custom(format!(
            "{} This column does not exist.",
            col_name
        )));
    }

    return Ok(());
}

pub async fn alter_add(
    alteradd: (String, String, String, String, String),
) -> Result<(), DataLakeError> {
    let table_name = alteradd.0;
    let col_name = alteradd.1;
    let type_name = alteradd.2;
    let key_words_1 = alteradd.3;
    let key_words_2 = alteradd.4;

    let mut tablestruct = get_metadata(&table_name).await?;



    let mut col_type = tablestruct.col_type;


    if col_type.contains_key(&col_name) {
        return Err(DataLakeError::custom(format!(
            "{} 列已存在， 不可重复添加",
            col_name
        )));
    } else {
        let data_type = match type_name.as_str() {
            "string" => DataType::string,
            "int" => DataType::int,
            "float" => DataType::float,
            "boolean" => DataType::boolean,
            "long" => DataType::string,
            _ => {
                return Err(DataLakeError::custom(format!(
                    "Unknown column type {}",
                    type_name
                )));
            }
        };

        let column_config = match (key_words_1.as_str(), key_words_2.as_str()) {
            ("primary", "key") => {
                return Err(DataLakeError::custom(format!("{} 不允许 PRIMARY KEY", col_name)));
            },
            ("not", "null") => {
                return Err(DataLakeError::custom(format!("{} 不允许 NOT NULL", col_name)));
            },
            ("default", _) =>{
                let value = if key_words_2.contains("'"){
                    key_words_2.replace("'", "")

                }else {
                    key_words_2.to_string()
                };

                (ColumnConfigJudgment::DEFAULT, Some(value))
            }
            ("", "") => (ColumnConfigJudgment::NOT, None),
            _ => {
                return Err(DataLakeError::custom(format!(
                    "Unknown column config {} {}",
                    key_words_1, key_words_2
                )));
            }
        };

        col_type.insert(col_name.to_string(), (data_type, column_config.0, column_config.1));
        tablestruct.col_type = col_type;

        {
            let mut vec_stream = Vec::<(String, TableStructure)>::new();
            let mut tcp_tablestructure = STREAM_TCP_TABLESTRUCTURE.lock().await;

            for key in tcp_tablestructure.keys() {
                if key.contains(table_name.as_str()) {
                    vec_stream.push((key.clone(), tablestruct.clone()));
                }
            }

            for (key, tabl) in vec_stream {
                let (stream, _) = tcp_tablestructure.remove(&key).unwrap();
                tcp_tablestructure.insert(key, (stream, tabl));
            }

        }


        let file_path = get_table_path(&table_name).await?;
        let data = bincode::serialize(&tablestruct)?;
        let mut file = File::create(file_path).await?;
        file.write_all(data.as_slice()).await?;
        
    }

    Ok(())
}
