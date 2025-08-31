use entity_lib::entity::Error::DataLakeError;
use tokio::fs::{File};
use tokio::io::AsyncWriteExt;
use entity_lib::entity::MasterEntity::{ColumnConfigJudgment, DataType};
use entity_lib::function::table_structure::{get_table_path, get_table_structure};

pub async fn alter_orop(alteradd: (String, String)) -> Result<(), DataLakeError> {
    let table_name = alteradd.0;
    let col_name = alteradd.1;
    let mut tablestruct = get_table_structure(&table_name).await?;

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

        
        let file_path = get_table_path(&table_name).await?;
        let mut file = File::create(file_path).await?;
        file.write_all(data.as_slice()).await?;
        file.flush().await?;
        // 清除掉表的缓存
        entity_lib::function::BufferObject::remove_table_cache(&table_name);
        
        return Ok(());

    } else {
        return Err(DataLakeError::custom(format!(
            "{} This column does not exist.",
            col_name
        )));
    }

}

pub async fn alter_add(
    alteradd: (String, String, String, String, String),
) -> Result<(), DataLakeError> {
    let table_name = alteradd.0;
    let col_name = alteradd.1;
    let type_name = alteradd.2;
    let key_words_1 = alteradd.3;
    let key_words_2 = alteradd.4;

    let mut tablestruct = get_table_structure(&table_name).await?;



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
            ("", "") => (ColumnConfigJudgment::NULL, None),
            _ => {
                return Err(DataLakeError::custom(format!(
                    "Unknown column config {} {}",
                    key_words_1, key_words_2
                )));
            }
        };

        col_type.insert(col_name.to_string(), (data_type, column_config.0, column_config.1));
        tablestruct.col_type = col_type;
        
        
        let file_path = get_table_path(&table_name).await?;
        let data = bincode::serialize(&tablestruct)?;
        let mut file = File::create(file_path).await?;
        file.write_all(data.as_slice()).await?;
        file.flush().await?;
        // 清除掉表的缓存
        entity_lib::function::BufferObject::remove_table_cache(&table_name);
        
        return Ok(());
    }

    
}
