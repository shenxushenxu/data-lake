use std::collections::HashMap;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{ColumnConfigJudgment, DataType, TableStructure};
use public_function::MASTER_CONFIG;

pub async fn create_analysis(sql: &String) -> Result<TableStructure, DataLakeError> {
    // 获得左括号的位置
    let left_parenthesis_index = sql.find("(").unwrap();
    // 获得表名
    let tablename = sql[12..left_parenthesis_index].trim();
    // 获得右括号的位置
    let right_parenthesis_index = sql.find(')').unwrap();
    // 获得列的信息
    let col_type_Identification = sql[left_parenthesis_index+1..right_parenthesis_index].trim();

    let col_information = col_type_Identification
        .split(",")
        .map(|c| {
            let spl = c.split_whitespace().collect::<Vec<&str>>();

            let res = match spl.len() {
                2 => {
                    Ok((spl[0], spl[1], "", ""))
                }
                4 => {
                    Ok((spl[0], spl[1], spl[2], spl[3]))
                }
                _ => {
                    Err(DataLakeError::CustomError(format!("{} 创建参数不对", tablename)))
                }
            };

            res
        }).collect::<Result<Vec<_>, _>>()?;

    let partition_config = sql[right_parenthesis_index+1..].trim();

    let config_split = partition_config
        .split("=")
        .map(|x| x.trim())
        .collect::<Vec<&str>>();

    // 解析列类型和配置
    let mut col_type = HashMap::<String, (DataType, ColumnConfigJudgment, Option<String>)>::new();

    for (col_name, type_name, key_words_1, key_words_2) in col_information {
        let data_type = match type_name {
            "string" => DataType::string,
            "int" => DataType::int,
            "float" => DataType::float,
            "boolean" => DataType::boolean,
            "long" => DataType::string,
            _ => {
                return Err(DataLakeError::CustomError(format!(
                    "Unknown column type {}",
                    type_name
                )));
            }
        };

        let column_config = match (key_words_1, key_words_2) {
            ("primary", "key") => (ColumnConfigJudgment::PRIMARY_KEY, None),
            ("not", "null") => (ColumnConfigJudgment::NOT_NULL, None),
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
                return Err(DataLakeError::CustomError(format!(
                    "Unknown column config {} {}",
                    key_words_1, key_words_2
                )));
            }
        };

        col_type.insert(col_name.to_string(), (data_type, column_config.0, column_config.1));
    }

    // 找到 该在哪些 slave 上创建分区
    let config_key = config_split[0];
    if !(config_key.eq("partition_number")) {
        return Err(DataLakeError::CustomError(format!("没有找到 partition_number")));
    }

    if config_split.len() != 2 {
        return Err(DataLakeError::CustomError(format!("{} 表 partition_number配置出错", tablename)));
    }

    let config_value = config_split[1];
    let patition_number = config_value.parse::<usize>()?;

    let mut partition_address = HashMap::<usize, String>::new();

    let slave_nodes = MASTER_CONFIG.get("slave.nodes").unwrap();
    let slave_nodes: Vec<&str> = serde_json::from_str(slave_nodes)?;
    let mut index = 0;
    for code in 0..patition_number {
        if index == slave_nodes.len() {
            index = 0;
        }
        let node_address = slave_nodes[index];
        index += 1;
        partition_address.insert(code, node_address.to_string());
    }

    let mut major_key:Option<String> = None;
    for (key, value) in col_type.iter() {
        let column_config = &value.1;

        match column_config {
            ColumnConfigJudgment::PRIMARY_KEY => {
                match major_key {
                    None => {  major_key = Some(key.clone()); }
                    Some(_) => { return Err(DataLakeError::CustomError(format!("{} 表内主键重复", tablename)));  }
                }
            }
            _ => (),
        }
    }

    if None == major_key {
        return Err(DataLakeError::CustomError(format!("{} 表内没有主键", tablename)));
    }


    let table_structure = TableStructure {
        table_name: tablename.to_string(),
        col_type: col_type,
        partition_address: partition_address,
        partition_number: patition_number,
        major_key: major_key.unwrap().to_string(),
    };

    Ok(table_structure)
}