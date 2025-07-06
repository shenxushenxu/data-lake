use std::collections::HashMap;
use entity_lib::entity::const_property;
use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{ColumnConfigJudgment, DataType, Info, PartitionInfo, TableStructure};
use public_function::RandomNumber::{random_number};
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
                    Err(DataLakeError::custom(format!("{} 创建参数不对", tablename)))
                }
            };

            res
        }).collect::<Result<Vec<_>, _>>()?;

    

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
                return Err(DataLakeError::custom(format!(
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
                return Err(DataLakeError::custom(format!(
                    "Unknown column config {} {}",
                    key_words_1, key_words_2
                )));
            }
        };

        col_type.insert(col_name.to_string(), (data_type, column_config.0, column_config.1));
    }

    // 找到 该在哪些 slave 上创建分区
    let partition_config = sql[right_parenthesis_index+1..].trim();

    let table_config = partition_config.split(",").map(|x1| {
        x1.split("=").map(|x2| {
            x2.trim().to_string()
        }).collect::<Vec<String>>()
    }).collect::<Vec<Vec<String>>>();



    let mut config = HashMap::<&str, String>::new();
    
    for vec in table_config.iter() {
        let key = &vec[0];
        if vec.len() == 2 {
            let value = &vec[1];
            match key.as_str() {
                const_property::PARTITION_NUMBER =>{
                    config.insert(const_property::PARTITION_NUMBER, value.to_string());
                }
                const_property::REPLICAS_NUMBER =>{
                    config.insert(const_property::REPLICAS_NUMBER, value.to_string());
                }
                _ => {
                    return Err(DataLakeError::custom(format!("{} This configuration does not exist..", key)));
                }
            }
        }else {
            return Err(DataLakeError::custom(format!("{} misallocate.", key)));
        }
    }


    let patition_number = if let Some(patition_number) = config.get(const_property::PARTITION_NUMBER){
        patition_number.parse::<usize>()?
    }else {
        return Err(DataLakeError::custom(format!("unfound {} .", const_property::PARTITION_NUMBER)));
    };
    
    
    let replicas_number = if let Some(replicas_number) = config.get(const_property::REPLICAS_NUMBER) {
        replicas_number.parse::<usize>()?
    }else {
        return Err(DataLakeError::custom(format!("unfound {} .", const_property::REPLICAS_NUMBER)));
    };
    
    
    let mut master_config = MASTER_CONFIG.lock().await;
    let slave_nodes = &master_config.slave_nodes;

    let mut major_key:Option<String> = None;
    for (key, value) in col_type.iter() {
        let column_config = &value.1;

        match column_config {
            ColumnConfigJudgment::PRIMARY_KEY => {
                match major_key {
                    None => {  major_key = Some(key.clone()); }
                    Some(_) => { return Err(DataLakeError::custom(format!("{} Duplicate primary key in the table.", tablename)));  }
                }
            }
            _ => (),
        }
    }

    if None == major_key {
        return Err(DataLakeError::custom(format!("{} There is no primary key in the table.", tablename)));
    }

    let replicas = replicas_number + 1;
    if replicas > slave_nodes.len(){
        return Err(DataLakeError::custom(format!("{} The number of copies is greater than the number of slaves.", tablename)));
    }
    
    
    
    let mut partition_address = HashMap::<usize, Vec<PartitionInfo>>::new();


    for code in 0..patition_number {
        
        let mut par_vec = Vec::<PartitionInfo>::new();
        for _ in 0..replicas {
            let addres = master_config.get_slave_nodes().await;
            let info = Info::Follower;
            let partition_info = PartitionInfo{
                address: addres.clone(),
                info: info,
            };

            par_vec.push(partition_info);
        }
        
        partition_address.insert(code, par_vec);
    }

    // 选举 活跃 分区的代码
    let mut vote_map = HashMap::<String, usize>::new();

    for (_, partition_info_vec) in partition_address.iter_mut() {
        
        let mut sign = 0;
        let size = partition_info_vec.iter().len();
        for partition_info in partition_info_vec.iter_mut() {
            
            let addr = &partition_info.address;
            
            if None == vote_map.get(addr){
                partition_info.info = Info::Leader;
                vote_map.insert(addr.to_string(), 1);
                sign = 1;
                break;
            }
        }
        
        if sign == 0 {
            let random_number = random_number(size);
            partition_info_vec.get_mut(random_number).unwrap().info = Info::Leader;
        }
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