mod analysis;

use crate::analysis::alter_daql::{alter_daql_add, alter_daql_orop};
use crate::analysis::select_daql::select_analysis;
use analysis::create_daql::create_analysis;
use entity_lib::entity::DaqlEntity::DaqlType;
use entity_lib::entity::Error::DataLakeError;
use public_function::string_trait::StringFunction;

fn get_beginning(sql: &String) -> String {
    let mut identification = String::new();
    for ch in sql.chars() {
        if ch == ' ' {
            break;
        }
        identification.push(ch);
    }
    return identification;
}

pub async fn daql_analysis_function<'a>(daql: &'a str) -> Result<DaqlType, DataLakeError> {
    let lowercase_sql = daql.to_lowercase();
    let sql = lowercase_sql.process_string();

    let identification = get_beginning(&sql);

    if identification.eq("create") {
        let table_Key_words = sql[7..12].trim();
        if table_Key_words.eq("table") {
            let create_table = create_analysis(&sql).await?;

            return Ok(DaqlType::CREATE_TABLE(create_table));
        }
    } else if identification.eq("select") {
        let select_table = select_analysis(&sql).await?;
        return Ok(DaqlType::SELECT_TABLE(select_table));
    } else if identification.eq("alter") {
        let alter_spl = sql.split(" ").collect::<Vec<&str>>();
        if alter_spl.len() == 5 {
            let orop_col = alter_daql_orop(&alter_spl).await?;
            return Ok(DaqlType::ALTER_OROP(orop_col));
        } else if alter_spl.len() == 6 || alter_spl.len() == 8 {
            let add_col = alter_daql_add(&alter_spl).await?;
            return Ok(DaqlType::ALTER_ADD(add_col));
        }
    }else if identification.eq("show") {
        let table_name = sql.split(" ").collect::<Vec<&str>>()[1].to_string();
        return Ok(DaqlType::SHOW_TABLE(table_name));
    }else if identification.eq("compress") {
        let table_name = sql.split(" ").collect::<Vec<&str>>()[1].to_string();
        return Ok(DaqlType::COMPRESS_TABLE(table_name));
    }else if identification.eq("drop") {
        let table_name = sql.split(" ").collect::<Vec<&str>>()[1].to_string();
        return Ok(DaqlType::DROP_TABLE(table_name));
    }else if identification.eq("max_offset") {
        let table_name = sql.split(" ").collect::<Vec<&str>>()[1].to_string();
        return Ok(DaqlType::MAX_OFFSET(table_name));
    }

    return Err(DataLakeError::custom(format!("{}  语句错误", sql)));
}
