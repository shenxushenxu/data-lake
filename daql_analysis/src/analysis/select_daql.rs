use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::SlaveEntity::QueryMessage;

pub async fn select_analysis(sql: &String) -> Result<QueryMessage, DataLakeError> {
    let convert_sql = sql.trim().to_lowercase();

    let from_index = convert_sql.find("from").unwrap();

    let cloums_str = &convert_sql[6..from_index];
    // 查询的列名
    let cloums = cloums_str.split(",").map(|s| s.trim().to_string()).collect::<Vec<String>>();


    let where_len = convert_sql.find("where").unwrap_or(convert_sql.len());
    //查询的表名
    let table_name = convert_sql[(from_index+4)..where_len].trim();



    let conditions = if where_len != convert_sql.len() {
        Some(&convert_sql[(where_len+5)..])
    }else {
        None
    };


    let con = if let Some(conditi) = conditions{
        let eeee = conditi.split("and").map(|c| {

            let biaoda = c.trim();

            if biaoda.contains("="){
                let vec = biaoda.split("=").collect::<Vec<&str>>();

                return Ok((vec[0].to_string(), "=".to_string(), vec[1].replace("'","")))
            }else if biaoda.contains("<") {
                let vec = biaoda.split("<").collect::<Vec<&str>>();

                return Ok((vec[0].to_string(), "<".to_string(), vec[1].replace("'","")))
            }else if biaoda.contains(">") {
                let vec = biaoda.split(">").collect::<Vec<&str>>();

                return Ok((vec[0].to_string(), ">".to_string(), vec[1].replace("'","")))
            }else {
                return Err(DataLakeError::custom("啥也没有  = < >".to_string()));
            }

        }).collect::<Result<Vec<(String, String, String)>, DataLakeError>>()?;

        Some(eeee)
    }else {
        None
    };

    let querymessage = QueryMessage{
        tablename: format!("{}", table_name),
        cloums:cloums.clone(),
        conditions:con.clone()
    };


    return Ok(querymessage);
}