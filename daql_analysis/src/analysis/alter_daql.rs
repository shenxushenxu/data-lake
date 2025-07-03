use entity_lib::entity::Error::DataLakeError;

pub async fn alter_daql_add(sql_element: &Vec<&str>) -> Result<(String, String,String, String,String), DataLakeError> {

    let table_name = sql_element[2].trim();
    let clo_name = sql_element[4].trim();
    let col_type = sql_element[5].trim();
    let key_word_1 = sql_element.get(6).unwrap_or(&"");
    let key_word_2 = sql_element.get(7).unwrap_or(&"");
    
    
    

    return Ok((table_name.to_string(), clo_name.to_string(), col_type.to_string(), key_word_1.to_string(), key_word_2.to_string()));

}


pub async fn alter_daql_orop(sql_element: &Vec<&str>) -> Result<(String, String), DataLakeError>{

    let table_name = sql_element[2].trim();
    let col_name = sql_element[4].trim();


    return Ok((table_name.to_string(), col_name.to_string()));
}