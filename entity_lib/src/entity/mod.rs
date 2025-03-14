use std::collections::HashMap;
use std::sync::LazyLock;
use serde::{Deserialize, Serialize};
use serde_json::json;









#[derive(Serialize, Deserialize, Debug)]
pub enum data_type {
    string(String),
    int(String),
    float(String),
    boolean(String),
    long(String),
}


/*
create 语句的 struct 类
*/
#[derive(Serialize, Deserialize, Debug)]
pub struct Create{
    // 表名
    pub name: String,
    // 表的列名和对应的类型
    pub col_type: Vec<data_type>,
    // 分区数
    pub partition_number: usize,
    // 主键的名字
    pub major_key: String,
}




/**
语句的 枚举类


**/
#[derive(Serialize, Deserialize, Debug)]
pub enum Statement {
    create(Create),
    select,
    insert,
}





#[test]
pub fn nnn(){
    let bb = r#"
        {
  "create": {
    "name": "test_table",
    "col_type": [
      {
        "string": "col_name"
      },
      {
        "int": "col_age"
      },
      {
        "long": "shengao"
      },
      {
        "boolean": "xingbie"
      }
    ],
    "partition_number": 4
  }
}
        "#;

    let state: Statement = serde_json::from_str(bb).unwrap();

    print!("{:?}", state);

}



















