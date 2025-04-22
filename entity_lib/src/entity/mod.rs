pub mod MasterEntity;
pub mod SlaveEntity;
pub mod Error;
pub mod DaqlEntity;

use serde::{Deserialize, Serialize};
use crate::entity::MasterEntity::Statement;



#[test]
pub fn nnn(){
    let bb = r#"
    {"insert":
    {
    "data":"{\"shengao\":\"180\",\"col_age\":\"22\",\"xingbie\":\"男\",\"col_id\":\"1\",\"col_name\":\"申旭\"}",
    "table_name":"test_table",
    "crud":"insert",
    "partition_code":1
    }
    }

    "#;

    let state: Statement = serde_json::from_str(bb).unwrap();

    print!("{:?}", state);

}



















