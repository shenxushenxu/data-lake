use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::format;
use crate::entity::Error::DataLakeError;
use crate::entity::SlaveEntity::SlaveMessage;
/*
master 批量插入的 struct
*/
#[derive(Serialize, Deserialize, Debug)]
pub struct BatchData {
    pub table_name: String,
    pub column: Vec<String>,
    pub data: Vec<Vec<String>>,
}

impl BatchData {
    pub fn get_line(&mut self) -> Vec<String> {
        return self.data.remove(self.get_data_size() - 1);
    }

    pub fn get_column_index(&self, column_name: &String) -> Result<usize, DataLakeError> {
        for index in 0..self.get_data_size() {
            let name = &self.column[index];
            if name == column_name {
                return Ok(index);
            }
        }
        return Err(DataLakeError::custom(format!("{} 主键列不存在", column_name)));
    }

    pub fn get_line_map(&self, index: usize) -> HashMap<&String, &String> {
        let line_data = &self.data[index];

        let mut map = HashMap::<&String, &String>::new();
        for index in 0..line_data.len() {
            let column_name = &self.column[index];
            let value = &line_data[index];
            map.insert(column_name, value);
        }

        return map;
    }
    pub fn is_column(&self, column_name: &String) -> bool {
        return self.column.contains(column_name);
    }

    pub fn get_map(&self) -> Vec<HashMap<&String, &String>> {
        let mut vec = Vec::<HashMap<&String, &String>>::new();
        let size = self.column.len();
        for x_index in 0..self.get_data_size() {
            let mut map = HashMap::<&String, &String>::new();
            let line_data = &self.data[x_index];
            for index in 0..size {
                let column_name = &self.column[index];
                let value = &line_data[index];
                map.insert(column_name, value);
            }
            vec.push(map);
        }

        return vec;
    }

    pub fn get_data_size(&self) -> usize {
        return self.data.len();
    }
}




/*
slave 批量插入的 
struct
*/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlaveBatchData {
    pub column: Vec<String>,
    pub data: Vec<Vec<String>>,
}
impl SlaveBatchData{
    pub fn new (column: Vec<String>) -> Self {
        return SlaveBatchData{
            column: column,
            data: Vec::new(),
        };
    }
    pub fn push_data(&mut self, data: Vec<String>) {
        self.data.push(data);
    }

    pub fn get_map(&self) -> Vec<HashMap<&String, &String>> {
        let mut vec = Vec::<HashMap<&String, &String>>::new();
        let size = self.column.len();
        for x_index in 0..self.data.len() {
            let mut map = HashMap::<&String, &String>::new();
            let line_data = &self.data[x_index];
            for index in 0..size {
                let column_name = &self.column[index];
                let value = &line_data[index];
                map.insert(column_name, value);
            }
            vec.push(map);
        }

        return vec;
    }
    
}