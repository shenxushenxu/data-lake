use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct BatchData{
    pub table_name: String,
    pub partition_code: Option<i32>,
    pub column: Vec<String>,
    pub data: Vec<Vec<String>>,
}

impl BatchData{
    pub fn get_line_map(&self, index:usize) -> HashMap<&String, &String>{
        let line_data = &self.data[index];

        let mut map = HashMap::<&String, &String>::new();
        for index in 0..line_data.len() {
            let column_name = &self.column[index];
            let value = &line_data[index];
            map.insert(column_name, value);
        }
        
        return map;
    }
    pub fn is_column(&self, column_name:&String) -> bool {
        
        return self.column.contains(column_name);
    }
    
    pub fn get_line(&self, index:usize) -> &Vec<String>{
        let line_data = &self.data[index];
        return line_data;
    }
    pub fn get_map(&self) -> Vec<HashMap<&String,&String>>{

        let mut vec = Vec::<HashMap<&String,&String>>::new();
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
    
    
    pub fn get_data_size(&self) -> usize{
        return self.data.len();
    }
    
}
