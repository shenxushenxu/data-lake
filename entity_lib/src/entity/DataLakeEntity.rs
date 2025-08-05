use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::format;
use crate::entity::Error::DataLakeError;
use crate::entity::SlaveEntity::SlaveMessage;
/*
master 批量插入的 struct
*/
#[derive(Serialize, Deserialize, Debug)]
pub struct BatchData<'a> {
    pub table_name: &'a str,
    pub column: Vec<&'a str>,
    pub data: Vec<Vec<&'a str>>,
}

impl <'a>BatchData<'a> {
    pub fn get_line(&mut self, index: usize) -> Vec<&'a str> {
        // return self.data.swap_remove(index);
        unsafe {
            return self.data.pop().unwrap_unchecked();
        }
    }

    pub fn get_column_index(&self, column_name: &String) -> Result<usize, DataLakeError> {
        for index in 0..self.column.len() {
            let name = &self.column[index];
            if name == column_name {
                return Ok(index);
            }
        }
        return Err(DataLakeError::custom(format!("{} 主键列不存在, {:?} ", column_name, self.column)));
    }

    pub fn get_line_map(&self, index: usize) -> HashMap<&'a str, &'a str> {
        let line_data = &self.data[index];

        let mut map = HashMap::<&'a str, &'a str>::new();
        for index in 0..line_data.len() {
            let column_name = &self.column[index];
            let value = &line_data[index];
            map.insert(column_name, value);
        }

        return map;
    }
    pub fn is_column(&self, column_name: &'a str) -> bool {
        return self.column.contains(&column_name);
    }

    pub fn get_map(&self) -> Vec<HashMap<&'a str, &'a str>> {
        let mut vec = Vec::<HashMap<&'a str, &'a str>>::new();
        let size = self.column.len();
        for x_index in 0..self.get_data_size() {
            let mut map = HashMap::<&'a str, &'a str>::new();
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
#[derive(Serialize,Deserialize ,Debug, Clone)]
#[serde(bound(deserialize = "'de: 'a"))] // 确保“寿命超过”某物
pub struct SlaveBatchData<'a> {
    pub column: Vec<&'a str>,
    pub data:  Vec<Vec<&'a str>>,
}
impl <'a> SlaveBatchData<'a>{
    pub fn new (column: Vec<&'a str>) -> Self {
        return SlaveBatchData{
            column: column,
            data: Vec::new(),
        };
    }
    pub fn push_data(&mut self, data: Vec<&'a str>) {
        self.data.push(data);
    }

    pub fn get_map(&self) -> Vec<HashMap<&'a str, &'a str>> {

        let vec = self.data
            .iter()
            .map(|row| {
                self.column
                    .iter()
                    .zip(row.iter())
                    .map(|(&col, &val)| (col, val))
                    .collect()
            }).collect();
        
        return vec;
    }

    pub fn get_line_map(&self, index:usize) -> HashMap<&'a str, &'a str> {

        let vec_data = &self.data[index];

        let hash_map = self.column.iter().zip(vec_data.iter()).map(|(&col, &val)| (col, val)).collect();
        
        return hash_map;
    }
    
    
    pub fn get_data_size(&self) -> usize {
        return self.data.len();
    }
    
}


/**
指针存储
**/
pub struct PtrByteBatchData{
    byte_ptr: Option<&'static mut Vec<u8>>,
    batch_data_ptr: Option<&'static mut BatchData<'static>>,
}
