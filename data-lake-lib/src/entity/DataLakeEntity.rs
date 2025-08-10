use crate::entity::Error::DataLakeError;
use crate::entity::bytes_reader::ArrayBytesReader;
use crate::entity::const_property::I32_BYTE_LEN;
use crate::function::vec_trait::VecPutVec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::format;
/*
master 批量插入的 struct
*/
#[derive(Serialize, Deserialize, Debug)]
pub struct BatchData<'a> {
    pub table_name: &'a str,
    pub column: Vec<&'a str>,
    pub data: Vec<Vec<&'a str>>,
}

impl<'a> BatchData<'a> {
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
        return Err(DataLakeError::custom(format!(
            "{} 主键列不存在, {:?} ",
            column_name, self.column
        )));
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

    pub fn entity_decode(bytes: &[u8]) -> Result<BatchData, DataLakeError> {
        unsafe {
            let mut arraybytesreader = ArrayBytesReader::new(bytes);
            // table_name
            let table_name_len = arraybytesreader.read_usize();
            let table_name = arraybytesreader.read_str(table_name_len);

            // column
            let columns_vec_len = arraybytesreader.read_usize();
            let mut columns = Vec::<&str>::with_capacity(columns_vec_len);

            for _ in 0..columns_vec_len {
                let column_len = arraybytesreader.read_usize();
                let column = arraybytesreader.read_str(column_len);
                columns.push(column);
            }

            // value
            let value_vec_len = arraybytesreader.read_usize();
            let mut value_vec = Vec::<Vec<&str>>::with_capacity(value_vec_len);
            let ptr_value_vec = value_vec.as_mut_ptr();
            for value_vec_index in 0..value_vec_len {
                let neicengvec_len = arraybytesreader.read_usize();

                let mut neicengvec = Vec::<&str>::with_capacity(neicengvec_len);
                let neicengv_ptr = neicengvec.as_mut_ptr();
                for neicengvec_index in 0..neicengvec_len {
                    let value_len = arraybytesreader.read_usize();
                    let value = arraybytesreader.read_str(value_len);
                    neicengv_ptr.add(neicengvec_index).write(value);
                }
                neicengvec.set_len(neicengvec_len);
                ptr_value_vec.add(value_vec_index).write(neicengvec);
            }

            value_vec.set_len(value_vec_len);

            let batch_data = BatchData {
                table_name: table_name,
                column: columns,
                data: value_vec,
            };

            return Ok(batch_data);
        }
    }
}

/*
slave 批量插入的
struct
*/
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(bound(deserialize = "'de: 'a"))] // 确保“寿命超过”某物
pub struct SlaveBatchData<'a> {
    pub column: Vec<&'a str>,
    pub data: Vec<Vec<&'a str>>,
}
impl<'a> SlaveBatchData<'a> {
    pub fn new(column: Vec<&'a str>) -> Self {
        return SlaveBatchData {
            column: column,
            data: Vec::new(),
        };
    }
    pub fn push_data(&mut self, data: Vec<&'a str>) {
        self.data.push(data);
    }

    pub fn get_map(&self) -> Vec<HashMap<&'a str, &'a str>> {
        let vec = self
            .data
            .iter()
            .map(|row| {
                self.column
                    .iter()
                    .zip(row.iter())
                    .map(|(&col, &val)| (col, val))
                    .collect()
            })
            .collect();

        return vec;
    }

    pub fn get_line_map(&self, index: usize) -> HashMap<&'a str, &'a str> {
        unsafe {
            let vec_data = self.data.get_unchecked(index);

            let hash_map = self
                .column
                .iter()
                .zip(vec_data.iter())
                .map(|(&col, &val)| (col, val))
                .collect();
            return hash_map;
        }
    }

    pub fn get_data_size(&self) -> usize {
        return self.data.len();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlaveInsert<'a> {
    pub table_name: &'a str,
    pub data: SlaveBatchData<'a>,
    pub partition_code: i32,
}

impl<'a> SlaveInsert<'a> {
    pub fn serialize(&self) -> Result<Vec<u8>, DataLakeError> {
        let vec_serialize_len = self.calculate_serialized_size();
        let mut vec_serialize = Vec::<u8>::with_capacity(vec_serialize_len);
        let vec_serialize_ptr = vec_serialize.as_mut_ptr();
        let mut index = 0;
        unsafe {
            // table_name
            let table_name_bytes = self.table_name.as_bytes();
            let table_name_len = table_name_bytes.len();
            let table_name_bytes_len = &(table_name_len as i32).to_le_bytes();

            std::ptr::copy_nonoverlapping(
                table_name_bytes_len.as_ptr(),
                vec_serialize_ptr.add(index),
                I32_BYTE_LEN,
            );
            index += I32_BYTE_LEN;

            std::ptr::copy_nonoverlapping(
                table_name_bytes.as_ptr(),
                vec_serialize_ptr.add(index),
                table_name_len,
            );
            index += table_name_len;

            // data::column
            let column_len = &(self.data.column.len() as i32).to_le_bytes();

            std::ptr::copy_nonoverlapping(
                column_len.as_ptr(),
                vec_serialize_ptr.add(index),
                I32_BYTE_LEN,
            );
            index += I32_BYTE_LEN;

            for element in self.data.column.iter() {
                
                let element_bytes = element.as_bytes();
                let element_len = element_bytes.len();
                let element_bytes_len = &(element_len as i32).to_le_bytes();
                std::ptr::copy_nonoverlapping(
                    element_bytes_len.as_ptr(),
                    vec_serialize_ptr.add(index),
                    I32_BYTE_LEN,
                );
                index += I32_BYTE_LEN;

                std::ptr::copy_nonoverlapping(
                    element_bytes.as_ptr(),
                    vec_serialize_ptr.add(index),
                    element_len,
                );
                index += element_len;
            }

            // data::data
            let data_len = &(self.data.data.len() as i32).to_le_bytes();
            std::ptr::copy_nonoverlapping(
                data_len.as_ptr(),
                vec_serialize_ptr.add(index),
                I32_BYTE_LEN,
            );
            index += I32_BYTE_LEN;

            for data_element in self.data.data.iter() {
                let data_element_len = &(data_element.len() as i32).to_le_bytes();

                std::ptr::copy_nonoverlapping(
                    data_element_len.as_ptr(),
                    vec_serialize_ptr.add(index),
                    I32_BYTE_LEN,
                );
                index += I32_BYTE_LEN;

                for element in data_element.iter() {
                    let element_bytes = element.as_bytes();
                    let element_len = element_bytes.len();
                    let element_bytes_len = &(element_len as i32).to_le_bytes();

                    std::ptr::copy_nonoverlapping(
                        element_bytes_len.as_ptr(),
                        vec_serialize_ptr.add(index),
                        I32_BYTE_LEN,
                    );
                    index += I32_BYTE_LEN;

                    std::ptr::copy_nonoverlapping(
                        element_bytes.as_ptr(),
                        vec_serialize_ptr.add(index),
                        element_len,
                    );
                    index += element_len;
                }
            }
            // partition_code
            let partition_code_bytes = &self.partition_code.to_le_bytes();
            std::ptr::copy_nonoverlapping(
                partition_code_bytes.as_ptr(),
                vec_serialize_ptr.add(index),
                partition_code_bytes.len(),
            );
            index += partition_code_bytes.len();

            if index == vec_serialize_len {
                vec_serialize.set_len(index);
            } else {
                return Err(DataLakeError::custom(format!(
                    "SlaveInsert  序列化失败  index:{}  vec_serialize_len:{}",
                    index, vec_serialize_len
                )));
            }

            return Ok(vec_serialize);
        }
    }

    pub fn deserialize(bytes: &[u8]) -> Result<SlaveInsert, DataLakeError> {
        let mut arraybytesreader = ArrayBytesReader::new(bytes);
        // table_name
        let table_name_len = arraybytesreader.read_usize();
        let table_name = arraybytesreader.read_str(table_name_len);

        // data
        let data = unsafe {
            // column
            let column_len = arraybytesreader.read_usize();
            let mut colum = Vec::<&str>::with_capacity(column_len);
            let colum_ptr = colum.as_mut_ptr();
            for index in 0..column_len {
                let column_element_len = arraybytesreader.read_usize();
                let column_element = arraybytesreader.read_str(column_element_len);
                colum_ptr.add(index).write(column_element);
            }
            colum.set_len(column_len);

            //data
            let data_vec_len = arraybytesreader.read_usize();
            let mut data = Vec::<Vec<&str>>::with_capacity(data_vec_len);
            let data_ptr = data.as_mut_ptr();
            for index in 0..data_vec_len {
                let data_element_vec_len = arraybytesreader.read_usize();
                let mut data_vec = Vec::<&str>::with_capacity(data_element_vec_len);
                let data_vec_ptr = data_vec.as_mut_ptr();

                for element_index in 0..data_element_vec_len {
                    let element_len = arraybytesreader.read_usize();
                    let element = arraybytesreader.read_str(element_len);
                    data_vec_ptr.add(element_index).write(element);
                }
                data_vec.set_len(data_element_vec_len);

                data_ptr.add(index).write(data_vec);
            }
            data.set_len(data_vec_len);

            let slave_batch_data = SlaveBatchData {
                column: colum,
                data: data,
            };
            slave_batch_data
        };
        // partition_code
        let partition_code = arraybytesreader.read_i32();
        let slave_insert = SlaveInsert {
            table_name: table_name,
            data: data,
            partition_code: partition_code,
        };

        return Ok(slave_insert);
    }

    // 预先计算序列化后的总字节数
    fn calculate_serialized_size(&self) -> usize {
        let mut total = 0;

        // table_name 的数量
        total += I32_BYTE_LEN;
        total += self.table_name.as_bytes().len();

        // data::column
        total += I32_BYTE_LEN;
        for col in &self.data.column {
            total += I32_BYTE_LEN; // 字符串长度(i32)
            total += col.len(); // 实际字节
        }

        // data::data
        total += I32_BYTE_LEN;
        // 所有行数据
        for row in &self.data.data {
            total += I32_BYTE_LEN; // 行元素数(i32)
            for element in row {
                total += I32_BYTE_LEN; // 字符串长度(i32)
                total += element.len(); // 实际字节
            }
        }
        // partition_code
        total += I32_BYTE_LEN;

        return total;
    }
}

/**
指针存储
**/
pub struct PtrByteBatchData {
    byte_ptr: Option<&'static mut Vec<u8>>,
    batch_data_ptr: Option<&'static mut BatchData<'static>>,
}
