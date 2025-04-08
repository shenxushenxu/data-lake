pub mod read_function;

use std::collections::HashMap;
use std::sync::LazyLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use entity_lib::entity::Error::DataLakeError;

pub static MASTER_CONFIG: LazyLock<HashMap<String,String>> = LazyLock::new(|| {
    let file_path = r"D:\rustproject\data-lake\config\master_config.properties";

    load_properties(file_path)
});


pub static SLAVE_CONFIG: LazyLock<HashMap<String,String>> = LazyLock::new(|| {

    let file_path = r"D:\rustproject\data-lake\config\slave_config.properties";
    load_properties(file_path)
});

fn load_properties(file_path:&str) -> HashMap<String, String> {


    let content = std::fs::read_to_string(file_path).unwrap();

    let mut map = HashMap::new();

    content.lines().into_iter().filter(|line|{
        line.contains("=")
    }).map(|x| {

        x.split("=").collect::<Vec<&str>>()

    }).for_each(|x1| {

        let key = x1[0].trim();
        let value = x1[1].trim();

        map.insert(key.to_string(), value.to_string());

    });


    return map;
}



pub trait hashcode{
    fn hash_code(&self) -> i32;
}

impl hashcode for &str{
    fn hash_code(&self) -> i32 {
        let mut hash = 0i32;  // 哈希值初始化为 0
        let multiplier = 31;   // Java 中常用的乘数 31

        // 遍历字符串中的每个字符
        for c in self.chars() {
            // 将字符转为其 Unicode 值
            let char_value = c as i32;
            // 更新哈希值
            hash = hash.wrapping_mul(multiplier).wrapping_add(char_value);
        }
        return hash;
    }
}

impl hashcode for String{
    fn hash_code(&self) -> i32 {
        let mut hash = 0i32;
        let multiplier = 31;
        for c in self.chars() {
            let char_value = c as i32;
            hash = hash.wrapping_mul(multiplier).wrapping_add(char_value);
        }
        return hash;
    }
}







pub async fn get_list_filename(path:&str) -> Vec<(String,String)> {


    let mut file_name_vec = Vec::new();


    let entries = std::fs::read_dir(path).unwrap(); // 读取目录中的条目

    for entry in entries {
        let entry = entry.unwrap(); // 获取每个条目
        let entry_path = entry.path();

        if entry_path.is_file(){

            let file_path = entry_path.display().to_string();
            let file_name = entry.file_name().into_string().unwrap().to_string();


            file_name_vec.push((file_name, file_path));
        }
    }


    return file_name_vec;

}

/**
发送异常
**/
pub async fn write_error(err: DataLakeError, write_half: &mut WriteHalf<'_>){

    write_half.write_i32(-2).await.unwrap();

    let err_mess = format!("{}",err);
    let bytes = err_mess.as_bytes();
    let bytes_len = bytes.len();

    write_half.write_i32(bytes_len as i32).await.unwrap();
    write_half.write_all(bytes).await.unwrap();

}


/**
读取异常
**/
pub async fn read_error(stream: &mut TcpStream) -> Result<(),DataLakeError>{
    let is = stream.read_i32().await?;
    if is == -2 {

        let len = stream.read_i32().await?;

        let mut mess = vec![0u8;len as usize];
        stream.read_exact(&mut mess).await?;
        let dd = String::from_utf8(mess)?;
        return Err(DataLakeError::CustomError(dd));
    }

    return Ok(());
}



