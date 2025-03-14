use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use entity_lib::entity::SlaveEntity::{QueryMessage, SlaveMessage};
use crate::controls::metadata::get_metadata;

pub async fn query_sql(sql: String) -> Option<Vec<String>> {

    let convert_sql = sql.trim().to_lowercase();

    let from_index = convert_sql.find("from").unwrap();

    let cloums_str = &convert_sql[6..from_index];
    // 查询的列名
    let cloums = cloums_str.split(",").map(|s| s.trim().to_string()).collect::<Vec<String>>();


    let where_len = convert_sql.find("where").unwrap_or(convert_sql.len());
    //查询的表名
    let table_name = &convert_sql[(from_index+4)..where_len].trim();



    let conditions = if where_len != convert_sql.len() {
        Some(&convert_sql[(where_len+5)..])
    }else {
        None
    };


    let con = if let Some(conditi) = conditions{
        let eeee = conditi.split("and").map(|c| {

            let biaoda = c.trim();

            let cc = if biaoda.contains("="){
                let vec = biaoda.split("=").collect::<Vec<&str>>();

                (vec[0], "=", vec[1])
            }else if biaoda.contains("<") {
                let vec = biaoda.split("<>").collect::<Vec<&str>>();

                (vec[0], "<", vec[1])
            }else if biaoda.contains(">") {
                let vec = biaoda.split(">").collect::<Vec<&str>>();

                (vec[0], ">", vec[1])
            }else {
                panic!("啥也没有  = < >")
            };
            cc
        }).map(|x| {
            (x.0.to_string(), x.1.to_string(), x.2.to_string().replace("'",""))
        }).collect::<Vec<(String, String, String)>>();

        Some(eeee)
    }else {
        None
    };





    let table_structure =  get_metadata(table_name).await;

    let address_map = &table_structure.partition_address;

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<String>>(100000);

    for key in address_map.keys() {

        let querymessage = QueryMessage{
            tablename: format!("{}-{}", table_name,key),
            cloums:cloums.clone(),
            conditions:con.clone()
        };



        let slave_message = SlaveMessage::query(querymessage);

        let bytes = Arc::new(bincode::serialize(&slave_message).unwrap());
        let bytes_len = bytes.len();


        let address = address_map.get(key).unwrap().clone();

        let se = sender.clone();

        tokio::spawn(async move{
            let mut stream = TcpStream::connect(address).await.unwrap();


            stream.write_i32(bytes_len.clone() as i32).await.unwrap();
            stream.write_all(&bytes).await.unwrap();

            loop {
                match stream.read_i32().await{
                    Ok(mess_len) => {
                        let mut mess = vec![0u8; mess_len as usize];
                        stream.read_exact(&mut mess).await.unwrap();
                        let data_map = bincode::deserialize::<Option<Vec<String>>>(&mess).unwrap();

                        if let Some(data) = data_map {
                            if let Err(_) = se.send(data).await {
                                println!("receiver dropped");
                                return;
                            }

                        }

                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });
    }

    drop(sender);
    let mut res_vec = Vec::<String>::new();

    for mut data_map in receiver.recv().await{

        res_vec.append(&mut data_map);
    }

    println!("{:?}", res_vec);

    if res_vec.len() > 0 {
        return Some(res_vec);
    }else {
        return None;
    }





}