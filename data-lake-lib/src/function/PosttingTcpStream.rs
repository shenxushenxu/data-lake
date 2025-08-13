use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::entity::Error::DataLakeError;
use crate::entity::MasterEntity::{Info, PartitionInfo};
use crate::entity::MasterEntity::Info::Leader;
use crate::function::RandomNumber::random_number;
use crate::function::table_structure::{get_table_path, get_table_structure};

pub struct DataLakeTcpStream {
    stream: TcpStream,
    par_info_vec: Vec<PartitionInfo>,
    table_name: String,
    partition_code: usize,
}
unsafe impl Sync for DataLakeTcpStream{}
unsafe impl Send for DataLakeTcpStream {}
impl DataLakeTcpStream {
    pub async fn connect(
        par_info_vec: Vec<PartitionInfo>,
        table_name: String,
        partition_code: usize,
    ) -> Result<DataLakeTcpStream, DataLakeError> {

        let partition_info = par_info_vec
            .iter()
            .filter(|x| 
                match x.info {
                Info::Leader => true,
                Info::Follower => false,
            }
            ).collect::<Vec<&PartitionInfo>>()[0];
        let address = &partition_info.address;

        
        let data_lake_tcp_stream = match TcpStream::connect(address).await {
            Ok(stream) => {
                DataLakeTcpStream { stream,  par_info_vec, table_name, partition_code}
            },
            Err(e) => {
                eprintln!("{}:{}  ERROR:{}",file!(), line!(), e);

                let stream = searchfac(&par_info_vec).await?;
                
                let mut data_lake_tcp_stream = DataLakeTcpStream { stream,  par_info_vec, table_name, partition_code};
                let server_addr = data_lake_tcp_stream.stream.peer_addr()?;
                let leader_ip_port = format!("{}:{}", server_addr.ip(), server_addr.port());
                data_lake_tcp_stream.modify_table_structure(&leader_ip_port).await?;
                
                data_lake_tcp_stream
            }
        };
        

        
        return  Ok(data_lake_tcp_stream);
        
    }
    
    pub fn get_stream(&self) -> &TcpStream {
        return &self.stream
    }

    pub async fn write_i32(&mut self, n: i32) -> Result<(), DataLakeError> {
        match self.stream.write_i32(n).await{
            Ok(_) => {
                return Ok(());

            }
            Err(e) => {
                eprintln!("{}:{}  ERROR:{}",file!(), line!(), e);
                
                let stream = searchfac(&self.par_info_vec).await?;
                
                let server_addr = stream.peer_addr()?;
                let leader_ip_port = format!("{}:{}", server_addr.ip(), server_addr.port());
                self.modify_table_structure(&leader_ip_port).await?;
                
                self.stream = stream;
                self.stream.write_i32(n).await?;
                return Ok(());
            }
        }

    }

    pub async fn write_all<'a>(&'a mut self, src: &'a [u8]) -> Result<(), DataLakeError> {
        match self.stream.write_all(src).await{
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                eprintln!("{}:{}  ERROR:{}",file!(), line!(), e);
                
                let stream = searchfac(&self.par_info_vec).await?;
                
                let server_addr = stream.peer_addr()?;
                let leader_ip_port = format!("{}:{}", server_addr.ip(), server_addr.port());
                self.modify_table_structure(&leader_ip_port).await?;
                
                self.stream = stream;
                self.stream.write_all(src).await?;
                return Ok(());
            }
        }
    }
    
    pub async fn read_i32(&mut self) -> Result<i32, DataLakeError> {
        match self.stream.read_i32().await{
            Ok(num) => {
                return Ok(num);

            }
            Err(e) => {
                eprintln!("{}:{}  ERROR:{}",file!(), line!(), e);
                
                let stream = searchfac(&self.par_info_vec).await?;
                
                let server_addr = stream.peer_addr()?;
                let leader_ip_port = format!("{}:{}", server_addr.ip(), server_addr.port());
                self.modify_table_structure(&leader_ip_port).await?;
                
                self.stream = stream;
                let num = self.stream.read_i32().await?;
                return Ok(num);
            }
        };
    }
    pub async fn read_exact(&mut self, bytes: &mut [u8]) -> Result<(), DataLakeError> { 
        match self.stream.read_exact(bytes).await{
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                eprintln!("{}:{}  ERROR:{}",file!(), line!(), e);
                
                let stream = searchfac(&self.par_info_vec).await?;
                
                let server_addr = stream.peer_addr()?;
                let leader_ip_port = format!("{}:{}", server_addr.ip(), server_addr.port());
                self.modify_table_structure(&leader_ip_port).await?;
                
                self.stream = stream;
                self.stream.read_exact(bytes).await?;
                return Ok(());
            }
        }
        
    }
    
    
    pub async fn modify_table_structure(&mut self, leader_ip_port:&String) -> Result<(), DataLakeError> {
        let table_name = &self.table_name;
        let partition_code = &self.partition_code;
        
        let mut table_structure = get_table_structure(table_name).await?;
        let partition_address = &mut table_structure.partition_address;
        let partition_info = partition_address.get_mut(partition_code).unwrap();
        
        partition_info.iter_mut().for_each(|x| {
            let this_ip_port = &x.address;
            if let Leader = x.info {
                x.info = Info::Follower;
            }
            if this_ip_port == leader_ip_port {
                x.info = Info::Leader;
            }
        });

        self.par_info_vec = partition_info.clone();

        // 清除掉表的缓存
        crate::function::BufferObject::remove_table_cache(&table_name);

        let file_path = get_table_path(&table_name).await?;
        let data = bincode::serialize(&table_structure)?;
        let mut file = File::create(file_path).await?;
        file.write_all(data.as_slice()).await?;
        
        
        
        return Ok(());
    }
    
}

/**
查找可用的副本
**/
async fn searchfac(
    partition_info_vec: &Vec<PartitionInfo>,
) -> Result<TcpStream, DataLakeError> {
    let mut partition_info_vec_clone = partition_info_vec.clone();
    
    let mut size = partition_info_vec_clone.len();
    loop {
        if size != 0 {
            
            let random_number = random_number(size);
            
            let partition_info = partition_info_vec_clone.get(random_number).unwrap();

            let address = &partition_info.address;

            if let Ok(stream) = TcpStream::connect(address).await {
                return Ok(stream);
            } else {
                partition_info_vec_clone.remove(random_number);
                size = size - 1;
            }
        } else {
            return Err(DataLakeError::custom("No available copy available .".to_string()));
        }
    }
}
