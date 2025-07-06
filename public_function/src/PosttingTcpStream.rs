use entity_lib::entity::Error::DataLakeError;
use entity_lib::entity::MasterEntity::{Info, PartitionInfo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::RandomNumber::{random_number};

pub struct DataLakeTcpStream {
    stream: TcpStream,
}
unsafe impl Sync for DataLakeTcpStream{}
unsafe impl Send for DataLakeTcpStream {}
impl DataLakeTcpStream {
    pub async fn connect(
        par_info_vec: &mut Vec<PartitionInfo>,
    ) -> Result<DataLakeTcpStream, DataLakeError> {
        let mut partition_info_vec_clone = par_info_vec.clone();

        let partition_info = partition_info_vec_clone
            .iter()
            .filter(|x| 
                match x.info {
                Info::Leader => true,
                Info::Follower => false,
            }
            )
            .collect::<Vec<&PartitionInfo>>()[0];
        let address = &partition_info.address;

        
        let stream = match TcpStream::connect(address).await {
            Ok(stream) => Ok(stream),
            Err(_) => {
                par_info_vec.iter_mut().for_each(|x| {
                    if let Info::Leader = x.info {
                        x.info = Info::Follower;
                    }
                });

                partition_info_vec_clone.retain(|x1| match x1.info {
                    Info::Leader => false,
                    Info::Follower => true,
                });

                let stream = searchfac(&mut partition_info_vec_clone).await;
                
                stream
            }
        };

        match stream {
            Ok(stream) => Ok(DataLakeTcpStream { stream }),
            Err(e) => Err(e),
        }
    }

    pub async fn write_i32(&mut self, n: i32) -> Result<(), DataLakeError> {
        self.stream.write_i32(n).await?;

        return Ok(());
    }

    pub async fn write_all<'a>(&'a mut self, src: &'a [u8]) -> Result<(), DataLakeError> {
        self.stream.write_all(src).await?;
        return Ok(());
    }
    
    pub async fn read_i32(&mut self) -> Result<i32, DataLakeError> {
        let num = self.stream.read_i32().await?;
        return Ok(num);
    }
    pub async fn read_exact(&mut self, bytes: &mut [u8]) -> Result<(), DataLakeError> { 
        self.stream.read_exact(bytes).await?;
        
        return Ok(());
    }
    
    
}

/**
查找可用的副本
**/
async fn searchfac(
    partition_info_vec: &mut Vec<PartitionInfo>,
) -> Result<TcpStream, DataLakeError> {
    let size = partition_info_vec.len();
    loop {
        if size != 0 {



            let random_number = random_number(size);
            
            let partition_info = partition_info_vec.get(random_number).unwrap();

            let address = &partition_info.address;

            if let Ok(stream) = TcpStream::connect(address).await {
                return Ok(stream);
            } else {
                partition_info_vec.remove(random_number);
            }
        } else {
            return Err(DataLakeError::custom("No available copy available .".to_string()));
        }
    }
}
