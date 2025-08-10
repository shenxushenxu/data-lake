pub trait VecPutVec{
    fn put_vec(&mut self, vec: &Vec<u8>);
    fn put_array(&mut self, val: &[u8]);
}

impl VecPutVec for Vec<u8>{
    fn put_vec(&mut self, vec: &Vec<u8>) {
        self.extend_from_slice(vec);
    }
    
    
    fn put_array(&mut self, val: &[u8]){
        self.extend_from_slice(&val[..]);
    }
    
}