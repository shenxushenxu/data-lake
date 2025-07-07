pub trait VecPutVec{
    fn put_vec(&mut self, vec: &mut Vec<u8>);
    fn put_i32_vec(&mut self, val: i32);
}

impl VecPutVec for Vec<u8>{
    fn put_vec(&mut self, vec: &mut Vec<u8>) {
        self.append(vec);
    }

    fn put_i32_vec(&mut self, val: i32) {
        let byte_val = val.to_be_bytes();
        for byte in byte_val {
            self.push(byte);
        }
    }
}