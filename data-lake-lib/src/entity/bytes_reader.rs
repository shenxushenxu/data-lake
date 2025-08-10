use crate::entity::const_property::{I32_BYTE_LEN, I64_BYTE_LEN};

pub struct ArrayBytesReader<'a> {
    data: &'a [u8],
    array_pointer: usize,
}
impl<'a> ArrayBytesReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        return ArrayBytesReader {
            data: data,
            array_pointer: 0,
        };
    }

    pub fn read_i32(&mut self) -> i32 {

        let bytes = &self.data[self.array_pointer..(self.array_pointer + I32_BYTE_LEN)];
        let len = unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const i32)};


        self.array_pointer += I32_BYTE_LEN;

        return len;
    }


    pub fn read_i64(&mut self) -> i64 {

        let bytes = &self.data[self.array_pointer..(self.array_pointer + I64_BYTE_LEN)];
        let len = unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const i64)};


        self.array_pointer += I64_BYTE_LEN;

        return len;
    }

    pub fn read_exact(&mut self, len: usize) -> &'a [u8] {
        let uuu = &self.data[self.array_pointer..(self.array_pointer + len)];

        self.array_pointer += len;

        return uuu;
    }

    pub fn is_stop(&self) -> bool {
        if self.array_pointer == self.data.len() {
            return true;
        } else {
            return false;
        }
    }

    pub fn read_usize(&mut self) -> usize {
        let len = self.read_i32() as usize;

        return len;
    }

    pub fn read_str(&mut self, len: usize) -> &'a str {
        let bytes = &self.data[self.array_pointer..(self.array_pointer + len)];
        let str_value = unsafe { std::str::from_utf8_unchecked(bytes) };
        self.array_pointer += len;

        return str_value;
    }



}