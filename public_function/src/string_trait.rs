pub trait StringFunction{
    fn hash_code(&self) -> i32;
    fn process_string(&self) -> String;



}

impl StringFunction for &str{
    fn hash_code(&self) -> i32 {
        let mut hash = 0i32;
        let multiplier = 31;
        for c in self.chars() {
            let char_value = c as i32;
            hash = hash.wrapping_mul(multiplier).wrapping_add(char_value);
        }
        return hash;
    }
    fn process_string(&self) -> String {
        let mut result = String::with_capacity(self.len());
        let mut prev_space = false;

        for c in self.chars() {
            if c == '\r' || c == '\n' || c == ' ' || c == ';' || c == '\t'{
                if !prev_space {
                    result.push(' ');
                    prev_space = true;
                }
            } else {
                result.push(c);
                prev_space = false;
            }
        }

        // 去除首尾空格
        result.trim().to_string()
    }



}

impl StringFunction for String{
    fn hash_code(&self) -> i32 {
        let mut hash = 0i32;
        let multiplier = 31;
        for c in self.chars() {
            let char_value = c as i32;
            hash = hash.wrapping_mul(multiplier).wrapping_add(char_value);
        }
        return hash.abs();
    }
    fn process_string(&self) -> String {
        let mut result = String::with_capacity(self.len());
        let mut prev_space = false;

        for c in self.chars() {
            if c == '\r' || c == '\n' || c == ' ' || c == ';' || c == '\t'{
                if !prev_space {
                    result.push(' ');
                    prev_space = true;
                }
            } else {
                result.push(c);
                prev_space = false;
            }
        }

        // 去除首尾空格
        result.trim().to_string()
    }

}
