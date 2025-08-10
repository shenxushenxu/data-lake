// 快速验证函数集合

// 验证 i32 (基于字节操作)
pub fn is_valid_i32(s: &str) -> bool {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len == 0 || len > 11 {
        return false;
    }

    let (start, negative) = match bytes[0] {
        b'+' => (1, false),
        b'-' => (1, true),
        _ => (0, false),
    };

    if start >= len {
        return false;
    }
    let num_bytes = &bytes[start..];
    let num_len = num_bytes.len();

    // 检查所有字符是否为数字
    for &b in num_bytes {
        if !b.is_ascii_digit() {
            return false;
        }
    }

    match num_len {
        0 => false,    // 只有符号的情况
        1..=9 => true, // 一定在范围内
        10 => {
            let bound = if negative {
                b"2147483648"
            } else {
                b"2147483647"
            };
            for i in 0..10 {
                if num_bytes[i] < bound[i] {
                    return true;
                } else if num_bytes[i] > bound[i] {
                    return false;
                }
            }
            true
        }
        _ => false, // 长度超过10
    }
}

// 验证 i64 (类似 i32 但使用 64 位边界)
pub fn is_valid_i64(s: &str) -> bool {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len == 0 || len > 20 {
        return false;
    }

    let (start, negative) = match bytes[0] {
        b'+' => (1, false),
        b'-' => (1, true),
        _ => (0, false),
    };

    if start >= len {
        return false;
    }
    let num_bytes = &bytes[start..];
    let num_len = num_bytes.len();

    for &b in num_bytes {
        if !b.is_ascii_digit() {
            return false;
        }
    }

    match num_len {
        0 => false,
        1..=18 => true,
        19 => {
            let bound = if negative {
                b"9223372036854775808"
            } else {
                b"9223372036854775807"
            };
            for i in 0..19 {
                if num_bytes[i] < bound[i] {
                    return true;
                } else if num_bytes[i] > bound[i] {
                    return false;
                }
            }
            true
        }
        _ => false,
    }
}

// 验证 f32 (快速浮点格式检查)
pub fn is_valid_f32(s: &str) -> bool {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len == 0 {
        return false;
    }

    let mut has_digit = false;
    let mut has_dot = false;
    let mut has_exp = false;
    let mut last_char = b'\0';

    for (i, &c) in bytes.iter().enumerate() {
        match c {
            b'0'..=b'9' => has_digit = true,
            b'.' => {
                if has_dot || has_exp {
                    return false;
                }
                has_dot = true;
            }
            b'e' | b'E' => {
                if has_exp || !has_digit {
                    return false;
                }
                has_exp = true;
                has_digit = false; // 重置，指数部分需要新数字
            }
            b'+' | b'-' => {
                // 符号只能在开头或指数符号后
                if i != 0 && (last_char != b'e' && last_char != b'E') {
                    return false;
                }
            }
            _ => return false,
        }
        last_char = c;
    }

    has_digit // 必须至少有一个数字
}

// 验证 bool (直接字节比较)
pub fn is_valid_bool(s: &str) -> bool {
    match s.as_bytes() {
        b"true" | b"false" => true,
        b"1" | b"0" => true, // 支持 1/0 表示法
        _ => false,
    }
}
