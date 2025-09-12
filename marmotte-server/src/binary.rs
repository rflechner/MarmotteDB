use std::borrow::Cow;
use std::fmt::format;
use bytes::{BytesMut, BufMut, Bytes};

pub struct BinaryWriter {
    pub buffer: BytesMut
}

impl BinaryWriter {

    pub fn with_capacity(capacity:usize) -> BinaryWriter {
        BinaryWriter { buffer: BytesMut::with_capacity(capacity) }
    }

    pub fn write_string(&mut self, value:&str) {
        let bytes = value.as_bytes();
        let len = bytes.len().to_be_bytes();
        (*self).buffer.put_slice(&len);
        (*self).buffer.put_slice(&bytes);
    }

    pub fn write_i32(&mut self, value:i32) {
        let bytes = value.to_be_bytes();
        (*self).buffer.put_slice(&bytes);
    }

    pub fn write_u32(&mut self, value:u32) {
        let bytes = value.to_be_bytes();
        (*self).buffer.put_slice(&bytes);
    }

    pub fn write_i64(&mut self, value:i64) {
        let bytes = value.to_be_bytes();
        (*self).buffer.put_slice(&bytes);
    }

    pub fn write_u64(&mut self, value:u64) {
        let bytes = value.to_be_bytes();
        (*self).buffer.put_slice(&bytes);
    }

    pub fn write_f64(&mut self, value:f64) {
        let v = value as i64;
        let bytes = v.to_be_bytes();
        (*self).buffer.put_slice(&bytes);
    }

    pub fn write_bool(&mut self, value:bool) {
        let byte:u8 = if value { 1 } else { 0 };
        (*self).buffer.put_u8(byte);
    }

    pub fn write_u8(&mut self, byte:u8) {
        (*self).buffer.put_u8(byte);
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        (*self).buffer.put_slice(bytes);
    }
}

pub struct BinaryReader {
    pub buffer: Bytes,
    pub position: usize
}

impl BinaryReader {

    pub fn from(buffer: BytesMut) -> BinaryReader {
        BinaryReader { buffer: buffer.freeze(), position: 0 }
    }

    pub fn read_string(&mut self) -> Result<String, &str> {
        if self.buffer.len() <= self.position {
            Err("Failed to read value due to buffer overflow.")
        }
        else {
            let mut bl: [u8; 8] = Default::default();
            bl.copy_from_slice(&self.buffer.slice(self.position .. self.position+8));
            let len = usize::from_be_bytes(bl);
            let pos_start = self.position+8;
            let pos_end = self.position+len+8;

            if pos_end > self.buffer.len() {
                //let msg = (format!("Corrupted data, trying to read string of from {} to {} but length is {}.", pos_start, pos_end, self.buffer.len())).as_str();
                println!("read_string pos_end > self.buffer.len() => {} > {}", pos_end, self.buffer.len());
                Err(&"Corrupted data")
            }
            else {
                let content = self.buffer.slice(pos_start .. pos_end).to_vec();

                self.position += 8 + len;

                match String::from_utf8(content) {
                    Ok(s) => Ok(s),
                    Err(_) => Err(&"Failed to decode UTF8 string.")
                }
            }
        }
    }

    pub fn read_i32(&mut self) -> Result<i32, &str> {
        let mut bl: [u8; 4] = Default::default();

        if self.buffer.len() <= self.position {
            Err("Failed to read value due to buffer overflow.")
        }
        else {
            bl.copy_from_slice(&self.buffer.slice(self.position .. self.position+4));
            self.position += 4;
            Ok(i32::from_be_bytes(bl))
        }
    }

    pub fn read_i64(&mut self) -> Result<i64, &str> {
        let mut bl: [u8; 8] = Default::default();

        if self.buffer.len() <= self.position {
            Err("Failed to read value due to buffer overflow.")
        }
        else {
            let len = std::mem::size_of::<i64>();
            bl.copy_from_slice(&self.buffer.slice(self.position .. self.position+len));
            self.position += len;
            Ok(i64::from_be_bytes(bl))
        }
    }

    pub fn read_u64(&mut self) -> Result<u64, &str> {
        let mut bl: [u8; 8] = Default::default();

        if self.buffer.len() <= self.position {
            Err("Failed to read value due to buffer overflow.")
        }
        else {
            let len = std::mem::size_of::<u64>();
            bl.copy_from_slice(&self.buffer.slice(self.position .. self.position+len));
            self.position += len;
            Ok(u64::from_be_bytes(bl))
        }
    }

    pub fn read_f64(&mut self) -> Result<f64, &str> {
        let mut bl: [u8; 8] = Default::default();

        if self.buffer.len() <= self.position {
            Err("Failed to read value due to buffer overflow.")
        }
        else {
            bl.copy_from_slice(&self.buffer.slice(self.position .. self.position+4));
            self.position += 8;
            Ok(i64::from_be_bytes(bl) as f64)
        }
    }

    pub fn read_u32(&mut self) -> Result<u32, &str> {
        let mut bl: [u8; 4] = Default::default();
        if self.buffer.len() <= self.position {
            Err("Failed to read value due to buffer overflow.")
        }
        else {
            bl.copy_from_slice(&self.buffer.slice(self.position .. self.position+4));
            self.position += 4;
            Ok(u32::from_be_bytes(bl))
        }
    }

    pub fn read_u8(&mut self) -> Result<u8, &str> {
        if self.buffer.len() <= self.position {
            Err("Failed to read bool value.")
        }
        else {
            let v = self.buffer[self.position];
            self.position += std::mem::size_of::<u8>();
            Ok(v)
        }
    }

    pub fn read_bool(&mut self) -> Result<bool, Cow<'static, str>> {
        if self.buffer.len() <= self.position {
            Err(Cow::from("Failed to read bool value."))
        }
        else {
            match &self.buffer[self.position] {
                0 => {
                    self.position += 1;
                    Ok(false)
                },
                1 => {
                    self.position += 1;
                    Ok(true)
                },
                x => {
                    let p = self.position;
                    Err(Cow::Owned(format!("Failed to read bool value due to corrupted data '{x}' at position '{p}'.")))
                }
            }
        }
    }

    pub fn end(&mut self) -> bool {
        let l = self.buffer.len();
        self.position >= l
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_should_be_written_and_read() -> Result<(), String> {
        let mut wr = BinaryWriter::with_capacity(200);
        let value = String::from("lorem ipsum");
        wr.write_string(&value);

        let mut reader = BinaryReader::from(wr.buffer);
        let result = reader.read_string();

        assert_eq!(Ok(value), result);

        Ok(())
    }

    #[test]
    fn int32_should_be_written_and_read() -> Result<(), String> {
        let mut wr = BinaryWriter::with_capacity(200);
        let value:i32 = 983424534;
        wr.write_i32(value);

        let mut reader = BinaryReader::from(wr.buffer);
        let result = reader.read_i32();

        assert_eq!(Ok(value), result);

        Ok(())
    }

    #[test]
    fn uint32_should_be_written_and_read() -> Result<(), String> {
        let mut wr = BinaryWriter::with_capacity(200);
        let value:u32 = 983424534;
        wr.write_u32(value);

        let mut reader = BinaryReader::from(wr.buffer);
        let result = reader.read_u32();

        assert_eq!(Ok(value), result);

        Ok(())
    }

    #[test]
    fn bool_true_should_be_written_and_read() -> Result<(), String> {
        let mut wr = BinaryWriter::with_capacity(200);
        let value = true;
        wr.write_bool(value);

        let mut reader = BinaryReader::from(wr.buffer);
        let result = reader.read_bool();

        assert_eq!(Ok(value), result);

        Ok(())
    }

    #[test]
    fn bool_false_should_be_written_and_read() -> Result<(), String> {
        let mut wr = BinaryWriter::with_capacity(200);
        let value = false;
        wr.write_bool(value);

        let mut reader = BinaryReader::from(wr.buffer);
        let result = reader.read_bool();

        assert_eq!(Ok(value), result);

        Ok(())
    }

    #[test]
    fn should_write_string_i32_bool_string() -> Result<(), String> {
        let mut wr = BinaryWriter::with_capacity(500);
        let s1 = String::from("lorem ipsum");
        let i:i32 = 987654;
        let b = true;
        let s2 = String::from("salut, c'est trop cool le RUST !!!");

        wr.write_string(&s1);
        wr.write_i32(i);
        wr.write_bool(b);
        wr.write_string(&s2);

        let mut reader = BinaryReader::from(wr.buffer);

        assert_eq!(Ok(s1), reader.read_string());
        assert_eq!(Ok(i), reader.read_i32());
        assert_eq!(Ok(b), reader.read_bool());
        assert_eq!(Ok(s2), reader.read_string());

        assert_eq!(Err(Cow::from("Failed to read bool value.")), reader.read_bool());

        Ok(())
    }

}

