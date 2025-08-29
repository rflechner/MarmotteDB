use std::borrow::Cow;
use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use bytes::BytesMut;
use crate::binary::BinaryReader;
use crate::storage::disk_writer::{Record, RecordsFileMeta};

pub struct DiskReaderOptions {
    pub max_record_size: u64
}

impl DiskReaderOptions {

    pub fn create_default() -> DiskReaderOptions {
        DiskReaderOptions { max_record_size: 80 * 1024 * 1024 }
    }

}

pub struct DiskReader {
    pub file_name: String,
    pub file: File,
    pub meta: Cell<RecordsFileMeta>,
    pub position: u64,
    pub options: DiskReaderOptions
}

impl DiskReader {

    pub fn new(file_name: &str, options: DiskReaderOptions) -> DiskReader {
        let file = OpenOptions::new().read(true).open(file_name).unwrap();

        let mut reader = DiskReader {
            file_name: String::from(file_name),
            file,
            meta: Cell::new(RecordsFileMeta::empty()),
            position: RecordsFileMeta::size() as u64,
            options
        };
        reader.load_metadata();
        reader
    }

    pub fn load_metadata(&mut self) {
        let m = RecordsFileMeta::read_metadata(&mut self.file);
        self.meta.set(m);
    }

    pub fn rewind_to_start(&mut self) {
        (&self.file).seek(SeekFrom::Start(RecordsFileMeta::size() as u64)).unwrap();
    }

    pub fn seek_to(&mut self, position: u64) {
        (&self.file).seek(SeekFrom::Start(position)).unwrap();
    }

    pub fn read_next_record (&mut self) -> Result<Box<Record>, Cow<'static, str>> {
        let meta = self.meta.get();
        let mut len_buf = vec![0; 8];
        (&self.file).read_exact(&mut len_buf).unwrap();
        let mut len_bin = BinaryReader::from(BytesMut::from(len_buf.as_slice()));
        let len = len_bin.read_u64().unwrap();

        let mut hash_buf = vec![0; 4];
        (&self.file).read_exact(&mut hash_buf).unwrap();
        let mut hash_bin = BinaryReader::from(BytesMut::from(hash_buf.as_slice()));
        let hash = hash_bin.read_u32().unwrap();

        if len > self.options.max_record_size {
            let message = format!("record length is {} bytes. max allowed id {} bytes", len, self.options.max_record_size);
            Err(Cow::Owned(message))
        } else {
            let mut buf: Vec<u8> = vec![0; len as usize];
            (&self.file).read_exact(&mut buf).unwrap();

            let mut deleted_buf: Vec<u8> = vec![0; 1];
            (&self.file).read_exact(&mut deleted_buf).unwrap();
            let deleted = deleted_buf[0] != 0;

            let checksum = crc32fast::hash(&buf);

            if checksum != hash {
                Err(Cow::Owned("corrupted record".to_owned()))
            }
            else {
                let record = Record { position: meta.position, content_size: len, content: buf.to_vec(), deleted: deleted, checksum: checksum };
                Ok(Box::new(record))
            }
        }
    }

    pub fn find_record<F> (&mut self, f: F) -> Option<Box<Record>> where F : Fn(Box<Record>, u64) -> bool {
        self.rewind_to_start();

        let mut current_id = 0;
        let meta = self.meta.get();

        loop {
            current_id += 1;

            if self.file.stream_position().unwrap() < meta.position {
                let res = self.read_next_record();
                match res {
                    Err(_) => break None,
                    Ok(record) => {
                        if f(record.clone(), current_id) {
                            break Some(record);
                        }
                    }
                }
            } else {
                break None
            }

        }
    }

}

impl Iterator for DiskReader {

    type Item = Box<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        let meta = self.meta.get();

        loop {
            if self.file.stream_position().unwrap() < meta.position {
                let res = self.read_next_record();
                match res {
                    Err(_) => break None,
                    Ok(record) => {
                        break Some(record);
                    }
                }
            } else {
                break None
            }
        }
    }

}
