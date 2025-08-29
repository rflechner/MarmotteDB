use bytes::{BufMut, BytesMut};

use crate::binary::*;
use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, SeekFrom};
use std::path::Path;
use std::vec;

#[derive(Clone, Copy)]
pub struct RecordsFileMeta {
    pub version: u64,
    pub records_count: u64,
    pub position: u64,
    pub page_size: u64
}

impl RecordsFileMeta {

    pub fn size() -> usize {
        8 + 8 + 8 + 8
    }

    pub fn empty() -> RecordsFileMeta {
        RecordsFileMeta { version: 1, records_count:0, position: RecordsFileMeta::size() as u64, page_size: 0 }
    }

    pub fn empty_with_page_size(page_size: u64) -> RecordsFileMeta {
        RecordsFileMeta { version: 1, records_count:0, position: RecordsFileMeta::size() as u64, page_size: page_size }
    }

    pub fn read_metadata(file: &mut File) -> RecordsFileMeta {
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = vec![0; RecordsFileMeta::size()];
        file.read(&mut buf).unwrap();
        let bytes = BytesMut::from(buf.as_slice());
        let mut bin = BinaryReader::from(bytes);

        let version = bin.read_u64().unwrap();
        let records_count = bin.read_u64().unwrap();
        let position = bin.read_u64().unwrap();
        let page_size = bin.read_u64().unwrap();

        RecordsFileMeta { version, records_count, position, page_size }
    }
}

pub struct Record {
    pub position: u64,
    pub content_size: u64,
    pub content: Vec<u8>,
    pub deleted: bool,
    pub checksum: u32
}

impl Clone for Record {
    fn clone(&self) -> Self {
        Self { position: self.position.clone(), content_size: self.content_size.clone(), content: self.content.clone(), deleted: self.deleted.clone(), checksum: self.checksum.clone() }
    }
}

impl Record {

    pub fn size (&self) -> u64 {
        // length prefix + checksum + content + deleted flag
        8 + 4 + self.content_size + 1
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(self.content_size as usize);
        buf.put_u64(self.content_size);
        buf.put_u32(self.checksum);
        buf.put_slice(&self.content);
        buf.put_u8(self.deleted as u8);

        buf.freeze().to_vec()
    }
}

pub struct DiskWriter {
    pub file_name: String,
    pub page_size: u64,
    pub file: File,
    pub meta: Cell<RecordsFileMeta>
}

impl DiskWriter {

    pub fn new(file_name: &str, page_size: u64) -> DiskWriter {
        let is_new_file = !Path::new(file_name).exists();
        let file = OpenOptions::new().create(true).read(true).write(true).open(file_name).unwrap();

        if file.metadata().unwrap().len() < page_size {
            file.set_len(page_size).unwrap();
        }

        let mut w = DiskWriter {
            file_name: String::from(file_name),
            page_size,
            file,
            meta: Cell::new(RecordsFileMeta::empty_with_page_size(page_size))
        };
        if !is_new_file {
            w.load_metadata();
        } else {
            w.write_metadata_and_fsync(w.meta.get());
            let m = RecordsFileMeta::read_metadata(&mut w.file);
        }
        w
    }

    pub fn load_metadata(&mut self) {
        let m = RecordsFileMeta::read_metadata(&mut self.file);
        self.meta.set(m);
    }

    pub fn write_metadata_and_fsync(&self, meta: RecordsFileMeta) {
        let mut bin = BinaryWriter::with_capacity(RecordsFileMeta::size());
        bin.write_u64(meta.version);
        bin.write_u64(meta.records_count);
        bin.write_u64(meta.position);
        bin.write_u64(meta.page_size);

        let content = bin.buffer.freeze().to_vec();

        (&self.file).seek(SeekFrom::Start(0)).unwrap();
        (&self.file).write_all(&content).unwrap();
        (&self.file).sync_all().unwrap();
    }

    pub fn allocate_page (&self) {
        let len = self.file.metadata().unwrap().len();
        self.file.set_len(len + self.page_size).unwrap();
    }

    pub fn allocate_page_if_needed (&self) {
        let meta = self.meta.get();

        if meta.position >= self.file.metadata().unwrap().len() {
            self.allocate_page();
        }
    }

    pub fn allocate_page_if_position_need (&self, position: u64) {
        let len = self.file.metadata().unwrap().len();
        if position > len {
            let page = position / self.page_size;
            self.file.set_len(position).unwrap();
        }
    }

    fn write_record (&mut self, record: Record) {
        self.allocate_page_if_needed();
        let meta = self.meta.get_mut();

        (&self.file).seek(SeekFrom::Start(meta.position)).unwrap();

        let buf = record.to_bytes();
        (&self.file).write_all(&buf).unwrap();

        (&self.file).sync_all().unwrap();

        meta.position += record.size();
        meta.records_count += 1;

        let m = *meta;
        self.meta.set(m);
        self.write_metadata_and_fsync(m);
    }

    pub fn add_record (&mut self, buf: &[u8]) -> u64 {
        let meta = self.meta.get_mut();
        let l = buf.len() as u64;
        let checksum = crc32fast::hash(buf);
        let record = Record { position: meta.position, content_size: l, content: buf.to_vec(), deleted: false, checksum: checksum };

        let record_position = meta.position;

        self.write_record(record);

        record_position
    }

    fn fsync(&mut self) {
        (&self.file).sync_all().unwrap();
    }

    fn bulk_write_records (&mut self, records: Vec<Record>, initial_position: u64, max_position: u64) {
        self.allocate_page_if_position_need(max_position);

        (&self.file).seek(SeekFrom::Start(initial_position)).unwrap();

        for record in records {
            let buf = record.to_bytes();
            (&self.file).write_all(&buf).unwrap();
        }

        self.fsync();
    }

    fn update_meta_and_fsync(&mut self, records_count: u64, position: u64) {
        let meta_copy;
        {
            let meta = self.meta.get_mut();
            meta.position = position;
            meta.records_count = records_count;
            meta_copy = *meta;
        }
        self.write_metadata_and_fsync(meta_copy);
    }

    pub fn bulk_add_records (&mut self, buffers: Vec<&[u8]>) {
        let mut position = {
            let meta = self.meta.get_mut();
            meta.position
        };
        (&self.file).seek(SeekFrom::Start(position)).unwrap();

        let records_count = buffers.len() as u64;
        let mut bin_records:Vec<u8> = Vec::new();

        for buf in buffers {
            let l = buf.len() as u64;
            let checksum = crc32fast::hash(buf);
            let record = Record { position, content_size: l, content: buf.to_vec(), deleted: false, checksum };
            position += record.size();

            let bin_record = record.to_bytes();
            bin_records.extend_from_slice(bin_record.as_slice());
        }

        self.allocate_page_if_position_need(position);

        (&self.file).write_all(&bin_records).unwrap();

        self.update_meta_and_fsync(records_count, position);
    }
    
    pub fn rewind_to_start(&mut self) {
        (&self.file).seek(SeekFrom::Start(RecordsFileMeta::size() as u64)).unwrap();
    }

}

