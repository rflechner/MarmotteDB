use std::cmp::Ordering;
use std::fs::{exists, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, Write};
use std::mem::{self, size_of};
use bytes::BytesMut;
use crate::binary::{BinaryReader, BinaryWriter};
use crate::storage::disk_writer::RecordsFileMeta;

pub struct FenseIndex<T: Ord> {
    pub active: bool,
    pub target: u64,
    pub value: T,
}

impl<T: Ord> FenseIndex<T> {
    pub fn new(target: u64, value: T) -> Self {
        Self {
            active: false,
            target,
            value,
        }
    }

    fn get_binary_size() -> usize {
        size_of::<bool>() + size_of::<u64>() + size_of::<T>()
    }
}

#[derive(Debug)]
pub struct SortedIndexFiles {
    pub folder: String,
    pub max_incomplete_fragments_count: u32,
    pub shift_threshold: u32,
    pub max_records_count_per_fragments: u32,
    pub write_handles: Vec<Box<File>>,
    pub fragment_count: Box<u32>,
}

pub struct SortedIndexTableFragmentHeader {
    pub max_records_count: u32,
    pub shift_threshold: u32
}

impl SortedIndexTableFragmentHeader {
    pub fn get_binary_size() -> usize {
        size_of::<u64>() + size_of::<u32>() + size_of::<u32>()
    }
}

type ValueReader<T> = fn(BinaryReader) -> Result<T, String>;

type ValueWriter<T> = fn(BinaryWriter, T) -> Result<(), String>;

impl SortedIndexFiles {

    pub fn new_with_defaults(folder: String) -> Result<Self, String> {
        Self::new(folder, 10, 10_000, 100_000)
    }

    pub fn new(folder: String, max_incomplete_fragments_count: u32, shift_threshold: u32, max_records_count_per_fragments: u32) -> Result<Self, String> {
        std::fs::create_dir_all(folder.clone()).map_err(|e| e.to_string())?;

        let entries = std::fs::read_dir(folder.clone()).map_err(|e| e.to_string())?;
        let fragment_count = entries
            .filter_map(|r| r.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .filter(|f| f.extension().map(|e| e == "ix").unwrap_or(false))
            .count() as u32;

        Ok(Self {
            folder,
            max_incomplete_fragments_count,
            shift_threshold,
            max_records_count_per_fragments,
            write_handles: Vec::new(),
            fragment_count: Box::new(fragment_count),
        })
    }
    
    pub fn open_fragment<T: Ord>(&mut self, num: usize) -> Result<(), String> {
        let file_name = format!("{}/{num:08}.ix", self.folder);
        let first_file_use = !exists(file_name.clone()).map_err(|e| e.to_string())?;

        let file = OpenOptions::new()
            .append(false)
            .create(true)
            .read(true)
            .write(true)
            .open(file_name)
            .map_err(|e| e.to_string())?;

        if first_file_use {
            let record_size = FenseIndex::<T>::get_binary_size() as u32;
            let initial_size = record_size * self.max_records_count_per_fragments;

            file.set_len(initial_size as u64).map_err(|e| e.to_string())?;

            self.write_handles.push(Box::new(file));

            self.write_header(num)?;
        }
        else {
            self.write_handles.push(Box::new(file));
        }

        Ok(())
    }

    fn write_header(&mut self, num: usize) -> Result<(), String> {
        let header = SortedIndexTableFragmentHeader {
                max_records_count: self.max_records_count_per_fragments,
                shift_threshold: self.shift_threshold
        };

        let handles = self.write_handles.as_mut_slice();
        let file = &mut handles[num];
        file.seek(io::SeekFrom::Start(0)).map_err(|e| e.to_string()).map_err(|e| e.to_string())?;
        file.write(&header.max_records_count.to_le_bytes()).map_err(|e| e.to_string())?;
        file.write(&header.shift_threshold.to_le_bytes()).map_err(|e| e.to_string())?;
        Ok(())
    }

    fn read_header(&mut self, num: usize) -> Result<SortedIndexTableFragmentHeader, String> {
        let mut max_records_count = [0u8; 4];
        let mut shift_threshold = [0u8; 4];

        let handles = self.write_handles.as_mut_slice();
        let file = &mut handles[num];

        file.seek(io::SeekFrom::Start(0)).map_err(|e| e.to_string()).map_err(|e| e.to_string())?;
        file.read(&mut max_records_count).map_err(|e| e.to_string())?;
        file.read(&mut shift_threshold).map_err(|e| e.to_string())?;

        Ok(SortedIndexTableFragmentHeader {
            max_records_count: u32::from_le_bytes(max_records_count),
            shift_threshold: u32::from_le_bytes(shift_threshold),
        })
    }

    fn read_offset<T: Ord>(&mut self, file: &mut File, offset: u32, read_value: ValueReader<T>) -> Result<FenseIndex<T>, String> {
        let after_header_offset_position = SortedIndexTableFragmentHeader::get_binary_size() as u64;
        let offset_position = after_header_offset_position + (offset as u64) * FenseIndex::<T>::get_binary_size() as u64;
        file.seek(io::SeekFrom::Start(offset_position)).map_err(|e| e.to_string()).map_err(|e| e.to_string())?;

        let mut buf = vec![0; FenseIndex::<T>::get_binary_size()];
        file.read(&mut buf).unwrap();
        let bytes = BytesMut::from(buf.as_slice());
        let mut bin = BinaryReader::from(bytes);

        let active = bin.read_bool()?;
        let target = bin.read_u64()?;
        let value = read_value(bin)?;

        Ok(FenseIndex {
            active,
            target,
            value
        })
    }

    fn write_offset<T: Ord>(&mut self, file: &mut File, ix: FenseIndex<T>, offset: u32, write_value: ValueWriter<T>) -> Result<(), String> {
        let after_header_offset_position = SortedIndexTableFragmentHeader::get_binary_size() as u64;
        let offset_position = after_header_offset_position + (offset as u64) * FenseIndex::<T>::get_binary_size() as u64;
        file.seek(io::SeekFrom::Start(offset_position)).map_err(|e| e.to_string()).map_err(|e| e.to_string())?;

        let mut bin = BinaryWriter::with_capacity(FenseIndex::<T>::get_binary_size());
        bin.write_bool(true);
        bin.write_u64(ix.target);

        write_value(bin, ix.value)
    }
}

pub struct SortedIndexTableFragment {
    pub file: File,
    pub header: SortedIndexTableFragmentHeader
}

impl SortedIndexTableFragment {
    // fn insert<T>(&mut self, ix: FenseIndex<T>);

    // fn flag_tombstone(&mut self, target: u64);
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_write_and_read_index_file_header() {
        let folder = "test_folder";

        if std::fs::exists(folder).unwrap() {
            std::fs::remove_dir_all(folder).unwrap();
        }

        let mut files = SortedIndexFiles::new(folder.to_string(), 3, 10, 50).unwrap();

        files.open_fragment::<String>(0).unwrap();

        let header = files.read_header(0).unwrap();

        assert_eq!(50, header.max_records_count);
        assert_eq!(10, header.shift_threshold);
    }

    #[test]
    fn string_index_should_be_greater() {
        // given
        let ix1: FenseIndex<String> = { FenseIndex { active: false, target: 1, value: "aaaa".to_string() } };
        let ix2: FenseIndex<String> = { FenseIndex { active: false, target: 2, value: "bbbb".to_string() } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(Ordering::Greater, r);
    }

    #[test]
    fn string_index_should_be_less() {
        // given
        let ix1: FenseIndex<String> = { FenseIndex { active: false, target: 1, value: "zzzz".to_string() } };
        let ix2: FenseIndex<String> = { FenseIndex { active: false, target: 2, value: "bbbb".to_string() } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(Ordering::Less, r);
    }

    #[test]
    fn string_index_should_be_equal() {
        // given
        let ix1: FenseIndex<String> = { FenseIndex { active: false, target: 1, value: "ddd".to_string() } };
        let ix2: FenseIndex<String> = { FenseIndex { active: false, target: 2, value: "ddd".to_string() } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(Ordering::Equal, r);
    }

    #[test]
    fn u64_index_should_be_greater() {
        // given
        let ix1: FenseIndex<u64> = { FenseIndex { active: false, target: 1, value: 45 } };
        let ix2: FenseIndex<u64> = { FenseIndex { active: false, target: 2, value: 60 } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(Ordering::Greater, r);
    }

}
