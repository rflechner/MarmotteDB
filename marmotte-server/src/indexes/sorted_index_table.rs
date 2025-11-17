use std::fmt::Display;
use crate::binary::{BinaryReader, BinaryWriter};
use bytes::{BufMut, Bytes, BytesMut};
use std::fs::{exists, File, OpenOptions};
use std::io;
use std::io::{Read, Seek, Write};
use std::mem::size_of;

pub trait BinarySizeable {
    fn get_binary_size(&self) -> usize;
}

impl BinarySizeable for String {
    fn get_binary_size(&self) -> usize {
        size_of::<u64>() + self.len()
    }
}

impl BinarySizeable for u32 {
    fn get_binary_size(&self) -> usize {
        size_of::<u32>()
    }
}

impl BinarySizeable for u64 {
    fn get_binary_size(&self) -> usize {
        size_of::<u64>()
    }
}

#[derive(Clone, Copy)]
pub struct FenseIndex<T: Ord + BinarySizeable> {
    pub active: bool,
    pub target: u64,
    pub value: T,
}

impl<T: Ord + BinarySizeable> FenseIndex<T> {
    pub fn new(target: u64, value: T) -> Self {
        Self {
            active: false,
            target,
            value,
        }
    }

    fn get_prefix_binary_size() -> usize {
        1 + size_of::<u64>()
    }

    fn get_binary_size(&self) -> usize {
        let prefix_size = 1 + size_of::<u64>(); // (active bool) + (target u64)
        prefix_size + self.value.get_binary_size()
    }

}

pub struct SortedIndexTableFragmentHeader<T: Ord + Clone> {
    pub records_count: u32,
    pub max_records_count: u32,
    pub shift_threshold: u32,
    pub min_value: T,
    pub max_value: T,
}

impl<T: Ord + Clone> SortedIndexTableFragmentHeader<T> {
    pub fn get_binary_size(value_binary_size: usize) -> usize {
        size_of::<u32>() + size_of::<u32>() + size_of::<u32>() + value_binary_size + value_binary_size
    }
}

type ValueReader<T> = Box<dyn Fn(&mut Box<File>) -> Result<T, String>>;

type ValueWriter<T> = Box<dyn Fn(T) -> Result<Bytes, String>>;

pub struct ValueDefaultSizeInfo {
    pub prefix_size: usize,
    pub total_size: usize,
}

type ComputeValueDefaultSize = fn() -> ValueDefaultSizeInfo;

pub struct SortedIndexFiles<T: Ord + Clone + BinarySizeable> {
    pub folder: String,

    // if we have more than max_incomplete_fragments_count fragments, we compact the fragment.
    pub max_incomplete_fragments_count: u32,

    // if we must insert an index record, we shift all the records to the right by this amount.
    // But if the number of records to shift is bigger than shift_threshold, we will create a new fragment.
    pub shift_threshold: u32,

    // If a fragment has more than max_records_count records, we will create a new fragment.
    pub max_records_count_per_fragments: u32,

    pub write_handles: Vec<Box<File>>,
    pub fragment_count: usize,

    // The default value is used when we have to create a new fragment for min_value and max_value range.
    pub default_value: T,

    pub read_value: ValueReader<T>,
    pub write_value: ValueWriter<T>,
}

impl<T: Ord + Clone + Display + BinarySizeable> SortedIndexFiles<T> {
    pub fn new_with_defaults(folder: String, default_value: T, read_value: ValueReader<T>, write_value: ValueWriter<T>) -> Result<Self, String> {
        Self::new(folder, default_value, read_value, write_value, 10, 10_000, 100_000)
    }

    pub fn count_fragments_in_folder(folder: String) -> Result<usize, String> {
        if !std::fs::exists(folder.clone()).map_err(|e| e.to_string())?{
            return Err(String::from(format!("Folder {} does not exist", folder)));
        }

        let entries = std::fs::read_dir(folder.clone()).map_err(|e| e.to_string())?;
        let fragment_count = entries
            .filter_map(|r| r.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .filter(|f| f.extension().map(|e| e == "ix").unwrap_or(false))
            .count();

        Ok(fragment_count)
    }

    pub fn new(folder: String,
               default_value: T,
               read_value: ValueReader<T>,
               write_value: ValueWriter<T>,
               max_incomplete_fragments_count: u32,
               shift_threshold: u32,
               max_records_count_per_fragments: u32) -> Result<Self, String> {

        std::fs::create_dir_all(folder.clone()).map_err(|e| e.to_string())?;

        let fragment_count = SortedIndexFiles::<T>::count_fragments_in_folder(folder.clone())?;

        Ok(Self {
            folder,
            max_incomplete_fragments_count,
            shift_threshold,
            max_records_count_per_fragments,
            write_handles: Vec::new(),
            fragment_count,
            default_value,
            read_value,
            write_value
        })
    }
    
    pub fn open_fragment(&mut self, num: usize) -> Result<(), String> {
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
            let default_value = self.default_value.clone();
            let default_value_size = default_value.get_binary_size();
            let record_size = (FenseIndex::<T>::get_prefix_binary_size() + default_value_size) as u32;
            let header_size = SortedIndexTableFragmentHeader::<T>::get_binary_size(default_value_size) as u32;
            let initial_size = header_size + record_size * self.max_records_count_per_fragments;

            file.set_len(initial_size as u64).map_err(|e| e.to_string())?;

            self.write_handles.push(Box::new(file));

            self.write_header(num, default_value.clone(), default_value.clone(), 0)?;

            self.fragment_count += 1;
        }
        else {
            self.write_handles.push(Box::new(file));
        }

        Ok(())
    }
    
    pub fn append_fragment(&mut self) -> Result<usize, String> {
        self.open_fragment(self.fragment_count)?;

        Ok(self.fragment_count)
    }

    fn write_header(&mut self, num: usize, min_value: T, max_value: T, records_count: u32) -> Result<(), String> {
        let header = SortedIndexTableFragmentHeader {
                max_records_count: self.max_records_count_per_fragments,
                shift_threshold: self.shift_threshold,
                min_value: min_value.clone(),
                max_value: max_value.clone(),
                records_count
        };
        let write_value = &self.write_value;

        let handles = self.write_handles.as_mut_slice();
        let file = &mut handles[num];

        file.seek(io::SeekFrom::Start(0)).map_err(|e| e.to_string()).map_err(|e| e.to_string())?;
        file.write(&header.max_records_count.to_le_bytes()).map_err(|e| e.to_string())?;
        file.write(&header.records_count.to_le_bytes()).map_err(|e| e.to_string())?;
        file.write(&header.shift_threshold.to_le_bytes()).map_err(|e| e.to_string())?;

        let b = write_value(min_value)?;
        let content = b.to_vec();
        file.write_all(&content).map_err(|e| e.to_string())?;

        let b = write_value(max_value)?;
        let content = b.to_vec();
        file.write_all(&content).map_err(|e| e.to_string())?;

        Ok(())
    }

    fn read_header(&mut self, num: usize) -> Result<SortedIndexTableFragmentHeader<T>, String> {
        let read_value = &self.read_value;
        let mut max_records_count = [0u8; 4];
        let mut records_count = [0u8; 4];
        let mut shift_threshold = [0u8; 4];

        let handles = self.write_handles.as_mut_slice();
        let file = &mut handles[num];

        file.seek(io::SeekFrom::Start(0)).map_err(|e| e.to_string()).map_err(|e| e.to_string())?;
        file.read(&mut max_records_count).map_err(|e| e.to_string())?;
        file.read(&mut records_count).map_err(|e| e.to_string())?;
        file.read(&mut shift_threshold).map_err(|e| e.to_string())?;

        let mut buf = Vec::new();
        file.read(&mut buf).map_err(|e| e.to_string())?;
        let min_value = read_value(file)?;

        let mut buf = Vec::new();
        file.read(&mut buf).map_err(|e| e.to_string())?;
        let max_value = read_value(file)?;

        Ok(SortedIndexTableFragmentHeader {
            max_records_count: u32::from_le_bytes(max_records_count),
            records_count: u32::from_le_bytes(records_count),
            shift_threshold: u32::from_le_bytes(shift_threshold),
            min_value,
            max_value
        })
    }

    fn read_offset(&mut self, num: usize, offset: u64, compute_value_default_size: ComputeValueDefaultSize) -> Result<FenseIndex<T>, String> {
        let record_binary_size = self.default_value.get_binary_size();
        let after_header_offset_position = SortedIndexTableFragmentHeader::<T>::get_binary_size(record_binary_size) as u64;
        // let offset_position = after_header_offset_position + (offset as u64) * FenseIndex::<T>::get_prefix_binary_size() as u64;
        let record_binary_size = compute_value_default_size();
        let offset_position = after_header_offset_position + offset * record_binary_size.total_size as u64;

        let handles = self.write_handles.as_mut_slice();
        let read_value = &self.read_value;

        let file = &mut handles[num];
        file.seek(io::SeekFrom::Start(offset_position)).map_err(|e| e.to_string()).map_err(|e| e.to_string())?;

        let mut buf = vec![0; FenseIndex::<T>::get_prefix_binary_size()];
        file.read(&mut buf).unwrap();
        let bytes = BytesMut::from(buf.as_slice());
        let mut bin = BinaryReader::from(bytes);

        let active = bin.read_bool()?;
        let target = bin.read_u64()?;
        let value = read_value(file)?;

        Ok(FenseIndex {
            active,
            target,
            value
        })
    }

    fn read_all_indexes(&mut self, num: usize, offset: u64, compute_value_default_size: ComputeValueDefaultSize) -> Result<Vec<FenseIndex<T>>, String> {
        let header_range_value_binary_size = self.default_value.get_binary_size();
        let after_header_offset_position = SortedIndexTableFragmentHeader::<T>::get_binary_size(header_range_value_binary_size) as u64;

        let record_binary_size = compute_value_default_size();
        let offset_position = after_header_offset_position + offset * record_binary_size.total_size as u64;

        let handles = self.write_handles.as_mut_slice();
        let file = &mut handles[num];
        file.seek(io::SeekFrom::Start(offset_position)).map_err(|e| e.to_string()).map_err(|e| e.to_string())?;

        let read_value = &self.read_value;

        let mut items = Vec::with_capacity(self.max_records_count_per_fragments as usize);
        for i in offset .. self.max_records_count_per_fragments as u64 {

            let position_before_read = file.stream_position().map_err(|e| e.to_string())?;

            let mut buf = vec![0; record_binary_size.prefix_size];
            file.read(&mut buf).unwrap();
            let bytes = BytesMut::from(buf.as_slice());
            let mut bin = BinaryReader::from(bytes);

            let active = bin.read_bool()?;
            let target = bin.read_u64()?;
            match read_value(file) {
                Ok(value) => {
                    if active {
                        items.push(FenseIndex { active, target, value });
                    }
                },
                Err(e) => {
                    return Err(String::from(format!("Failed to read value at offset {i}: {e}")));
                }
            }
        }

        Ok(items)
    }

    fn write_index_content(&mut self, num: usize, ix: FenseIndex<T>, offset: u32) -> Result<(), String> {
        let record_binary_size = self.default_value.get_binary_size();
        let after_header_offset_position = SortedIndexTableFragmentHeader::<T>::get_binary_size(record_binary_size) as u64;
        let index_size = ix.get_binary_size();
        let offset_position = after_header_offset_position + (offset as u64) * index_size as u64;

        let handles = self.write_handles.as_mut_slice();
        let write_value = &self.write_value;

        let file = &mut handles[num];
        file.seek(io::SeekFrom::Start(offset_position)).map_err(|e| e.to_string()).map_err(|e| e.to_string())?;

        let mut bin = BinaryWriter::with_capacity(FenseIndex::<T>::get_prefix_binary_size());
        bin.write_bool(ix.active);
        bin.write_u64(ix.target);

        let b = write_value(ix.value)?;
        let bytes = b.iter().as_slice();
        bin.write_bytes(bytes);

        let content = bin.buffer.freeze().to_vec();

        file.write_all(&content).map_err(|e| e.to_string())
    }

    pub fn write_offset(&mut self, num: usize, ix: FenseIndex<T>, offset: u32) -> Result<(), String> {
        let header = self.read_header(num)?;
        let records_count = header.records_count + 1;
        let mut min_value = header.min_value.clone();
        let mut max_value = header.max_value.clone();
        if ix.value < header.min_value || self.default_value == header.min_value {
            min_value = ix.value.clone()
        }
        if ix.value > header.max_value || self.default_value == header.max_value {
            max_value = ix.value.clone()
        }

        self.write_index_content(num, ix, offset)?;

        self.write_header(num, min_value, max_value, records_count)
    }

    pub fn reorder_indexes(&mut self, num: usize, compute_value_default_size: ComputeValueDefaultSize) -> Result<(), String> {
        let mut items = self.read_all_indexes(num, 0, compute_value_default_size)?;

        items.retain(|ix| ix.active);
        items.sort_by(|a, b| {
            a.value.cmp(&b.value).then_with(|| a.target.cmp(&b.target))
        });

        for (i, ix) in items.into_iter().enumerate() {
            self.write_offset(num, ix, i as u32)?;
        }

        Ok(())
    }

    pub fn clear_offset(&mut self, num: usize, offset: u32) -> Result<(), String> {
        let header = self.read_header(num)?;
        let records_count = header.records_count - 1;
        let ix = FenseIndex { active: false, target: 0, value: self.default_value.clone() };

        self.write_index_content(num, ix, offset)?;

        self.write_header(num, header.min_value, header.max_value, records_count)
    }

    fn store(&mut self, ix: FenseIndex<T>, compute_value_default_size: ComputeValueDefaultSize) -> Result<(), String> {
        let mut table_fragment = SortedIndexTableFragment::<T>::new(self);

        match table_fragment.get_index_file_num_for_store(&ix)? {
            FileNumberAssignment::Specific(num) => {
                let header = self.read_header(num)?;
                self.write_offset(num, ix, header.records_count)?;
            }
            FileNumberAssignment::NextAvailable => {
                let next_num = self.fragment_count;
                self.open_fragment(next_num)?;
                self.write_offset(next_num, ix, 0)?;
            },
            FileNumberAssignment::Split(num) => {
                let header = self.read_header(num)?;
                let value_is_in_range = ix.value > header.min_value && ix.value < header.max_value;
                if !value_is_in_range {
                    return Err(String::from("Index value is not in range of the fragment. Cannot split the fragment."));
                }

                let next_num = self.fragment_count;
                self.open_fragment(next_num)?;

                // move all indexes bigger than ix to the next fragment
                let mut next_fragment_min_value = self.default_value.clone();
                let mut next_fragment_max_value = self.default_value.clone();
                let mut next_fragment_records_count = 0;
                let mut old_fragment_records_count = header.records_count;

                for offset in 0..header.records_count {
                    let old_ix = self.read_offset(num, offset as u64, compute_value_default_size)?;

                    if old_ix.target == 14 {
                        println!("old_ix.target: {:?}", old_ix.target);
                    }

                    if old_ix.value > ix.value.clone() {

                        if old_ix.value != self.default_value && next_fragment_min_value == self.default_value {
                            next_fragment_min_value = old_ix.value.clone();
                        }

                        if old_ix.value != self.default_value && old_ix.value > next_fragment_max_value {
                            next_fragment_max_value = old_ix.value.clone();
                        }

                        self.write_offset(next_num, old_ix, next_fragment_records_count)?;
                        self.clear_offset(num, offset)?;

                        next_fragment_records_count += 1;
                        old_fragment_records_count -= 1;
                    }
                }
                self.write_header(num, header.min_value, ix.value.clone(), old_fragment_records_count)?;
                self.write_header(next_num, next_fragment_min_value, next_fragment_max_value, next_fragment_records_count)?;

                // TODO: reorder the indexes in the old fragment
                self.reorder_indexes(num, compute_value_default_size)?;
                self.write_offset(num, ix, old_fragment_records_count)?; // TODO: store a the end after reordering the indexes in the old fragment

            }
        }

        Ok(())
    }
}

pub struct SortedIndexTableFragment<'a, T: Ord + Clone + Display + BinarySizeable> {
    pub files: &'a mut SortedIndexFiles<T>,
}

#[derive(PartialEq)]
#[derive(Debug)]
pub enum FileNumberAssignment {
    Specific(usize),
    NextAvailable,
    Split(usize),
}

impl<'a, T: Ord + Clone + Display + BinarySizeable> SortedIndexTableFragment<'a, T> {

    pub fn new(files: &'a mut SortedIndexFiles<T>) -> Self {
        SortedIndexTableFragment { files }
    }

    pub fn get_index_file_num_for_store(&mut self, ix: &FenseIndex<T>) -> Result<FileNumberAssignment, String> {
        for i in 0..self.files.write_handles.len() {
            let default_value = self.files.default_value.clone();
            let header = self.files.read_header(i)?;

            let value_is_in_range = ix.value > header.min_value && ix.value < header.max_value;

            if header.records_count >= header.max_records_count && value_is_in_range {
                // TODO: if ix value is in range, then we should split the file and store the index in the new file
                return Ok(FileNumberAssignment::Split(i));
            }

            if header.min_value == default_value && header.max_value == default_value {
                return Ok(FileNumberAssignment::Specific(i));
            }

            if header.records_count < header.max_records_count {
                return Ok(FileNumberAssignment::Specific(i));
            }

            if value_is_in_range {
                return Ok(FileNumberAssignment::Specific(i));
            }
        }

        Ok(FileNumberAssignment::NextAvailable)
    }

    pub fn insert(&mut self, ix: FenseIndex<T>) -> Result<(), String> {
        Err(String::from("Not implemented"))
    }

}

pub fn pad_or_truncate_string(s: String, pad: char, len: usize) -> String {
    let mut result: String = s.chars().take(len).collect();
    let current_len = result.chars().count();

    for _ in current_len..len {
        result.push(pad);
    }
    result
}


pub fn default_string_writer(index_value_size: usize) -> ValueWriter<String> {
    Box::new(
        move | v | {
            let v = pad_or_truncate_string(v, ' ', index_value_size);
            let bin = BinaryWriter::with_capacity(index_value_size + 1); // +1 because of the length prefix
            let bytes = v.as_bytes();
            let len = bytes.len() as u64;
            let len_bytes = len.to_be_bytes();
            let mut buffer = bin.buffer;
            buffer.put_slice(&len_bytes);
            buffer.put_slice(&bytes);

            Ok(buffer.freeze())
        }
    )
}

pub fn default_string_fixed_size_reader(index_value_size: usize) -> ValueReader<String> {
    Box::new(
        move |file| {
            let position = file.stream_position().map_err(|e| e.to_string())?;
            let file_length: u64 = file.metadata().map_err(|e| e.to_string())?.len();

            let mut bl: [u8; 8] = Default::default();
            let read_bytes_count = file.read(&mut bl).map_err(|e| e.to_string())?;
            if read_bytes_count != 8 {
                return Err(String::from("Could not read 8 bytes from file. File is too short."))
            }

            let text_len = usize::from_be_bytes(bl);
            let max_possible_len = file_length - position;

            if text_len == 0 {
                file.seek(io::SeekFrom::Current(index_value_size as i64)).map_err(|e| e.to_string())?;
                return Ok(String::from(""));
            }

            if text_len != index_value_size {
                return Err(String::from("Invalid text length. Text length is not equal to index value size."));
            }

            if text_len > max_possible_len as usize {
                return Err(String::from("Corrupted file. Text length is greater than file length."));
            }
            if text_len > index_value_size {
                return Err(String::from("Corrupted file. Text length is greater than index value size."));
            }

            let mut buf = vec![0; text_len];
            let read_bytes_count = file.read(buf.as_mut_slice()).map_err(|e| e.to_string())?;
            if read_bytes_count != text_len {
                return Err(String::from(format!("Could not read {text_len} bytes from file (read {read_bytes_count} bytes instead).")))
            }

            match String::from_utf8(buf) {
                Ok(s) => Ok(s),
                Err(e) => Err(format!("Failed to read value due to invalid UTF-8 sequence: {}", e))
            }
        }
    )
}

pub fn default_u32_writer() -> ValueWriter<u32> {
    Box::new(
        move |v| {
            let mut bin = BinaryWriter::with_capacity(size_of::<u32>());
            bin.write_u32(v);
            Ok(bin.buffer.freeze())
        }
    )
}

pub fn default_u32_reader() -> ValueReader<u32> {
    Box::new(
        move |file| {
            let mut buf = vec![0; size_of::<u32>()];
            file.read(buf.as_mut_slice()).map_err(|e| e.to_string())?;
            let bytes = BytesMut::from(buf.as_slice());
            let mut bin = BinaryReader::from(bytes);
            let value = bin.read_u32()?;
            Ok(value)
        }
    )
}

pub fn default_u64_writer() -> ValueWriter<u64> {
    Box::new(
        move |v| {
            let mut bin = BinaryWriter::with_capacity(size_of::<u64>());
            bin.write_u64(v);
            Ok(bin.buffer.freeze())
        }
    )
}

pub fn default_u64_reader() -> ValueReader<u64> {
    Box::new(
        move |mut file| {
            let mut buf = vec![0; size_of::<u64>()];
            file.read(buf.as_mut_slice()).map_err(|e| e.to_string())?;
            let bytes = BytesMut::from(buf.as_slice());
            let mut bin = BinaryReader::from(bytes);
            Ok(bin.read_u64()?)
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_new_file_when_is_more_records_than_max_records_count_per_fragment() {
        let folder = "test_folder/should_create_new_file_when_is_more_records_than_max_records_count_per_fragment";
        if std::fs::exists(folder).unwrap() {
            std::fs::remove_dir_all(folder).unwrap();
        }

        let read_value: ValueReader<String> = default_string_fixed_size_reader(200);
        let write_value: ValueWriter<String> = default_string_writer(200);
        let default_value = pad_or_truncate_string(String::from(""), 0 as char, 200);

        let mut files = SortedIndexFiles::<String>::new(folder.to_string(), default_value, read_value, write_value, 3, 5, 20).unwrap();
        files.open_fragment(0).unwrap();

        let compute_size = move || {
            let ix: FenseIndex<String> = FenseIndex { active: true, target: 0, value: pad_or_truncate_string(String::from(""), ' ', 200) };
            ValueDefaultSizeInfo { prefix_size: FenseIndex::<String>::get_prefix_binary_size(), total_size: ix.get_binary_size() }
        };

        for i in 0..22 {
            let value = format!("string value {i}");
            let value = pad_or_truncate_string(value, ' ', 200);
            let item: FenseIndex<String> = FenseIndex { active: true, target: i, value };

            files.store(item, compute_size).unwrap();
        }

        // let fragment_count = SortedIndexFiles::<String>::count_fragments_in_folder(String::from(folder)).unwrap();
        // assert_eq!(4, fragment_count);

        let items0 = files.read_all_indexes(0, 0, compute_size).unwrap();
        let items1 = files.read_all_indexes(1, 0, compute_size).unwrap();
        let items2 = files.read_all_indexes(2, 0, compute_size).unwrap();
        let items3 = files.read_all_indexes(3, 0, compute_size).unwrap();

        let all: Vec<_> = [items0, items1, items2, items3]
            .into_iter()
            .flatten()
            .filter(|ix| ix.active)
            //.map(|ix| ix.value)
            .collect();

       // all.sort();

        let mut targets: Vec<(usize, u64)> = all
            .iter()
            .enumerate()
            .map(|(i, ix)| (i, ix.target))
            .collect();
        targets.sort_by(|a, b| a.1.cmp(&b.1));

        let count = all.len();
        assert_eq!(65, count);

        for i in 0..65 {
            assert_eq!(format!("string value {i}"), all[i].value);
        }

    }

    #[test]
    fn should_find_index_file_num_for_index_value() {
        let folder = "test_folder/should_find_index_file_num_for_index_value";
        if std::fs::exists(folder).unwrap() {
            std::fs::remove_dir_all(folder).unwrap();
        }

        let read_value: ValueReader<String> = default_string_fixed_size_reader(200);
        let write_value: ValueWriter<String> = default_string_writer(200);

        let default_value = String::from("");
        let default_value = pad_or_truncate_string(default_value, ' ', 200);
        let mut files = SortedIndexFiles::<String>::new(folder.to_string(), default_value, read_value, write_value, 3, 10, 1000).unwrap();

        for num in 0..10 {
            files.open_fragment(num).unwrap();

            let letter = (b'a' + num as u8) as char;

            for i in num..(num+20) {
                let v = i * 10;
                let value = format!("string value {letter} - {v}");
                let value = pad_or_truncate_string(value, ' ', 200);
                let item: FenseIndex<String> = FenseIndex { active: true, target: 100 * i as u64, value };
                files.write_offset(num, item, i as u32).unwrap();
            }
        }

        // files.write_header(0, String::from(("string value a - 0")), String::from(("string value k - 300"))).unwrap();

        let mut table_fragment = SortedIndexTableFragment::<String>::new(&mut files);
        // let header = table_fragment.files.read_header(0).unwrap();

        let ix1 = FenseIndex { active: true, target: 100, value: String::from("string value d - 15") };
        let index_file_num_1 = table_fragment.get_index_file_num_for_store(&ix1).unwrap();

        let ix2 = FenseIndex { active: true, target: 100, value: String::from("string value g - 20") };
        let index_file_num_2 = table_fragment.get_index_file_num_for_store(&ix2).unwrap();

        assert_eq!(index_file_num_1, FileNumberAssignment::Specific(0));
        assert_eq!(index_file_num_2, FileNumberAssignment::Specific(0));
    }

    #[test]
    fn should_read_offset_by_offset() {
        let folder = "test_folder/should_read_offset_by_offset";
        if std::fs::exists(folder).unwrap() {
            std::fs::remove_dir_all(folder).unwrap();
        }

        let read_value: ValueReader<String> = default_string_fixed_size_reader(200);
        let write_value: ValueWriter<String> = default_string_writer(200);

        let default_value = String::from("");
        let default_value = pad_or_truncate_string(default_value, ' ', 200);

        let mut files = SortedIndexFiles::<String>::new(folder.to_string(), default_value, read_value, write_value, 3, 10, 500).unwrap();
        files.open_fragment(0).unwrap();

        for i in 0..500 {
            let value = format!("string value {i}");
            let value = pad_or_truncate_string(value, ' ', 200);
            let item: FenseIndex<String> = FenseIndex { active: true, target: (100 * i as u64), value };
            files.write_offset(0, item, i).unwrap();
        }

        for i in 0..500 {
            let ix = files.read_offset(0, i, move || {
                let ix: FenseIndex<String> = FenseIndex { active: true, target: 0, value: pad_or_truncate_string(String::from(""), ' ', 200) };
                ValueDefaultSizeInfo { prefix_size: FenseIndex::<String>::get_prefix_binary_size(), total_size: ix.get_binary_size() }
            }).unwrap();
            assert_eq!(ix.value.trim(), format!("string value {i}"));
            assert_eq!(ix.target, (100 * i as u64));
        }
    }

    #[test]
    fn should_read_only_written_index_records() {
        let folder = "test_folder/should_read_only_written_index_records";
        if std::fs::exists(folder).unwrap() {
            std::fs::remove_dir_all(folder).unwrap();
        }

        let read_value: ValueReader<String> = default_string_fixed_size_reader(200);
        let write_value: ValueWriter<String> = default_string_writer(200);

        let default_value = String::from("");
        let default_value = pad_or_truncate_string(default_value, ' ', 200);

        let mut files = SortedIndexFiles::<String>::new(folder.to_string(), default_value, read_value, write_value, 3, 10, 500).unwrap();
        files.open_fragment(0).unwrap();

        for i in 20u32..30u32 {
            let value = format!("string value {i}");
            let value = pad_or_truncate_string(value, ' ', 200);
            let item: FenseIndex<String> = FenseIndex { active: true, target: (100 * i as u64), value };
            files.write_offset(0, item, i).unwrap();
        }

        let fetched_records = files.read_all_indexes(0, 20, move || {
            let ix: FenseIndex<String> = FenseIndex { active: true, target: 0, value: pad_or_truncate_string(String::from(""), ' ', 200) };
            ValueDefaultSizeInfo { prefix_size: FenseIndex::<String>::get_prefix_binary_size(), total_size: ix.get_binary_size() }
        }).unwrap();
        let stored_values = fetched_records.iter().filter(|r| r.active).map(|r| r.value.clone()).collect::<Vec<String>>();

        assert_eq!(10, stored_values.len());
        assert_eq!("string value 20", stored_values[0].trim());
        assert_eq!("string value 21", stored_values[1].trim());
        assert_eq!("string value 22", stored_values[2].trim());
        assert_eq!("string value 29", stored_values[9].trim());
    }

    #[test]
    fn should_read_only_written_u32_index_records() {
        let folder = "test_folder/should_read_only_written_u32_index_records";
        if std::fs::exists(folder).unwrap() {
            std::fs::remove_dir_all(folder).unwrap();
        }

        let read_value: ValueReader<u32> = default_u32_reader();
        let write_value: ValueWriter<u32> = default_u32_writer();

        let mut files = SortedIndexFiles::<u32>::new(folder.to_string(), 0, read_value, write_value, 3, 10, 500).unwrap();
        files.open_fragment(0).unwrap();

        for i in 20u32..30u32 {
            let item: FenseIndex<u32> = FenseIndex { active: true, target: (100 * i as u64), value: i };
            files.write_offset(0, item, i).unwrap();
        }

        let fetched_records = files.read_all_indexes(0, 20, move || {
            let ix: FenseIndex<u32> = FenseIndex { active: true, target: 0, value: 0 };
            ValueDefaultSizeInfo { prefix_size: FenseIndex::<u32>::get_prefix_binary_size(), total_size: ix.get_binary_size() }
        }).unwrap();
        let stored_values = fetched_records.iter().filter(|r| r.active).map(|r| r.value.clone()).collect::<Vec<u32>>();

        assert_eq!(10, stored_values.len());
        assert_eq!(20, stored_values[0]);
        assert_eq!(21, stored_values[1]);
        assert_eq!(22, stored_values[2]);
        assert_eq!(29, stored_values[9]);
    }

    #[test]
    fn should_write_and_read_index_file_header() {
        let folder = "test_folder/should_write_and_read_index_file_header";

        if std::fs::exists(folder).unwrap() {
            std::fs::remove_dir_all(folder).unwrap();
        }

        let read_value: ValueReader<String> = default_string_fixed_size_reader(200);
        let write_value: ValueWriter<String> = default_string_writer(200);

        let mut files = SortedIndexFiles::<String>::new(folder.to_string(), String::from(""), read_value, write_value, 3, 10, 50).unwrap();
        files.open_fragment(0).unwrap();

        let header = files.read_header(0).unwrap();

        assert_eq!(50, header.max_records_count, "Max records count should be 50");
        assert_eq!(10, header.shift_threshold, "Shift thresold should be 10");
        assert_eq!("", header.min_value.as_str().trim(), "Min value should be empty");
        assert_eq!("", header.max_value.as_str().trim(), "Max value should be empty");
    }

    #[test]
    fn string_index_should_be_greater() {
        // given
        let ix1: FenseIndex<String> = { FenseIndex { active: false, target: 1, value: "aaaa".to_string() } };
        let ix2: FenseIndex<String> = { FenseIndex { active: false, target: 2, value: "bbbb".to_string() } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(std::cmp::Ordering::Greater, r);
    }

    #[test]
    fn string_index_should_be_less() {
        // given
        let ix1: FenseIndex<String> = { FenseIndex { active: false, target: 1, value: "zzzz".to_string() } };
        let ix2: FenseIndex<String> = { FenseIndex { active: false, target: 2, value: "bbbb".to_string() } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(std::cmp::Ordering::Less, r);
    }

    #[test]
    fn string_index_should_be_equal() {
        // given
        let ix1: FenseIndex<String> = { FenseIndex { active: false, target: 1, value: "ddd".to_string() } };
        let ix2: FenseIndex<String> = { FenseIndex { active: false, target: 2, value: "ddd".to_string() } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(std::cmp::Ordering::Equal, r);
    }

    #[test]
    fn u64_index_should_be_greater() {
        // given
        let ix1: FenseIndex<u64> = { FenseIndex { active: false, target: 1, value: 45 } };
        let ix2: FenseIndex<u64> = { FenseIndex { active: false, target: 2, value: 60 } };
        // when
        let r = ix2.value.cmp(&ix1.value);
        // then
        assert_eq!(std::cmp::Ordering::Greater, r);
    }

}
