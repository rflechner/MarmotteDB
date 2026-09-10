//! A disk-backed sorted index split across independently sorted fragments.
//!
//! Version 2 uses a fixed header and a fixed-width slot directory. Values are
//! appended to a separate payload area, so changing a string's length cannot
//! move the header or overwrite another value. Sorting moves slots, not values.
//! One owner must exclusively access a folder. See docs/sorted-index-table.md
//! for the format, public API and durability limitations.

use bytes::Bytes;
use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use crate::indexes::indexes_binary::{BinarySizeable, FenseIndex, SLOT_SIZE, HEADER_SIZE, MAGIC, TEMP_ID};

#[derive(Clone, Debug)]
pub struct SortedIndexTableFragmentHeader<T: Ord + Clone> {
    pub records_count: u32,
    pub max_records_count: u32,
    pub shift_threshold: u32,
    // Bounds are rebuilt from the slot directory when opening a fragment.
    // records_count, never default_value, distinguishes an empty fragment.
    pub min_value: T,
    pub min_value_size: usize,
    pub max_value: T,
    pub max_value_size: usize,
}

impl<T: Ord + Clone> SortedIndexTableFragmentHeader<T> {
    /// The v2 on-disk header has a fixed size, independent of the bounds.
    pub fn get_default_binary_size(_value_binary_size: usize) -> usize {
        HEADER_SIZE
    }
    pub fn compute_binary_size(&self) -> usize {
        HEADER_SIZE
    }
}

pub type ValueReader<T> = Box<dyn Fn(u32, &mut Box<File>) -> Result<T, String>>;
pub type ValueWriter<T> = Box<dyn Fn(T) -> Result<Bytes, String>>;

#[derive(Clone, Copy, Debug)]
struct Slot {
    target: u64,
    position: u64,
    size: u32,
}

struct Fragment<T: Ord + Clone> {
    // Only the used prefix is kept; None preserves holes written by write_offset.
    slots: Vec<Option<Slot>>,
    header: SortedIndexTableFragmentHeader<T>,
    sorted: bool,
}

/// A prepared replacement never truncates the original file on encoding failure.
struct PreparedFragment<T: Ord + Clone> {
    path: PathBuf,
    file: Option<Box<File>>,
    fragment: Option<Fragment<T>>,
}

impl<T: Ord + Clone> Drop for PreparedFragment<T> {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}

pub struct SortedIndexFiles<T: Ord + Clone + BinarySizeable> {
    folder: PathBuf,
    max_incomplete_fragments_count: u32,
    shift_threshold: u32,
    max_records_count_per_fragments: u32,
    write_handles: Vec<Box<File>>,
    fragments: Vec<Fragment<T>>,
    default_value: T,
    read_value: ValueReader<T>,
    write_value: ValueWriter<T>,
}

fn io_error(error: std::io::Error) -> String {
    error.to_string()
}

fn compare<T: Ord + BinarySizeable>(a: &FenseIndex<T>, b: &FenseIndex<T>) -> Ordering {
    a.value.cmp(&b.value).then_with(|| a.target.cmp(&b.target))
}

fn directory_end(capacity: u32) -> u64 {
    HEADER_SIZE as u64 + u64::from(capacity) * SLOT_SIZE as u64
}

fn slot_bytes(slot: Option<Slot>) -> [u8; SLOT_SIZE] {
    let mut bytes = [0; SLOT_SIZE];
    if let Some(slot) = slot {
        bytes[0] = 1;
        bytes[1..9].copy_from_slice(&slot.target.to_be_bytes());
        bytes[9..17].copy_from_slice(&slot.position.to_be_bytes());
        bytes[17..21].copy_from_slice(&slot.size.to_be_bytes());
    }
    bytes
}

fn write_file_header<T: Ord + Clone>(
    file: &mut File,
    fragment: &Fragment<T>,
) -> Result<(), String> {
    let mut bytes = Vec::with_capacity(HEADER_SIZE);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&fragment.header.max_records_count.to_be_bytes());
    bytes.extend_from_slice(&fragment.header.records_count.to_be_bytes());
    bytes.extend_from_slice(&fragment.header.shift_threshold.to_be_bytes());
    bytes.extend_from_slice(&u32::from(fragment.sorted).to_be_bytes());
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    file.write_all(&bytes).map_err(io_error)
}

impl<T: Ord + Clone + BinarySizeable> SortedIndexFiles<T> {
    pub fn new_with_defaults(
        folder: String,
        default_value: T,
        read_value: ValueReader<T>,
        write_value: ValueWriter<T>,
    ) -> Result<Self, String> {
        Self::new(
            folder,
            default_value,
            read_value,
            write_value,
            10,
            10_000,
            100_000,
        )
    }

    fn fragment_numbers(folder: &Path) -> Result<Vec<usize>, String> {
        let mut numbers = Vec::new();
        for entry in fs::read_dir(folder).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("ix") {
                continue;
            }
            if !entry.file_type().map_err(io_error)?.is_file() {
                return Err(format!("Not a regular index file: {}", path.display()));
            }
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| format!("Invalid fragment name: {}", path.display()))?;
            let num = stem
                .parse::<usize>()
                .map_err(|_| format!("Invalid fragment name: {}", path.display()))?;
            if stem != format!("{num:08}") {
                return Err(format!("Non-canonical fragment name: {}", path.display()));
            }
            numbers.push(num);
        }
        numbers.sort_unstable();
        for (expected, actual) in numbers.iter().enumerate() {
            if expected != *actual {
                return Err(format!("Missing index fragment {expected:08}.ix"));
            }
        }
        Ok(numbers)
    }

    pub fn count_fragments_in_folder(folder: String) -> Result<usize, String> {
        Ok(Self::fragment_numbers(Path::new(&folder))?.len())
    }

    pub fn new(
        folder: String,
        default_value: T,
        read_value: ValueReader<T>,
        write_value: ValueWriter<T>,
        max_incomplete_fragments_count: u32,
        shift_threshold: u32,
        max_records_count_per_fragments: u32,
    ) -> Result<Self, String> {
        if max_records_count_per_fragments == 0 || max_incomplete_fragments_count == 0 {
            return Err("Fragment capacity and incomplete-fragment limit must be positive".into());
        }
        let folder = PathBuf::from(folder);
        fs::create_dir_all(&folder).map_err(io_error)?;
        let numbers = Self::fragment_numbers(&folder)?;
        let mut files = Self {
            folder,
            default_value,
            read_value,
            write_value,
            max_incomplete_fragments_count,
            shift_threshold,
            max_records_count_per_fragments,
            write_handles: Vec::new(),
            fragments: Vec::new(),
        };
        for num in numbers {
            let path = files.fragment_path(num);
            let mut file = Box::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .map_err(io_error)?,
            );
            let fragment = files
                .load_fragment(&mut file)
                .map_err(|e| format!("{}: {e}", path.display()))?;
            files.write_handles.push(file);
            files.fragments.push(fragment);
        }
        Ok(files)
    }

    pub fn fragment_count(&self) -> usize {
        self.fragments.len()
    }

    fn fragment_path(&self, num: usize) -> PathBuf {
        self.folder.join(format!("{num:08}.ix"))
    }

    fn empty_header(&self) -> SortedIndexTableFragmentHeader<T> {
        SortedIndexTableFragmentHeader {
            records_count: 0,
            max_records_count: self.max_records_count_per_fragments,
            shift_threshold: self.shift_threshold,
            min_value: self.default_value.clone(),
            min_value_size: self.default_value.get_binary_size(),
            max_value: self.default_value.clone(),
            max_value_size: self.default_value.get_binary_size(),
        }
    }

    fn check_fragment(&self, num: usize) -> Result<(), String> {
        if num >= self.fragment_count() {
            Err(format!("Unknown fragment {num}"))
        } else {
            Ok(())
        }
    }

    pub fn read_header(&self, num: usize) -> Result<SortedIndexTableFragmentHeader<T>, String> {
        self.check_fragment(num)?;
        Ok(self.fragments[num].header.clone())
    }

    fn decode_slot(&self, file: &mut Box<File>, slot: Slot) -> Result<FenseIndex<T>, String> {
        let end = slot
            .position
            .checked_add(u64::from(slot.size))
            .ok_or("Payload offset overflow")?;
        if slot.position < directory_end(self.max_records_count_per_fragments)
            || end > file.metadata().map_err(io_error)?.len()
        {
            return Err("Invalid or truncated index payload".into());
        }
        file.seek(SeekFrom::Start(slot.position))
            .map_err(io_error)?;
        let value = (self.read_value)(slot.size, file)?;
        if file.stream_position().map_err(io_error)? != end
            || value.get_binary_size() != slot.size as usize
        {
            return Err("ValueReader did not consume the declared value size".into());
        }
        Ok(FenseIndex::new(slot.target, value, slot.size as usize))
    }

    fn load_fragment(&self, file: &mut Box<File>) -> Result<Fragment<T>, String> {
        let mut bytes = [0; HEADER_SIZE];
        file.read_exact(&mut bytes).map_err(io_error)?;
        if &bytes[..8] != MAGIC {
            return Err("Unsupported index format; expected MRMTIX02 (rebuild old indexes)".into());
        }
        let capacity = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let records_count = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        let threshold = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let sorted = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
        if capacity != self.max_records_count_per_fragments || threshold != self.shift_threshold {
            return Err(
                "Fragment capacity or shift threshold does not match stored configuration".into(),
            );
        }
        if records_count > capacity || sorted > 1 {
            return Err("Invalid fragment header".into());
        }
        if file.metadata().map_err(io_error)?.len() < directory_end(capacity) {
            return Err("Truncated slot directory".into());
        }
        // Buffer the directory once; payload reads seek elsewhere.
        let directory_size = usize::try_from(u64::from(capacity) * SLOT_SIZE as u64)
            .map_err(|_| "Slot directory is too large")?;
        let mut directory = Vec::new();
        directory
            .try_reserve_exact(directory_size)
            .map_err(|e| e.to_string())?;
        directory.resize(directory_size, 0);
        file.read_exact(&mut directory).map_err(io_error)?;
        let mut fragment = Fragment {
            slots: Vec::new(),
            header: self.empty_header(),
            sorted: sorted == 1,
        };
        let mut previous: Option<FenseIndex<T>> = None;
        let mut active_count = 0u32;
        let mut saw_hole = false;
        for raw in directory.chunks_exact(SLOT_SIZE) {
            match raw[0] {
                0 => {
                    if raw.iter().any(|b| *b != 0) {
                        return Err("Invalid inactive slot".into());
                    }
                    saw_hole = true;
                    fragment.slots.push(None);
                }
                1 => {
                    let slot = Slot {
                        target: u64::from_be_bytes(raw[1..9].try_into().unwrap()),
                        position: u64::from_be_bytes(raw[9..17].try_into().unwrap()),
                        size: u32::from_be_bytes(raw[17..21].try_into().unwrap()),
                    };
                    let item = self.decode_slot(file, slot)?;
                    if fragment.sorted
                        && (saw_hole
                            || previous.as_ref().is_some_and(|p| compare(p, &item).is_gt()))
                    {
                        return Err(
                            "Fragment marked sorted has holes or out-of-order entries".into()
                        );
                    }
                    if active_count == 0 || item.value < fragment.header.min_value {
                        fragment.header.min_value = item.value.clone();
                    }
                    if active_count == 0 || item.value > fragment.header.max_value {
                        fragment.header.max_value = item.value.clone();
                    }
                    previous = Some(item);
                    active_count += 1;
                    fragment.slots.push(Some(slot));
                }
                _ => return Err("Invalid slot active flag".into()),
            }
        }
        if active_count != records_count {
            return Err("Fragment record count does not match directory".into());
        }
        while fragment.slots.last().is_some_and(Option::is_none) {
            fragment.slots.pop();
        }
        fragment.header.records_count = active_count;
        fragment.header.min_value_size = fragment.header.min_value.get_binary_size();
        fragment.header.max_value_size = fragment.header.max_value.get_binary_size();
        Ok(fragment)
    }

    pub fn open_fragment(&mut self, num: usize) -> Result<(), String> {
        if num < self.fragment_count() {
            return Ok(());
        }
        if num != self.fragment_count() {
            return Err("Fragments must be created consecutively".into());
        }
        let mut file = Box::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(self.fragment_path(num))
                .map_err(io_error)?,
        );
        let fragment = Fragment {
            slots: Vec::new(),
            header: self.empty_header(),
            sorted: true,
        };
        file.set_len(directory_end(self.max_records_count_per_fragments))
            .map_err(io_error)?;
        write_file_header(&mut file, &fragment)?;
        self.write_handles.push(file);
        self.fragments.push(fragment);
        Ok(())
    }

    /// Returns the actual zero-based fragment number.
    pub fn append_fragment(&mut self) -> Result<usize, String> {
        let num = self.fragment_count();
        self.open_fragment(num)?;
        Ok(num)
    }

    fn encode(&self, item: &FenseIndex<T>) -> Result<Bytes, String> {
        let size = u32::try_from(item.size).map_err(|_| "Index value exceeds u32 size")?;
        if item.size != item.value.get_binary_size() {
            return Err("Incorrect index value size".into());
        }
        let bytes = (self.write_value)(item.value.clone())?;
        if bytes.len() != size as usize {
            return Err("ValueWriter returned an incorrect byte count".into());
        }
        Ok(bytes)
    }

    fn append_payload(&mut self, num: usize, target: u64, bytes: &[u8]) -> Result<Slot, String> {
        let size = u32::try_from(bytes.len()).map_err(|_| "Index value exceeds u32 size")?;
        let file = &mut self.write_handles[num];
        let position = file.seek(SeekFrom::End(0)).map_err(io_error)?;
        file.write_all(bytes).map_err(io_error)?;
        Ok(Slot {
            target,
            position,
            size,
        })
    }

    pub fn read_offset(
        &mut self,
        num: usize,
        offset: u32,
    ) -> Result<Option<FenseIndex<T>>, String> {
        self.check_fragment(num)?;
        if offset >= self.max_records_count_per_fragments {
            return Err("Offset exceeds fragment capacity".into());
        }
        let slot = self.fragments[num]
            .slots
            .get(offset as usize)
            .copied()
            .flatten();
        match slot {
            None => Ok(None),
            Some(slot) => {
                // Split borrows explicitly to keep the codec and handle independent.
                let file = &mut self.write_handles[num];
                let end = slot
                    .position
                    .checked_add(u64::from(slot.size))
                    .ok_or("Payload offset overflow")?;
                if slot.position < directory_end(self.max_records_count_per_fragments)
                    || end > file.metadata().map_err(io_error)?.len()
                {
                    return Err("Invalid or truncated index payload".into());
                }
                file.seek(SeekFrom::Start(slot.position))
                    .map_err(io_error)?;
                let value = (self.read_value)(slot.size, file)?;
                if file.stream_position().map_err(io_error)? != end
                    || value.get_binary_size() != slot.size as usize
                {
                    return Err("ValueReader did not consume the declared value size".into());
                }
                Ok(Some(FenseIndex::new(
                    slot.target,
                    value,
                    slot.size as usize,
                )))
            }
        }
    }

    pub fn read_fragment(&mut self, num: usize) -> Result<Vec<FenseIndex<T>>, String> {
        self.check_fragment(num)?;
        let mut items = Vec::with_capacity(self.fragments[num].header.records_count as usize);
        for offset in 0..self.fragments[num].slots.len() {
            if let Some(item) = self.read_offset(num, offset as u32)? {
                items.push(item);
            }
        }
        Ok(items)
    }

    // Compatibility helper: in v2 these arguments identify the slot directory.
    fn read_all_indexes(
        &mut self,
        num: usize,
        prefix: usize,
        start: u64,
    ) -> Result<Vec<FenseIndex<T>>, String> {
        Self::check_layout(prefix, start)?;
        self.read_fragment(num)
    }

    fn check_layout(prefix: usize, start: u64) -> Result<(), String> {
        if prefix != SLOT_SIZE || start != HEADER_SIZE as u64 {
            Err("Incorrect v2 slot directory layout".into())
        } else {
            Ok(())
        }
    }

    fn update_bounds(&mut self, num: usize) -> Result<(), String> {
        let items = self.read_fragment(num)?;
        let header = &mut self.fragments[num].header;
        header.records_count = items.len() as u32;
        header.min_value = items
            .iter()
            .map(|ix| &ix.value)
            .min()
            .unwrap_or(&self.default_value)
            .clone();
        header.max_value = items
            .iter()
            .map(|ix| &ix.value)
            .max()
            .unwrap_or(&self.default_value)
            .clone();
        header.min_value_size = header.min_value.get_binary_size();
        header.max_value_size = header.max_value.get_binary_size();
        Ok(())
    }

    /// Low-level slot replacement. It may leave holes or unsorted entries.
    /// Use insert for normal sorted insertion; an overwrite does not increment count.
    pub fn write_offset(
        &mut self,
        num: usize,
        ix: FenseIndex<T>,
        offset: u32,
    ) -> Result<(), String> {
        self.check_fragment(num)?;
        if offset >= self.max_records_count_per_fragments {
            return Err("Offset exceeds fragment capacity".into());
        }
        let slot = if ix.active {
            let bytes = self.encode(&ix)?;
            Some(self.append_payload(num, ix.target, &bytes)?)
        } else {
            None
        };
        let file = &mut self.write_handles[num];
        file.seek(SeekFrom::Start(
            HEADER_SIZE as u64 + u64::from(offset) * SLOT_SIZE as u64,
        ))
        .map_err(io_error)?;
        file.write_all(&slot_bytes(slot)).map_err(io_error)?;
        let fragment = &mut self.fragments[num];
        fragment
            .slots
            .resize(fragment.slots.len().max(offset as usize + 1), None);
        fragment.slots[offset as usize] = slot;
        while fragment.slots.last().is_some_and(Option::is_none) {
            fragment.slots.pop();
        }
        fragment.sorted = false;
        self.update_bounds(num)?;
        write_file_header(&mut self.write_handles[num], &self.fragments[num])
    }

    pub fn clear_offset(&mut self, num: usize, offset: u32) -> Result<(), String> {
        let ix = FenseIndex {
            active: false,
            target: 0,
            value: self.default_value.clone(),
            size: 0,
        };
        self.write_offset(num, ix, offset)
    }

    fn persist_sorted_slots(
        &mut self,
        num: usize,
        slots: Vec<Slot>,
        min: T,
        max: T,
        from: usize,
    ) -> Result<(), String> {
        if slots.len() > self.max_records_count_per_fragments as usize {
            return Err("Fragment is full".into());
        }
        let old_len = self.fragments[num].slots.len();
        let end = old_len.max(slots.len());
        let mut bytes = Vec::with_capacity((end - from) * SLOT_SIZE);
        for i in from..end {
            bytes.extend_from_slice(&slot_bytes(slots.get(i).copied()));
        }
        let file = &mut self.write_handles[num];
        file.seek(SeekFrom::Start(
            HEADER_SIZE as u64 + from as u64 * SLOT_SIZE as u64,
        ))
        .map_err(io_error)?;
        file.write_all(&bytes).map_err(io_error)?;
        let header = &mut self.fragments[num].header;
        header.records_count = slots.len() as u32;
        header.min_value_size = min.get_binary_size();
        header.max_value_size = max.get_binary_size();
        header.min_value = min;
        header.max_value = max;
        self.fragments[num].slots = slots.into_iter().map(Some).collect();
        self.fragments[num].sorted = true;
        write_file_header(file, &self.fragments[num])
    }

    pub fn reorder_indexes(&mut self, num: usize, prefix: usize, start: u64) -> Result<(), String> {
        Self::check_layout(prefix, start)?;
        self.check_fragment(num)?;
        let mut entries = Vec::new();
        for offset in 0..self.fragments[num].slots.len() {
            if let Some(ix) = self.read_offset(num, offset as u32)? {
                entries.push((ix, self.fragments[num].slots[offset].unwrap()));
            }
        }
        entries.sort_by(|a, b| compare(&a.0, &b.0));
        let min = entries
            .first()
            .map(|e| &e.0.value)
            .unwrap_or(&self.default_value)
            .clone();
        let max = entries
            .last()
            .map(|e| &e.0.value)
            .unwrap_or(&self.default_value)
            .clone();
        self.persist_sorted_slots(num, entries.into_iter().map(|e| e.1).collect(), min, max, 0)
    }

    fn ensure_sorted(&mut self, num: usize) -> Result<(), String> {
        if !self.fragments[num].sorted {
            self.reorder_indexes(num, SLOT_SIZE, HEADER_SIZE as u64)?;
        }
        Ok(())
    }

    fn insertion_offset(&mut self, num: usize, ix: &FenseIndex<T>) -> Result<usize, String> {
        let mut lo = 0;
        let mut hi = self.fragments[num].slots.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let other = self
                .read_offset(num, mid as u32)?
                .ok_or("Hole in sorted directory")?;
            if compare(&other, ix).is_le() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    fn assignment(&mut self, ix: &FenseIndex<T>) -> Result<FileNumberAssignment, String> {
        let mut split_candidate = None;
        for num in 0..self.fragment_count() {
            self.ensure_sorted(num)?;
            let h = &self.fragments[num].header;
            if h.records_count < h.max_records_count {
                let count = h.records_count as usize;
                let offset = self.insertion_offset(num, ix)?;
                if count - offset <= self.shift_threshold as usize {
                    return Ok(FileNumberAssignment::Specific(num));
                }
            } else if ix.value > h.min_value && ix.value < h.max_value && split_candidate.is_none()
            {
                split_candidate = Some(num);
            }
        }
        Ok(split_candidate
            .map(FileNumberAssignment::Split)
            .unwrap_or(FileNumberAssignment::NextAvailable))
    }

    pub fn insert(&mut self, ix: FenseIndex<T>) -> Result<(), String> {
        if !ix.active {
            return Err("Cannot insert an inactive index entry".into());
        }
        let bytes = self.encode(&ix)?;
        match self.assignment(&ix)? {
            FileNumberAssignment::Specific(num) => self.insert_into(num, &ix, &bytes)?,
            FileNumberAssignment::NextAvailable => {
                let num = self.append_fragment()?;
                self.insert_into(num, &ix, &bytes)?;
            }
            FileNumberAssignment::Split(num) => self.split_and_insert(num, &ix, &bytes)?,
        }
        let incomplete: Vec<_> = (0..self.fragment_count())
            .filter(|&num| {
                let h = &self.fragments[num].header;
                h.records_count > 0 && h.records_count < h.max_records_count
            })
            .collect();
        if incomplete.len() > self.max_incomplete_fragments_count as usize {
            self.compact_fragments(&incomplete)?;
        }
        Ok(())
    }

    fn insert_into(&mut self, num: usize, ix: &FenseIndex<T>, bytes: &[u8]) -> Result<(), String> {
        let offset = self.insertion_offset(num, ix)?;
        let h = &self.fragments[num].header;
        let min = if h.records_count == 0 || ix.value < h.min_value {
            ix.value.clone()
        } else {
            h.min_value.clone()
        };
        let max = if h.records_count == 0 || ix.value > h.max_value {
            ix.value.clone()
        } else {
            h.max_value.clone()
        };
        let slot = self.append_payload(num, ix.target, bytes)?;
        let mut slots: Vec<_> = self.fragments[num]
            .slots
            .iter()
            .flatten()
            .copied()
            .collect();
        slots.insert(offset, slot);
        self.persist_sorted_slots(num, slots, min, max, offset)
    }

    fn split_and_insert(
        &mut self,
        num: usize,
        ix: &FenseIndex<T>,
        bytes: &[u8],
    ) -> Result<(), String> {
        let all = self.read_fragment(num)?;
        // Preserve the original design: keep values <= the inserted value here.
        let split = all.partition_point(|old| old.value <= ix.value);
        if split == 0 || split == all.len() {
            return Err("Split value must be inside fragment bounds".into());
        }
        let next = self.fragment_count();
        let mut prepared = self.prepare_fragment(next, &all[split..])?;
        // New fragment is installed before references are removed from the old one.
        self.install_prepared(next, &mut prepared)?;
        let offset = self.insertion_offset(num, ix)?;
        let min = self.fragments[num].header.min_value.clone();
        let slot = self.append_payload(num, ix.target, bytes)?;
        let mut left: Vec<_> = self.fragments[num].slots[..split]
            .iter()
            .flatten()
            .copied()
            .collect();
        left.insert(offset, slot);
        self.persist_sorted_slots(num, left, min, ix.value.clone(), offset)
    }

    fn store(&mut self, ix: FenseIndex<T>, prefix: usize) -> Result<(), String> {
        if prefix != SLOT_SIZE {
            return Err("Incorrect v2 slot size".into());
        }
        self.insert(ix)
    }

    fn prepare_fragment(
        &self,
        num: usize,
        items: &[FenseIndex<T>],
    ) -> Result<PreparedFragment<T>, String> {
        if items.len() > self.max_records_count_per_fragments as usize {
            return Err("Fragment is full".into());
        }
        let id = TEMP_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let path = self
            .folder
            .join(format!(".{num:08}-{}-{id}.tmp", std::process::id()));
        let file = Box::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(io_error)?,
        );
        let mut prepared = PreparedFragment {
            path,
            file: Some(file),
            fragment: None,
        };
        let file = prepared.file.as_mut().unwrap();
        file.set_len(directory_end(self.max_records_count_per_fragments))
            .map_err(io_error)?;
        file.seek(SeekFrom::Start(directory_end(
            self.max_records_count_per_fragments,
        )))
        .map_err(io_error)?;
        let mut slots = Vec::with_capacity(items.len());
        for item in items {
            let bytes = self.encode(item)?;
            let position = file.stream_position().map_err(io_error)?;
            file.write_all(&bytes).map_err(io_error)?;
            slots.push(Some(Slot {
                target: item.target,
                position,
                size: bytes.len() as u32,
            }));
        }
        let mut header = self.empty_header();
        header.records_count = items.len() as u32;
        if let (Some(first), Some(last)) = (items.first(), items.last()) {
            header.min_value = first.value.clone();
            header.max_value = last.value.clone();
            header.min_value_size = first.size;
            header.max_value_size = last.size;
        }
        let fragment = Fragment {
            slots,
            header,
            sorted: true,
        };
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))
            .map_err(io_error)?;
        let directory: Vec<_> = fragment
            .slots
            .iter()
            .flat_map(|slot| slot_bytes(*slot))
            .collect();
        file.write_all(&directory).map_err(io_error)?;
        write_file_header(file, &fragment)?;
        file.sync_all().map_err(io_error)?;
        prepared.fragment = Some(fragment);
        Ok(prepared)
    }

    fn install_prepared(
        &mut self,
        num: usize,
        prepared: &mut PreparedFragment<T>,
    ) -> Result<(), String> {
        fs::rename(&prepared.path, self.fragment_path(num)).map_err(io_error)?;
        let file = prepared.file.take().unwrap();
        let fragment = prepared.fragment.take().unwrap();
        if num == self.fragment_count() {
            self.write_handles.push(file);
            self.fragments.push(fragment);
        } else {
            self.write_handles[num] = file;
            self.fragments[num] = fragment;
        }
        Ok(())
    }

    fn compact_fragments(&mut self, numbers: &[usize]) -> Result<(), String> {
        if numbers.is_empty() {
            return Ok(());
        }
        let mut items = Vec::new();
        for &num in numbers {
            items.extend(self.read_fragment(num)?);
        }
        items.sort_by(compare);
        let capacity = self.max_records_count_per_fragments as usize;
        let mut prepared = Vec::with_capacity(numbers.len());
        for (i, &num) in numbers.iter().enumerate() {
            let start = (i * capacity).min(items.len());
            let end = (start + capacity).min(items.len());
            prepared.push((num, self.prepare_fragment(num, &items[start..end])?));
        }
        // All encoding and temporary writes succeed before replacing any original.
        for (num, fragment) in &mut prepared {
            self.install_prepared(*num, fragment)?;
        }
        // Keep empty interior files for reuse, but remove empty trailing files.
        while self
            .fragments
            .last()
            .is_some_and(|f| f.header.records_count == 0)
        {
            let num = self.fragment_count() - 1;
            // Handles use Rust's normal sharing flags, including delete sharing on Windows.
            fs::remove_file(self.fragment_path(num)).map_err(io_error)?;
            self.write_handles.pop();
            self.fragments.pop();
        }
        Ok(())
    }

    /// Repack all fragments and reclaim obsolete payloads. Uses memory for all entries.
    pub fn compact(&mut self) -> Result<(), String> {
        let numbers: Vec<_> = (0..self.fragment_count()).collect();
        self.compact_fragments(&numbers)
    }

    /// Return a globally sorted view, including entries in overlapping fragments.
    pub fn all(&mut self) -> Result<Vec<FenseIndex<T>>, String> {
        let mut items = Vec::new();
        for num in 0..self.fragment_count() {
            items.extend(self.read_fragment(num)?);
        }
        items.sort_by(compare);
        Ok(items)
    }

    pub fn find(&mut self, value: &T) -> Result<Vec<FenseIndex<T>>, String> {
        self.range(value, value)
    }

    /// Inclusive bounds. Results are sorted by (value, target), preserving duplicates.
    pub fn range(&mut self, min: &T, max: &T) -> Result<Vec<FenseIndex<T>>, String> {
        if min > max {
            return Err("Range minimum exceeds maximum".into());
        }
        let mut result = Vec::new();
        for num in 0..self.fragment_count() {
            let h = &self.fragments[num].header;
            if h.records_count == 0 || &h.max_value < min || &h.min_value > max {
                continue;
            }
            self.ensure_sorted(num)?;
            let mut lo = 0;
            let mut hi = self.fragments[num].slots.len();
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let item = self
                    .read_offset(num, mid as u32)?
                    .ok_or("Hole in sorted directory")?;
                if &item.value < min {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            for offset in lo..self.fragments[num].slots.len() {
                let item = self
                    .read_offset(num, offset as u32)?
                    .ok_or("Hole in sorted directory")?;
                if &item.value > max {
                    break;
                }
                result.push(item);
            }
        }
        result.sort_by(compare);
        Ok(result)
    }

    /// Sync current files. Splits and compaction are not crash-atomic across files.
    pub fn flush(&self) -> Result<(), String> {
        for file in &self.write_handles {
            file.sync_all().map_err(io_error)?;
        }
        Ok(())
    }
}

pub struct SortedIndexTableFragment<'a, T: Ord + Clone + BinarySizeable> {
    pub files: &'a mut SortedIndexFiles<T>,
}

#[derive(PartialEq, Eq, Debug)]
pub enum FileNumberAssignment {
    Specific(usize),
    NextAvailable,
    Split(usize),
}

impl<'a, T: Ord + Clone + BinarySizeable> SortedIndexTableFragment<'a, T> {
    pub fn new(files: &'a mut SortedIndexFiles<T>) -> Self {
        Self { files }
    }
    pub fn get_index_file_num_for_store(
        &mut self,
        ix: &FenseIndex<T>,
    ) -> Result<FileNumberAssignment, String> {
        self.files.assignment(ix)
    }
    pub fn insert(&mut self, ix: FenseIndex<T>) -> Result<(), String> {
        self.files.insert(ix)
    }
}

pub fn pad_or_truncate_string(s: String, pad: char, len: usize) -> String {
    let mut result: String = s.chars().take(len).collect();
    result.extend(std::iter::repeat_n(pad, len - result.chars().count()));
    result
}

fn read_value_bytes(size: u32, file: &mut Box<File>) -> Result<Vec<u8>, String> {
    let available = file
        .metadata()
        .map_err(io_error)?
        .len()
        .checked_sub(file.stream_position().map_err(io_error)?)
        .ok_or("Invalid file position")?;
    if u64::from(size) > available {
        return Err("Truncated index value".into());
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size as usize)
        .map_err(|e| e.to_string())?;
    bytes.resize(size as usize, 0);
    file.read_exact(&mut bytes).map_err(io_error)?;
    Ok(bytes)
}

pub fn default_string_writer() -> ValueWriter<String> {
    Box::new(|value| Ok(Bytes::from(value)))
}
pub fn default_string_reader() -> ValueReader<String> {
    Box::new(|size, file| {
        String::from_utf8(read_value_bytes(size, file)?).map_err(|e| e.to_string())
    })
}
pub fn default_u32_writer() -> ValueWriter<u32> {
    Box::new(|value| Ok(Bytes::copy_from_slice(&value.to_be_bytes())))
}
pub fn default_u32_reader() -> ValueReader<u32> {
    Box::new(|size, file| {
        if size != 4 {
            return Err("Invalid u32 size".into());
        }
        let bytes = read_value_bytes(size, file)?;
        Ok(u32::from_be_bytes(
            bytes.try_into().map_err(|_| "Invalid u32 bytes")?,
        ))
    })
}
pub fn default_u64_writer() -> ValueWriter<u64> {
    Box::new(|value| Ok(Bytes::copy_from_slice(&value.to_be_bytes())))
}
pub fn default_u64_reader() -> ValueReader<u64> {
    Box::new(|size, file| {
        if size != 8 {
            return Err("Invalid u64 size".into());
        }
        let bytes = read_value_bytes(size, file)?;
        Ok(u64::from_be_bytes(
            bytes.try_into().map_err(|_| "Invalid u64 bytes")?,
        ))
    })
}

#[cfg(test)]
#[path = "sorted_index_table_tests.rs"]
mod tests;
