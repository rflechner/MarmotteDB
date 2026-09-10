use std::sync::atomic::AtomicU64;

pub const MAGIC: &[u8; 8] = b"MRMTIX02";
pub const HEADER_SIZE: usize = 24;
pub const SLOT_SIZE: usize = 21; // active:u8, target:u64, payload_offset:u64, size:u32
pub static TEMP_ID: AtomicU64 = AtomicU64::new(0);


/// Exact number of bytes produced by the corresponding ValueWriter.
pub trait BinarySizeable {
    fn get_binary_size(&self) -> usize;
}

impl BinarySizeable for String {
    fn get_binary_size(&self) -> usize {
        self.len()
    }
}
impl BinarySizeable for u32 {
    fn get_binary_size(&self) -> usize {
        4
    }
}
impl BinarySizeable for u64 {
    fn get_binary_size(&self) -> usize {
        8
    }
}

impl BinarySizeable for bool {
    fn get_binary_size(&self) -> usize {
        1
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenseIndex<T: Ord + BinarySizeable> {
    pub active: bool,
    pub target: u64,
    pub value: T,
    pub size: usize,
}

impl<T: Ord + BinarySizeable> FenseIndex<T> {
    /// Kept for existing callers; insertion validates the supplied size.
    pub fn new(target: u64, value: T, size: usize) -> Self {
        Self {
            active: true,
            target,
            value,
            size,
        }
    }

    pub fn from_value(target: u64, value: T) -> Self {
        let size = value.get_binary_size();
        Self::new(target, value, size)
    }

    pub fn get_prefix_binary_size() -> usize {
        SLOT_SIZE
    }
}
