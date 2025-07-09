#![allow(unused)]

use pack1::{U16LE, U32LE, U64LE};
use watto::Pod;

pub const PART_MAGIC: [u8; 2] = *b"\xf5P";
pub const FILE_MAGIC: [u8; 2] = *b"\xf5F";
pub const PART_VERSION: u16 = 1;
pub const FILE_VERSION: u16 = 1;

#[derive(Debug)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Zstd = 1,
}

/// Part metadata, like compression and size.
#[derive(Debug)]
#[repr(C)]
pub struct Part {
    pub magic: [u8; 2],
    pub version: U16LE,
    pub part_size: U32LE,
    pub compression_algorithm: u8,
    pub _padding: [u8; 3],
    pub compressed_size: U32LE,
}
unsafe impl Pod for Part {}

/// File metadata, like how many parts (if any) it consists of.
#[derive(Debug)]
#[repr(C)]
pub struct File {
    pub magic: [u8; 2],
    pub version: U16LE,
    pub num_parts: U32LE,
    pub file_size: U64LE,
}
unsafe impl Pod for File {}

/// The (uncompressed) size and UUID of the file part.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct FilePart {
    pub part_size: U32LE,
    pub part_uuid: [u8; 16],
}
unsafe impl Pod for FilePart {}
