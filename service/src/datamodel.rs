#![allow(unused)]

use uuid::Uuid;

#[derive(Clone)]
pub struct StorageId {
    pub id: Uuid,
}

#[repr(u8)]
pub enum Compression {
    None,
    Zstd,
}

pub struct Blob {
    pub blob_size: u64,
    pub parts: Vec<BlobPart>,
}

pub struct BlobPart {
    // Part Metadata:
    pub compression: Compression,
    pub part_size: u32,
    pub compressed_size: u32,

    // Location of the Part
    pub segment_id: Uuid,
    pub segment_offset: u32,
}

#[repr(u8)]
pub enum StorageLocation {
    /// Segment stored/cached on local disk
    Local,
    /// Segment stored in remote object storage
    Remote,
}

pub struct Segment {
    pub location: StorageLocation,
}
