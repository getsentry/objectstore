//! This crate just abstracts the automated `protobuf` codegen.
//!
//! It essentially just encapsulates the build steps, and exports generated types.

pub mod storage {
    include!(concat!(env!("OUT_DIR"), "/storage.rs"));
}
