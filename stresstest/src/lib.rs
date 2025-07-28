//! This is a stresstest library which can run different [`Workload`]s against
//! a backend storage service.
//!
//! The [`Workload`] currently supports configuring a *LogNormal* distribution
//! of file sizes, defined by the `p50` and `p99` of file sizes.
//! This models our real-world distribution of many small files, with a long
//! tail of larger files as well.
//!
//! It also allows configuring a distribution of actions, such as write, read or delete.
//! The goal is that we have a lot of *write-heavy* workloads which only write
//! blobs, but never read those.
//!
//! *Read* or *delete* actions are using a *zipfian* distribution, meaning that
//! more recently written blobs are the ones that will be read/deleted.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

pub mod http;
pub mod stresstest;
pub mod workload;

pub use crate::stresstest::run;
pub use crate::workload::Workload;
