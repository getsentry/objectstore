//! This is a stresstest library which can run different [`Workload`]s against
//! a backend storage service.
//!
//! The [`Workload`] supports two ways of configuring file sizes. A *LogNormal*
//! distribution, defined by the `p50` and `p99` of file sizes, models many
//! small files with a long tail of larger files. A histogram of [`SizeBuckets`]
//! replays a file size distribution that was measured in production.
//!
//! It also allows configuring a distribution of actions, such as write, read or delete.
//! The goal is that we have a lot of *write-heavy* workloads which only write
//! blobs, but never read those.
//!
//! *Read* or *delete* actions are using a *zipfian* distribution, meaning that
//! more recently written blobs are the ones that will be read/deleted.
//!
//! To run a stresstest, instantiate the [`Stresstest`] struct with a remote backend
//! implementation, such as the [`HttpRemote`](http::HttpRemote) and configure it with
//! the desired duration and workload.
//!
//! [`SizeBuckets`]: workload::SizeBuckets
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

pub mod http;
pub mod stresstest;
pub mod workload;

pub use crate::stresstest::Stresstest;
pub use crate::workload::Workload;
