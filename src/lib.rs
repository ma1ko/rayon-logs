//! This crate provides logging facilities to evaluate performances
//! of code parallelized with the rayon parallel computing library.
//! It also doubles down as a debugging tool.
//!
//! Ideally using it should be as easy as adding
//! `extern crate rayon_logs as rayon;`
//! at top of your main file (replacing `extern crate rayon`).
//!
//! However there are currently limitations because we do not
//! currently log all parts of rayon.
//!
//! - the global `ThreadPool` is not logged so it is *required* to use a `ThreadPoolBuilder`.
//! - not all of rayon's traits are implemented. In particular no `IndexedParallelIterator` (no zip),
//! no `FromParallelIterator`  (no  collect)...
//! - `par_sort` is logged but it is not directly rayon's `par_sort` but a copy-pasted version of
//! it (as a demonstration). so the algorithm is hard-coded into rayon_logs.
//! - you should not mix logged and not logged computations.
//! - each call to `ThreadPool::install` generates a json file which can then be converted to svg
//! using `json2svg`.
//! - each log generates an overhead of around 1 micro seconds. This is due to thread_local being
//! very slow.
//!
//! With this being said, here is a small example:
//!
//! Example:
//! ```
//! extern crate rayon_logs as rayon; // comment me out to go back to using rayon
//! use rayon::prelude::*;
//! use rayon::ThreadPoolBuilder;
//! let v = vec![1; 100_000];
//! // let's create a logged pool of threads
//! let pool = ThreadPoolBuilder::new().num_threads(2).build().expect("failed creating pool");
//! // run and log some computations
//! assert_eq!(100_000, pool.install(|| v.par_iter().sum::<u32>()));
//! ```
//!
//! Running this code will create a `log_0.json` file.
//! You can then use `cargo run --bin json2svg -- log_0.json example_sum.svg` to view the log.
//! The resulting file should be viewed in a web browser since it is animated.
//! The bars below the graph represent idle times.
//!
//! <div>
//! <img src="http://www-id.imag.fr/Laboratoire/Membres/Wagner_Frederic/images/downgraded_iter_sum.svg"/>
//! </div>
#![type_length_limit = "2097152"] // it seems we have types with long names
#![deny(missing_docs)]
#![allow(deprecated,dead_code)]
#![warn(clippy::all)]

mod pool; // this comes first because it exports the logs macro

mod iterator;
mod storage;
pub use crate::iterator::Logged;
pub use crate::pool::{
    custom_subgraph, end_subgraph, join, join_context, start_subgraph, subgraph, ThreadPool,
};
#[cfg(feature = "perf")]
pub use crate::pool::{subgraph_cache_event, subgraph_hardware_event, subgraph_software_event};
mod builder;
pub mod prelude;
pub use crate::builder::ThreadPoolBuilder;
mod scope;
pub use crate::scope::{scope, scope_fifo, Scope, ScopeFifo};
mod fork_join_graph;
mod stats;
pub use crate::fork_join_graph::visualisation;
pub(crate) mod compare;
mod log;
pub use crate::log::RunLog;
mod rayon_algorithms;
pub(crate) mod svg;
pub use crate::compare::Comparator;
pub(crate) mod raw_events;
/// We re-export rayon's `current_num_threads`.
pub use rayon::current_num_threads;
pub use rayon::current_thread_index;

/// We reexport perf-related types here.
#[cfg(feature = "perf")]
pub use perfcnt::linux::{
    CacheId, CacheOpId, CacheOpResultId, HardwareEventType, SoftwareEventType,
};
