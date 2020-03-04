use crate::pool::LOGS;
use crate::storage::Storage;
use crate::ThreadPool;
use rayon::{self, ThreadPoolBuildError};
use std::sync::{Arc, Mutex};
type Builder = rayon::ThreadPoolBuilder;

/// We rewrite ThreadPoolBuilders since we need to overload the start handler
/// in order to give each thread a place to write its logs.
#[derive(Default)]
pub struct ThreadPoolBuilder {
    builder: Builder,
}

impl ThreadPoolBuilder {
    /// Creates a new ThreadPoolBuilder.
    pub fn new() -> Self {
        ThreadPoolBuilder {
            builder: Builder::new(),
        }
    }

    /// Set number of threads wanted.
    pub fn num_threads(self, threads_number: usize) -> Self {
        ThreadPoolBuilder {
            builder: self.builder.num_threads(threads_number),
        }
    }
    
    /// Set a steal function, see https://github.com/ma1ko/rayon/commit/78c551f105badc590a1ae6be7cddb3094663f4eb
    pub fn steal_callback<H>(mut self, steal_callback: H) -> Self
    where
        H: Fn(usize) -> Option<()> + Send + Sync + 'static,
    {
        self.builder = self.builder.steal_callback(steal_callback);
        self
    }

    /// Build the `ThreadPool`.
    pub fn build(self) -> Result<ThreadPool, ThreadPoolBuildError> {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let shared_logs = logs.clone();
        let pool = self
            .builder
            .start_handler(move |_| {
                LOGS.with(|l| {
                    let thread_storage = Arc::new(Storage::new());
                    shared_logs.lock().unwrap().push(thread_storage.clone());
                    *l.borrow_mut() = thread_storage;
                });
            })
            .build();

        pool.map(|p| ThreadPool { pool: p, logs })
    }
}
