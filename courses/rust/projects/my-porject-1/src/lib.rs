#![deny(missing_docs)]
//! an String to String Key-Value Stroe

mod error;
mod kv;
mod storages;

pub use error::Error;
pub use error::Result;
pub use kv::KvStore;
