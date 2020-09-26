#![deny(missing_docs)]
//! an String to String Key-Value Stroe

mod error;
mod kv;

pub use error::Error;
pub use error::Result;
pub use kv::KvStore;
