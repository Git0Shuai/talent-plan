use super::Error;
use super::Result;
use crate::storages::{Storage, ValuePosition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tempfile::TempDir;

/// The `KvStore` use HashMap to store an String to String Key-Value pair in memory
///
/// #Example
/// ```
/// use kvs::KvStore;
/// use tempfile::TempDir;
/// let mut store = KvStore::open(TempDir::new().unwrap()).unwrap();
/// store.set("key".to_owned(), "value".to_owned());
/// assert_eq!(store.get("key".to_owned()).unwrap(), Some("value".to_owned()));
/// ```
pub struct KvStore {
    storage: Storage,
    index: HashMap<String, ValuePosition>,
    compact_record_count: u32,
}

impl KvStore {
    /// Set value with key. If the key already exist, update the value
    /// otherwise, insert this key with the value
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidKey(key));
        }
        let record = Record::Set(key.clone(), value);
        let bytes = serde_json::to_vec(&record)?;
        let value_position = self.storage.write_record(&bytes)?;
        self.index.insert(key, value_position);
        if self.storage.record_count() >= self.compact_record_count {
            self.compact()?;
        }
        Ok(())
    }

    /// Get value with key. If the key is not present, return None
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if key.is_empty() {
            return Err(Error::InvalidKey(key));
        }

        if let Some(position) = self.index.get(&key) {
            let bytes = self.storage.read_record(position)?;
            let record = serde_json::from_slice(&bytes)?;
            match record {
                Record::Remove(_) => Ok(None),
                Record::Set(_, value) => Ok(Some(value)),
            }
        } else {
            Ok(None)
        }
    }

    /// Remove the key
    pub fn remove(&mut self, key: String) -> Result<()> {
        if key.is_empty() {
            return Err(super::error::Error::InvalidKey(key));
        }

        if let Some(_) = self.index.get(&key) {
            let record = Record::Remove(key.clone());
            let bytes = serde_json::to_vec(&record)?;
            self.storage.write_record(&bytes)?;
            self.index.remove(&key);

            if self.storage.record_count() >= self.compact_record_count {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(Error::KeyNotFound(key))
        }
    }

    /// open directory which contains all db files
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<KvStore> {
        let mut index = HashMap::new();
        let storage = Storage::init(path, |bytes, position| {
            match serde_json::from_slice(bytes)? {
                Record::Remove(key) => {
                    index.remove(&key);
                }
                Record::Set(key, _) => {
                    index.insert(key, position);
                }
            }
            Ok(())
        })?;

        let compact_record_count = storage.record_count() * 2 + 371;

        Ok(KvStore {
            storage,
            index,
            compact_record_count,
        })
    }

    fn compact(&mut self) -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut storage = Storage::init(&temp_dir, |_, _| Ok(()))?;
        let mut index = HashMap::with_capacity(self.index.capacity());

        for (k, v) in &self.index {
            let p = storage.write_record(&self.storage.read_record(&v)?)?;
            index.insert(k.to_owned(), p);
        }

        let compact_record_count = storage.record_count();

        self.storage.replace(storage)?;
        self.index = index;
        self.compact_record_count = compact_record_count * 2 + 371;

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Record {
    Set(String, String),
    Remove(String),
}
