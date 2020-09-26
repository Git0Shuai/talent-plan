use super::Error;
use super::Result;
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, HashMap};
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;

/// The `KvStore` use HashMap to store an String to String Key-Value pair in memory
///
/// #Example
/// ```
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// assert_eq!(store.get("key".to_owned()), Some("value".to_owned()));
/// ```
pub struct KvStore {
    file: File,
    index: HashMap<String, (u64, usize)>,
}

impl KvStore {
    /// Set value with key. If the key already exist, update the value
    /// otherwise, insert this key with the value
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        if key.is_empty() {
            return Err(Error::InvalidKey(key));
        }
        let record = Record::Set(key, value);
        self.write_record(record)
    }

    /// Get value with key. If the key is not present, return None
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if key.is_empty() {
            return Err(Error::InvalidKey(key));
        }

        if let Some(position) = self.index.get(&key) {
            let length = position.1;
            self.file
                .seek(SeekFrom::Start(position.0 + size_of::<usize>() as u64))?;
            let mut bytes = Vec::with_capacity(length);
            unsafe {
                bytes.set_len(length);
            }
            self.file.read_exact(&mut bytes)?;
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
            let record = Record::Remove(key);
            self.write_record(record)
        } else {
            Err(Error::KeyNotFound(key))
        }
    }

    /// open directory which contains all db files
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<KvStore> {
        let file_path = path.as_ref().join("db");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        let mut index = HashMap::new();

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let total_length = bytes.len();

        let mut cursor = 0u64;
        while cursor < total_length as u64 {
            let length = usize::from_be_bytes(
                (&bytes[cursor as usize..(cursor as usize + size_of::<usize>())]).try_into()?,
            );
            let position = (cursor, length);
            cursor += size_of::<usize>() as u64;

            let json_str =
                String::from_utf8_lossy(&bytes[cursor as usize..(cursor as usize + length)]);
            cursor += length as u64;

            match serde_json::from_str(&json_str)? {
                Record::Remove(key) => {
                    index.remove(&key);
                }
                Record::Set(key, _) => match index.entry(key) {
                    hash_map::Entry::Vacant(v) => {
                        v.insert(position);
                    }
                    hash_map::Entry::Occupied(mut o) => {
                        o.insert(position);
                    }
                },
            }
        }

        Ok(KvStore { file, index })
    }

    fn write_record(&mut self, record: Record) -> Result<()> {
        let key = match &record {
            Record::Set(k, _) => k,
            Record::Remove(k) => k,
        };

        let json_str = serde_json::to_string(&record)?;
        let bytes = json_str.as_bytes();
        let length = bytes.len();
        let offset = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&length.to_be_bytes())?;
        self.file.write_all(&bytes)?;
        match self.index.entry(key.to_owned()) {
            hash_map::Entry::Vacant(v) => *(v.insert((offset, length))),
            hash_map::Entry::Occupied(mut o) => o.insert((offset, length)),
        };
        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.file.flush();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Record {
    Set(String, String),
    Remove(String),
}
