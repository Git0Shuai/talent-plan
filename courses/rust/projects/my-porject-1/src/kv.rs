use std::collections::HashMap;

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
    data: HashMap<String, String>,
}

impl KvStore {
    /// Create a new KvStore
    pub fn new() -> KvStore {
        KvStore {
            data: HashMap::new(),
        }
    }

    /// Set value with key. If the key already exist, update the value
    /// otherwise, insert this key with the value
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    /// Get value with key. If the key is not present, return None
    pub fn get(&self, key: String) -> Option<String> {
        self.data.get(&key).map(|it| it.to_owned())
    }

    /// Remove the key
    pub fn remove(&mut self, key: String) {
        self.data.remove(&key);
    }
}
