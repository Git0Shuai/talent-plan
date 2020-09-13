pub struct KvStore;

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {}
    }

    #[allow(unused_variables)]
    pub fn set(&mut self, key: String, value: String) {
        unimplemented!("unimplemented")
    }

    #[allow(unused_variables)]
    pub fn get(&self, key: String) -> Option<String> {
        unimplemented!("unimplemented")
    }

    #[allow(unused_variables)]
    pub fn remove(&mut self, key: String) {
        unimplemented!("unimplemented")
    }
}
