mod error;

use std::collections::HashMap;
use std::path::PathBuf;
pub use error::{KvsError, Result};

#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<String> {
        let tmp = self.map.insert(key, value);
        Ok(tmp.map(|c| c).unwrap_or_default())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        let res = self.map.get(&key);
        if res == None {
            return Err(KvsError::KeyNotFound);
        }
        Ok(Some(res.unwrap().to_owned()))
    }

    pub fn remove(&mut self, key: String) -> Result<String> {
        self.map.remove(&key).ok_or(KvsError::KeyNotFound)
    }

    pub fn open(_: impl Into<PathBuf>) -> Result<KvStore> {
        Ok(KvStore::new())
    }
}
