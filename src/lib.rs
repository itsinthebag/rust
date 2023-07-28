use std::collections::HashMap;
use std::path::PathBuf;
use std::result;
use thiserror::Error;
use std::io;

pub type Result<T> = result::Result<T, KvsError>;

#[derive(Debug, Error)]
pub enum KvsError{
   /// IO error.
   #[error("IO failure: {0}")]
   Io(#[from] io::Error),
   /// Serialization or deserialization error.
   #[error("serde failure: {0}")]
   Serde(#[from] serde_json::Error),
   /// Removing non-existent key error.
   #[error("Key not found")]
   KeyNotFound,
   /// Unexpected command type error.
   /// It indicated a corrupted log or a program bug.
   #[error("Unexpected command type")]
   UnexpectedCommandType,
}

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
            return Err(Box::new(MyError{err: String::from("map doesn't contain the key")}));
        }
        Ok(Some(res.unwrap().to_owned()))
    }

    pub fn remove(&mut self, key: String) -> Result<String> {
        self.map.remove(&key).ok_or(Box::new(MyError{err: String::from("can't remove from map")}))
    }

    pub fn open(_: PathBuf) -> Result<KvStore> {
        Ok(KvStore::new())
    }
}
