mod error;

pub use error::{KvsError, Result};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, Command>,
}

impl KvStore {
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(
            key.to_owned(),
            Command::Set {
                key: key,
                value: value,
            },
        );
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(Command::Set { value, .. }) = self.map.get(&key) {
            Ok(Some(value.to_string()))
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.map.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        self.map.remove(&key);
        Ok(())
    }

    pub fn open(_: impl Into<PathBuf>) -> Result<KvStore> {
        let kvs = KvStore {
            map: HashMap::new(),
        };
        Ok(kvs)
    }
}

enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
