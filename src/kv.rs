use std::collections::{BTreeMap, HashMap};
use std::fs::{create_dir_all, File, OpenOptions, read, read_to_string};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use crate::{KvsError, Result};

/// kv store: myDB
pub struct KvStore {
    path: PathBuf,
    // gen number to log file reader
    reader: HashMap<u64, BuffReaderWithPos<File>>,
    // writer of the current log file
    writer: BuffWriterWithPos<File>,
    index: BTreeMap<String, CommandPos>,
    current_gen: u64,
    uncompacted: u64,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        todo!()
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        todo!()
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        todo!()
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        todo!()
    }

    pub fn compact() -> Result<()> {
        todo!()
    }

    pub fn new_log_file(&mut self, gen: u64) -> Result<()> {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

struct BuffReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BuffReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self>{
        let pos = inner.seek(SeekFrom::Current(0))?;

        Ok(BuffReaderWithPos{
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BuffReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let inc = self.reader.read(buf)?;
        self.pos += inc as u64;
        Ok(inc)
    }
}

impl<R: Read + Seek> Seek for BuffReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.seek(pos)?;
        Ok(self.pos)
    }
}

struct BuffWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BuffWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BuffWriterWithPos{
            writer: BufWriter::new(inner),
            pos,
        })
    }
}
impl<W: Write + Seek> Write for BuffWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BuffWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let pos = self.writer.seek(pos)?;
        self.pos = pos;
        Ok(self.pos)
    }
}

struct CommandPos {
    gen: u64, // log file number
    pos: u64, // seek position in log file
    len: u64, // length to read after seek position
}