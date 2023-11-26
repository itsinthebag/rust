use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::{fs, io};
use std::fs::{create_dir_all, File, OpenOptions, read, read_dir, read_to_string};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use serde_json::Deserializer;
use crate::{KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// kv store: myDB
pub struct KvStore {
    path: PathBuf,
    // gen number to log file reader
    readers: HashMap<u64, BuffReaderWithPos<File>>,
    // writer of the current log file
    writer: BuffWriterWithPos<File>,
    index: BTreeMap<String, CommandPos>,
    current_gen: u64,
    uncompacted: u64,
}

impl KvStore {
    /// open directory [path]
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        create_dir_all(&path)?;
        let gen_list = sorted_gen_list(&path)?;
        let mut readers: HashMap<u64, BuffReaderWithPos<File>> = HashMap::new();
        let mut uncompacted = 0u64;
        let mut index: BTreeMap<String, CommandPos> = BTreeMap::new();

        for &gen in &gen_list {
            let mut reader = BuffReaderWithPos::new(File::open(log_file_path(&path, gen))?)?;
            uncompacted += load_log_file(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen, &mut readers)?;

        Ok(KvStore{
            path,
            readers,
            writer,
            index,
            current_gen,
            uncompacted,
        })
    }

    /// set k/v pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {key, value};
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Command::Set {key, ..} = cmd {
            if let Some(old_cmd) = self
                .index
                .insert(key, (self.current_gen, pos..self.writer.pos).into()) {
                self.uncompacted += old_cmd.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// retrieve value from key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self.readers.get_mut(&cmd_pos.gen).expect("Can not find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let content = reader.take(cmd_pos.len);
            if let Command::Set {value, ..} = serde_json::from_reader(content)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// remove k/v pair
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Remove {key};
            serde_json::to_writer(&mut self.writer, &cmd)?;

            if let Command::Remove {key} = cmd {
               let old_cmd = self.index.remove(&key).expect("Key does not exist");
                self.uncompacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// release reset entry
    pub fn compact(&mut self) -> Result<()> {
        self.writer = self.new_log_file(self.current_gen + 2)?;
        let compact_gen = self.current_gen + 1;
        let mut compact_writer= self.new_log_file(compact_gen)?;
        self.current_gen += 2;

        // copy from old file to new file
        let mut compact_pos = 0u64;
        for cmd_pos in self.index.values_mut() {
            let reader = self.readers.get_mut(&cmd_pos.gen).expect("log file reader does not exist");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }
            let mut reader_take = reader.take(cmd_pos.len);
            let len = io::copy(&mut reader_take, &mut compact_writer)?;
            *cmd_pos = (compact_gen, (compact_pos..compact_pos+len)).into();
            compact_pos += len;
        }
        compact_writer.flush()?;

        let stale_gen: Vec<u64> = self.readers.keys().filter(|it| **it < compact_gen).cloned().collect();
        // remove stale files and entry
        for gen in stale_gen {
            self.readers.remove(&gen);
            fs::remove_file(log_file_path(&self.path, gen))?;
        }

        self.uncompacted = 0;
        Ok(())
    }

    /// Create a new log file with given generation number and add the reader to the readers map.
    ///
    /// Returns the writer to the log.
    fn new_log_file(&mut self, gen: u64) -> Result<BuffWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.readers)
    }
}

/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(path: &PathBuf, gen: u64, readers: &mut HashMap<u64, BuffReaderWithPos<File>>) -> Result<BuffWriterWithPos<File>> {
    let path = log_file_path(path, gen);
    let writer = BuffWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    );

    readers.insert(gen, BuffReaderWithPos::new(File::open(path)?)?);
    writer
}

/// load single log file, store values location in index map and return uncompatted bytes
fn load_log_file(gen: u64, reader: &mut BuffReaderWithPos<File>, index: &mut BTreeMap<String, CommandPos>) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut uncompacted = 0u64;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set {key, ..} => {
                if let Some(old_cmd) = index.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }

            Command::Remove {key, ..} => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
            }
        }
        pos = new_pos;
    }

    Ok(uncompacted)
}

fn log_file_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// create sorted list of generated log file number
fn sorted_gen_list(path: &PathBuf) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = read_dir(&path)?
        .flat_map(|it| -> Result<_> { Ok(it?.path())})
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
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
        self.pos = self.reader.seek(pos)?;
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

// represent position and length of json-serialized command in log file
struct CommandPos {
    gen: u64, // log file number
    pos: u64, // seek position in log file
    len: u64, // length to read after seek position
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}