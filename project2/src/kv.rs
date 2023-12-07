use std::{fs, io};
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{KvsError, Result};
use crate::KvsError::KeyNotFound;

///
pub struct KvStore {
    path: PathBuf,
    readers: HashMap<u64, BufReaderWithPos<File>>,
    writer: BufWriterWithPos<File>,
    index: BTreeMap<String, CommandPos>,
    current_gen: u64,
    uncompacted: u64,
}

impl KvStore {
    /// open
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let gen_list = sorted_gen_list(&path)?;
        let mut index: BTreeMap<String, CommandPos> = BTreeMap::new();
        let mut uncompacted = 0u64;
        let mut readers: HashMap<u64, BufReaderWithPos<File>> = HashMap::new();

        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(&mut reader, &mut index, gen)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen, &mut readers)?;
        Ok(KvStore {
            path,
            readers,
            writer,
            index,
            current_gen,
            uncompacted,
        })
    }

    /// set
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key, value };
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Command::Set { key, .. } = cmd {
            if let Some(cmd_pos) = self
                .index
                .insert(key, (self.current_gen, pos..self.writer.pos).into())
            {
                self.uncompacted += cmd_pos.len;
            }
        }

        if self.uncompacted > 1024 * 1024 {
            self.compact()?;
        }
        Ok(())
    }

    /// get
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd) = self.index.get(&key) {
            let reader = self.readers.get_mut(&cmd.gen).expect("asdf");
            if reader.pos != cmd.pos {
                reader.seek(SeekFrom::Start(cmd.pos))?;
            }

            let taker = reader.take(cmd.len);
            if let Command::Set { value, .. } = serde_json::from_reader(taker)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// remove
    pub fn remove(&mut self, key: String) -> Result<()> {
        let copy = key.clone();
        if self.index.contains_key(&key) {
            let cmd = Command::Remove { key };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;

            if let Some(cmd_pos) = self.index.remove(&copy) {
                self.uncompacted += cmd_pos.len;
            }
            Ok(())
        } else {
            Err(KeyNotFound)
        }
    }

    /// compact
    pub fn compact(&mut self) -> Result<()> {
        let compact_gen = self.current_gen + 1;
        let mut compact_writer = self.new_log_file(compact_gen)?;
        let mut compact_pos = 0;

        self.current_gen += 2;
        self.writer = self.new_log_file(self.current_gen)?;

        for cmd_pos in self.index.values_mut() {
            let reader = self.readers.get_mut(&cmd_pos.gen).expect("asdf");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }
            let mut taker = reader.take(cmd_pos.len);
            let len = io::copy(&mut taker, &mut compact_writer)?;
            *cmd_pos = (compact_gen, compact_pos..compact_pos + len).into();
            compact_pos += len;
        }

        let stale_gen: Vec<_> = self
            .readers
            .keys()
            .filter(|gen| **gen < compact_gen)
            .cloned()
            .collect();

        for gen in stale_gen {
            self.readers.remove(&gen);
            fs::remove_file(log_path(&self.path, gen))?;
        }

        compact_writer.flush()?;
        self.uncompacted = 0;
        Ok(())
    }
    /// new log file
    pub fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.readers)
    }
}

fn new_log_file(
    dir: &PathBuf,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(dir, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(gen, BufReaderWithPos::new(File::open(path)?)?);
    Ok(writer)
}

fn load(
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
    gen: u64,
) -> Result<u64> {
    let mut uncompacted = 0u64;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut pos = 0u64;

    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd_pos) = index.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_cmd_pos.len;
                }
            }

            Command::Remove { key } => {
                if let Some(old_cmd_pos) = index.remove(&key) {
                    uncompacted += old_cmd_pos.len;
                }

                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }

    Ok(uncompacted)
}

fn log_path(dir: &PathBuf, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn sorted_gen_list(dir: &PathBuf) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&dir)?
        .flat_map(|path| -> Result<_> { Ok(path?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|path| path.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    gen_list.sort_unstable();
    Ok(gen_list)
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
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

pub struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<BufReaderWithPos<R>> {
        let pos = inner.seek(SeekFrom::Start(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}
impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

pub struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<BufWriterWithPos<W>> {
        let pos = inner.seek(SeekFrom::Start(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
