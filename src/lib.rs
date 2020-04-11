#[macro_use] extern crate failure;

use std::path::{PathBuf, Path};
use serde::{Serialize, Deserialize};
use serde_json::Deserializer;
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::{SeekFrom, BufReader};
use std::fs::{OpenOptions, File, read_dir};
pub use err::Result;

pub mod err;

const SINGLE_LOG_SIZE: usize = 1024*1024; // 1M
const COMPACT_THRESHOLD: u64 = 1024*1024; // 1M
static NOT_COMMIT_FILE: &str = "not_commit.dat";

pub struct KvStore {
    readers: HashMap<u64, BufReader<File>>,
    writer: BufWriter,
    index: HashMap<String, Pointer>,
    fid: u64,
    file_path: PathBuf,
    rubbish: u64,
}

struct BufWriter {
    inner: std::io::BufWriter<File>,
    pos: usize,
}

impl BufWriter {
    fn new(inner: File) -> Self {
        Self {
            inner: std::io::BufWriter::new(inner),
            pos:0,
        }
    }
}

impl Write for BufWriter{
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(data)?;
        self.pos += n;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct Pointer {
    fid: u64,
    start: u64,
    len: u64,
}

impl Pointer {
    fn new(fid: u64, start: u64, len: u64) -> Self {
        Self{
            fid, start, len,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Operation {
    Set(String, String),
    Rm(String),
}


impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let file_path = path.into();
        std::fs::create_dir_all(file_path.clone())?;
        let recover_file = file_path.join(NOT_COMMIT_FILE);
        if std::fs::metadata(recover_file.as_path()).is_ok() {
            return Self::recover_from_crash(file_path);
        }
        let mut readers = HashMap::new();
        let file_ids = Self::sort_log(file_path.as_path())?;
        let mut index = HashMap::new();
        let mut rubbish = 0;
        for &file_id in &file_ids {
            let mut fd = BufReader::new(
                OpenOptions::new().read(true).open(
                    file_path.join(format!("{}.log", file_id))
                )?
            );
            rubbish += Self::load_data(file_id, &mut fd, &mut index)?;
            readers.insert(file_id, fd);
        }

        let last_id = *file_ids.last().unwrap_or(&0) + 1;
        let mut writer = Self::new_log_file(last_id, file_path.as_path(), &mut readers)?;
        Ok(Self{readers, writer, index, fid: last_id, file_path, rubbish})
    }

    fn recover_from_crash(path: PathBuf) -> Result<Self> {
        let f_path = path.join(NOT_COMMIT_FILE);
        let mut reader = BufReader::new(OpenOptions::new().read(true).open(f_path)?);
        let mut index = HashMap::new();
        let rubbish = Self::load_data(1, &mut reader, &mut index)?;
        let mut readers = HashMap::new();
        readers.insert(1, reader);
        let fid = 2;
        let mut writer = Self::new_log_file(fid, path.as_path(), &mut readers)?;
        Ok(Self{readers, writer, file_path: path, fid, index, rubbish})
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::Set(key, value);
        let cur = self.writer.pos;
        let data = serde_json::to_string(&op)?;
        let len = self.writer.write(data.as_bytes())?;
        self.writer.flush()?;
        if let Operation::Set(key, _) = op {
            if let Some(old) = self.index.insert(key, Pointer::new(self.fid, cur as u64, len as u64)) {
                self.rubbish += old.len;
            }
        }
        if self.rubbish >= COMPACT_THRESHOLD {
            self.compact()?;
        } else if cur >= SINGLE_LOG_SIZE {
            self.fid += 1;
            let w = Self::new_log_file(self.fid, self.file_path.as_path(), &mut self.readers)?;
            self.writer = w;
        }
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(&Pointer{fid, start, len}) = self.index.get(&key) {
            if let Some(r) = self.readers.get_mut(&fid) {
                r.seek(SeekFrom::Start(start))?;
                let mut t = r.take(len);
                if let Operation::Set(_, value) = serde_json::from_reader(t)? {
                    Ok(Some(value))
                } else {
                    Err(err::KvError::UnKnownCommand)
                }
            } else {
                Err(err::KvError::KeyNotFound)
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(Pointer{len,..}) = self.index.remove(&key) {
            self.rubbish += len;
            let op = Operation::Rm(key);
            let s = serde_json::to_string(&op)?;
            let n = self.writer.write(s.as_bytes())?;
            self.rubbish += n as u64;
            Ok(())
        } else {
            Err(err::KvError::KeyNotFound)
        }
    }

    fn compact(&mut self) -> Result<()> {
        // the easiest way of compacting is to rewrite all the live data to a new log file
        // but there's a problem, which log we should write to?
        // if we indefinitely increase the file number, u64 will one day overflow.
        // so, just remove all the log files and rewrite the live data to 1.log
        // but if the machine shutdown unexpectedly after we delete all the log file
        // while we haven't stored the live data on the disk, we'll lose all the data!!
        // It's unacceptable!!
        // So before deleting the old log files, we must write the live data to another file
        // say, not_commit.dat
        // then we delete old log files, then rename not_commit.dat to 1.log

        // rebuild the kv in memory
        let mut temp_index = HashMap::new();
        let keys: Vec<String> = self.index.keys().cloned().collect();
        for key in keys {
            if let Some(value) = self.get(key.clone())? {
                temp_index.insert(key, value);
            }
        }

        // write the kv data in not_commit.dat
        let not_commit_compact_file = self.file_path.join(NOT_COMMIT_FILE);
        let compact_fd = OpenOptions::new().create(true).append(true).open(not_commit_compact_file.clone())?;
        let mut compact_writer = BufWriter::new(compact_fd);
        let mut pos = 0;
        self.index = HashMap::new();
        for (k,v) in temp_index {
            let op = Operation::Set(k,v);
            let s = serde_json::to_string(&op)?;
            let n = compact_writer.write(s.as_bytes())?;
            if let Operation::Set(key, _) = op {
                self.index.insert(key, Pointer{fid: 1, start: pos, len: n as u64});
            }
            pos += n as u64;
        }
        compact_writer.flush()?;

        // delete the old log files
        for &fid in self.readers.keys() {
            std::fs::remove_file(self.file_path.join(format!("{}.log", fid)))?;
        }

        // rename not_commit.dat to 1.log
        let commit_compact_file = self.file_path.join(format!("{}.log", 1));
        std::fs::rename(not_commit_compact_file, commit_compact_file.clone())?;
        let compact_fd = OpenOptions::new().read(true).open(commit_compact_file)?;
        self.readers = HashMap::new();
        self.readers.insert(1, BufReader::new(compact_fd));
        let mut writer = Self::new_log_file(2, self.file_path.as_path(), &mut self.readers)?;
        self.writer = writer;
        self.fid = 2;
        self.rubbish = 0;

        Ok(())
    }

    fn new_log_file(fid: u64, path: &Path, readers: &mut HashMap<u64, BufReader<File>>) -> Result<BufWriter> {
        let p = path.join(format!("{}.log", fid));
        let mut fd = OpenOptions::new()
            .create(true).append(true).open(p.clone())?;
        let mut writer = BufWriter::new(fd);
        let mut r = OpenOptions::new().read(true).open(p)?;
        readers.insert(fid, BufReader::new(r));
        Ok(writer)
    }

    pub fn sort_log(path: &Path) -> Result<Vec<u64>> {
        let mut file_ids: Vec<u64> = read_dir(path)?
            .flat_map(|dir| -> Result<_> {
                Ok(dir?.path())
            })
            .filter(|p|
                // there's a problem, if I use p.ends_with(".log")
                p.is_file() && p.extension() == Some("log".as_ref())
            )
            .flat_map(|p|
                p.file_stem()
                .and_then(|name| name.to_str())
                .map(str::parse::<u64>)
            )
            .flatten()
            .collect();
        file_ids.sort();
        Ok(file_ids)
    }

    // I don't like JSON for this case because it's very inefficient.
    // Nobody need watch the log file themselves
    // But the serde_json crate is convenient, because it can handle the byte stream correctly
    // without marking some isolation flags.
    // If we don't use serde_json, we have to store the index in disk,
    // only by doing that can we rebuild the data
    fn load_data(fid: u64, r: &mut BufReader<File>, index: &mut HashMap<String, Pointer>) -> Result<u64> {
        let mut pos = r.seek(SeekFrom::Start(0))?;
        let mut stream = Deserializer::from_reader(r).into_iter::<Operation>();
        let mut acc = 0;
        while let Some(v) = stream.next() {
            let cursor =  stream.byte_offset();
            let op = v?;
            match op {
                Operation::Set(key, _) => {
                    if let Some(old) = index.insert(key, Pointer::new(fid, pos, cursor as u64-pos)) {
                        acc += old.len;
                    }
                },
                Operation::Rm(key) => {
                    if let Some(old) = index.remove(&key) {
                        acc += old.len;
                    }
                    acc += cursor as u64 - pos;
                },
            }
            pos = cursor as u64;
        }
        Ok(acc)
    }
}