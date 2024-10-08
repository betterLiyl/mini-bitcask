use fs4::FileExt;
use std::{
    io::{BufReader,BufWriter,Read,Seek,SeekFrom,Write},
    path::PathBuf,
};
use crate::bitcask::{KEY_VAL_HEADER_LEN, KeyDir,Result};

pub struct Log {
    pub path: PathBuf,
    pub file: std::fs::File,
}

impl Log {
    pub fn new(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // 加 exclusive lock 防止并发更新
        file.try_lock_exclusive()?;

        Ok(Self { path, file })
    }

    // 构建内存索引
    pub fn load_index(&mut self) -> Result<KeyDir> {
        let mut len_buf = [0u8; KEY_VAL_HEADER_LEN as usize];
        let mut keydir = KeyDir::new();
        let file_len = self.file.metadata()?.len();
        let mut r = BufReader::new(&mut self.file);
        let mut pos: u64 = r.seek(SeekFrom::Start(0))?;

        while pos < file_len {
            let read_one = || -> Result<(Vec<u8>, u64, Option<u32>)> {
                // 读取 key 的长度
                r.read_exact(&mut len_buf)?;
                let key_len = u32::from_be_bytes(len_buf);
                // 读取 value 的长度
                r.read_exact(&mut len_buf)?;
                let value_lent_or_tombstone = match i32::from_be_bytes(len_buf) {
                    l if l >= 0 => Some(l as u32),
                    _ => None,
                };

                // value 的位置
                let value_pos = pos + KEY_VAL_HEADER_LEN as u64 * 2 + key_len as u64;

                // 读取 key 的内容
                let mut key = vec![0; key_len as usize];
                r.read_exact(&mut key)?;

                // 跳过 value 的长度
                if let Some(value_len) = value_lent_or_tombstone {
                    r.seek_relative(value_len as i64)?;
                }

                Ok((key, value_pos, value_lent_or_tombstone))
            }();

            match read_one {
                Ok((key, value_pos, Some(value_len))) => {
                    keydir.insert(key, (value_pos, value_len));
                    pos = value_pos + value_len as u64;
                }
                Ok((key, value_pos, None)) => {
                    keydir.remove(&key);
                    pos = value_pos;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(keydir)
    }

    // 根据 value 的位置和长度获取 value 的值
    pub fn read_value(&mut self, value_pos: u64, value_len: u32) -> Result<Vec<u8>> {
        let mut value = vec![0; value_len as usize];
        self.file.seek(SeekFrom::Start(value_pos))?;
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    // +-------------+-------------+----------------+----------------+
    // | key len(4)    val len(4)     key(varint)       val(varint)  |
    // +-------------+-------------+----------------+----------------+
    pub fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u32)> {
        let key_len = key.len() as u32;
        let value_len = value.map_or(0, |v| v.len() as u32);
        let value_len_or_tomestone = value.map_or(-1, |v| v.len() as i32);

        // 总共占据的长度
        let len = KEY_VAL_HEADER_LEN * 2 + key_len + value_len;

        let offset = self.file.seek(SeekFrom::End(0))?;
        let mut w = BufWriter::with_capacity(len as usize, &mut self.file);
        w.write_all(&key_len.to_be_bytes())?;
        w.write_all(&value_len_or_tomestone.to_be_bytes())?;
        w.write_all(key)?;
        if let Some(value) = value {
            w.write_all(value)?;
        }
        w.flush()?;

        Ok((offset, len))
    }
}