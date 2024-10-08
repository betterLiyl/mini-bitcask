use crate::bitcask::{MERGE_FILE_EXT, KeyDir,Result};
use crate::log::*;
use crate::scan_iter::*;
use std::{
    ops::Bound,
    path::PathBuf,
};
pub struct MiniBitcask{
    pub log:Log,
    pub keydir:KeyDir,
}

impl Drop for MiniBitcask {
    fn drop(&mut self) {
        if let Err(error) = self.flush() {
            log::error!("failed to flush file: {:?}", error)
        }
    }
}

impl MiniBitcask {
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut log = Log::new(path)?;
        let keydir = log.load_index()?;
        Ok(Self { log, keydir })
    }

    pub fn merge(&mut self) -> Result<()> {
        // 创建一个新的临时用于用于写入
        let mut merge_path = self.log.path.clone();
        merge_path.set_extension(MERGE_FILE_EXT);

        let mut new_log = Log::new(merge_path)?;
        let mut new_keydir = KeyDir::new();

        // 重写数据
        for (key, (value_pos, value_len)) in self.keydir.iter() {
            let value = self.log.read_value(*value_pos, *value_len)?;
            let (offset, len) = new_log.write_entry(key, Some(&value))?;
            new_keydir.insert(
                key.clone(),
                (offset + len as u64 - *value_len as u64, *value_len),
            );
        }

        // 重写完成，重命名文件
        std::fs::rename(new_log.path, self.log.path.clone())?;

        new_log.path = self.log.path.clone();
        // 替换现在的
        self.log = new_log;
        self.keydir = new_keydir;

        Ok(())
    }

    //   0-----3------7   8--------------21 22---------------38
    //        4 + 4     +      14 + 17
    // offset 0
    // len 39
    // value_len 17
    //
    //   10-----13------17   18--------------31 32---------------48
    //        4 + 4     +      14 + 17
    // offset 10
    // len 39
    // value_len 17
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let (offset, len) = self.log.write_entry(key, Some(&value))?;
        let value_len = value.len() as u32;
        self.keydir.insert(
            key.to_vec(),
            (offset + len as u64 - value_len as u64, value_len),
        );
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((value_pos, value_len)) = self.keydir.get(key) {
            let val = self.log.read_value(*value_pos, *value_len)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.log.write_entry(key, None)?;
        self.keydir.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(self.log.file.sync_all()?)
    }

    pub fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> ScanIterator<'_> {
        ScanIterator {
            inner: self.keydir.range(range),
            log: &mut self.log,
        }
    }

    pub fn scan_prefix(&mut self, prefix: &[u8]) -> ScanIterator<'_> {
        let start = Bound::Included(prefix.to_vec());

        // 最后一位加一，例如原始前缀是 "aaaa"，变为 "aaab"
        let mut bound_prefix = prefix.to_vec().clone();
        if let Some(last) = bound_prefix.iter_mut().last() {
            *last += 1;
        };
        let end = Bound::Excluded(bound_prefix.to_vec());

        self.scan((start, end))
    }
}
