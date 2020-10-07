use crate::error::Error::Io;
use crate::Result;
use std::convert::TryInto;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::{
    fs,
    fs::{read_dir, File, OpenOptions},
};
use tempfile::TempDir;

pub(crate) struct ValuePosition {
    file: usize,
    offset: u64,
    length: usize,
}

pub(crate) struct Storage {
    path: Box<PathBuf>,
    files: Vec<File>,
    record_count: u32,
}

impl Storage {
    pub const MAX_RECORDS_COUNT_PER_FILE: u32 = 1000;

    pub(crate) fn record_count(&self) -> u32 {
        self.record_count
    }

    pub(crate) fn init<P: AsRef<Path>, F: FnMut(&[u8], ValuePosition) -> Result<()>>(
        path: P,
        mut de: F,
    ) -> Result<Storage> {
        if !path.as_ref().exists() {
            fs::create_dir(&path)?;
        }

        let mut file_path = Vec::new();
        for r in read_dir(&path)? {
            let entry = r?;
            if entry.file_type()?.is_file() && entry.file_name().to_string_lossy().ends_with(".kv")
            {
                file_path.push(entry.path());
            }
        }

        file_path.sort_by_key(|it| it.file_name().unwrap().to_owned());

        let mut files = Vec::new();
        for path in file_path {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            files.push(file);
        }

        if files.len() == 0 {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path.as_ref().join("0.kv"))?;
            files.push(file);
        }

        let mut record_count = 0;

        for (i, mut file) in files.iter().enumerate() {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            let total_length = bytes.len();

            let mut cursor = 0u64;
            let is_last = i == files.len() - 1;
            while cursor < total_length as u64 {
                let length = usize::from_be_bytes(
                    (&bytes[cursor as usize..(cursor as usize + size_of::<usize>())]).try_into()?,
                );
                let position = ValuePosition {
                    file: i,
                    offset: cursor,
                    length,
                };
                cursor += size_of::<usize>() as u64;

                de(
                    &bytes[cursor as usize..(cursor as usize + length)],
                    position,
                )?;

                cursor += length as u64;

                if is_last {
                    record_count += 1;
                }
            }
        }

        Ok(Storage {
            path: Box::new(path.as_ref().to_owned()),
            files,
            record_count,
        })
    }

    /// log an record to disk
    pub(crate) fn write_record(&mut self, bytes: &[u8]) -> Result<ValuePosition> {
        let length = bytes.len();
        let mut file = self.files.last().unwrap();
        let offset = file.seek(SeekFrom::End(0))?;
        file.write_all(&length.to_be_bytes())?;
        file.write_all(bytes)?;

        self.record_count += 1;

        let ret = Ok(ValuePosition {
            file: self.files.len() - 1,
            offset,
            length,
        });

        if self.record_count >= Storage::MAX_RECORDS_COUNT_PER_FILE {
            file.flush()?;
            self.files.push(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(self.path.as_ref().join(format!("{}.kv", self.files.len())))?,
            );
        }

        return ret;
    }

    /// read an Set-Record from disk
    pub(crate) fn read_record(&mut self, value_position: &ValuePosition) -> Result<Vec<u8>> {
        let mut file = self.files.get(value_position.file).unwrap();
        let offset = value_position.offset;
        let length = value_position.length;
        file.seek(SeekFrom::Start(offset + size_of::<usize>() as u64))?;
        let mut bytes = Vec::with_capacity(length);
        unsafe {
            bytes.set_len(length);
        }
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub(crate) fn replace(&mut self, storage: Storage) -> Result<()> {
        let back_up = TempDir::new()?;
        for mut old_file in &self.files {
            old_file.flush()?;
        }

        let mut old_files = Vec::new();

        for r in read_dir(&*self.path)? {
            let entry = r?;
            if entry.file_type()?.is_file() && entry.file_name().to_string_lossy().ends_with(".kv")
            {
                old_files.push(entry.path().to_owned());
            }
        }

        for file_path in &old_files {
            fs::copy(
                file_path,
                back_up.path().join(file_path.file_name().unwrap()),
            )?;
        }

        for file_path in &old_files {
            fs::remove_file(file_path)?;
        }

        let roll_back = || -> Result<()> {
            for r in read_dir(&*self.path)? {
                let entry = r?;
                if entry.file_type()?.is_file()
                    && entry.file_name().to_string_lossy().ends_with(".kv")
                {
                    fs::remove_file(entry.path())?;
                }
            }

            for r in read_dir(&back_up)? {
                let entry = r?;
                if entry.file_type()?.is_file()
                    && entry.file_name().to_string_lossy().ends_with(".kv")
                {
                    fs::copy(entry.path(), self.path.join(entry.file_name()))?;
                }
            }

            Ok(())
        };

        for r in read_dir(&*storage.path)? {
            match r {
                Err(e) => {
                    roll_back()?;
                    return Err(Io(e));
                }
                Ok(entry) => {
                    if entry.file_type()?.is_file()
                        && entry.file_name().to_string_lossy().ends_with(".kv")
                    {
                        match fs::copy(entry.path(), &self.path.join(entry.file_name())) {
                            Err(e) => {
                                roll_back()?;
                                return Err(Io(e));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        self.files.clear();
        for r in read_dir(&*self.path)? {
            let entry = r?;
            if entry.file_type()?.is_file() && entry.file_name().to_string_lossy().ends_with(".kv")
            {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(entry.path())?;
                self.files.push(file);
            }
        }
        self.record_count = storage.record_count;

        Ok(())
    }
}
