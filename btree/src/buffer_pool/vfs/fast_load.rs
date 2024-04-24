use std::{
    cell::UnsafeCell,
    path::{Path, PathBuf},
};

use super::{direct_file::DirectFile, io_uring_file::IoUringFile, VFS};

const LOAD_FILE: &str = "/mnt/ramdisk/test.db.load";

enum File {
    Load(DirectFile),
    Run(IoUringFile), // IO uring can't operate on memory-backed file.
}

impl File {
    fn read(&self, offset: usize, buf: &mut [u8]) {
        match self {
            File::Load(file) => file.read(offset, buf),
            File::Run(file) => file.read(offset, buf),
        }
    }

    fn write(&self, offset: usize, buf: &[u8]) {
        match self {
            File::Load(file) => file.write(offset, buf),
            File::Run(file) => file.write(offset, buf),
        }
    }

    fn raw_fd(&self) -> i32 {
        match self {
            File::Load(file) => file.raw_fd,
            File::Run(file) => file.raw_fd,
        }
    }
}

pub struct FastLoadFile {
    file: UnsafeCell<File>,
    run_path: PathBuf,
}

unsafe impl Send for FastLoadFile {}
unsafe impl Sync for FastLoadFile {}

impl FastLoadFile {
    pub(crate) unsafe fn copy_to_real_disk(&self) {
        // first copy the file on ramdisk to path, then change the file to point to it.
        unsafe {
            libc::fsync(self.file().raw_fd());
        }
        println!(
            "Copying file from {} to {}",
            LOAD_FILE,
            self.run_path.display()
        );
        std::fs::copy(LOAD_FILE, &self.run_path).unwrap();

        let new_file = IoUringFile::new(&self.run_path);
        self.file.get().write(File::Run(new_file)); // TODO: this will leak the old file.

        println!(
            "Copied file from {} to {}",
            LOAD_FILE,
            self.run_path.display()
        );
    }

    fn file(&self) -> &File {
        unsafe { &*self.file.get() }
    }
}

impl VFS for FastLoadFile {
    fn new(path: impl AsRef<Path>) -> Self {
        Self {
            file: UnsafeCell::new(File::Load(DirectFile::new(LOAD_FILE))),
            run_path: path.as_ref().to_path_buf(),
        }
    }
    fn read(&self, offset: usize, buf: &mut [u8]) {
        self.file().read(offset, buf);
    }

    fn write(&self, offset: usize, buf: &[u8]) {
        self.file().write(offset, buf);
    }
}
