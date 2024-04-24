use std::{
    ffi::CString,
    os::unix::prelude::{OsStrExt, RawFd},
    path::{Path, PathBuf},
};

use super::VFS;

/// A wrapper around a file descriptor, it opens files with `O_DIRECT`
pub(crate) struct DirectFile {
    pub(crate) raw_fd: RawFd,
    path: PathBuf,
}

impl Drop for DirectFile {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.raw_fd);
        }
        _ = std::fs::remove_file(&self.path);
    }
}

impl VFS for DirectFile {
    fn new(path: impl AsRef<Path>) -> Self {
        Self::open(path)
    }

    fn read(&self, offset: usize, buf: &mut [u8]) {
        let bytes_read = unsafe {
            libc::pread(
                self.raw_fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                offset as i64,
            )
        };

        // In our case the bytes_read should be equal to PAGE_SIZE because
        // 1. We read a page at a time and the full page should present in the file
        // 2. We are not reading from pipe
        // 3. The read should never be interrupted by signals
        assert_eq!(
            bytes_read,
            buf.len() as isize,
            "Failed to read the page from disk, bytes read: {}, expected: {}, offset: {}",
            bytes_read,
            buf.len(),
            offset
        );
    }

    fn write(&self, offset: usize, buf: &[u8]) {
        let bytes_written = unsafe {
            libc::pwrite(
                self.raw_fd,
                buf.as_ptr() as *mut libc::c_void,
                buf.len(),
                offset as i64,
            )
        };

        assert_eq!(
            bytes_written,
            buf.len() as isize,
            "Failed to write the page to disk, bytes_written: {}, expected: {}, offset: {}, error:{}",
            bytes_written,
            buf.len(),
            offset,
            std::io::Error::last_os_error()
        );
    }
}

impl DirectFile {
    pub(crate) fn open(path: impl AsRef<Path>) -> DirectFile {
        let path = path.as_ref();
        let path_cstr = CString::new(path.as_os_str().as_bytes()).unwrap();
        let raw_fd = unsafe {
            libc::open(
                path_cstr.as_ptr(),
                libc::O_DIRECT | libc::O_RDWR | libc::O_CREAT,
                libc::S_IRUSR | libc::S_IWUSR,
            )
        };
        let raw_fd = if raw_fd < 0 {
            let fd = unsafe {
                libc::open(
                    path_cstr.as_ptr(),
                    libc::O_RDWR | libc::O_CREAT,
                    libc::S_IRUSR | libc::S_IWUSR,
                )
            };

            assert!(
                fd >= 0,
                "Failed to open file {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            );
            fd
        } else {
            raw_fd
        };

        DirectFile {
            raw_fd,
            path: path.to_path_buf(),
        }
    }
}
