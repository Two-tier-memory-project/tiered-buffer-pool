use std::{
    ffi::CString,
    os::{fd::RawFd, unix::prelude::OsStrExt},
    path::{Path, PathBuf},
};

use io_uring::{opcode, IoUring};

use super::VFS;

thread_local! {
    /// Io_uring discourages sharing the ring across mutliple threads,
    /// so we create a new ring for each thread.
    static IO_URING: std::cell::RefCell::<IoUring>  = {
        std::cell::RefCell::new(
            IoUring::builder().setup_iopoll().build(2).unwrap()
        )
    };
}

pub(crate) struct IoUringFile {
    pub(crate) raw_fd: RawFd,
    path: PathBuf,
}

impl Drop for IoUringFile {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.raw_fd);
        }
        _ = std::fs::remove_file(&self.path);
    }
}

impl VFS for IoUringFile {
    fn new(path: impl AsRef<Path>) -> Self {
        Self::open(path)
    }

    fn read(&self, offset: usize, buf: &mut [u8]) {
        let read_e = opcode::Read::new(
            io_uring::types::Fd(self.raw_fd),
            buf.as_mut_ptr(),
            buf.len() as _,
        )
        .offset64(offset as i64)
        .build()
        .user_data(0x42);

        IO_URING.with(|ring| {
            let mut ring_mut = ring.borrow_mut();

            unsafe {
                ring_mut.submission().push(&read_e).unwrap();
            }

            // FIXME: I don't know why we need to busy wait here, something wrong must be going on, should figure out later.
            // If we don't do this, the complection queue might be empty.
            loop {
                let _io_consumed = ring_mut.submit_and_wait(1).unwrap();

                let mut cq = ring_mut.completion();
                if let Some(cqe) = cq.next() {
                    assert_eq!(cqe.user_data(), 0x42);
                    assert_eq!(
                        cqe.result(),
                        buf.len() as i32,
                        "Read cqe result error: {}",
                        std::io::Error::last_os_error()
                    );
                    break;
                }
            }
        });
    }

    fn write(&self, offset: usize, buf: &[u8]) {
        let write_e = opcode::Write::new(
            io_uring::types::Fd(self.raw_fd),
            buf.as_ptr(),
            buf.len() as _,
        )
        .offset64(offset as i64)
        .build()
        .user_data(0x42);

        IO_URING.with(|ring| {
            let mut ring_mut = ring.borrow_mut();
            unsafe {
                ring_mut
                    .submission()
                    .push(&write_e)
                    .expect("submission queue is full");
            }

            loop {
                let _io_consumed = ring_mut.submit_and_wait(1).unwrap();

                let mut cq = ring_mut.completion();
                if let Some(cqe) = cq.next() {
                    assert_eq!(cqe.user_data(), 0x42);
                    assert_eq!(
                        cqe.result(),
                        buf.len() as i32,
                        "Write cqe result error: {}",
                        std::io::Error::last_os_error()
                    );
                    break;
                }
            }
        });
    }
}

impl IoUringFile {
    pub(crate) fn open(path: impl AsRef<Path>) -> IoUringFile {
        let path = path.as_ref();
        let path_cstr = CString::new(path.as_os_str().as_bytes()).unwrap();
        let raw_fd = unsafe {
            libc::open(
                path_cstr.as_ptr(),
                libc::O_DIRECT | libc::O_RDWR | libc::O_CREAT,
                libc::S_IRUSR | libc::S_IWUSR,
            )
        };
        assert!(
            raw_fd >= 0,
            "Failed to open file {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );

        IoUringFile {
            raw_fd,
            path: path.to_path_buf(),
        }
    }
}
