mod direct_file;
#[allow(dead_code)]
pub(crate) mod fast_load;
pub(crate) mod io_uring_file;

use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

#[allow(unused_imports)]
pub(crate) use fast_load::FastLoadFile;
#[allow(unused_imports)]
pub(crate) use io_uring_file::IoUringFile;

const BASE_DIR: &str = "target/table_data/";

/// The filesystem interface for the buffer pool.
#[allow(clippy::upper_case_acronyms)]
pub(crate) trait VFS: Sized {
    fn new(path: impl AsRef<Path>) -> Self;
    fn read(&self, offset: usize, buf: &mut [u8]);
    fn write(&self, offset: usize, buf: &[u8]);
    fn default() -> Self {
        let file_name = format!(
            "test-{}.db",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                % 10000
        );

        let log_dir = std::env::var("TABLE_DATA_DIR").unwrap_or_else(|_| BASE_DIR.to_string());
        let path = PathBuf::from_str(&log_dir).unwrap().join(file_name);

        std::fs::create_dir_all(log_dir).unwrap();

        Self::new(path)
    }
}
