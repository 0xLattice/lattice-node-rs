//! Persist the directory entry after the caller has synced and closed the file.
use std::{io, path::Path};

#[cfg(not(windows))]
pub(super) fn replace(source: &Path, destination: &Path) -> io::Result<()> {
    std::fs::rename(source, destination)?;
    std::fs::File::open(destination.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()
}

#[cfg(windows)]
pub(super) fn replace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    // canonicalize supplies absolute verbatim paths, including long/Unicode
    // paths. The new ledger name may not exist yet; canonicalize its parent.
    let source = std::fs::canonicalize(source)?;
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    let filename = destination.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "ledger destination needs a filename",
        )
    })?;
    let destination = std::fs::canonicalize(parent)?.join(filename);
    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    // No COPY_ALLOWED: temporary and destination files are siblings. Unlike
    // opening a directory with std::fs::File, this supports Windows persistence.
    // https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw
    // SAFETY: both buffers are live, NUL-terminated absolute filesystem paths.
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
