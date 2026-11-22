//! A cache of remote file bytes that all GDAL workers share.
//!
//! Each worker has its own curl cache, so a file region one worker downloaded is downloaded again
//! by the next worker that reads it, e.g. the blocks of a finer level after zooming in. This cache
//! keeps the bytes in aligned chunks, one file per chunk in a directory all workers see, usually
//! on `/dev/shm`. A worker reads remote files through the GDAL file system `/vsigecache/`, which
//! serves the chunks it finds and reads the missing ones through the remote file system as before,
//! so authentication, retries and GDAL's network statistics stay unchanged.
//!
//! Chunks are written to a temporary file and renamed, so readers never see a partial chunk and no
//! locks are needed; two workers missing the same chunk at once both download it. The server
//! deletes the oldest chunks when the directory exceeds its size ([`sweep`]).
//!
//! A path is `/vsigecache/<key>/vsicurl/...`: the key covers the file path and the GDAL options of
//! the dataset, which may carry credentials, so a dataset never reads bytes another dataset was
//! allowed to download but it is not. Each open adds the file's size and, if the remote file system
//! reports them, its `ETag` and `Last-Modified`, so a changed file gets new chunks.

use serde::{Deserialize, Serialize};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// The prefix of the GDAL file system that reads through the cache.
pub const PREFIX: &str = "/vsigecache/";
/// [`PREFIX`] for GDAL, which keeps the pointer, so it must live as long as the process.
static PREFIX_C: &CStr = c"/vsigecache/";

/// The size of a chunk, the unit of caching. Equal to the recommended `CPL_VSIL_CURL_CHUNK_SIZE`,
/// so that a missing chunk costs one chunk of the remote file system.
pub const CHUNK_SIZE: u64 = 64 * 1024;

/// Remote GDAL file systems whose files are read through the cache.
const REMOTE_FILE_SYSTEMS: [&str; 7] = [
    "/vsicurl/",
    "/vsis3/",
    "/vsigs/",
    "/vsiaz/",
    "/vsiadls/",
    "/vsioss/",
    "/vsiswift/",
];

/// Where the chunks live and how large the directory may grow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteCacheConfig {
    pub directory: PathBuf,
    pub size_bytes: u64,
}

/// Files whose chunk directory a process remembers, so that it asks the remote file system for a
/// file's version once, not for every handle (GDAL opens one per decoding thread).
const REMEMBERED_FILES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(4096).expect("not zero");

/// A file as the cache tells files apart: key, inner path and size.
type FileId = (String, Vec<u8>, u64);

/// The cache of this process: where the chunks are, and its chunk counters since the last
/// [`ByteCache::take_counts`].
#[derive(Debug)]
pub struct ByteCache {
    directory: PathBuf,
    hits: AtomicU64,
    misses: AtomicU64,
    chunk_dirs: std::sync::Mutex<lru::LruCache<FileId, PathBuf>>,
}

impl ByteCache {
    /// Returns and resets the chunks read from the cache and the chunks downloaded.
    pub fn take_counts(&self) -> (u64, u64) {
        (
            self.hits.swap(0, Ordering::Relaxed),
            self.misses.swap(0, Ordering::Relaxed),
        )
    }
}

/// Creates the directory, readable only by this user, as it holds the bytes of datasets.
///
/// # Errors
/// Fails if the directory cannot be created or its permissions set.
pub fn create_directory(directory: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(directory)?;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
}

/// Installs the file system `/vsigecache/` in this process, reading chunks from `directory`. Call
/// once per process; the cache lives as long as the process, as GDAL keeps the handler.
///
/// # Errors
/// Fails if the directory cannot be created or GDAL rejects the handler.
pub fn install(directory: &Path) -> std::io::Result<&'static ByteCache> {
    create_directory(directory)?;
    let cache: &'static ByteCache = Box::leak(Box::new(ByteCache {
        directory: directory.to_path_buf(),
        hits: AtomicU64::new(0),
        misses: AtomicU64::new(0),
        chunk_dirs: std::sync::Mutex::new(lru::LruCache::new(REMEMBERED_FILES)),
    }));
    // SAFETY: GDAL copies the callbacks but keeps the prefix pointer, so the prefix is a static
    // string; `cache` is never freed, as every handle refers to it
    let installed = unsafe {
        let callbacks = gdal_sys::VSIAllocFilesystemPluginCallbacksStruct();
        (*callbacks).pUserData = std::ptr::from_ref(cache).cast_mut().cast();
        (*callbacks).stat = Some(stat);
        (*callbacks).open = Some(open);
        (*callbacks).tell = Some(tell);
        (*callbacks).seek = Some(seek);
        (*callbacks).read = Some(read);
        (*callbacks).read_multi_range = Some(read_multi_range);
        (*callbacks).eof = Some(eof);
        (*callbacks).close = Some(close);
        // no buffering on top: the chunks are the buffer
        (*callbacks).nBufferSize = 0;
        (*callbacks).nCacheSize = 0;
        let result = gdal_sys::VSIInstallPluginHandler(PREFIX_C.as_ptr(), callbacks);
        gdal_sys::VSIFreeFilesystemPluginCallbacksStruct(callbacks);
        result
    };
    if installed != 0 {
        return Err(std::io::Error::other(
            "GDAL rejected the /vsigecache/ handler",
        ));
    }
    Ok(cache)
}

/// The path that reads `path` through the cache, or `None` if it is not a plain path of a remote
/// file system. `key` must differ for datasets whose bytes must not be shared, e.g. other
/// credentials.
///
/// shortcut: paths inside other paths (`/vsizip//vsicurl/...`, `SENTINEL2_L2A:/vsis3/...`) are not
/// cached; wrap the inner path where the driver builds it if such datasets need the cache.
pub fn wrap(path: &Path, key: u64) -> Option<PathBuf> {
    let path = path.to_str()?;
    REMOTE_FILE_SYSTEMS
        .iter()
        .any(|fs| path.starts_with(fs))
        .then(|| PathBuf::from(format!("{PREFIX}{key:016x}{path}")))
}

/// The key and inner path of a cache path, with or without the prefix: GDAL passes the plugin's
/// callbacks the path after it.
fn unwrap(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix(PREFIX).unwrap_or(path);
    let (key, inner) = rest.split_at_checked(16)?;
    inner.starts_with('/').then_some((key, inner))
}

/// Deletes the oldest chunks until the directory holds at most `size_bytes`, and temporary files
/// of writes that never finished. Returns the bytes kept and the chunks deleted.
///
/// shortcut: "oldest" is by write time, not by last read, so a chunk read all the time is deleted
/// like any other; track reads if hot chunks turn out to be evicted.
///
/// # Errors
/// Fails if the directory cannot be listed.
pub fn sweep(directory: &Path, size_bytes: u64) -> std::io::Result<(u64, u64)> {
    let stale = SystemTime::now() - Duration::from_secs(60);
    let mut chunks = Vec::new();
    let mut total = 0;
    for file_dir in std::fs::read_dir(directory)? {
        let file_dir = file_dir?.path();
        let Ok(entries) = std::fs::read_dir(&file_dir) else {
            continue;
        };
        let mut empty = true;
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            if entry.path().extension().is_some_and(|e| e == "tmp") {
                if modified < stale {
                    let _ = std::fs::remove_file(entry.path());
                }
                empty = false;
                continue;
            }
            empty = false;
            total += meta.len();
            chunks.push((modified, meta.len(), entry.path()));
        }
        if empty {
            let _ = std::fs::remove_dir(&file_dir);
        }
    }
    let mut deleted = 0;
    if total > size_bytes {
        chunks.sort_unstable_by_key(|(modified, _, _)| *modified);
        for (_, len, path) in chunks {
            if total <= size_bytes {
                break;
            }
            if std::fs::remove_file(path).is_ok() {
                total -= len;
                deleted += 1;
            }
        }
    }
    Ok((total, deleted))
}

/// An open file: the inner handle, where its chunks are, and the position.
struct Handle {
    cache: &'static ByteCache,
    inner: *mut gdal_sys::VSILFILE,
    chunk_dir: PathBuf,
    size: u64,
    position: u64,
    eof: bool,
}

impl Handle {
    /// Reads `buffer.len()` bytes at `offset`, or fewer at the end of the file.
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> usize {
        let end = (offset + buffer.len() as u64).min(self.size);
        if offset >= end {
            return 0;
        }
        let (first, last) = (offset / CHUNK_SIZE, (end - 1) / CHUNK_SIZE);
        let mut chunks: Vec<Option<Vec<u8>>> = (first..=last)
            .map(|index| self.cached_chunk(index))
            .collect();

        // download each run of missing chunks in one read
        let mut i = 0;
        while i < chunks.len() {
            if chunks[i].is_some() {
                self.cache.hits.fetch_add(1, Ordering::Relaxed);
                i += 1;
                continue;
            }
            let mut j = i;
            while j + 1 < chunks.len() && chunks[j + 1].is_none() {
                j += 1;
            }
            let (a, b) = (first + i as u64, first + j as u64);
            let Some(run) = self.download(a, b) else {
                return 0;
            };
            for (k, chunk) in run.into_iter().enumerate() {
                self.store_chunk(a + k as u64, &chunk);
                chunks[i + k] = Some(chunk);
            }
            self.cache.misses.fetch_add(b - a + 1, Ordering::Relaxed);
            i = j + 1;
        }

        let mut written = 0;
        for (index, chunk) in (first..=last).zip(chunks.into_iter().flatten()) {
            let chunk_start = index * CHUNK_SIZE;
            let from = offset.max(chunk_start) - chunk_start;
            let to = end.min(chunk_start + chunk.len() as u64) - chunk_start;
            let part = &chunk[from as usize..to as usize];
            buffer[written..written + part.len()].copy_from_slice(part);
            written += part.len();
        }
        written
    }

    fn chunk_len(&self, index: u64) -> u64 {
        CHUNK_SIZE.min(self.size - index * CHUNK_SIZE)
    }

    /// A chunk from the directory, if it is there and complete.
    fn cached_chunk(&self, index: u64) -> Option<Vec<u8>> {
        let bytes = std::fs::read(self.chunk_dir.join(index.to_string())).ok()?;
        (bytes.len() as u64 == self.chunk_len(index)).then_some(bytes)
    }

    /// Chunks `a..=b` from the inner file, or `None` if it returns fewer bytes than it has.
    fn download(&mut self, a: u64, b: u64) -> Option<Vec<Vec<u8>>> {
        let start = a * CHUNK_SIZE;
        let len = (b * CHUNK_SIZE + self.chunk_len(b) - start) as usize;
        let mut bytes = vec![0_u8; len];
        // SAFETY: `inner` is open until `close`, and `bytes` holds `len` bytes
        let got = unsafe {
            if gdal_sys::VSIFSeekL(self.inner, start, libc_seek_set()) != 0 {
                return None;
            }
            gdal_sys::VSIFReadL(bytes.as_mut_ptr().cast(), 1, len, self.inner)
        };
        (got == len).then(|| {
            bytes
                .chunks(CHUNK_SIZE as usize)
                .map(<[u8]>::to_vec)
                .collect()
        })
    }

    /// Writes a chunk under a temporary name and renames it, so that readers see it whole or not
    /// at all. A failure only means the chunk is downloaded again.
    fn store_chunk(&self, index: u64, chunk: &[u8]) {
        let path = self.chunk_dir.join(index.to_string());
        let tmp = self.chunk_dir.join(format!(
            "{index}.{}.{:?}.tmp",
            std::process::id(),
            std::thread::current().id()
        ));
        let written = std::fs::create_dir_all(&self.chunk_dir)
            .and_then(|()| std::fs::File::create(&tmp))
            .and_then(|mut file| file.write_all(chunk))
            .and_then(|()| std::fs::rename(&tmp, &path));
        if written.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
    }
}

const fn libc_seek_set() -> c_int {
    0 // SEEK_SET
}

/// The directory of a file's chunks: the cache key, the inner path and the file's version.
fn chunk_dir(cache: &ByteCache, key: &str, inner: &CStr, size: u64) -> PathBuf {
    let id = (key.to_string(), inner.to_bytes().to_vec(), size);
    if let Some(dir) = lock(&cache.chunk_dirs).get(&id) {
        return dir.clone();
    }
    let dir = cache
        .directory
        .join(format!("{:016x}", file_version(key, inner, size)));
    lock(&cache.chunk_dirs).put(id, dir.clone());
    dir
}

/// The lock even if a thread panicked while holding it: the map stays valid.
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A hash of the key, the inner path, the size and, if the remote file system reports them, the
/// file's `ETag` and `Last-Modified`. Asking for them may cost a request.
fn file_version(key: &str, inner: &CStr, size: u64) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    key.hash(&mut hasher);
    inner.to_bytes().hash(&mut hasher);
    size.hash(&mut hasher);
    // SAFETY: GDAL returns a list owned by the caller or null; it is freed below
    unsafe {
        let domain = c"HEADERS";
        let headers =
            gdal_sys::VSIGetFileMetadata(inner.as_ptr(), domain.as_ptr(), std::ptr::null_mut());
        if !headers.is_null() {
            let mut entry = headers;
            while !(*entry).is_null() {
                let header = CStr::from_ptr(*entry).to_bytes();
                let name_len = header
                    .iter()
                    .position(|&c| c == b'=')
                    .unwrap_or(header.len());
                let name = &header[..name_len];
                if name.eq_ignore_ascii_case(b"etag") || name.eq_ignore_ascii_case(b"last-modified")
                {
                    header.hash(&mut hasher);
                }
                entry = entry.add(1);
            }
            gdal_sys::CSLDestroy(headers);
        }
    }
    hasher.finish()
}

unsafe extern "C" fn open(
    user_data: *mut c_void,
    filename: *const c_char,
    access: *const c_char,
) -> *mut c_void {
    // SAFETY: GDAL passes the pointer given at install and null-terminated strings
    let (cache, filename, access) = unsafe {
        (
            &*user_data.cast::<ByteCache>(),
            CStr::from_ptr(filename),
            CStr::from_ptr(access),
        )
    };
    if !access.to_bytes().starts_with(b"r") || access.to_bytes().contains(&b'+') {
        return std::ptr::null_mut(); // read only
    }
    let Some((key, inner)) = filename.to_str().ok().and_then(unwrap) else {
        return std::ptr::null_mut();
    };
    let Ok(inner) = CString::new(inner) else {
        return std::ptr::null_mut();
    };
    // SAFETY: valid strings; the handle is closed in `close` or below
    let (handle, size) = unsafe {
        let handle = gdal_sys::VSIFOpenL(inner.as_ptr(), c"rb".as_ptr());
        if handle.is_null() {
            return std::ptr::null_mut();
        }
        if gdal_sys::VSIFSeekL(handle, 0, 2 /* SEEK_END */) != 0 {
            gdal_sys::VSIFCloseL(handle);
            return std::ptr::null_mut();
        }
        (handle, gdal_sys::VSIFTellL(handle))
    };
    let chunk_dir = chunk_dir(cache, key, &inner, size);
    Box::into_raw(Box::new(Handle {
        cache,
        inner: handle,
        chunk_dir,
        size,
        position: 0,
        eof: false,
    }))
    .cast()
}

unsafe extern "C" fn stat(
    _user_data: *mut c_void,
    filename: *const c_char,
    stat_buf: *mut gdal_sys::VSIStatBufL,
    flags: c_int,
) -> c_int {
    // SAFETY: GDAL passes a null-terminated string
    let filename = unsafe { CStr::from_ptr(filename) };
    let Some((_, inner)) = filename.to_str().ok().and_then(unwrap) else {
        return -1;
    };
    let Ok(inner) = CString::new(inner) else {
        return -1;
    };
    // SAFETY: the buffer is GDAL's, filled by the inner file system
    unsafe { gdal_sys::VSIStatExL(inner.as_ptr(), stat_buf, flags) }
}

/// # Safety
/// `file` must be a handle returned by `open` and not yet closed.
unsafe fn handle<'a>(file: *mut c_void) -> &'a mut Handle {
    unsafe { &mut *file.cast::<Handle>() }
}

unsafe extern "C" fn tell(file: *mut c_void) -> gdal_sys::vsi_l_offset {
    unsafe { handle(file) }.position
}

unsafe extern "C" fn seek(
    file: *mut c_void,
    offset: gdal_sys::vsi_l_offset,
    whence: c_int,
) -> c_int {
    let h = unsafe { handle(file) };
    h.position = match whence {
        0 => offset,                          // SEEK_SET
        1 => h.position.wrapping_add(offset), // SEEK_CUR, GDAL passes negative offsets wrapped
        2 => h.size.wrapping_add(offset),     // SEEK_END
        _ => return -1,
    };
    h.eof = false;
    0
}

unsafe extern "C" fn read(
    file: *mut c_void,
    buffer: *mut c_void,
    size: usize,
    count: usize,
) -> usize {
    let h = unsafe { handle(file) };
    let Some(len) = size.checked_mul(count).filter(|&len| len > 0) else {
        return 0;
    };
    // SAFETY: GDAL passes a buffer of `size * count` bytes
    let buffer = unsafe { std::slice::from_raw_parts_mut(buffer.cast::<u8>(), len) };
    let got = h.read_at(h.position, buffer);
    h.position += got as u64;
    if got < len {
        h.eof = true;
    }
    got / size
}

unsafe extern "C" fn read_multi_range(
    file: *mut c_void,
    ranges: c_int,
    data: *mut *mut c_void,
    offsets: *const gdal_sys::vsi_l_offset,
    sizes: *const usize,
) -> c_int {
    let h = unsafe { handle(file) };
    for i in 0..usize::try_from(ranges).unwrap_or(0) {
        // SAFETY: GDAL passes `ranges` buffers, offsets and sizes
        let (buffer, offset, size) = unsafe { (*data.add(i), *offsets.add(i), *sizes.add(i)) };
        if size == 0 {
            continue;
        }
        let buffer = unsafe { std::slice::from_raw_parts_mut(buffer.cast::<u8>(), size) };
        if h.read_at(offset, buffer) != size {
            return -1;
        }
    }
    0
}

unsafe extern "C" fn eof(file: *mut c_void) -> c_int {
    c_int::from(unsafe { handle(file) }.eof)
}

unsafe extern "C" fn close(file: *mut c_void) -> c_int {
    // SAFETY: GDAL closes each handle once
    let h = unsafe { Box::from_raw(file.cast::<Handle>()) };
    unsafe { gdal_sys::VSIFCloseL(h.inner) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    /// The handler is installed once per process, so all tests share one directory.
    fn cache() -> &'static (tempfile::TempDir, &'static ByteCache) {
        static CACHE: OnceLock<(tempfile::TempDir, &'static ByteCache)> = OnceLock::new();
        CACHE.get_or_init(|| {
            let dir = tempfile::tempdir().unwrap();
            let cache = install(&dir.path().join("chunks")).unwrap();

            (dir, cache)
        })
    }

    /// A file with `len` bytes that differ per position, in its own directory.
    fn file(len: usize) -> (tempfile::TempDir, PathBuf, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.bin");
        let bytes: Vec<u8> = (0..len).map(|i| (i * 7 + i / 251) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        (dir, path, bytes)
    }

    fn cached_path(path: &Path, key: u64) -> CString {
        // the plugin reads any inner path; `wrap` limits it to remote ones
        CString::new(format!("{PREFIX}{key:016x}{}", path.display())).unwrap()
    }

    fn read_range(path: &CStr, offset: u64, len: usize) -> Vec<u8> {
        let mut buffer = vec![0_u8; len];
        unsafe {
            let file = gdal_sys::VSIFOpenL(path.as_ptr(), c"rb".as_ptr());
            assert!(!file.is_null());
            assert_eq!(gdal_sys::VSIFSeekL(file, offset, 0), 0);
            let got = gdal_sys::VSIFReadL(buffer.as_mut_ptr().cast(), 1, len, file);
            buffer.truncate(got);
            gdal_sys::VSIFCloseL(file);
        }
        buffer
    }

    #[test]
    fn it_wraps_only_plain_remote_paths() {
        assert_eq!(
            wrap(Path::new("/vsis3/bucket/a.jp2"), 0xab),
            Some(PathBuf::from(
                "/vsigecache/00000000000000ab/vsis3/bucket/a.jp2"
            ))
        );
        assert_eq!(wrap(Path::new("/data/a.tif"), 1), None);
        assert_eq!(wrap(Path::new("/vsizip//vsicurl/http://x/a.zip"), 1), None);
        assert_eq!(
            unwrap("/vsigecache/00000000000000ab/vsis3/bucket/a.jp2"),
            Some(("00000000000000ab", "/vsis3/bucket/a.jp2"))
        );
        assert_eq!(
            unwrap("00000000000000ab/vsis3/bucket/a.jp2"),
            Some(("00000000000000ab", "/vsis3/bucket/a.jp2"))
        );
        assert_eq!(unwrap("/vsigecache/short"), None);
    }

    /// The chunk files of a cached path, as the plugin names them.
    fn chunk_files(path: &Path, key: u64, size: u64) -> Vec<String> {
        let (_dir, cache) = cache();
        let inner = CString::new(path.to_str().unwrap()).unwrap();
        let dir = chunk_dir(cache, &format!("{key:016x}"), &inner, size);
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    #[test]
    fn it_reads_the_bytes_of_the_file_and_serves_them_again_from_chunks() {
        let _ = cache();
        let len = 3 * CHUNK_SIZE as usize + 1234; // a partial last chunk
        let (_dir, path, bytes) = file(len);
        let cached = cached_path(&path, 1);
        // from the end of chunk 0 into chunk 3
        let (offset, size) = (CHUNK_SIZE as usize - 10, 2 * CHUNK_SIZE as usize + 100);

        assert_eq!(
            read_range(&cached, offset as u64, size),
            bytes[offset..offset + size]
        );
        assert_eq!(chunk_files(&path, 1, len as u64), ["0", "1", "2", "3"]);

        // other bytes of the same size in the file: the chunks still serve the old ones
        std::fs::write(&path, vec![0_u8; len]).unwrap();
        assert_eq!(
            read_range(&cached, offset as u64, size),
            bytes[offset..offset + size]
        );

        // reads end at the end of the file
        assert_eq!(
            read_range(&cached, len as u64 - 100, 500),
            bytes[len - 100..]
        );
    }

    #[test]
    fn another_key_does_not_see_the_chunks() {
        let _ = cache();
        let len = CHUNK_SIZE as usize * 2;
        let (_dir, path, bytes) = file(len);
        let _ = read_range(&cached_path(&path, 2), 0, 100);
        std::fs::write(&path, vec![0_u8; len]).unwrap();
        assert_eq!(read_range(&cached_path(&path, 2), 0, 100), bytes[..100]);
        assert_eq!(read_range(&cached_path(&path, 3), 0, 100), vec![0_u8; 100]);
    }

    #[test]
    fn it_reads_multiple_ranges() {
        let _ = cache();
        let (_dir, path, bytes) = file(CHUNK_SIZE as usize * 4);
        let cached = cached_path(&path, 4);
        let offsets = [10_u64, 3 * CHUNK_SIZE - 5];
        let sizes = [100_usize, 20];
        let mut a = vec![0_u8; sizes[0]];
        let mut b = vec![0_u8; sizes[1]];
        let mut data = [a.as_mut_ptr().cast::<c_void>(), b.as_mut_ptr().cast()];
        unsafe {
            let file = gdal_sys::VSIFOpenL(cached.as_ptr(), c"rb".as_ptr());
            assert_eq!(
                gdal_sys::VSIFReadMultiRangeL(
                    2,
                    data.as_mut_ptr(),
                    offsets.as_ptr(),
                    sizes.as_ptr(),
                    file
                ),
                0
            );
            gdal_sys::VSIFCloseL(file);
        }
        assert_eq!(a, bytes[10..110]);
        assert_eq!(
            b,
            bytes[3 * CHUNK_SIZE as usize - 5..3 * CHUNK_SIZE as usize + 15]
        );
    }

    #[test]
    fn a_raster_reads_the_same_through_the_cache() {
        let _ = cache();
        let path = geoengine_datatypes::test_data!("raster/landcover/landcover.tif").to_path_buf();
        let path = path.as_path();
        let direct = gdal::Dataset::open(path).unwrap();
        let cached = gdal::Dataset::open(cached_path(path, 5).to_str().unwrap()).unwrap();
        let read = |ds: &gdal::Dataset| {
            ds.rasterband(1)
                .unwrap()
                .read_band_as::<u8>()
                .unwrap()
                .data()
                .to_vec()
        };
        assert_eq!(read(&direct), read(&cached));
    }

    /// The case the cache is for: a remote file, read again by a process whose curl cache does not
    /// hold it, downloads nothing but what it asks the server about the file.
    #[test]
    #[serial_test::serial]
    fn a_remote_file_read_again_downloads_nothing() {
        let _ = cache();
        let server = super::super::test_range_server::RangeServer::start(
            geoengine_datatypes::test_data!("raster/landcover").to_path_buf(),
            Duration::ZERO,
        );
        let remote = PathBuf::from(server.vsicurl_path("landcover.tif"));
        let cached = wrap(&remote, 6).expect("a remote path");
        let _options = crate::util::GdalConfigOptions::new(
            &[
                ("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif"),
                ("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR"),
                ("GDAL_HTTP_NETRC", "NO"),
            ]
            .map(|(name, value)| (name.to_string(), value.to_string())),
        )
        .unwrap();
        let read = || {
            gdal::Dataset::open(&cached)
                .unwrap()
                .rasterband(1)
                .unwrap()
                .read_band_as::<u8>()
                .unwrap()
                .data()
                .to_vec()
        };
        let direct = gdal::Dataset::open(geoengine_datatypes::test_data!(
            "raster/landcover/landcover.tif"
        ))
        .unwrap()
        .rasterband(1)
        .unwrap()
        .read_band_as::<u8>()
        .unwrap()
        .data()
        .to_vec();

        assert_eq!(read(), direct);
        let gets = server.gets();
        assert!(gets > 0);

        // as a new worker: GDAL's own cache of the file is gone
        unsafe { gdal_sys::VSICurlClearCache() };
        assert_eq!(read(), direct);
        assert_eq!(server.gets(), gets, "no chunk downloaded again");
    }

    #[test]
    fn the_sweep_keeps_the_limit_and_deletes_the_oldest_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let file_dir = dir.path().join("0000000000000001");
        std::fs::create_dir_all(&file_dir).unwrap();
        for index in 0..4 {
            std::fs::write(file_dir.join(index.to_string()), vec![0_u8; 1000]).unwrap();
            // distinct write times, oldest first
            std::thread::sleep(Duration::from_millis(20));
        }
        std::fs::create_dir_all(dir.path().join("0000000000000002")).unwrap();

        assert_eq!(sweep(dir.path(), 2500).unwrap(), (2000, 2));
        assert!(!file_dir.join("0").exists());
        assert!(!file_dir.join("1").exists());
        assert!(file_dir.join("3").exists());
        assert!(
            !dir.path().join("0000000000000002").exists(),
            "empty directories go"
        );
    }
}
