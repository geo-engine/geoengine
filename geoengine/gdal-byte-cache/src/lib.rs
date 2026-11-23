//! A cache of remote file bytes that all GDAL workers share.
//!
//! Each worker has its own curl cache, so a file region one worker downloaded is downloaded again
//! by the next worker that reads it, e.g. the blocks of a finer level after zooming in. A worker
//! reads remote files through the GDAL file system `/vsigecache/`, which serves the bytes this
//! cache holds and reads the missing ones through the remote file system as before, so
//! authentication, retries and GDAL's network statistics stay unchanged.
//!
//! Per version of a remote file, the cache directory holds two files:
//! - `<version>.data`: a sparse file of the remote file's size, with each cached chunk of
//!   [`CHUNK_SIZE`] bytes at its offset in the remote file;
//! - `<version>.map`: one byte per chunk, 1 if the chunk is in the data file.
//!
//! The map, not the file system's holes, says what is cached: file systems may add or remove
//! zero blocks of a sparse file. A chunk is written to the data file before its map byte is set,
//! so a reader that sees the byte sees the data. Workers that miss the same chunks take an
//! open-file-description lock on their map bytes, so one downloads and the others wait and then
//! read its bytes. The kernel releases the lock when a worker dies.
//!
//! The server deletes whole files, least recently used first (by the map's modification time,
//! which every open sets), when the directory exceeds its size ([`sweep`]). A reader that still
//! has a deleted file open keeps reading it. One remote file may take at most `max_file_bytes`,
//! so that reading a whole large file cannot empty the cache.
//!
//! A path is `/vsigecache/<key>/vsicurl/...`: the key is a SHA-256 of the dataset's path and GDAL
//! options, which may carry credentials, so a dataset never reads bytes another dataset was
//! allowed to download but it is not. The version adds the file's size and, if the remote file
//! system reports them, its `ETag` and `Last-Modified`, so a changed file gets new files.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

/// The prefix of the GDAL file system that reads through the cache.
pub const PREFIX: &str = "/vsigecache/";
/// [`PREFIX`] for GDAL, which keeps the pointer, so it must live as long as the process.
static PREFIX_C: &CStr = c"/vsigecache/";

/// The size of a chunk, the unit of caching and of locking. Equal to the recommended
/// `CPL_VSIL_CURL_CHUNK_SIZE`, so that a missing chunk costs one chunk of the remote file system.
pub const CHUNK_SIZE: u64 = 64 * 1024;

/// Length of a key in hex digits: a SHA-256.
const KEY_LEN: usize = 64;

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

/// Files whose version a process remembers, so that it asks the remote file system for a file's
/// version once, not for every handle (GDAL opens one per decoding thread).
const REMEMBERED_FILES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(4096).expect("not zero");

/// The sweep deletes down to this share of the size, so that it does not delete on every run.
const LOW_WATERMARK_PERCENT: u64 = 90;

/// How often an open handle marks its file as used, so that the sweep keeps files that are read
/// from the cache through handles that stay open.
const TOUCH_INTERVAL: Duration = Duration::from_secs(60);

// Public GDAL C API, not in the gdal-sys bindings.
unsafe extern "C" {
    fn VSIErrorReset();
    fn VSIGetLastErrorMsg() -> *const c_char;
}

/// Where the cache lives and how large it and each remote file in it may grow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteCacheConfig {
    pub directory: PathBuf,
    pub size_bytes: u64,
    /// the most one remote file may take
    pub max_file_bytes: u64,
}

/// Chunk counters of a process since the last [`ByteCache::take_counts`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ByteCacheCounts {
    /// chunks served from the cache
    pub hits: u64,
    /// chunks downloaded
    pub misses: u64,
    /// missing chunks another worker downloaded while this one waited for its lock
    pub waited: u64,
    /// chunks or files the cache could not write, e.g. a full directory
    pub write_failures: u64,
}

/// A file as the cache tells versions apart: key, inner path and size.
type FileId = (String, Vec<u8>, u64);

/// The cache of this process.
#[derive(Debug)]
pub struct ByteCache {
    directory: PathBuf,
    max_file_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
    waited: AtomicU64,
    write_failures: AtomicU64,
    /// file name stem by key, inner path and size
    versions: std::sync::Mutex<lru::LruCache<FileId, String>>,
}

impl ByteCache {
    /// Returns and resets the counters of this process.
    pub fn take_counts(&self) -> ByteCacheCounts {
        ByteCacheCounts {
            hits: self.hits.swap(0, Ordering::Relaxed),
            misses: self.misses.swap(0, Ordering::Relaxed),
            waited: self.waited.swap(0, Ordering::Relaxed),
            write_failures: self.write_failures.swap(0, Ordering::Relaxed),
        }
    }

    fn failed_write(&self) {
        self.write_failures.fetch_add(1, Ordering::Relaxed);
    }
}

/// A [`Hasher`] that feeds a SHA-256, for keys from the `Hash` implementations of the dataset
/// parameters. Unlike the fast hashers, it is collision-resistant, as keys come partly from
/// definitions users write.
#[derive(Default)]
pub struct KeyHasher(Sha256);

impl KeyHasher {
    /// The key in hex digits.
    pub fn key(self) -> String {
        hex(&self.0.finalize())
    }
}

impl Hasher for KeyHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// Not used for keys; the first 8 bytes of the hash, for the trait.
    fn finish(&self) -> u64 {
        let digest = self.0.clone().finalize();
        u64::from_be_bytes(digest[..8].try_into().expect("8 bytes"))
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// Creates the directory, readable only by this user, as it holds the bytes of datasets. An
/// existing directory keeps its permissions.
///
/// # Errors
/// Fails if the directory cannot be created or its permissions set.
pub fn create_directory(directory: &Path) -> std::io::Result<()> {
    if directory.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(directory)?;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
}

/// Whether `stem` is a file name the cache writes: a SHA-256 in lower-case hex digits. The sweep
/// touches nothing else, so a directory shared with other files by mistake loses none of them.
fn is_cache_stem(stem: &str) -> bool {
    stem.len() == KEY_LEN && stem.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Installs the file system `/vsigecache/` in this process, with its files in `config.directory`.
/// Call once per process; the cache lives as long as the process, as GDAL keeps the handler.
///
/// # Errors
/// Fails if the directory cannot be created or GDAL rejects the handler.
pub fn install(config: &ByteCacheConfig) -> std::io::Result<&'static ByteCache> {
    create_directory(&config.directory)?;
    let cache: &'static ByteCache = Box::leak(Box::new(ByteCache {
        directory: config.directory.clone(),
        max_file_bytes: config.max_file_bytes,
        hits: AtomicU64::new(0),
        misses: AtomicU64::new(0),
        waited: AtomicU64::new(0),
        write_failures: AtomicU64::new(0),
        versions: std::sync::Mutex::new(lru::LruCache::new(REMEMBERED_FILES)),
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
        // no buffering on top: the data file and the page cache are the buffer
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
/// file system. `key` ([`KeyHasher::key`]) must differ for datasets whose bytes must not be
/// shared, e.g. other credentials.
///
/// shortcut: paths inside other paths (`/vsizip//vsicurl/...`, `SENTINEL2_L2A:/vsis3/...`) are not
/// cached; wrap the inner path where the driver builds it if such datasets need the cache.
pub fn wrap(path: &Path, key: &str) -> Option<PathBuf> {
    let path = path.to_str()?;
    (key.len() == KEY_LEN && REMOTE_FILE_SYSTEMS.iter().any(|fs| path.starts_with(fs)))
        .then(|| PathBuf::from(format!("{PREFIX}{key}{path}")))
}

/// The key and inner path of a cache path, with or without the prefix: GDAL passes the plugin's
/// callbacks the path after it.
fn unwrap(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix(PREFIX).unwrap_or(path);
    let (key, inner) = rest.split_at_checked(KEY_LEN)?;
    inner.starts_with('/').then_some((key, inner))
}

/// Deletes the least recently used remote files (data and map) while the directory holds more
/// than `size_bytes`, down to [`LOW_WATERMARK_PERCENT`] of it, and halves of pairs older than a
/// minute (of a worker that died while creating a pair). Counts the allocated blocks, not the
/// length, as the data files are sparse. Touches only files the cache names ([`is_cache_stem`]).
/// Returns the bytes kept and the files deleted.
///
/// # Errors
/// Fails if the directory cannot be listed.
pub fn sweep(directory: &Path, size_bytes: u64) -> std::io::Result<(u64, u64)> {
    #[derive(Default)]
    struct Pair {
        bytes: u64,
        used: Option<SystemTime>,
        modified: Option<SystemTime>,
        halves: u8,
    }
    let stale = SystemTime::now() - Duration::from_secs(60);
    let mut pairs: std::collections::HashMap<String, Pair> = std::collections::HashMap::new();
    let mut deleted = 0;
    for entry in std::fs::read_dir(directory)?.flatten() {
        let path = entry.path();
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let ext = path.extension().and_then(|e| e.to_str());
        let stem = path.file_stem().and_then(|s| s.to_str());
        let (Some(stem), Some(ext @ ("data" | "map")), true) = (stem, ext, meta.is_file()) else {
            continue;
        };
        if !is_cache_stem(stem) {
            continue;
        }
        let pair = pairs.entry(stem.to_string()).or_default();
        pair.bytes += meta.blocks() * 512;
        pair.halves += 1;
        pair.modified = pair.modified.max(Some(modified));
        if ext == "map" {
            pair.used = Some(modified);
        }
    }

    let mut total: u64 = pairs.values().map(|p| p.bytes).sum();
    let mut by_use: Vec<(SystemTime, String, Pair)> = pairs
        .into_iter()
        .map(|(stem, pair)| {
            let used = pair
                .used
                .or(pair.modified)
                .unwrap_or(SystemTime::UNIX_EPOCH);
            (used, stem, pair)
        })
        .collect();
    by_use.sort_unstable_by_key(|(used, _, _)| *used);

    let low = size_bytes / 100 * LOW_WATERMARK_PERCENT;
    let over = total > size_bytes;
    for (used, stem, pair) in by_use {
        let incomplete = pair.halves < 2 && used < stale;
        if !incomplete && (!over || total <= low) {
            continue;
        }
        for ext in ["data", "map"] {
            if std::fs::remove_file(directory.join(format!("{stem}.{ext}"))).is_ok() {
                deleted += 1;
            }
        }
        total -= pair.bytes;
    }
    Ok((total, deleted))
}

/// The cache files of one remote file version, as a handle has them open.
struct CacheFiles {
    data: File,
    map: File,
}

impl CacheFiles {
    /// Opens or creates the pair and marks it as used now.
    fn open(directory: &Path, stem: &str, size: u64) -> std::io::Result<Self> {
        let open = |ext: &str, len: u64| -> std::io::Result<File> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(directory.join(format!("{stem}.{ext}")))?;
            if file.metadata()?.len() != len {
                file.set_len(len)?; // sparse; all workers set the same length
            }
            Ok(file)
        };
        let data = open("data", size)?;
        let map = open("map", size.div_ceil(CHUNK_SIZE))?;
        // the sweep deletes the least recently used files first
        let _ = map.set_modified(SystemTime::now());
        Ok(Self { data, map })
    }

    /// The map bytes of chunks `first..=last`; missing bytes, e.g. of a short map, read as 0.
    fn present(&self, first: u64, last: u64) -> Vec<u8> {
        let mut bytes = vec![0_u8; (last - first + 1) as usize];
        let mut read = 0;
        while read < bytes.len() {
            match self.map.read_at(&mut bytes[read..], first + read as u64) {
                Ok(0) | Err(_) => break,
                Ok(n) => read += n,
            }
        }
        bytes
    }

    fn allocated(&self) -> u64 {
        self.data.metadata().map_or(u64::MAX, |m| m.blocks() * 512)
    }
}

/// An open-file-description lock on bytes of a map, released on drop or when the process dies.
/// Each handle opens its own map, so handles of one process lock each other out, too.
struct ChunkLock<'a> {
    map: &'a File,
    start: u64,
    len: u64,
}

impl<'a> ChunkLock<'a> {
    /// Waits for the lock on map bytes `start..start + len`. `None` if the file system does not
    /// support such locks; the caller then reads without one and may download a chunk twice.
    fn wait(map: &'a File, start: u64, len: u64) -> Option<Self> {
        loop {
            match fcntl_lock(map, libc::F_OFD_SETLKW, libc::F_WRLCK, start, len) {
                Ok(()) => return Some(Self { map, start, len }),
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => return None,
            }
        }
    }
}

impl Drop for ChunkLock<'_> {
    fn drop(&mut self) {
        let _ = fcntl_lock(
            self.map,
            libc::F_OFD_SETLK,
            libc::F_UNLCK,
            self.start,
            self.len,
        );
    }
}

fn fcntl_lock(
    file: &File,
    command: c_int,
    kind: c_int,
    start: u64,
    len: u64,
) -> std::io::Result<()> {
    // SAFETY: an all-zero `flock` is valid; the fields below are set as `fcntl` expects
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = kind as libc::c_short;
    lock.l_whence = libc::SEEK_SET as libc::c_short;
    lock.l_start = start as libc::off_t;
    lock.l_len = len as libc::off_t;
    // SAFETY: a valid descriptor and a valid `flock`; OFD locks require `l_pid` 0
    if unsafe { libc::fcntl(file.as_raw_fd(), command, &raw const lock) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// An open file: the inner handle, its cache files, and the position.
struct Handle {
    cache: &'static ByteCache,
    inner: *mut gdal_sys::VSILFILE,
    /// `None` if they could not be opened, e.g. a full directory: reads then go to the inner file
    files: Option<CacheFiles>,
    size: u64,
    position: u64,
    eof: bool,
    /// when this handle last marked its file as used
    touched: Instant,
}

impl Handle {
    /// Reads `buffer.len()` bytes at `offset`, or fewer at the end of the file.
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> usize {
        let end = (offset + buffer.len() as u64).min(self.size);
        if offset >= end {
            return 0;
        }
        if self.touched.elapsed() >= TOUCH_INTERVAL {
            // a worker keeps its dataset open, so reads that hit the cache must mark it as used
            if let Some(files) = &self.files {
                let _ = files.map.set_modified(SystemTime::now());
            }
            self.touched = Instant::now();
        }
        let len = (end - offset) as usize;
        let Some(files) = &self.files else {
            return self
                .inner_read(offset, &mut buffer[..len])
                .map_or(0, |()| len);
        };
        let (first, last) = (offset / CHUNK_SIZE, (end - 1) / CHUNK_SIZE);
        let present = files.present(first, last);
        let hits: u64 = present.iter().map(|&b| u64::from(b == 1)).sum();
        self.cache.hits.fetch_add(hits, Ordering::Relaxed);

        // download each run of missing chunks once: under a lock on its map bytes, so that other
        // workers missing them wait and then read them
        let mut downloaded: Vec<(u64, Vec<u8>)> = Vec::new();
        for (a, b) in missing_runs(&present) {
            let (a, b) = (first + a, first + b);
            let _lock = ChunkLock::wait(&files.map, a, b - a + 1);
            let now = files.present(a, b);
            let missing: Vec<_> = missing_runs(&now).collect();
            let still = missing.iter().map(|(c, d)| d - c + 1).sum::<u64>();
            self.cache
                .waited
                .fetch_add(b - a + 1 - still, Ordering::Relaxed);
            for (c, d) in missing {
                let (c, d) = (a + c, a + d);
                let start = c * CHUNK_SIZE;
                let mut bytes = vec![0_u8; (d * CHUNK_SIZE + self.chunk_len(d) - start) as usize];
                if self.inner_read(start, &mut bytes).is_err() {
                    return 0;
                }
                self.cache.misses.fetch_add(d - c + 1, Ordering::Relaxed);
                // as many chunks as the file's share still holds; the rest is served, not stored
                let room = self.cache.max_file_bytes.saturating_sub(files.allocated()) / CHUNK_SIZE;
                let chunks = (d - c + 1).min(room);
                if chunks > 0 {
                    let stored_len = ((chunks * CHUNK_SIZE) as usize).min(bytes.len());
                    let stored = files.data.write_all_at(&bytes[..stored_len], start).is_ok()
                        && files
                            .map
                            .write_all_at(&vec![1_u8; chunks as usize], c)
                            .is_ok();
                    if !stored {
                        self.cache.failed_write();
                    }
                }
                downloaded.push((start, bytes));
            }
        }

        // the cached bytes from the data file, then the downloaded ones over them, so that a
        // chunk the cache could not store is still served
        if files
            .data
            .read_exact_at(&mut buffer[..len], offset)
            .is_err()
        {
            // e.g. a disk error: the cached bytes are unknown, so read the range from the source
            return self
                .inner_read(offset, &mut buffer[..len])
                .map_or(0, |()| len);
        }
        for (start, bytes) in downloaded {
            let from = offset.max(start);
            let to = end.min(start + bytes.len() as u64);
            if from < to {
                buffer[(from - offset) as usize..(to - offset) as usize]
                    .copy_from_slice(&bytes[(from - start) as usize..(to - start) as usize]);
            }
        }
        len
    }

    fn chunk_len(&self, index: u64) -> u64 {
        CHUNK_SIZE.min(self.size - index * CHUNK_SIZE)
    }

    /// Reads exactly `buffer.len()` bytes at `start` from the inner file.
    fn inner_read(&self, start: u64, buffer: &mut [u8]) -> Result<(), ()> {
        // SAFETY: `inner` is open until `close`, and `buffer` holds `buffer.len()` bytes
        let got = unsafe {
            if gdal_sys::VSIFSeekL(self.inner, start, 0 /* SEEK_SET */) != 0 {
                return Err(());
            }
            gdal_sys::VSIFReadL(buffer.as_mut_ptr().cast(), 1, buffer.len(), self.inner)
        };
        (got == buffer.len()).then_some(()).ok_or(())
    }
}

/// The runs `(first, last)` of consecutive chunks whose map byte says missing.
fn missing_runs(bytes: &[u8]) -> impl Iterator<Item = (u64, u64)> + '_ {
    let matches = |byte: u8| byte != 1;
    let mut i = 0;
    std::iter::from_fn(move || {
        while i < bytes.len() && !matches(bytes[i]) {
            i += 1;
        }
        if i == bytes.len() {
            return None;
        }
        let start = i;
        while i < bytes.len() && matches(bytes[i]) {
            i += 1;
        }
        Some((start as u64, i as u64 - 1))
    })
}

/// The lock even if a thread panicked while holding it: the map stays valid.
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The file name stem of a remote file version, remembered per process.
fn version(cache: &ByteCache, key: &str, inner: &CStr, size: u64) -> String {
    let id = (key.to_string(), inner.to_bytes().to_vec(), size);
    if let Some(stem) = lock(&cache.versions).get(&id) {
        return stem.clone();
    }
    let mut hash = Sha256::new();
    hash.update(key.as_bytes());
    hash.update([0]);
    hash.update(inner.to_bytes());
    hash.update([0]);
    hash.update(size.to_be_bytes());
    // SAFETY: GDAL returns a list owned by the caller or null; it is freed below. Asking may cost
    // a request, hence the memo.
    unsafe {
        let headers =
            gdal_sys::VSIGetFileMetadata(inner.as_ptr(), c"HEADERS".as_ptr(), std::ptr::null_mut());
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
                    hash.update(header);
                    hash.update([0]);
                }
                entry = entry.add(1);
            }
            gdal_sys::CSLDestroy(headers);
        }
    }
    let stem = hex(&hash.finalize());
    lock(&cache.versions).put(id, stem.clone());
    stem
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
        // with errors reported, so that a missing file stays "not found" (see `open_failed`)
        VSIErrorReset();
        let handle = gdal_sys::VSIFOpenExL(inner.as_ptr(), c"rb".as_ptr(), 1);
        if handle.is_null() {
            open_failed();
            return std::ptr::null_mut();
        }
        if gdal_sys::VSIFSeekL(handle, 0, 2 /* SEEK_END */) != 0 {
            gdal_sys::VSIFCloseL(handle);
            return std::ptr::null_mut();
        }
        (handle, gdal_sys::VSIFTellL(handle))
    };
    let stem = version(cache, key, &inner, size);
    let files = CacheFiles::open(&cache.directory, &stem, size)
        .inspect_err(|_| cache.failed_write())
        .ok();
    Box::into_raw(Box::new(Handle {
        cache,
        inner: handle,
        files,
        size,
        position: 0,
        eof: false,
        touched: Instant::now(),
    }))
    .cast()
}

/// Sets `errno` for GDAL, which reports a failed open of the plugin as `<path>: strerror(errno)`:
/// "No such file or directory" for a remote file that does not exist (HTTP 404), as the worker
/// tells missing files by that message, and an I/O error otherwise, not whatever `errno` was left.
///
/// # Safety
/// Call right after the inner open failed, before other GDAL calls.
unsafe fn open_failed() {
    // SAFETY: GDAL returns its last error message, valid until the next error
    let not_found = unsafe {
        let msg = VSIGetLastErrorMsg();
        !msg.is_null()
            && CStr::from_ptr(msg)
                .to_bytes()
                .starts_with(b"HTTP response code: 404")
    };
    // SAFETY: the calling thread's errno
    unsafe { *libc::__errno_location() = if not_found { libc::ENOENT } else { libc::EIO } };
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

/// The operators' bench HTTP range server, for tests of remote reads.
#[cfg(test)]
#[path = "../../operators/benches/common/range_server.rs"]
#[allow(dead_code)]
mod test_range_server;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;

    /// A file of the repository's test data.
    fn test_data(path: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../test_data")
            .join(path)
    }

    /// The handler is installed once per process, so all tests share one directory. One remote
    /// file may take at most [`TEST_FILE_CAP`].
    const TEST_FILE_CAP: u64 = 256 * CHUNK_SIZE;

    fn cache() -> &'static (tempfile::TempDir, &'static ByteCache) {
        static CACHE: OnceLock<(tempfile::TempDir, &'static ByteCache)> = OnceLock::new();
        CACHE.get_or_init(|| {
            let dir = tempfile::tempdir().unwrap();
            let cache = install(&ByteCacheConfig {
                directory: dir.path().join("cache"),
                size_bytes: 1 << 30,
                max_file_bytes: TEST_FILE_CAP,
            })
            .unwrap();
            (dir, cache)
        })
    }

    /// A key for tests: `n` as 64 hex digits.
    fn key(n: u64) -> String {
        format!("{n:064x}")
    }

    /// A file with `len` bytes that differ per position, in its own directory.
    fn file(len: usize) -> (tempfile::TempDir, PathBuf, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.bin");
        let bytes: Vec<u8> = (0..len).map(|i| (i * 7 + i / 251) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        (dir, path, bytes)
    }

    fn cached_path(path: &Path, n: u64) -> CString {
        // the plugin reads any inner path; `wrap` limits it to remote ones
        CString::new(format!("{PREFIX}{}{}", key(n), path.display())).unwrap()
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

    /// The cache files of a test file, opened as a handle opens them.
    fn files_of(path: &Path, n: u64, size: u64) -> CacheFiles {
        let (_dir, cache) = cache();
        let inner = CString::new(path.to_str().unwrap()).unwrap();
        CacheFiles::open(
            &cache.directory,
            &version(cache, &key(n), &inner, size),
            size,
        )
        .unwrap()
    }

    #[test]
    fn it_wraps_only_plain_remote_paths_with_a_full_key() {
        assert_eq!(
            wrap(Path::new("/vsis3/bucket/a.jp2"), &key(0xab)),
            Some(PathBuf::from(format!(
                "/vsigecache/{}/vsis3/bucket/a.jp2",
                key(0xab)
            )))
        );
        assert_eq!(wrap(Path::new("/vsis3/bucket/a.jp2"), "short"), None);
        assert_eq!(wrap(Path::new("/data/a.tif"), &key(1)), None);
        assert_eq!(
            wrap(Path::new("/vsizip//vsicurl/http://x/a.zip"), &key(1)),
            None
        );
        let wrapped = format!("{}/vsis3/bucket/a.jp2", key(0xab));
        assert_eq!(
            unwrap(&wrapped),
            Some((key(0xab).as_str(), "/vsis3/bucket/a.jp2"))
        );
        assert_eq!(unwrap(&format!("{PREFIX}{wrapped}")), unwrap(&wrapped));
        assert_eq!(unwrap("/vsigecache/short"), None);
    }

    #[test]
    fn it_reads_the_bytes_of_the_file_and_serves_them_again_from_the_data_file() {
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
        assert_eq!(files_of(&path, 1, len as u64).present(0, 3), [1, 1, 1, 1]);

        // other bytes of the same size in the file: the cache still serves the old ones
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
    fn the_data_file_is_sparse() {
        let _ = cache();
        let len = 40 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        let offset = 20 * CHUNK_SIZE as usize;
        assert_eq!(
            read_range(&cached_path(&path, 7), offset as u64, 100),
            bytes[offset..offset + 100]
        );
        let files = files_of(&path, 7, len as u64);
        assert_eq!(files.data.metadata().unwrap().len(), len as u64);
        assert!(
            files.allocated() <= 2 * CHUNK_SIZE,
            "only the cached chunk takes space"
        );
    }

    #[test]
    fn another_key_does_not_see_the_bytes() {
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
            assert!(!file.is_null());
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
    fn a_miss_waits_for_the_worker_that_downloads_the_same_chunks() {
        let _ = cache();
        let len = CHUNK_SIZE as usize * 2;
        let (_dir, path, _) = file(len);
        // another worker holds the lock on chunk 0 while it downloads
        let other = files_of(&path, 5, len as u64);
        let lock = ChunkLock::wait(&other.map, 0, 1).expect("OFD locks on the test file system");

        let cached = cached_path(&path, 5);
        let reader = std::thread::spawn(move || read_range(&cached, 0, 100));
        std::thread::sleep(Duration::from_millis(200));
        assert!(!reader.is_finished(), "the reader waits for the lock");

        // the other worker stores bytes the file does not have, then releases the lock
        let sentinel = vec![42_u8; CHUNK_SIZE as usize];
        other.data.write_all_at(&sentinel, 0).unwrap();
        other.map.write_all_at(&[1], 0).unwrap();
        drop(lock);

        assert_eq!(
            reader.join().unwrap(),
            sentinel[..100],
            "read the other worker's chunk"
        );
    }

    #[test]
    fn closing_the_file_releases_its_locks() {
        let _ = cache();
        let (_dir, path, _) = file(CHUNK_SIZE as usize);
        let first = files_of(&path, 6, CHUNK_SIZE);
        let held = ChunkLock::wait(&first.map, 0, 1).unwrap();
        let second = files_of(&path, 6, CHUNK_SIZE);
        assert!(fcntl_lock(&second.map, libc::F_OFD_SETLK, libc::F_WRLCK, 0, 1).is_err());
        std::mem::forget(held); // no unlock, as when a worker dies
        drop(first); // its descriptors close
        assert!(fcntl_lock(&second.map, libc::F_OFD_SETLK, libc::F_WRLCK, 0, 1).is_ok());
    }

    #[test]
    fn one_file_takes_at_most_its_share_but_reads_all_of_it() {
        let _ = cache();
        let len = 300 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        assert_eq!(read_range(&cached_path(&path, 8), 0, len), bytes);
        let files = files_of(&path, 8, len as u64);
        assert!(
            files.allocated() <= TEST_FILE_CAP,
            "at most the file's share"
        );
        let present = files.present(0, 299);
        assert_eq!(present.iter().map(|&b| u64::from(b == 1)).sum::<u64>(), 256);
    }

    #[test]
    fn a_reader_keeps_reading_a_file_the_sweep_deletes() {
        let (_, cache) = cache();
        let len = 3 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        let cached = cached_path(&path, 9);
        let inner = CString::new(path.to_str().unwrap()).unwrap();
        let stem = version(cache, &key(9), &inner, len as u64);
        unsafe {
            let file = gdal_sys::VSIFOpenL(cached.as_ptr(), c"rb".as_ptr());
            let mut buffer = vec![0_u8; 100];
            assert_eq!(
                gdal_sys::VSIFReadL(buffer.as_mut_ptr().cast(), 1, 100, file),
                100
            );
            // as the sweep deletes it, without touching the other tests' files
            for ext in ["data", "map"] {
                std::fs::remove_file(cache.directory.join(format!("{stem}.{ext}"))).unwrap();
            }
            assert_eq!(gdal_sys::VSIFSeekL(file, 2 * CHUNK_SIZE, 0), 0);
            assert_eq!(
                gdal_sys::VSIFReadL(buffer.as_mut_ptr().cast(), 1, 100, file),
                100
            );
            assert_eq!(
                buffer,
                bytes[2 * CHUNK_SIZE as usize..2 * CHUNK_SIZE as usize + 100]
            );
            gdal_sys::VSIFCloseL(file);
        }
    }

    #[test]
    fn a_raster_reads_the_same_through_the_cache() {
        let _ = cache();
        let path = test_data("raster/landcover/landcover.tif");
        let read = |path: &str| {
            gdal::Dataset::open(path)
                .unwrap()
                .rasterband(1)
                .unwrap()
                .read_band_as::<u8>()
                .unwrap()
                .data()
                .to_vec()
        };
        assert_eq!(
            read(path.to_str().unwrap()),
            read(cached_path(&path, 10).to_str().unwrap())
        );
    }

    /// The case the cache is for: a remote file, read again by a process whose curl cache does not
    /// hold it, downloads nothing but what it asks the server about the file.
    #[test]
    #[serial_test::serial]
    fn a_remote_file_read_again_downloads_nothing() {
        let _ = cache();
        let server = super::test_range_server::RangeServer::start(
            test_data("raster/landcover"),
            Duration::ZERO,
        );
        let remote = PathBuf::from(server.vsicurl_path("landcover.tif"));
        let cached = wrap(&remote, &key(11)).expect("a remote path");
        // process-wide, hence serial
        for (name, value) in [
            ("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif"),
            ("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR"),
            ("GDAL_HTTP_NETRC", "NO"),
        ] {
            gdal::config::set_config_option(name, value).unwrap();
        }
        let read = |path: &Path| {
            gdal::Dataset::open(path)
                .unwrap()
                .rasterband(1)
                .unwrap()
                .read_band_as::<u8>()
                .unwrap()
                .data()
                .to_vec()
        };
        let direct = read(&test_data("raster/landcover/landcover.tif"));

        assert_eq!(read(&cached), direct);
        let gets = server.gets();
        assert!(gets > 0);

        // as a new worker: GDAL's own cache of the file is gone
        unsafe { gdal_sys::VSICurlClearCache() };
        assert_eq!(read(&cached), direct);
        assert_eq!(server.gets(), gets, "no chunk downloaded again");
    }

    #[test]
    fn the_sweep_deletes_least_recently_used_files_down_to_the_low_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let pair = |stem: &str, chunks: u64, age_s: u64| {
            let files = CacheFiles::open(dir.path(), stem, chunks * CHUNK_SIZE).unwrap();
            files
                .data
                .write_all_at(&vec![1_u8; (chunks * CHUNK_SIZE) as usize], 0)
                .unwrap();
            let used = SystemTime::now() - Duration::from_secs(age_s);
            files.map.set_modified(used).unwrap();
            files.data.metadata().unwrap().blocks() * 512
                + files.map.metadata().unwrap().blocks() * 512
        };
        let (old, middle, recent, lost) = (key(1), key(2), key(3), key(4));
        let old_bytes = pair(&old, 4, 300);
        let middle_bytes = pair(&middle, 4, 200);
        let recent_bytes = pair(&recent, 4, 100);
        // a half of a pair, left by a worker that died while creating it
        let lost_data = dir.path().join(format!("{lost}.data"));
        std::fs::write(&lost_data, [0_u8; 10]).unwrap();
        File::options()
            .write(true)
            .open(&lost_data)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(120))
            .unwrap();

        // above the size: delete the oldest until at most 90% of it remain
        let (middle, recent) = (middle_bytes, recent_bytes);
        let size = old_bytes + middle + recent - 1;
        let (kept, deleted) = sweep(dir.path(), size).unwrap();
        assert!(!dir.path().join(format!("{old}.data")).exists());
        assert!(!dir.path().join(format!("{old}.map")).exists());
        assert!(!lost_data.exists());
        assert_eq!(kept, middle + recent);
        assert_eq!(deleted, 3);
        assert!(dir.path().join(format!("{}.data", key(2))).exists());

        // under the size: nothing goes
        assert_eq!(sweep(dir.path(), size).unwrap(), (middle + recent, 0));
        // size 0, as when clearing the caches: everything goes
        assert_eq!(sweep(dir.path(), 0).unwrap(), (0, 4));
    }

    /// A directory set to one with other files by mistake: the sweep and clearing the caches
    /// delete only the cache's own files, and the directory keeps its permissions.
    #[test]
    fn the_sweep_touches_only_files_the_cache_names() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let old = SystemTime::now() - Duration::from_secs(3600);
        let foreign = [
            dir.path().join("Settings.toml"),
            dir.path().join("scene.data"),
            dir.path().join(format!("{}.map", "A".repeat(64))),
        ];
        for path in &foreign {
            std::fs::write(path, [1_u8; 100]).unwrap();
            File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(old)
                .unwrap();
        }
        let folder = dir.path().join("0000000000000001");
        std::fs::create_dir(&folder).unwrap();
        std::fs::write(folder.join("0"), [1_u8; 100]).unwrap();
        let own = CacheFiles::open(dir.path(), &key(1), CHUNK_SIZE).unwrap();
        own.data.write_all_at(&[1_u8; 100], 0).unwrap();

        create_directory(dir.path()).unwrap();
        let (_, deleted) = sweep(dir.path(), 0).unwrap();

        assert_eq!(deleted, 2, "the cache's data and map");
        assert!(foreign.iter().all(|path| path.exists()));
        assert!(folder.join("0").exists());
        let mode = std::fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
    }

    /// A handle on a local file through cache files, as `open` builds it for a remote one.
    fn handle_on(path: &Path, n: u64, size: u64) -> Handle {
        let (_, cache) = cache();
        let inner = CString::new(path.to_str().unwrap()).unwrap();
        // SAFETY: a valid path; the test closes the handle
        let file = unsafe { gdal_sys::VSIFOpenL(inner.as_ptr(), c"rb".as_ptr()) };
        assert!(!file.is_null());
        Handle {
            cache,
            inner: file,
            files: Some(files_of(path, n, size)),
            size,
            position: 0,
            eof: false,
            touched: Instant::now(),
        }
    }

    fn close(handle: &Handle) {
        // SAFETY: opened in `handle_on`, closed once
        unsafe { gdal_sys::VSIFCloseL(handle.inner) };
    }

    #[test]
    fn reads_from_the_cache_mark_the_file_as_used() {
        let len = 2 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        let mut handle = handle_on(&path, 12, len as u64);
        let mut buffer = vec![0_u8; 100];
        assert_eq!(handle.read_at(0, &mut buffer), 100); // now cached
        let long_ago = SystemTime::now() - Duration::from_secs(3600);
        let map = &handle.files.as_ref().unwrap().map;
        map.set_modified(long_ago).unwrap();

        // a hit after the interval marks the file as used again
        handle.touched = Instant::now().checked_sub(TOUCH_INTERVAL).unwrap();
        assert_eq!(handle.read_at(0, &mut buffer), 100);
        assert_eq!(buffer, bytes[..100]);
        let used = handle
            .files
            .as_ref()
            .unwrap()
            .map
            .metadata()
            .unwrap()
            .modified()
            .unwrap();
        assert!(used > long_ago + Duration::from_mins(50));
        close(&handle);
    }

    #[test]
    fn a_failed_read_of_the_cache_reads_the_source() {
        let len = 2 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        let mut handle = handle_on(&path, 13, len as u64);
        let mut buffer = vec![0_u8; 100];
        assert_eq!(handle.read_at(10, &mut buffer), 100); // now cached
        // the map says cached, but the data file cannot deliver it
        handle.files.as_ref().unwrap().data.set_len(0).unwrap();

        buffer.fill(0);
        assert_eq!(handle.read_at(10, &mut buffer), 100);
        assert_eq!(buffer, bytes[10..110]);
        close(&handle);
    }
}
