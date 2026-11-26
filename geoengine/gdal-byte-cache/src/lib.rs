//! A cache of remote file bytes that all GDAL workers share.
//!
//! Each worker has its own curl cache, so a file region one worker downloaded is downloaded again
//! by the next worker that reads it, e.g. the blocks of a finer level after zooming in. A worker
//! reads remote files through the GDAL file system `/vsigecache/`, which serves the bytes this
//! cache holds and reads the missing ones through the remote file system as before, so
//! authentication, retries and GDAL's network statistics stay unchanged.
//!
//! Per version of a remote file, the cache directory holds one sparse file `<version>.cache`: the
//! remote file's bytes at their offsets `0..size`, each cached chunk of [`CHUNK_SIZE`] bytes in
//! place, followed by a map with one byte per chunk, 1 if the chunk is cached. Data and map share
//! the file, so a worker can never see the data of one version with the map of another.
//!
//! The map, not the file system's holes, says what is cached: file systems may add or remove
//! zero blocks of a sparse file. A chunk is written before its map byte, so a reader that sees
//! the byte sees the data. Nothing is synced to disk, so the server empties the directory when
//! it starts ([`sweep`] with size 0). Workers that miss the same chunks take an
//! open-file-description lock on their map bytes, so one downloads and the others wait and then
//! read its bytes. The kernel releases the lock when a worker dies.
//!
//! The server deletes whole files, least recently used first (by their modification time, which
//! every open and, once a minute, every read through an open handle sets), when the directory
//! exceeds its size. A reader that still has a deleted file open keeps reading it. One remote
//! file may take at most `max_file_bytes`, so that reading a whole large file cannot empty the
//! cache.
//!
//! A path is `/vsigecache/<key>/vsicurl/...`: the key is a SHA-256 of the dataset's path and GDAL
//! options, which may carry credentials, so a dataset never reads bytes another dataset was
//! allowed to download but it is not. The version adds the file's size and modification time
//! (`Last-Modified`), so a changed file gets a new file. The worker that asked the remote file
//! system for a version shares it with the others through a symbolic link `<hash>.version` whose
//! target is the version. For [`VERSION_CHECK_INTERVAL`], opening a known file costs no request,
//! and the remote file is only opened when a chunk is missing. After that time, or at once after
//! [`ByteCache::forget`], a worker asks again.

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

/// Length of a key and of a file name stem in hex digits: a SHA-256.
const KEY_LEN: usize = 64;

/// The extension of the cache's files.
const EXTENSION: &str = "cache";

/// The extension of the links through which workers share the versions of remote files.
const VERSION_EXTENSION: &str = "version";

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
/// version once per [`VERSION_CHECK_INTERVAL`], not for every handle (GDAL opens one per decoding
/// thread).
const REMEMBERED_FILES: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(4096).expect("not zero");

/// How long a process trusts a file's version before asking the remote file system again, so that
/// a file replaced by one of the same size gets a new cache file within this time.
pub const VERSION_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// The sweep deletes down to this share of the size, so that it does not delete on every run.
const LOW_WATERMARK_PERCENT: u64 = 90;

/// How often an open handle marks its file as used, so that the sweep keeps files that are read
/// from the cache through handles that stay open.
const TOUCH_INTERVAL: Duration = Duration::from_secs(60);

/// The longest pause between two tries of a lock another worker holds.
const LOCK_POLL_MAX: Duration = Duration::from_millis(10);

/// `VSIErrorNum` of a missing object in an object store (`cpl_vsi_error.h`).
const VSIE_OBJECT_NOT_FOUND: c_int = 8;

// Public GDAL C API, not in the gdal-sys bindings.
unsafe extern "C" {
    fn VSIErrorReset();
    fn VSIGetLastErrorNo() -> c_int;
    fn VSIGetLastErrorMsg() -> *const c_char;
}

/// Where the cache lives and how large it and each remote file in it may grow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteCacheConfig {
    pub directory: PathBuf,
    pub size_bytes: u64,
    /// the most one remote file may take
    pub max_file_bytes: u64,
    /// how long a worker waits for chunks another worker downloads before it downloads them
    /// itself, so that a download stuck in retries does not stall every worker that needs it
    pub lock_wait: Duration,
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

/// A remote file as the cache tells them apart: key and inner path.
type FileId = (String, Vec<u8>);

/// A version of a remote file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Version {
    size: u64,
    /// seconds since the epoch, from the remote file system's `Last-Modified`; 0 if unknown
    mtime: i64,
    /// the name stem of its cache file
    stem: String,
}

/// The cache of this process.
#[derive(Debug)]
pub struct ByteCache {
    directory: PathBuf,
    max_file_bytes: u64,
    lock_wait: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
    waited: AtomicU64,
    write_failures: AtomicU64,
    /// version and when it was checked, by key and inner path
    versions: std::sync::Mutex<lru::LruCache<FileId, (Version, Instant)>>,
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

    /// Forgets the versions of the remote file `inner` (e.g. `/vsicurl/https://...`), so that the
    /// next open asks for its version again. For a read that failed, e.g. because the file was
    /// replaced by one of the same size.
    pub fn forget(&self, inner: &Path) {
        let inner = inner.as_os_str().as_encoded_bytes();
        let mut versions = lock(&self.versions);
        let ids: Vec<FileId> = versions
            .iter()
            .filter(|((_, path), _)| path == inner)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            // the other workers must not take the version from the link either
            let _ = std::fs::remove_file(version_link(self, &id.0, &id.1));
            versions.pop(&id);
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
        lock_wait: config.lock_wait,
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
        (*callbacks).read_dir = Some(read_dir);
        (*callbacks).sibling_files = Some(sibling_files);
        (*callbacks).open = Some(open);
        (*callbacks).tell = Some(tell);
        (*callbacks).seek = Some(seek);
        (*callbacks).read = Some(read);
        (*callbacks).read_multi_range = Some(read_multi_range);
        (*callbacks).eof = Some(eof);
        (*callbacks).close = Some(close);
        // no buffering on top: the cache file and the page cache are the buffer
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

/// The inner path of a path GDAL passes a callback, for the inner file system.
///
/// # Safety
/// `filename` must be a null-terminated string.
unsafe fn inner_path(filename: *const c_char) -> Option<CString> {
    // SAFETY: as the caller promises
    let filename = unsafe { CStr::from_ptr(filename) };
    let (_, inner) = unwrap(filename.to_str().ok()?)?;
    CString::new(inner).ok()
}

/// Deletes the least recently used remote files while the directory holds more than
/// `size_bytes`, down to [`LOW_WATERMARK_PERCENT`] of it; with a size of 0, all of them. Counts
/// the allocated blocks, not the length, as the files are sparse. Touches only files the cache
/// names ([`is_cache_stem`]). Also deletes version links older than [`VERSION_CHECK_INTERVAL`].
/// Returns the bytes kept and the files deleted.
///
/// # Errors
/// Fails if the directory cannot be listed.
pub fn sweep(directory: &Path, size_bytes: u64) -> std::io::Result<(u64, u64)> {
    let mut files: Vec<(SystemTime, u64, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(directory)?.flatten() {
        let path = entry.path();
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let stem_ok = path
            .file_stem()
            .and_then(|s| s.to_str())
            .is_some_and(is_cache_stem);
        if meta.is_symlink() && stem_ok && path.extension().is_some_and(|e| e == VERSION_EXTENSION)
        {
            let outdated = meta
                .modified()
                .ok()
                .and_then(|m| m.elapsed().ok())
                .is_none_or(|age| age >= VERSION_CHECK_INTERVAL);
            if outdated || size_bytes == 0 {
                let _ = std::fs::remove_file(&path);
            }
            continue;
        }
        let ours = meta.is_file()
            && path.extension().is_some_and(|e| e == EXTENSION)
            && path
                .file_stem()
                .and_then(|s| s.to_str())
                .is_some_and(is_cache_stem);
        if ours {
            let used = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            files.push((used, meta.blocks() * 512, path));
        }
    }
    let mut total: u64 = files.iter().map(|(_, bytes, _)| bytes).sum();
    if total <= size_bytes && size_bytes > 0 {
        return Ok((total, 0));
    }
    files.sort_unstable_by_key(|(used, _, _)| *used);
    let low = size_bytes / 100 * LOW_WATERMARK_PERCENT;
    let mut deleted = 0;
    for (_, bytes, path) in files {
        if total <= low && size_bytes > 0 {
            break;
        }
        if std::fs::remove_file(path).is_ok() {
            total -= bytes;
            deleted += 1;
        }
    }
    Ok((total, deleted))
}

/// The cache file of one remote file version, as a handle has it open: the remote file's bytes
/// at `0..size`, then the map, one byte per chunk.
struct CacheFile {
    file: File,
    size: u64,
}

impl CacheFile {
    /// Opens or creates the file and marks it as used now.
    fn open(directory: &Path, stem: &str, size: u64) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join(format!("{stem}.{EXTENSION}")))?;
        let len = size + size.div_ceil(CHUNK_SIZE);
        if file.metadata()?.len() != len {
            file.set_len(len)?; // sparse; all workers set the same length
        }
        // the sweep deletes the least recently used files first
        let _ = file.set_modified(SystemTime::now());
        Ok(Self { file, size })
    }

    /// The map bytes of chunks `first..=last`; missing bytes, e.g. of a short file, read as 0.
    fn present(&self, first: u64, last: u64) -> Vec<u8> {
        let mut bytes = vec![0_u8; (last - first + 1) as usize];
        let mut read = 0;
        while read < bytes.len() {
            match self
                .file
                .read_at(&mut bytes[read..], self.size + first + read as u64)
            {
                Ok(0) | Err(_) => break,
                Ok(n) => read += n,
            }
        }
        bytes
    }

    /// Stores `chunks` whole chunks of `bytes` at chunk `first`: the data, then the map bytes.
    fn store(&self, first: u64, chunks: u64, bytes: &[u8]) -> std::io::Result<()> {
        let len = ((chunks * CHUNK_SIZE) as usize).min(bytes.len());
        self.file.write_all_at(&bytes[..len], first * CHUNK_SIZE)?;
        self.file
            .write_all_at(&vec![1_u8; chunks as usize], self.size + first)
    }

    /// Waits at most `limit` for the lock on the map bytes of chunks `first..first + count`.
    fn lock(&self, first: u64, count: u64, limit: Duration) -> Option<ChunkLock<'_>> {
        ChunkLock::wait(&self.file, self.size + first, count, limit)
    }

    fn allocated(&self) -> u64 {
        self.file.metadata().map_or(u64::MAX, |m| m.blocks() * 512)
    }
}

/// An open-file-description lock on bytes of a file, released on drop or when the process dies.
/// Each handle opens its own file, so handles of one process lock each other out, too.
struct ChunkLock<'a> {
    file: &'a File,
    start: u64,
    len: u64,
}

impl<'a> ChunkLock<'a> {
    /// Waits at most `limit` for the lock on bytes `start..start + len`. `None` if it is still
    /// held then, or the file system does not support such locks; the caller then reads without
    /// one and may download a chunk twice. Tries without blocking, as a blocking wait cannot end
    /// early.
    fn wait(file: &'a File, start: u64, len: u64, limit: Duration) -> Option<Self> {
        let deadline = Instant::now() + limit;
        let mut pause = Duration::from_millis(1);
        loop {
            match fcntl_lock(file, libc::F_OFD_SETLK, libc::F_WRLCK, start, len) {
                Ok(()) => return Some(Self { file, start, len }),
                Err(err)
                    if matches!(err.raw_os_error(), Some(libc::EAGAIN | libc::EACCES))
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(pause);
                    pause = (pause * 2).min(LOCK_POLL_MAX);
                }
                Err(_) => return None,
            }
        }
    }
}

impl Drop for ChunkLock<'_> {
    fn drop(&mut self) {
        let _ = fcntl_lock(
            self.file,
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

/// An open file: the inner handle, its cache file, and the position.
struct Handle {
    cache: &'static ByteCache,
    /// the remote file's path for the inner file system
    path: CString,
    /// opened at the first missing chunk, null before: reads from the cache need no request
    inner: std::cell::Cell<*mut gdal_sys::VSILFILE>,
    /// `None` if it could not be opened, e.g. a full directory: reads then go to the inner file
    file: Option<CacheFile>,
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
        let len = (end - offset) as usize;
        if self.touched.elapsed() >= TOUCH_INTERVAL {
            // a worker keeps its dataset open, so reads that hit the cache must mark it as used
            if let Some(file) = &self.file {
                let _ = file.file.set_modified(SystemTime::now());
            }
            self.touched = Instant::now();
        }
        let Some(file) = &self.file else {
            return self
                .inner_read(offset, &mut buffer[..len])
                .map_or(0, |()| len);
        };
        let (first, last) = (offset / CHUNK_SIZE, (end - 1) / CHUNK_SIZE);
        let present = file.present(first, last);
        let hits: u64 = present.iter().map(|&b| u64::from(b == 1)).sum();
        self.cache.hits.fetch_add(hits, Ordering::Relaxed);

        // download each run of missing chunks once: under a lock on its map bytes, so that other
        // workers missing them wait and then read them. Downloaded bytes go straight to the
        // caller's buffer.
        let mut downloaded = vec![false; present.len()];
        for (a, b) in runs(present.len(), |i| present[i] != 1) {
            let (a, b) = (first + a, first + b);
            let _lock = file.lock(a, b - a + 1, self.cache.lock_wait);
            let now = file.present(a, b);
            let missing = runs(now.len(), |i| now[i] != 1);
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
                // as many chunks as the file's share still holds; the rest is served, not stored.
                // shortcut: workers storing runs of one file at once may each see room and exceed
                // its share by their runs; lock the whole map while checking if that matters.
                let room = self.cache.max_file_bytes.saturating_sub(file.allocated()) / CHUNK_SIZE;
                let chunks = (d - c + 1).min(room);
                if chunks > 0 && file.store(c, chunks, &bytes).is_err() {
                    self.cache.failed_write();
                }
                let (from, to) = (offset.max(start), end.min(start + bytes.len() as u64));
                buffer[(from - offset) as usize..(to - offset) as usize]
                    .copy_from_slice(&bytes[(from - start) as usize..(to - start) as usize]);
                for chunk in c..=d {
                    downloaded[(chunk - first) as usize] = true;
                }
            }
        }

        // the chunks this read did not download, from the cache file
        for (a, b) in runs(downloaded.len(), |i| !downloaded[i]) {
            let from = offset.max((first + a) * CHUNK_SIZE);
            let to = end.min((first + b) * CHUNK_SIZE + self.chunk_len(first + b));
            let part = &mut buffer[(from - offset) as usize..(to - offset) as usize];
            if file.file.read_exact_at(part, from).is_err()
                // e.g. a disk error: the cached bytes are unknown, so read them from the source
                && self.inner_read(from, part).is_err()
            {
                return 0;
            }
        }
        len
    }

    fn chunk_len(&self, index: u64) -> u64 {
        CHUNK_SIZE.min(self.size - index * CHUNK_SIZE)
    }

    /// Reads exactly `buffer.len()` bytes at `start` from the inner file, which it opens first if
    /// this handle has not yet.
    fn inner_read(&self, start: u64, buffer: &mut [u8]) -> Result<(), ()> {
        if self.inner.get().is_null() {
            // SAFETY: a valid path; the handle is closed in `close`. The size is known, so the open
            // need not download the file's start (`GDAL_INGESTED_BYTES_AT_OPEN`, at least one chunk)
            let inner = unsafe {
                with_thread_local_option(c"GDAL_INGESTED_BYTES_AT_OPEN", c"1024", || {
                    gdal_sys::VSIFOpenExL(self.path.as_ptr(), c"rb".as_ptr(), 0)
                })
            };
            if inner.is_null() {
                return Err(());
            }
            self.inner.set(inner);
        }
        let inner = self.inner.get();
        // SAFETY: `inner` is open until `close`, and `buffer` holds `buffer.len()` bytes
        let got = unsafe {
            if gdal_sys::VSIFSeekL(inner, start, 0 /* SEEK_SET */) != 0 {
                return Err(());
            }
            gdal_sys::VSIFReadL(buffer.as_mut_ptr().cast(), 1, buffer.len(), inner)
        };
        (got == buffer.len()).then_some(()).ok_or(())
    }
}

/// The runs `(first, last)` of consecutive indices below `len` that `pick` selects.
fn runs(len: usize, pick: impl Fn(usize) -> bool) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < len {
        if pick(i) {
            let start = i;
            while i < len && pick(i) {
                i += 1;
            }
            out.push((start as u64, i as u64 - 1));
        } else {
            i += 1;
        }
    }
    out
}

/// The lock even if a thread panicked while holding it: the map stays valid.
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The link through which workers share the version of a remote file: a SHA-256 of key and inner
/// path.
fn version_link(cache: &ByteCache, key: &str, inner: &[u8]) -> PathBuf {
    let mut hash = Sha256::new();
    hash.update(key.as_bytes());
    hash.update([0]);
    hash.update(inner);
    cache
        .directory
        .join(format!("{}.{VERSION_EXTENSION}", hex(&hash.finalize())))
}

/// The version of a remote file that this or another worker checked less than
/// [`VERSION_CHECK_INTERVAL`] ago.
fn known(cache: &ByteCache, key: &str, inner: &CStr) -> Option<Version> {
    let id = (key.to_string(), inner.to_bytes().to_vec());
    if let Some((version, checked)) = lock(&cache.versions).get(&id)
        && checked.elapsed() < VERSION_CHECK_INTERVAL
    {
        return Some(version.clone());
    }
    let link = version_link(cache, key, inner.to_bytes());
    let age = std::fs::symlink_metadata(&link)
        .ok()?
        .modified()
        .ok()?
        .elapsed()
        .ok()?;
    if age >= VERSION_CHECK_INTERVAL {
        return None;
    }
    let target = std::fs::read_link(&link).ok()?;
    let mut parts = target.to_str()?.split(' ');
    let version = Version {
        size: parts.next()?.parse().ok()?,
        mtime: parts.next()?.parse().ok()?,
        stem: parts.next().filter(|stem| is_cache_stem(stem))?.to_string(),
    };
    let checked = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
    lock(&cache.versions).put(id, (version.clone(), checked));
    Some(version)
}

/// Remembers the version of a remote file that the inner file system just reported, for this
/// worker and, through its link, for the others.
fn remember(cache: &ByteCache, key: &str, inner: &CStr, stat: &libc::stat64) -> Version {
    static TEMPORARY: AtomicU64 = AtomicU64::new(0);
    let (size, mtime) = (stat.st_size as u64, stat.st_mtime);
    let mut hash = Sha256::new();
    hash.update(key.as_bytes());
    hash.update([0]);
    hash.update(inner.to_bytes());
    hash.update([0]);
    hash.update(size.to_be_bytes());
    hash.update(mtime.to_be_bytes());
    let version = Version {
        size,
        mtime,
        stem: hex(&hash.finalize()),
    };
    lock(&cache.versions).put(
        (key.to_string(), inner.to_bytes().to_vec()),
        (version.clone(), Instant::now()),
    );
    // replaced at once by a rename, so another worker reads the old or the new version
    let link = version_link(cache, key, inner.to_bytes());
    let temporary = link.with_extension(format!(
        "{}-{}",
        std::process::id(),
        TEMPORARY.fetch_add(1, Ordering::Relaxed)
    ));
    let target = format!("{size} {mtime} {}", version.stem);
    if std::os::unix::fs::symlink(target, &temporary).is_ok()
        && std::fs::rename(&temporary, &link).is_err()
    {
        let _ = std::fs::remove_file(&temporary);
    }
    version
}

/// Runs `f` with the GDAL config option `name` set to `value` in this thread.
///
/// # Safety
/// `f` must not change the option itself.
unsafe fn with_thread_local_option<R>(name: &CStr, value: &CStr, f: impl FnOnce() -> R) -> R {
    // SAFETY: valid strings; GDAL copies them, and the old value is copied before it is replaced
    unsafe {
        let old = gdal_sys::CPLGetThreadLocalConfigOption(name.as_ptr(), std::ptr::null());
        let old = (!old.is_null()).then(|| CStr::from_ptr(old).to_owned());
        gdal_sys::CPLSetThreadLocalConfigOption(name.as_ptr(), value.as_ptr());
        let result = f();
        gdal_sys::CPLSetThreadLocalConfigOption(
            name.as_ptr(),
            old.as_ref().map_or(std::ptr::null(), |old| old.as_ptr()),
        );
        result
    }
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
    let (version, handle) = match known(cache, key, &inner) {
        Some(version) => (version, std::ptr::null_mut()),
        None => {
            // SAFETY: a valid path and buffer; the handle is closed in `close` or below. Opened as
            // without the cache, with errors reported, so that a missing file stays "not found"
            // (see `open_failed`); the stat after it is answered from what the open learned.
            unsafe {
                VSIErrorReset();
                let handle = gdal_sys::VSIFOpenExL(inner.as_ptr(), c"rb".as_ptr(), 1);
                if handle.is_null() {
                    open_failed();
                    return std::ptr::null_mut();
                }
                let mut stat: libc::stat64 = std::mem::zeroed();
                if gdal_sys::VSIStatExL(inner.as_ptr(), (&raw mut stat).cast(), 0) != 0
                    || stat.st_mode & libc::S_IFMT != libc::S_IFREG
                {
                    gdal_sys::VSIFCloseL(handle);
                    return std::ptr::null_mut();
                }
                (remember(cache, key, &inner, &stat), handle)
            }
        }
    };
    let size = version.size;
    let file = CacheFile::open(&cache.directory, &version.stem, size)
        .inspect_err(|_| cache.failed_write())
        .ok();
    Box::into_raw(Box::new(Handle {
        cache,
        path: inner,
        inner: std::cell::Cell::new(handle),
        file,
        size,
        position: 0,
        eof: false,
        touched: Instant::now(),
    }))
    .cast()
}

/// Sets `errno` for GDAL, which reports a failed open of the plugin as `<path>: strerror(errno)`:
/// "No such file or directory" for a remote file that does not exist (HTTP 404, or an object
/// store's "object not found"), as the worker tells missing files by that message, and an I/O
/// error otherwise, not whatever `errno` was left.
///
/// # Safety
/// Call right after the inner open failed, before other GDAL calls.
unsafe fn open_failed() {
    // SAFETY: GDAL returns its last error and message, the message valid until the next error
    let not_found = unsafe {
        let msg = VSIGetLastErrorMsg();
        VSIGetLastErrorNo() == VSIE_OBJECT_NOT_FOUND
            || (!msg.is_null()
                && CStr::from_ptr(msg)
                    .to_bytes()
                    .starts_with(b"HTTP response code: 404"))
    };
    // SAFETY: the calling thread's errno
    unsafe { *libc::__errno_location() = if not_found { libc::ENOENT } else { libc::EIO } };
}

/// The size and modification time of a file whose version is known, without a request; else what
/// the inner file system says, and the version is remembered.
unsafe extern "C" fn stat(
    user_data: *mut c_void,
    filename: *const c_char,
    stat_buf: *mut gdal_sys::VSIStatBufL,
    flags: c_int,
) -> c_int {
    // SAFETY: GDAL passes the pointer given at install and a null-terminated string
    let (cache, filename) = unsafe { (&*user_data.cast::<ByteCache>(), CStr::from_ptr(filename)) };
    let Some((key, inner)) = filename.to_str().ok().and_then(unwrap) else {
        return -1;
    };
    let Ok(inner) = CString::new(inner) else {
        return -1;
    };
    // `VSIStatBufL` is `stat64` on Linux, opaque in the bindings
    let buffer = stat_buf.cast::<libc::stat64>();
    if let Some(version) = known(cache, key, &inner) {
        // SAFETY: GDAL passes a buffer for one stat
        unsafe {
            let stat_buf = buffer;
            std::ptr::write_bytes(stat_buf, 0, 1);
            (*stat_buf).st_size = version.size as _;
            (*stat_buf).st_mtime = version.mtime;
            (*stat_buf).st_mode = libc::S_IFREG | 0o444;
        }
        return 0;
    }
    // SAFETY: the buffer is GDAL's, filled by the inner file system
    let result = unsafe { gdal_sys::VSIStatExL(inner.as_ptr(), stat_buf, flags) };
    // SAFETY: filled, as the stat succeeded
    if result == 0 && unsafe { (*buffer).st_mode } & libc::S_IFMT == libc::S_IFREG {
        remember(cache, key, &inner, unsafe { &*buffer });
    }
    result
}

/// The listing of the inner directory, so that drivers find sibling files (`.ovr`, `.aux.xml`)
/// by one listing, as without the cache, and not by one request per candidate.
unsafe extern "C" fn read_dir(
    _user_data: *mut c_void,
    dirname: *const c_char,
    max_files: c_int,
) -> *mut *mut c_char {
    // SAFETY: GDAL passes a null-terminated string and takes ownership of the returned list
    match unsafe { inner_path(dirname) } {
        Some(inner) => unsafe { gdal_sys::VSIReadDirEx(inner.as_ptr(), max_files) },
        None => std::ptr::null_mut(),
    }
}

/// The sibling files the inner file system knows, for the same reason as [`read_dir`].
unsafe extern "C" fn sibling_files(
    _user_data: *mut c_void,
    filename: *const c_char,
) -> *mut *mut c_char {
    // SAFETY: GDAL passes a null-terminated string and takes ownership of the returned list
    match unsafe { inner_path(filename) } {
        Some(inner) => unsafe { gdal_sys::VSISiblingFiles(inner.as_ptr()) },
        None => std::ptr::null_mut(),
    }
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
    let inner = h.inner.get();
    if inner.is_null() {
        return 0;
    }
    unsafe { gdal_sys::VSIFCloseL(inner) }
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

    /// How long the tests' workers wait for another worker's lock.
    const TEST_LOCK_WAIT: Duration = Duration::from_secs(1);

    fn cache() -> &'static (tempfile::TempDir, &'static ByteCache) {
        static CACHE: OnceLock<(tempfile::TempDir, &'static ByteCache)> = OnceLock::new();
        CACHE.get_or_init(|| {
            let dir = tempfile::tempdir().unwrap();
            let cache = install(&ByteCacheConfig {
                directory: dir.path().join("cache"),
                size_bytes: 1 << 30,
                max_file_bytes: TEST_FILE_CAP,
                lock_wait: TEST_LOCK_WAIT,
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

    /// The version of a test file, as an open finds it.
    fn version_of(path: &Path, n: u64) -> Version {
        let (_dir, cache) = cache();
        let inner = CString::new(path.to_str().unwrap()).unwrap();
        // SAFETY: a valid path and buffer
        let stat = unsafe {
            let mut stat: libc::stat64 = std::mem::zeroed();
            assert_eq!(
                gdal_sys::VSIStatExL(inner.as_ptr(), (&raw mut stat).cast(), 0),
                0
            );
            stat
        };
        remember(cache, &key(n), &inner, &stat)
    }

    /// The cache file of a test file, opened as a handle opens it.
    fn file_of(path: &Path, n: u64, size: u64) -> CacheFile {
        let (_dir, cache) = cache();
        CacheFile::open(&cache.directory, &version_of(path, n).stem, size).unwrap()
    }

    /// A handle on a local file through a cache file, as `open` builds it for a remote one.
    fn handle_on(path: &Path, n: u64, size: u64) -> Handle {
        let (_, cache) = cache();
        let inner = CString::new(path.to_str().unwrap()).unwrap();
        // SAFETY: a valid path; the test closes the handle
        let file = unsafe { gdal_sys::VSIFOpenL(inner.as_ptr(), c"rb".as_ptr()) };
        assert!(!file.is_null());
        Handle {
            cache,
            path: inner,
            inner: std::cell::Cell::new(file),
            file: Some(file_of(path, n, size)),
            size,
            position: 0,
            eof: false,
            touched: Instant::now(),
        }
    }

    fn close(handle: &Handle) {
        // SAFETY: opened in `handle_on`, closed once
        unsafe { gdal_sys::VSIFCloseL(handle.inner.get()) };
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
    fn it_reads_the_bytes_of_the_file_and_serves_them_again_from_the_cache_file() {
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
        assert_eq!(file_of(&path, 1, len as u64).present(0, 3), [1, 1, 1, 1]);

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
    fn it_keeps_the_cache_file_sparse() {
        let _ = cache();
        let len = 40 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        let offset = 20 * CHUNK_SIZE as usize;
        assert_eq!(
            read_range(&cached_path(&path, 7), offset as u64, 100),
            bytes[offset..offset + 100]
        );
        let file = file_of(&path, 7, len as u64);
        assert_eq!(file.file.metadata().unwrap().len(), len as u64 + 40);
        assert!(
            file.allocated() <= 3 * CHUNK_SIZE,
            "only the cached chunk and the map take space"
        );
    }

    #[test]
    fn it_does_not_share_bytes_between_keys() {
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
    fn it_waits_for_the_worker_that_downloads_the_same_chunks() {
        let _ = cache();
        let len = CHUNK_SIZE as usize * 2;
        let (_dir, path, _) = file(len);
        // another worker holds the lock on chunk 0 while it downloads
        let other = file_of(&path, 5, len as u64);
        let lock = other
            .lock(0, 1, Duration::ZERO)
            .expect("OFD locks on the test file system");

        let cached = cached_path(&path, 5);
        let reader = std::thread::spawn(move || read_range(&cached, 0, 100));
        std::thread::sleep(Duration::from_millis(200));
        assert!(!reader.is_finished(), "the reader waits for the lock");

        // the other worker stores bytes the file does not have, then releases the lock
        let sentinel = vec![42_u8; CHUNK_SIZE as usize];
        other.store(0, 1, &sentinel).unwrap();
        drop(lock);

        assert_eq!(
            reader.join().unwrap(),
            sentinel[..100],
            "read the other worker's chunk"
        );
    }

    /// A worker whose download hangs, e.g. in retries, holds its lock: the others wait at most
    /// the limit, then read the chunks themselves.
    #[test]
    fn it_reads_the_chunks_itself_after_waiting_the_limit() {
        let _ = cache();
        let len = CHUNK_SIZE as usize * 2;
        let (_dir, path, bytes) = file(len);
        let other = file_of(&path, 17, len as u64);
        let _hanging = other.lock(0, 1, Duration::ZERO).unwrap();

        let start = Instant::now();
        assert_eq!(read_range(&cached_path(&path, 17), 0, 100), bytes[..100]);
        let waited = start.elapsed();
        assert!(waited >= TEST_LOCK_WAIT, "waited {waited:?}");
        assert!(waited < TEST_LOCK_WAIT * 3, "waited {waited:?}");
    }

    #[test]
    fn it_releases_the_locks_of_a_closed_file() {
        let _ = cache();
        let (_dir, path, _) = file(CHUNK_SIZE as usize);
        let first = file_of(&path, 6, CHUNK_SIZE);
        let held = first.lock(0, 1, Duration::ZERO).unwrap();
        let second = file_of(&path, 6, CHUNK_SIZE);
        let try_lock = |file: &CacheFile| {
            fcntl_lock(&file.file, libc::F_OFD_SETLK, libc::F_WRLCK, CHUNK_SIZE, 1)
        };
        assert!(try_lock(&second).is_err());
        std::mem::forget(held); // no unlock, as when a worker dies
        drop(first); // its descriptor closes
        assert!(try_lock(&second).is_ok());
    }

    #[test]
    fn it_stores_at_most_the_share_of_one_file_but_reads_all_of_it() {
        let _ = cache();
        let len = 300 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        assert_eq!(read_range(&cached_path(&path, 8), 0, len), bytes);
        let file = file_of(&path, 8, len as u64);
        assert!(
            file.allocated() <= TEST_FILE_CAP + CHUNK_SIZE,
            "at most the file's share and its map"
        );
        let present = file.present(0, 299);
        let stored = present.iter().map(|&b| u64::from(b == 1)).sum::<u64>();
        assert!((250..=256).contains(&stored), "{stored} chunks stored");
    }

    #[test]
    fn it_keeps_reading_a_file_the_sweep_deletes() {
        let (_, cache) = cache();
        let len = 3 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        let cached = cached_path(&path, 9);
        let stem = version_of(&path, 9).stem;
        unsafe {
            let file = gdal_sys::VSIFOpenL(cached.as_ptr(), c"rb".as_ptr());
            let mut buffer = vec![0_u8; 100];
            assert_eq!(
                gdal_sys::VSIFReadL(buffer.as_mut_ptr().cast(), 1, 100, file),
                100
            );
            // as the sweep deletes it, without touching the other tests' files
            std::fs::remove_file(cache.directory.join(format!("{stem}.{EXTENSION}"))).unwrap();
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
    fn it_reads_a_raster_the_same_through_the_cache() {
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
    /// hold it, costs no request, as another worker shared the file's version.
    #[test]
    #[serial_test::serial]
    fn it_downloads_nothing_when_reading_a_remote_file_again() {
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
        let (requests, _) = server.totals();
        assert!(requests > 0);

        // as a new worker: GDAL's own cache of the file and this worker's versions are gone
        unsafe { gdal_sys::VSICurlClearCache() };
        lock(&cache().1.versions).clear();
        assert_eq!(read(&cached), direct);
        assert_eq!(
            server.totals().0,
            requests,
            "no request: the version is shared"
        );
    }

    /// The inner directory's listing, so that drivers find sibling files by one listing.
    #[test]
    fn it_lists_the_inner_directory() {
        let _ = cache();
        let (dir, _, _) = file(10);
        std::fs::write(dir.path().join("file.bin.ovr"), [0_u8]).unwrap();
        let cached = CString::new(format!("{PREFIX}{}{}", key(14), dir.path().display())).unwrap();
        let mut names = Vec::new();
        unsafe {
            let list = gdal_sys::VSIReadDirEx(cached.as_ptr(), 0);
            assert!(!list.is_null());
            let mut entry = list;
            while !(*entry).is_null() {
                names.push(CStr::from_ptr(*entry).to_string_lossy().into_owned());
                entry = entry.add(1);
            }
            gdal_sys::CSLDestroy(list);
        }
        names.retain(|name| name != "." && name != ".."); // as the local file system lists them
        names.sort();
        assert_eq!(names, ["file.bin", "file.bin.ovr"]);
    }

    #[test]
    fn it_marks_files_read_from_the_cache_as_used() {
        let len = 2 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        let mut handle = handle_on(&path, 12, len as u64);
        let mut buffer = vec![0_u8; 100];
        assert_eq!(handle.read_at(0, &mut buffer), 100); // now cached
        let long_ago = SystemTime::now() - Duration::from_hours(1);
        let cache_file = &handle.file.as_ref().unwrap().file;
        cache_file.set_modified(long_ago).unwrap();

        // a hit after the interval marks the file as used again
        handle.touched = Instant::now().checked_sub(TOUCH_INTERVAL).unwrap();
        assert_eq!(handle.read_at(0, &mut buffer), 100);
        assert_eq!(buffer, bytes[..100]);
        let used = handle
            .file
            .as_ref()
            .unwrap()
            .file
            .metadata()
            .unwrap()
            .modified()
            .unwrap();
        assert!(used > long_ago + Duration::from_mins(50));
        close(&handle);
    }

    /// A read through a handle downloads the missing runs straight into the buffer and reads the
    /// chunks another worker stored from the cache file.
    #[test]
    fn it_combines_downloaded_and_cached_chunks_in_one_read() {
        let len = 4 * CHUNK_SIZE as usize;
        let (_dir, path, bytes) = file(len);
        let mut handle = handle_on(&path, 15, len as u64);
        // another worker stored chunks 1 and 3, with bytes the file does not have
        let other = file_of(&path, 15, len as u64);
        other.store(1, 1, &vec![7_u8; CHUNK_SIZE as usize]).unwrap();
        other.store(3, 1, &vec![9_u8; CHUNK_SIZE as usize]).unwrap();

        let mut buffer = vec![0_u8; len];
        assert_eq!(handle.read_at(0, &mut buffer), len);
        let c = CHUNK_SIZE as usize;
        assert_eq!(buffer[..c], bytes[..c], "chunk 0 downloaded");
        assert!(buffer[c..2 * c].iter().all(|&b| b == 7), "chunk 1 cached");
        assert_eq!(
            buffer[2 * c..3 * c],
            bytes[2 * c..3 * c],
            "chunk 2 downloaded"
        );
        assert!(buffer[3 * c..].iter().all(|&b| b == 9), "chunk 3 cached");
        close(&handle);
    }

    #[test]
    fn it_asks_for_a_forgotten_version_again() {
        let (_, cache) = cache();
        let (_dir, path, _) = file(10);
        let inner = CString::new(path.to_str().unwrap()).unwrap();
        let version = version_of(&path, 16);
        assert_eq!(known(cache, &key(16), &inner), Some(version.clone()));
        // as another worker: only the link knows it
        lock(&cache.versions).pop(&(key(16), inner.to_bytes().to_vec()));
        assert_eq!(known(cache, &key(16), &inner), Some(version), "shared");
        cache.forget(&path);
        assert_eq!(
            known(cache, &key(16), &inner),
            None,
            "forgotten by all workers"
        );
    }

    #[test]
    fn it_sweeps_least_recently_used_files_down_to_the_low_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let cache_file = |n: u64, chunks: u64, age_s: u64| {
            let file = CacheFile::open(dir.path(), &key(n), chunks * CHUNK_SIZE).unwrap();
            file.store(0, chunks, &vec![1_u8; (chunks * CHUNK_SIZE) as usize])
                .unwrap();
            let used = SystemTime::now() - Duration::from_secs(age_s);
            file.file.set_modified(used).unwrap();
            file.allocated()
        };
        let old = cache_file(1, 4, 300);
        let middle = cache_file(2, 4, 200);
        let recent = cache_file(3, 4, 100);
        let name = |n: u64| dir.path().join(format!("{}.{EXTENSION}", key(n)));

        // above the size: delete the oldest until at most 90% of it remain
        let size = old + middle + recent - 1;
        assert_eq!(sweep(dir.path(), size).unwrap(), (middle + recent, 1));
        assert!(!name(1).exists());
        assert!(name(2).exists());

        // under the size: nothing goes
        assert_eq!(sweep(dir.path(), size).unwrap(), (middle + recent, 0));
        // size 0, as at start and when clearing the caches: everything goes
        assert_eq!(sweep(dir.path(), 0).unwrap(), (0, 2));
    }

    /// A directory set to one with other files by mistake: the sweep and clearing the caches
    /// delete only the cache's own files, and the directory keeps its permissions.
    #[test]
    fn it_sweeps_only_files_the_cache_names() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let old = SystemTime::now() - Duration::from_hours(1);
        let foreign = [
            dir.path().join("Settings.toml"),
            dir.path().join("scene.cache"),
            dir.path().join(format!("{}.cache", "A".repeat(64))),
            dir.path().join(format!("{}.data", key(1))),
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
        let own = CacheFile::open(dir.path(), &key(1), CHUNK_SIZE).unwrap();
        own.store(0, 1, &[1_u8; 100]).unwrap();

        create_directory(dir.path()).unwrap();
        let (_, deleted) = sweep(dir.path(), 0).unwrap();

        assert_eq!(deleted, 1, "the cache's own file");
        assert!(foreign.iter().all(|path| path.exists()));
        assert!(folder.join("0").exists());
        let mode = std::fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
    }

    #[test]
    fn it_finds_runs() {
        let bytes = [1_u8, 0, 0, 1, 0];
        assert_eq!(runs(bytes.len(), |i| bytes[i] != 1), [(1, 2), (4, 4)]);
        assert_eq!(runs(bytes.len(), |i| bytes[i] == 1), [(0, 0), (3, 3)]);
        assert_eq!(runs(0, |_| true), []);
    }
}
