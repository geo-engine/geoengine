//! A local HTTP server for `/vsicurl/` that answers `HEAD` and `GET` with `Range: bytes=a-b` from
//! files of one directory. It counts requests and bytes per path and can wait a fixed time
//! before each answer, so that local files behave like an object store.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
pub struct PathCounters {
    pub requests: AtomicU64,
    pub gets: AtomicU64,
    pub bytes: AtomicU64,
}

pub struct RangeServer {
    port: u16,
    counters: Arc<Mutex<HashMap<String, Arc<PathCounters>>>>,
}

impl RangeServer {
    /// Serves the files of `root` on a free local port, waiting `latency` before each answer.
    pub fn start(root: PathBuf, latency: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a free local port");
        let port = listener.local_addr().expect("a bound address").port();
        let counters: Arc<Mutex<HashMap<String, Arc<PathCounters>>>> = Arc::default();

        let server_counters = counters.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let root = root.clone();
                let counters = server_counters.clone();
                std::thread::spawn(move || {
                    // a broken connection only ends this connection
                    let _ = serve_connection(stream, &root, &counters, latency);
                });
            }
        });

        Self { port, counters }
    }

    /// The GDAL path of a file in the served directory.
    pub fn vsicurl_path(&self, file_name: &str) -> String {
        format!("/vsicurl/http://127.0.0.1:{}/{file_name}", self.port)
    }

    /// Requests and bytes served for all paths since the last reset.
    pub fn totals(&self) -> (u64, u64) {
        self.counters
            .lock()
            .expect("counter lock")
            .values()
            .fold((0, 0), |(requests, bytes), c| {
                (
                    requests + c.requests.load(Ordering::Relaxed),
                    bytes + c.bytes.load(Ordering::Relaxed),
                )
            })
    }

    /// `GET` requests served for all paths since the last reset.
    pub fn gets(&self) -> u64 {
        self.counters
            .lock()
            .expect("counter lock")
            .values()
            .map(|c| c.gets.load(Ordering::Relaxed))
            .sum()
    }

    pub fn reset(&self) {
        self.counters.lock().expect("counter lock").clear();
    }
}

fn serve_connection(
    stream: TcpStream,
    root: &std::path::Path,
    counters: &Mutex<HashMap<String, Arc<PathCounters>>>,
    latency: Duration,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    loop {
        let mut request_line = String::new();
        if reader.read_line(&mut request_line)? == 0 {
            return Ok(());
        }
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let path = parts
            .next()
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();

        let mut range = None;
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header)? == 0 {
                return Ok(());
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.eq_ignore_ascii_case("range")
            {
                range = parse_range(value.trim());
            }
        }

        if !latency.is_zero() {
            std::thread::sleep(latency);
        }

        // only plain file names: nothing outside of `root` is served
        let file_path = root.join(&path);
        let file = (!path.is_empty() && !path.contains('/') && !path.contains(".."))
            .then(|| std::fs::File::open(&file_path).ok())
            .flatten();
        let Some(mut file) = file else {
            writer.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")?;
            continue;
        };
        let size = file.metadata()?.len();

        let counter = counters
            .lock()
            .expect("counter lock")
            .entry(path)
            .or_default()
            .clone();
        counter.requests.fetch_add(1, Ordering::Relaxed);
        if method == "GET" {
            counter.gets.fetch_add(1, Ordering::Relaxed);
        }

        if method == "HEAD" {
            write!(
                writer,
                "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nAccept-Ranges: bytes\r\n\r\n"
            )?;
            continue;
        }

        let (start, end) = match range {
            Some((start, end)) if start < size => (start, end.unwrap_or(size - 1).min(size - 1)),
            Some(_) => {
                write!(
                    writer,
                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{size}\r\nContent-Length: 0\r\n\r\n"
                )?;
                continue;
            }
            None => (0, size.saturating_sub(1)),
        };
        let length = if size == 0 { 0 } else { end - start + 1 };

        if range.is_some() {
            write!(
                writer,
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {length}\r\nContent-Range: bytes {start}-{end}/{size}\r\nAccept-Ranges: bytes\r\n\r\n"
            )?;
        } else {
            write!(
                writer,
                "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nAccept-Ranges: bytes\r\n\r\n"
            )?;
        }

        file.seek(SeekFrom::Start(start))?;
        std::io::copy(&mut (&mut file).take(length), &mut writer)?;
        counter.bytes.fetch_add(length, Ordering::Relaxed);
    }
}

/// Parses `bytes=a-b` or `bytes=a-`. Suffix and multi ranges, which GDAL does not send, are not
/// supported.
fn parse_range(value: &str) -> Option<(u64, Option<u64>)> {
    let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
    let start = start.parse().ok()?;
    let end = if end.is_empty() {
        None
    } else {
        Some(end.parse().ok()?)
    };
    Some((start, end))
}
