//! A real socket peer can stop reading an upload before HTTP body extraction.

use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone, Copy, Debug)]
pub(super) enum Mode {
    Upload,
    EarlyUpload,
    PostHeaders,
    PostBody,
    SlowPost,
    PacedHeaders,
    PacedEndpoint,
}

pub(super) struct State {
    pub(super) held: Notify,
    pub(super) progress: Notify,
    pub(super) proceed: Notify,
    pub(super) released: Notify,
    pub(super) closed: Notify,
    changed: Notify,
    opened: AtomicUsize,
    pub(super) requests: Mutex<Vec<String>>,
    pub(super) messages: Mutex<Vec<Value>>,
    sender: Mutex<Option<mpsc::UnboundedSender<String>>>,
    mode: Mode,
}

pub(super) struct Peer {
    pub(super) url: String,
    pub(super) state: Arc<State>,
    task: JoinHandle<()>,
}

impl Peer {
    pub(super) async fn start(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let state = Arc::new(State {
            held: Notify::new(),
            progress: Notify::new(),
            proceed: Notify::new(),
            released: Notify::new(),
            closed: Notify::new(),
            changed: Notify::new(),
            opened: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            messages: Mutex::new(Vec::new()),
            sender: Mutex::new(None),
            mode,
        });
        let shared = Arc::clone(&state);
        let task = tokio::spawn(async move {
            let mut calls = JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                calls.spawn(serve(stream, Arc::clone(&shared)));
            }
        });
        Self { url, state, task }
    }

    pub(super) async fn messages(&self, count: usize) {
        loop {
            let changed = self.state.changed.notified();
            if self.state.messages.lock().unwrap().len() >= count {
                return;
            }
            changed.await;
        }
    }

    pub(super) fn comment(&self) {
        self.state
            .sender
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .send(": keepalive\n\n".into())
            .unwrap();
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(stream: TcpStream, state: Arc<State>) {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    loop {
        let mut line = String::new();
        if read.read_line(&mut line).await.unwrap_or_default() == 0 {
            return;
        }
        state.requests.lock().unwrap().push(line.trim().into());
        let get = line.starts_with("GET ");
        let mut length = 0;
        loop {
            line.clear();
            if read.read_line(&mut line).await.unwrap_or_default() == 0 {
                return;
            }
            if line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().unwrap();
            }
        }
        if get {
            state.opened.fetch_add(1, Ordering::Relaxed);
            let (sender, mut receiver) = mpsc::unbounded_channel::<String>();
            *state.sender.lock().unwrap() = Some(sender);
            opening(&mut write, &state).await;
            let mut byte = [0];
            loop {
                tokio::select! {
                    _ = read.read(&mut byte) => { state.closed.notify_one(); return; }
                    Some(message) = receiver.recv() => { if write.write_all(message.as_bytes()).await.is_err() { state.closed.notify_one(); return; } }
                }
            }
        }
        if length > 1024 * 1024 {
            assert!(matches!(state.mode, Mode::Upload | Mode::EarlyUpload));
            if matches!(state.mode, Mode::EarlyUpload) {
                write
                    .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .unwrap();
            }
            state.held.notify_one();
            // No body reader: an 8-MiB call exceeds the real socket buffers.
            state.released.notified().await;
            return;
        }
        let mut body = vec![0; length];
        read.read_exact(&mut body).await.unwrap();
        let message: Value = serde_json::from_slice(&body).unwrap();
        let initialize = message.get("method").unwrap() == "initialize";
        state.messages.lock().unwrap().push(message.clone());
        state.changed.notify_one();
        if initialize
            && matches!(
                state.mode,
                Mode::PostHeaders | Mode::PostBody | Mode::SlowPost
            )
            && state.opened.load(Ordering::Relaxed) == 1
        {
            if matches!(state.mode, Mode::PostBody) {
                write
                    .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 1\r\n\r\n")
                    .await
                    .unwrap();
            }
            state.held.notify_one();
            state.released.notified().await;
            if matches!(state.mode, Mode::PostBody) {
                let _sent = write.write_all(b"x").await;
            } else if write
                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                .await
                .is_err()
            {
                return;
            }
            reply(&state, &message);
            continue;
        }
        if write
            .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
            .await
            .is_err()
        {
            return;
        }
        if message.get("id").is_some() {
            reply(&state, &message);
        }
    }
}

fn reply(state: &State, message: &Value) {
    let result = if message.get("method").unwrap() == "initialize" {
        json!({"protocolVersion":"2025-11-25", "capabilities":{}, "serverInfo":{"name":"deadline", "version":"1"}})
    } else {
        assert_eq!(message.get("method").unwrap(), "host/read");
        json!({"done":true})
    };
    let reply = json!({"jsonrpc":"2.0", "id":message.get("id").unwrap(), "result":result});
    state
        .sender
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .send(format!("event: message\ndata: {reply}\n\n"))
        .unwrap();
}

async fn opening(write: &mut tokio::net::tcp::OwnedWriteHalf, state: &State) {
    if matches!(state.mode, Mode::PacedHeaders) {
        for fragment in [
            b"HTTP/1.1 200 OK\r\n".as_slice(),
            b"Content-Type: text/event-stream\r\n".as_slice(),
        ] {
            write.write_all(fragment).await.unwrap();
            state.progress.notify_one();
            state.proceed.notified().await;
        }
        write.write_all(b"\r\n").await.unwrap();
    } else {
        write
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
            .await
            .unwrap();
    }
    if matches!(state.mode, Mode::PacedEndpoint) {
        for _ in 0..2 {
            write.write_all(b": keepalive\n\n").await.unwrap();
            state.progress.notify_one();
            state.proceed.notified().await;
        }
    }
    write
        .write_all(b"event: endpoint\ndata: /messages\n\n")
        .await
        .unwrap();
}
