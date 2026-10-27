//! A web page to chat with the bakery's agent: `server_agent`'s server (an
//! axum server where each WebSocket connection is a person in a room, an
//! agent answering through an OpenAI-compatible endpoint with its
//! `lookup_order` tool, the rooms in SQLite) serving a page in front of its
//! socket. The page is plain HTML, CSS and JavaScript, the files of
//! `web_chat/` beside this one, compiled into the binary: no build step and
//! nothing fetched from elsewhere, so it works offline.
//!
//! - `GET /` serves the page, `GET /app.js` and `GET /app.css` its script
//!   and its style, and `/conversations/{conversation}?user=...&name=...`
//!   is the socket the page opens with `&stream=1`, the one
//!   `server_agent`'s header describes.
//! - The page asks for a name and a conversation, `bakery` by default. The
//!   participant's id is the name in lowercase letters, digits, `_` and
//!   `-`, or a random one when nothing of the name is left.
//! - A connection is sent its conversation's past as it opens, as an
//!   integrator of the Python implementation reads it: once the connection
//!   is registered, so that nothing committed meanwhile is missed, the
//!   newest 50 events (`get_timeline` with `newest_first`), of which the
//!   `websocket` channel's view (`visible_events`, RFC §7.5 rule 8). The
//!   page is cut before the view is taken, as in the Python
//!   implementation, so it may hold fewer than 50.
//! - The frames it reads:
//!   - `{"type": "history", "room_id", "events": [...]}`, first: the past,
//!     oldest first, each event framed as a delivery frames it, with its
//!     participant's `name`; a message whose `participant_id` is the
//!     person's own is shown as theirs, and an event the page shows
//!     already, which a delivery made meanwhile brought too, is shown once,
//!     told by its `index`;
//!   - `{"type": "delta", "stream_id", "from", "text"}`, a piece of the
//!     agent's answer: it grows a bubble of its own for each `stream_id`;
//!   - `{"type": "message", "id", "index", "from", "participant_id",
//!     "text", "stream_id", "ended"}`, a message as the store holds it: it
//!     takes the place of its stream's bubble, and each row is set at its
//!     `index`;
//!   - `{"type": "tool_call_start" | "tool_call_end", "tool": {"name",
//!     "id", "status", "result"}}`, a call: one line, `lookup_order…`, then
//!     its result or its failure;
//!   - `{"error": "..."}`, a refused message or a failed turn: a line of
//!     the conversation.
//! - A person's own message is shown as it is sent: a room delivers a
//!   message to every channel but its source (RFC §10.2 step 2), and every
//!   person of the page is on the one `websocket` channel, so no message of
//!   theirs comes back, nor another person's. Two people in one
//!   conversation each see, live, their own messages and the agent's
//!   answers to both; the past a page is sent as it opens holds the other
//!   person's messages as well, which live delivery does not bring yet
//!   (RMK-992).
//! - An answer's leading and trailing whitespace is trimmed where it is
//!   shown; the store keeps the text as the model wrote it.
//! - The state of the connection is shown, and the message field is
//!   disabled while it is not open; Enter sends.
//!
//! What it secures, as an example to copy:
//! - Every text from the server is set with `textContent`, never parsed as
//!   HTML.
//! - Each response carries a `Content-Security-Policy`, the page's own
//!   files only and its socket to the same host, which runs no inline
//!   script nor style, and `X-Content-Type-Options: nosniff`; each file is
//!   served with its `Content-Type`.
//! - The socket opens from the server's own page only: an upgrade whose
//!   `Origin` is not `http://` and the `Host` the request names, or that
//!   has none, is refused with 403, before any room is joined. Another
//!   site's page cannot open a socket as its visitor (cross-site WebSocket
//!   hijacking). A client outside a browser names the page's origin, as
//!   `websocat --origin http://127.0.0.1:8080` does.
//! - It listens on 127.0.0.1 unless told another address.
//!
//! What it leaves out, which a service built from it adds, beside what
//! `server_agent`'s header lists:
//! - TLS: the page and its socket are plain `http://` and `ws://`. Behind a
//!   TLS proxy, the `Origin` to compare is the service's public one,
//!   `https://...`, not one made of the `Host` the proxy forwards.
//! - Authentication: the name and the id are whatever the person types; a
//!   service takes the participant from its session.
//! - A check of the `Host` itself: a page loaded under a name an attacker
//!   points at this address (DNS rebinding) has that name for its origin
//!   and its `Host` alike, so a service also refuses a `Host` that is not
//!   one of its own names.
//! - Rate limits, on messages and on connections.
//! - More of the past: a page is sent the newest 50 events, and reads no
//!   earlier page (`before_index` would); nor does it show edits,
//!   deletions or the room's other events.
//!
//! Its checks run offline, against the fake of the OpenAI wire and a
//! temporary file: the page and its files are served with their types and
//! headers, a socket from another origin or from none is refused, a socket
//! from the page's own reads an answer as it is written, the lookup's call
//! between its two rounds, and a second page that joins then is sent that
//! conversation's past, the first page's message included, in order, each
//! event once. `node --check` reads the page's script when `node` is on the
//! PATH; that check is skipped otherwise.
//!
//! With `--serve`, it stays up on 127.0.0.1:8080, or the address given
//! after the flag, its rooms in `web_chat.db`, until Ctrl+C: open the URL
//! it prints. The agent calls the endpoint `OPENAI_BASE_URL`,
//! `OPENAI_API_KEY` and `OPENAI_MODEL` name, or, without them, the fake,
//! which answers from the checks' script.
//!
//! ```sh
//! cargo run --example web_chat
//! OPENAI_BASE_URL=https://api.openai.com/v1 OPENAI_API_KEY=sk-... \
//!     OPENAI_MODEL=gpt-6-luna cargo run --example web_chat -- --serve
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{ExitCode, Stdio};
use std::time::Duration;
use std::{env, io};

use axum::Router;
use axum::extract::Request;
use axum::http::header::{
    CONTENT_SECURITY_POLICY, CONTENT_TYPE, HOST, ORIGIN, X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest};

use shared::bakery::ORDER_42;
use shared::server::{AgentServer, CHECKING, SERVING, Site};
use shared::{Checks, FakeOpenAi, OpenAiEndpoint, Reply, TempDatabase};

/// The page, its script and its style, compiled in from `web_chat/`.
const PAGE: &str = include_str!("web_chat/index.html");
const SCRIPT: &str = include_str!("web_chat/app.js");
const STYLE: &str = include_str!("web_chat/app.css");

/// What every response allows the page: its own files only, its socket to
/// the same host (`'self'` takes in `ws:` there), no inline script nor
/// style, no `<base>` of another address, no form sent anywhere, and no
/// frame of another site around it.
const POLICY: &str = "default-src 'self'; connect-src 'self'; base-uri 'none'; \
                      form-action 'none'; frame-ancestors 'none'";

/// The types the files are served with.
const HTML: &str = "text/html; charset=utf-8";
const JAVASCRIPT: &str = "text/javascript; charset=utf-8";
const CSS: &str = "text/css; charset=utf-8";

/// What a socket from another origin is told.
const FOREIGN: &str = "a conversation opens from this server's own page only";

/// What the server serves around the agent's socket: the page, and the
/// newest 50 events of its conversation to each connection as it opens.
const SITE: Site = Site {
    routes: site,
    history: 50,
};

/// Where `--serve` listens unless told another address, and the file it
/// keeps its rooms in.
const SERVE_ADDRESS: &str = "127.0.0.1:8080";
const SERVE_DATABASE: &str = "web_chat.db";

/// What the checks' page asks, and what the fake answers: a round that
/// looks the order up, then the answer.
const CONVERSATION: &str = "bakery";
const ASKS: &str = "Is my order 42 ready?";
const LOOKING_UP: &str = "Let me look up your order.";
const READY: &str = "Yes, order 42 is ready: two loaves, to pick up at 5 pm.";

fn script() -> Vec<Reply> {
    let lookup = Reply::calling(
        LOOKING_UP,
        [("call_order_42", "lookup_order", r#"{"order": "42"}"#)],
    );
    vec![lookup, Reply::from(READY)]
}

/// What the page must hold: its script and its style, loaded as files, and
/// its forms and its list.
const MARKERS: &[&str] = &[
    r#"<script src="/app.js" defer></script>"#,
    r#"<link rel="stylesheet" href="/app.css">"#,
    r#"<form id="join""#,
    r#"<ol id="messages""#,
    r#"<form id="composer""#,
];

/// Where the checks' server listens: a free port of the loopback.
const ANY_PORT: &str = "127.0.0.1:0";

/// How long a check waits for a response, and for an answer.
const PATIENCE: Duration = Duration::from_secs(5);
const ANSWER_PATIENCE: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let mut args = env::args().skip(1);
    if args.next().as_deref() == Some("--serve") {
        serve(args.next().as_deref().unwrap_or(SERVE_ADDRESS)).await?;
        return Ok(ExitCode::SUCCESS);
    }
    check().await
}

/// The server's routes: the page and its two files, then the conversations'
/// socket, which only the server's own origin opens, and the security
/// headers on every response.
fn site(conversations: Router) -> Router {
    Router::new()
        .route("/", get(|| async { file(HTML, PAGE) }))
        .route("/app.js", get(|| async { file(JAVASCRIPT, SCRIPT) }))
        .route("/app.css", get(|| async { file(CSS, STYLE) }))
        .merge(conversations.route_layer(middleware::from_fn(own_origin_only)))
        .layer(middleware::map_response(secured))
}

/// A file of the page, `body`, served as `content_type`.
fn file(content_type: &'static str, body: &'static str) -> Response {
    ([(CONTENT_TYPE, content_type)], body).into_response()
}

/// Lets a request through only when it comes from the server's own page.
async fn own_origin_only(request: Request, next: Next) -> Response {
    if !same_origin(request.headers()) {
        return (StatusCode::FORBIDDEN, FOREIGN).into_response();
    }
    next.run(request).await
}

/// Whether a request's `Origin` is the server's own, as the page has it:
/// `http://` and the `Host` the request was sent to. A request without
/// either is not.
fn same_origin(headers: &HeaderMap) -> bool {
    let text = |name| headers.get(name).and_then(|value| value.to_str().ok());
    let (Some(origin), Some(host)) = (text(ORIGIN), text(HOST)) else {
        return false;
    };
    origin
        .strip_prefix("http://")
        .is_some_and(|origin| origin.eq_ignore_ascii_case(host))
}

/// `response` with the headers every response carries: the policy, and no
/// guessing of a type other than the one it states.
async fn secured(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(CONTENT_SECURITY_POLICY, HeaderValue::from_static(POLICY));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

/// `--serve`: the server on `address`, until Ctrl+C.
async fn serve(address: &str) -> Result<(), Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::from_env(&script()).await?;
    let database = Path::new(SERVE_DATABASE);
    let address = address.parse()?;
    let server = AgentServer::start_with(database, &endpoint, address, SERVING, SITE).await?;
    println!("The agent calls {}.", endpoint.shown_base_url());
    println!(
        "Open http://{}/ to chat, the rooms in {SERVE_DATABASE}. Ctrl+C stops.",
        server.address()
    );
    tokio::signal::ctrl_c().await?;
    server.stop().await
}

/// The checks: the page served, a foreign socket refused, a chat from the
/// page's own origin, then the script read by `node`.
async fn check() -> Result<ExitCode, Box<dyn Error>> {
    let mut checks = Checks::default();
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&script()).await?);
    let database = TempDatabase::new("web_chat");
    let address = ANY_PORT.parse()?;
    let server =
        AgentServer::start_with(database.path(), &endpoint, address, CHECKING, SITE).await?;
    the_page_is_served(&mut checks, server.address()).await?;
    another_origin_is_refused(&mut checks, server.address()).await?;
    let read = the_page_reads_an_answer_as_it_is_written(&mut checks, server.address()).await?;
    another_page_reads_the_conversation_s_past(&mut checks, server.address(), &read).await?;
    tokio::time::timeout(PATIENCE, server.stop()).await??;
    the_script_parses(&mut checks).await?;
    the_page_holds_what_the_script_finds(&mut checks);
    Ok(checks.finish())
}

/// A browser fetches the page, its script and its style.
async fn the_page_is_served(
    checks: &mut Checks,
    address: SocketAddr,
) -> Result<(), Box<dyn Error>> {
    println!("A browser fetches the page and its files.");
    let page = fetch(address, "/").await?;
    let script = fetch(address, "/app.js").await?;
    let style = fetch(address, "/app.css").await?;
    let fetched = [("/", &page), ("/app.js", &script), ("/app.css", &style)];
    for (path, response) in fetched {
        println!(
            "  {path}: {} {:?}, {} bytes",
            response.status,
            response.header("content-type").unwrap_or_default(),
            response.body.len()
        );
    }
    let served = |response: &Fetched, content_type: &str, body: &str| {
        response.status == 200
            && response.header("content-type") == Some(content_type)
            && response.body == body
    };
    checks.expect(
        "the page is served as HTML, with its markers",
        served(&page, HTML, PAGE) && MARKERS.iter().all(|marker| page.body.contains(marker)),
    );
    checks.expect(
        "the script and the style are served as JavaScript and CSS",
        served(&script, JAVASCRIPT, SCRIPT) && served(&style, CSS, STYLE),
    );
    checks.expect(
        "each file carries the content security policy and nosniff",
        fetched.iter().all(|(_, response)| {
            response.header("content-security-policy") == Some(POLICY)
                && response.header("x-content-type-options") == Some("nosniff")
        }),
    );
    checks.expect(
        "the page runs no inline script nor style, which the policy would block",
        runs_nothing_inline(&page.body),
    );
    Ok(())
}

/// Whether `html` loads every script from a file, and holds no style
/// element, no style attribute and no event handler attribute.
fn runs_nothing_inline(html: &str) -> bool {
    let handler = html.split(" on").skip(1).any(|rest| {
        let after = rest.trim_start_matches(|c: char| c.is_ascii_lowercase());
        after.len() < rest.len() && after.starts_with('=')
    });
    html.matches("<script").count() == html.matches("<script src=").count()
        && !html.contains("<style")
        && !html.contains(" style=")
        && !handler
}

/// Pages of other origins, and a client naming none, open the socket.
async fn another_origin_is_refused(
    checks: &mut Checks,
    address: SocketAddr,
) -> Result<(), Box<dyn Error>> {
    println!("Pages of other origins open the conversation's socket.");
    let path = format!("/conversations/{CONVERSATION}?user=mallory&name=Mallory");
    let port = address.port();
    let mut statuses = Vec::new();
    for (what, origin) in [
        ("another site", Some("http://attacker.example".to_owned())),
        (
            "another name of the host",
            Some(format!("http://localhost:{port}")),
        ),
        ("another scheme", Some(format!("https://{address}"))),
        ("an opaque origin", Some("null".to_owned())),
        ("no origin", None),
    ] {
        let status = open(address, &path, origin.as_deref()).await?.err();
        println!("  from {what}: refused with {status:?}");
        statuses.push(status);
    }
    checks.expect(
        "a socket from another origin, or from none, is refused with 403",
        statuses.iter().all(|status| *status == Some(403)),
    );
    Ok(())
}

/// Alice's page, on the server's own origin, asks about her order and reads
/// the answer as it is written: each round's pieces, then its row, the
/// lookup's two rows between the rounds, all of one stream.
async fn the_page_reads_an_answer_as_it_is_written(
    checks: &mut Checks,
    address: SocketAddr,
) -> Result<Vec<Value>, Box<dyn Error>> {
    println!("Alice's page joins {CONVERSATION}, reading answers as they are written.");
    let path = format!("/conversations/{CONVERSATION}?user=alice&name=Alice&stream=1");
    let origin = format!("http://{address}");
    let opened = open(address, &path, Some(&origin)).await?;
    println!(
        "  from the page's own origin: refused with {:?}",
        opened.as_ref().err()
    );
    let Ok(mut alice) = opened else {
        checks.expect("a socket from the page's own origin opens", false);
        return Ok(Vec::new());
    };
    let history = next_frame(&mut alice).await;
    checks.expect(
        "a socket that opens on a new conversation is sent its history, empty",
        history.as_ref().is_some_and(|frame| {
            frame["type"] == "history" && frame["events"].as_array().is_some_and(Vec::is_empty)
        }),
    );
    println!("Alice says {ASKS:?}");
    alice.send(tungstenite::Message::text(ASKS)).await?;
    let frames = turn(&mut alice).await;
    let _closed = alice.close(None).await;
    print_frames("Alice", &frames);

    let rows: Vec<&Value> = frames
        .iter()
        .filter(|frame| frame["type"] != "delta")
        .collect();
    let kinds: Vec<&str> = rows
        .iter()
        .filter_map(|frame| frame["type"].as_str())
        .collect();
    let stream = frames.first().and_then(|frame| frame.get("stream_id"));
    checks.expect(
        "a socket from the page's own origin reads the answer's pieces and rows, the \
         lookup's start and end between its two rounds, all of one stream",
        kinds == ["message", "tool_call_start", "tool_call_end", "message"]
            && frames.iter().any(|frame| frame["type"] == "delta")
            && stream.is_some()
            && frames.iter().all(|frame| frame.get("stream_id") == stream),
    );
    checks.expect(
        "each round's pieces join to its row's text, and the last row ends the turn",
        rounds(&frames) == [LOOKING_UP, READY].map(|text| (text.to_owned(), text.to_owned()))
            && rows.last().is_some_and(|row| row["ended"] == "completed"),
    );
    let ended = rows.iter().find(|row| row["type"] == "tool_call_end");
    checks.expect(
        "the lookup's end row names the tool and carries its result",
        ended.is_some_and(|row| {
            row.pointer("/tool/name") == Some(&Value::from("lookup_order"))
                && row.pointer("/tool/status") == Some(&Value::from("completed"))
                && row.pointer("/tool/result") == Some(&Value::from(ORDER_42))
        }),
    );
    Ok(frames)
}

/// Bob's page joins the conversation once Alice's turn ended: its socket's
/// first frame is the conversation's past, Alice's message, which never
/// reached her socket, then every row she read, each once, in index order.
async fn another_page_reads_the_conversation_s_past(
    checks: &mut Checks,
    address: SocketAddr,
    alice_read: &[Value],
) -> Result<(), Box<dyn Error>> {
    println!("Bob's page joins {CONVERSATION} after Alice's turn.");
    let path = format!("/conversations/{CONVERSATION}?user=bob&name=Bob&stream=1");
    let Ok(mut bob) = open(address, &path, Some(&format!("http://{address}"))).await? else {
        checks.expect("Bob's socket opens", false);
        return Ok(());
    };
    let history = next_frame(&mut bob).await.unwrap_or_default();
    let _closed = bob.close(None).await;
    let past = history
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    print_frames("Bob", &past);
    // The message the stream answers is Alice's, named in its stream id.
    let asked = alice_read
        .first()
        .and_then(|frame| frame["stream_id"].as_str())
        .and_then(|stream| stream.strip_prefix("assistant:"));
    let rows = alice_read.iter().filter(|frame| frame["type"] != "delta");
    let expected: Vec<Option<&str>> = asked
        .into_iter()
        .chain(rows.filter_map(|row| row["id"].as_str()))
        .map(Some)
        .collect();
    let first = past.first();
    checks.expect(
        "a socket that opens on a conversation is sent its past first: the other page's \
         message, with its participant and name, then the rows that page read, in index \
         order, each once",
        history.get("type") == Some(&Value::from("history"))
            && past.iter().map(|event| event["id"].as_str()).eq(expected)
            && indexes_rise(&past)
            && first.is_some_and(|event| {
                event["participant_id"] == "alice"
                    && event["name"] == "Alice"
                    && event["text"] == ASKS
            }),
    );
    Ok(())
}

/// Whether the indexes of `events` rise, each event after the one before.
fn indexes_rise(events: &[Value]) -> bool {
    let indexes: Vec<Option<u64>> = events.iter().map(|event| event["index"].as_u64()).collect();
    indexes.iter().all(Option::is_some)
        && indexes
            .windows(2)
            .all(|pair| matches!(pair, [a, b] if a < b))
}

/// For each message row of `frames`, the text its pieces join to and the
/// row's own.
fn rounds(frames: &[Value]) -> Vec<(String, String)> {
    let mut rounds = Vec::new();
    let mut pieces = String::new();
    for frame in frames {
        let text = frame["text"].as_str().unwrap_or_default();
        match frame["type"].as_str() {
            Some("delta") => pieces.push_str(text),
            Some("message") => rounds.push((std::mem::take(&mut pieces), text.to_owned())),
            _ => pieces.clear(),
        }
    }
    rounds
}

/// Prints the frames `who` read, each run of pieces as the text it joins
/// to.
fn print_frames(who: &str, frames: &[Value]) {
    let mut pieces: Vec<&str> = Vec::new();
    for frame in frames {
        let text = frame["text"].as_str().unwrap_or_default();
        if frame["type"] == "delta" {
            pieces.push(text);
            continue;
        }
        if !pieces.is_empty() {
            println!(
                "  {who} reads {} pieces: {:?}",
                pieces.len(),
                pieces.concat()
            );
            pieces.clear();
        }
        let shown = frame
            .pointer("/tool/name")
            .and_then(Value::as_str)
            .map_or_else(|| format!("{text:?}"), str::to_owned);
        println!(
            "  {who} reads #{} {} {shown}",
            frame["index"], frame["type"]
        );
    }
}

/// The script, read by `node --check` as the server serves it, when `node`
/// is on the PATH.
async fn the_script_parses(checks: &mut Checks) -> Result<(), Box<dyn Error>> {
    const WHAT: &str = "the page's script parses (node --check)";
    let node = Command::new("node")
        .arg("--check")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn();
    let mut node = match node {
        Ok(node) => node,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            checks.skip(WHAT, "node is not on the PATH");
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    if let Some(mut input) = node.stdin.take() {
        input.write_all(SCRIPT.as_bytes()).await?;
    }
    let output = tokio::time::timeout(PATIENCE, node.wait_with_output()).await??;
    if !output.status.success() {
        println!("{}", String::from_utf8_lossy(&output.stderr));
    }
    checks.expect(WHAT, output.status.success());
    Ok(())
}

/// Every element the script finds by its id, `byId("...")`, is on the
/// page.
fn the_page_holds_what_the_script_finds(checks: &mut Checks) {
    let ids: Vec<&str> = SCRIPT
        .split("byId(\"")
        .skip(1)
        .filter_map(|rest| rest.split_once('"').map(|(id, _)| id))
        .collect();
    let missing: Vec<&str> = ids
        .iter()
        .copied()
        .filter(|id| !PAGE.contains(&format!("id=\"{id}\"")))
        .collect();
    println!(
        "The script finds {} elements by id; the page lacks {missing:?}.",
        ids.len()
    );
    checks.expect(
        "every element the script finds by id is on the page",
        !ids.is_empty() && missing.is_empty(),
    );
}

/// A response the checks fetched: its status, its headers, their names in
/// lowercase, and its body.
struct Fetched {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Fetched {
    /// The value of the header `name`, given in lowercase.
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }
}

/// `GET path` on the server at `address`, as HTTP/1.1 with the connection
/// closed after the response.
async fn fetch(address: SocketAddr, path: &str) -> Result<Fetched, Box<dyn Error>> {
    let mut stream = TcpStream::connect(address).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut received = Vec::new();
    tokio::time::timeout(PATIENCE, stream.read_to_end(&mut received)).await??;
    let received = String::from_utf8(received)?;
    let (head, body) = received
        .split_once("\r\n\r\n")
        .ok_or("a response without its head")?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or("a response without its status")?;
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    Ok(Fetched {
        status,
        headers,
        body: body.to_owned(),
    })
}

/// A socket to `path` on the server at `address`, sent from `origin`, or
/// the HTTP status the server refused it with.
async fn open(
    address: SocketAddr,
    path: &str,
    origin: Option<&str>,
) -> Result<Result<WebSocketStream<TcpStream>, u16>, Box<dyn Error>> {
    let mut request = format!("ws://{address}{path}").into_client_request()?;
    if let Some(origin) = origin {
        request.headers_mut().insert(ORIGIN, origin.parse()?);
    }
    let stream = TcpStream::connect(address).await?;
    match tokio_tungstenite::client_async(request, stream).await {
        Ok((socket, _response)) => Ok(Ok(socket)),
        Err(tungstenite::Error::Http(response)) => Ok(Err(response.status().as_u16())),
        Err(error) => Err(error.into()),
    }
}

/// The next JSON frame on `socket`, if one comes in time.
async fn next_frame(socket: &mut WebSocketStream<TcpStream>) -> Option<Value> {
    let next = async {
        while let Some(Ok(received)) = socket.next().await {
            if let tungstenite::Message::Text(frame) = received {
                return serde_json::from_str(frame.as_str()).ok();
            }
        }
        None
    };
    tokio::time::timeout(ANSWER_PATIENCE, next)
        .await
        .ok()
        .flatten()
}

/// The frames of the next turn on `socket`, up to the message that ends it
/// (`ended`) or an error, as many as come in time.
async fn turn(socket: &mut WebSocketStream<TcpStream>) -> Vec<Value> {
    let mut frames = Vec::new();
    let reading = async {
        while let Some(Ok(received)) = socket.next().await {
            let tungstenite::Message::Text(frame) = received else {
                continue;
            };
            let frame: Value = serde_json::from_str(frame.as_str()).unwrap_or_default();
            let ends = frame.get("ended").is_some() || frame.get("error").is_some();
            frames.push(frame);
            if ends {
                break;
            }
        }
    };
    let _in_time = tokio::time::timeout(ANSWER_PATIENCE, reading).await;
    frames
}
