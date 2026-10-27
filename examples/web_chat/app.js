// The page of the web chat example (examples/web_chat.rs). A person joins a
// conversation, and the room's frames come in on a WebSocket to
// /conversations/{conversation}. Every text from the server is set with
// textContent, never parsed as HTML.
"use strict";

// The server's limit on a message, in bytes: a larger one closes the socket.
const MESSAGE_LIMIT = 64 * 1024;

const byId = (id) => document.getElementById(id);
const page = {
  state: byId("state"),
  join: byId("join"),
  name: byId("name"),
  conversation: byId("conversation"),
  connect: byId("connect"),
  scroller: byId("scroller"),
  messages: byId("messages"),
  composer: byId("composer"),
  text: byId("text"),
  send: byId("send"),
};

// The connection, who the person is in it, and what the page keeps of the
// conversation it shows: the index of each stored event shown, the answer
// growing for each stream, the line of each tool call, and the line that
// says an answer is on its way.
let socket = null;
let leaving = false;
let user = null;
let guest = null;
const indexes = new Set();
const streams = new Map();
const calls = new Map();
const waiting = Object.assign(document.createElement("li"), {
  className: "waiting",
  textContent: "The agent is answering…",
});

page.join.addEventListener("submit", (event) => {
  event.preventDefault();
  if (socket) {
    leaving = true;
    socket.close(1000);
  } else {
    connect();
  }
});

page.composer.addEventListener("submit", (event) => {
  event.preventDefault();
  say();
});

show("closed", "Not connected");

// Opens the socket of the conversation the form names, as the participant
// its name makes, reading the agent's answers as they are written.
// The conversation starts over: its history comes first on the socket.
function connect() {
  const name = page.name.value.trim();
  const conversation = page.conversation.value.trim();
  user = userOf(name);
  page.messages.replaceChildren();
  indexes.clear();
  streams.clear();
  calls.clear();
  const query = new URLSearchParams({ user, stream: "1" });
  if (name) query.set("name", name);
  const scheme = location.protocol === "https:" ? "wss:" : "ws:";
  const path = `/conversations/${encodeURIComponent(conversation)}?${query}`;
  socket = new WebSocket(`${scheme}//${location.host}${path}`);
  leaving = false;
  show("connecting", `Joining ${conversation}…`);
  socket.addEventListener("open", () => show("open", `In ${conversation} as ${name || user}`));
  socket.addEventListener("message", (event) => received(event.data));
  socket.addEventListener("close", closed);
}

// The participant's id: the name in lowercase letters, digits, _ and -, or a
// random one, kept while the page is open, when nothing of the name is left.
function userOf(name) {
  const plain = name
    .normalize("NFKD")
    .replace(/\p{M}+/gu, "")
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
  if (plain) return plain;
  if (!guest) {
    const random = crypto.getRandomValues(new Uint8Array(6));
    guest = "guest-" + Array.from(random, (byte) => byte.toString(16).padStart(2, "0")).join("");
  }
  return guest;
}

// Sends what the person wrote, and shows it: the room delivers a message to
// every channel but its source, so it never comes back.
function say() {
  const text = page.text.value;
  if (!socket || socket.readyState !== WebSocket.OPEN || !text.trim()) return;
  if (new TextEncoder().encode(text).length > MESSAGE_LIMIT) {
    keepingTheEnd(() => note(`The message is over the server's limit of ${MESSAGE_LIMIT} bytes.`));
    return;
  }
  socket.send(text);
  page.text.value = "";
  keepingTheEnd(() => {
    page.messages.append(line("message mine", ownName(), text.trim()));
    page.messages.append(waiting);
  });
}

// One frame of the room: an error, a piece of an answer, or a row as the
// store holds it.
function received(data) {
  let frame;
  try {
    frame = JSON.parse(data);
  } catch {
    return;
  }
  keepingTheEnd(() => {
    waiting.remove();
    if (typeof frame.error === "string") {
      note(frame.error);
      return;
    }
    switch (frame.type) {
      case "history":
        history(frame.events);
        break;
      case "delta":
        grow(frame);
        break;
      case "message":
        message(frame);
        break;
      case "tool_call_start":
      case "tool_call_end":
        call(frame);
        break;
      default:
        // The room's other events are not shown.
        break;
    }
  });
}

// The conversation's past, oldest first, as the server sent it when the
// socket opened: each stored event the page does not show yet, a message of
// the person's own shown as theirs.
function history(events) {
  for (const event of Array.isArray(events) ? events : []) {
    if (event.type === "message") message(event);
    else if (event.type === "tool_call_start" || event.type === "tool_call_end") call(event, false);
  }
}

// A piece of an answer: it grows the bubble of its stream, made at the end
// of the conversation by its first piece.
function grow(frame) {
  let live = streams.get(frame.stream_id);
  if (!live) {
    live = { item: line("message theirs streaming", String(frame.from), ""), text: "" };
    streams.set(frame.stream_id, live);
    page.messages.append(live.item);
  }
  live.text += String(frame.text ?? "");
  setText(live.item, live.text);
}

// A message as the store holds it: it takes the place of its stream's
// bubble, at its index, once: one the history and a delivery both bring is
// shown once.
function message(frame) {
  const live = streams.get(frame.stream_id);
  streams.delete(frame.stream_id);
  if (indexes.has(frame.index)) {
    live?.item.remove();
    return;
  }
  const own = frame.participant_id != null && frame.participant_id === user;
  const who = own ? ownName() : String(frame.name ?? frame.participant_id ?? frame.from);
  const item = live ? live.item : line(own ? "message mine" : "message theirs", who, "");
  item.classList.remove("streaming");
  setText(item, String(frame.text ?? ""));
  place(item, frame.index);
}

// A tool call's row: one line per call, made by its start, and its end's
// result or failure written over it; a live end says the answer is coming.
function call(frame, live = true) {
  end(frame.stream_id);
  const tool = frame.tool ?? {};
  const name = String(tool.name ?? "a tool");
  let item = calls.get(tool.id);
  if (!item) {
    item = Object.assign(document.createElement("li"), { className: "tool" });
    calls.set(tool.id, item);
    place(item, frame.index);
  }
  if (frame.type === "tool_call_start") {
    item.textContent = `${name}…`;
    return;
  }
  const result = String(tool.result ?? "");
  item.classList.toggle("failed", tool.status !== "completed");
  item.textContent = tool.status === "completed" ? `${name}: ${result}` : `${name} failed: ${result}`;
  // The model reads the result before it answers.
  if (live) page.messages.append(waiting);
}

// Ends the bubble of a stream whose row is not a message: kept as it was
// written, or dropped when it shows nothing.
function end(streamId) {
  const live = streams.get(streamId);
  if (!live) return;
  streams.delete(streamId);
  live.item.classList.remove("streaming");
  if (!live.text.trim()) live.item.remove();
}

// Puts an item of the conversation at its index: before the first item with
// a higher one or an answer still streaming, which is stored after every row
// shown, else where it is, or at the end when it is new.
function place(item, index) {
  indexes.add(index);
  item.dataset.index = String(index);
  for (const other of page.messages.children) {
    const later = other.classList.contains("streaming") || Number(other.dataset.index) > index;
    if (other !== item && later) {
      page.messages.insertBefore(item, other);
      return;
    }
  }
  if (!item.isConnected) page.messages.append(item);
}

// How the person's own messages are labelled.
function ownName() {
  return page.name.value.trim() || "You";
}

// A message's item: who wrote it, and its text.
function line(kind, who, text) {
  const item = Object.assign(document.createElement("li"), { className: kind });
  const author = Object.assign(document.createElement("span"), { className: "who" });
  author.textContent = who;
  const body = Object.assign(document.createElement("p"), { className: "text" });
  item.append(author, body);
  setText(item, text);
  return item;
}

// Shows an answer's text without its leading or trailing whitespace, which
// the store keeps; an item with nothing to show is hidden.
function setText(item, text) {
  const trimmed = text.trim();
  item.querySelector(".text").textContent = trimmed;
  item.hidden = trimmed === "";
}

// A line of the conversation saying what went wrong.
function note(text) {
  page.messages.append(Object.assign(document.createElement("li"), {
    className: "note",
    textContent: text,
  }));
}

// The socket closed: the person left, the server closed it, or it never
// opened, a refused upgrade included, which a browser does not tell apart.
function closed(event) {
  socket = null;
  waiting.remove();
  for (const streamId of [...streams.keys()]) end(streamId);
  if (leaving) {
    show("closed", "Left the conversation");
  } else if (event.code === 1006) {
    show("closed", "Disconnected: the server refused the connection or could not be reached");
  } else {
    show("closed", `Disconnected: ${event.reason || `code ${event.code}`}`);
  }
}

// Shows the connection's state, and lets the person write only while open.
function show(state, text) {
  page.state.dataset.state = state;
  page.state.textContent = text;
  const open = state === "open";
  page.text.disabled = !open;
  page.send.disabled = !open;
  page.name.disabled = state !== "closed";
  page.conversation.disabled = state !== "closed";
  page.connect.textContent = state === "closed" ? "Join" : "Leave";
  if (open) page.text.focus();
}

// Runs a change of the conversation, then follows its end if the person was
// reading there.
function keepingTheEnd(change) {
  const list = page.scroller;
  const atTheEnd = list.scrollHeight - list.scrollTop - list.clientHeight < 48;
  change();
  if (atTheEnd) list.scrollTop = list.scrollHeight;
}
