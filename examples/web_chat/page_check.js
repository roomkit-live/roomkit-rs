// The web chat's check of its page (examples/web_chat.rs runs it with node):
// app.js, read from stdin as the server serves it, on a stub of the few DOM
// calls it makes, fed frames through a fake WebSocket. It prints, for each
// scenario, the conversation's items in their order, as JSON. It is not
// served.
"use strict";

const fs = require("fs");
const vm = require("vm");

// An element: its classes, data, text, children and listeners.
class Element {
  constructor() {
    this.parent = null;
    this.children = [];
    this.classes = new Set();
    this.dataset = {};
    this.hidden = false;
    this.disabled = false;
    this.value = "";
    this.listeners = {};
    this.ownText = "";
    this.scrollTop = 0;
    this.scrollHeight = 0;
    this.clientHeight = 0;
    const classes = this.classes;
    this.classList = {
      add: (name) => classes.add(name),
      remove: (name) => classes.delete(name),
      contains: (name) => classes.has(name),
      toggle: (name, on) => (on ?? !classes.has(name) ? classes.add(name) : classes.delete(name)),
    };
  }
  get className() { return [...this.classes].join(" "); }
  set className(names) { this.classes.clear(); names.split(/\s+/).filter(Boolean).forEach((name) => this.classes.add(name)); }
  get textContent() { return this.ownText + this.children.map((child) => child.textContent).join(""); }
  set textContent(text) { this.children.forEach((child) => (child.parent = null)); this.children = []; this.ownText = String(text); }
  get isConnected() { return this.parent !== null && (this.parent.root || this.parent.isConnected); }
  append(...nodes) { nodes.forEach((node) => this.insertBefore(node, null)); }
  insertBefore(node, before) {
    node.remove();
    const at = before ? this.children.indexOf(before) : -1;
    this.children.splice(at < 0 ? this.children.length : at, 0, node);
    node.parent = this;
  }
  remove() {
    if (!this.parent) return;
    this.parent.children.splice(this.parent.children.indexOf(this), 1);
    this.parent = null;
  }
  replaceChildren() { [...this.children].forEach((child) => child.remove()); }
  querySelector(selector) {
    const name = selector.replace(/^\./, "");
    for (const child of this.children) {
      if (child.classes.has(name)) return child;
      const found = child.querySelector(selector);
      if (found) return found;
    }
    return null;
  }
  addEventListener(type, listener) { (this.listeners[type] ??= []).push(listener); }
  dispatch(type, event) { (this.listeners[type] ?? []).forEach((listener) => listener(event)); }
  focus() {}
}

// The page's elements, attached to a root, as index.html holds them.
const root = Object.assign(new Element(), { root: true });
const elements = {};
for (const id of ["state", "join", "name", "conversation", "connect", "scroller", "messages", "composer", "text", "send"]) {
  elements[id] = new Element();
  root.append(elements[id]);
}

// A WebSocket the check feeds.
const sockets = [];
class FakeWebSocket {
  constructor(url) { this.url = url; this.readyState = 0; this.listeners = {}; sockets.push(this); }
  addEventListener(type, listener) { (this.listeners[type] ??= []).push(listener); }
  dispatch(type, event) { (this.listeners[type] ?? []).forEach((listener) => listener(event)); }
  send() {}
  close(code) { this.readyState = 3; this.dispatch("close", { code: code ?? 1000, reason: "" }); }
}
FakeWebSocket.OPEN = 1;

const context = vm.createContext({
  document: {
    getElementById: (id) => elements[id] ?? null,
    createElement: () => new Element(),
  },
  location: { protocol: "http:", host: "127.0.0.1:1" },
  WebSocket: FakeWebSocket,
  TextEncoder,
  URLSearchParams,
  crypto: globalThis.crypto,
});
vm.runInContext(fs.readFileSync(0, "utf8"), context);

const message = (index, from, text, extra = {}) => ({ type: "message", room_id: "r", id: `evt_${index}`, index, from, text, ...extra });
const STREAM = "assistant:evt_2";
const SCENARIOS = {
  // A piece of the answer reaches the socket before the history.
  "delta before history": [
    { type: "delta", stream_id: STREAM, from: "assistant", text: "Yes, order 42" },
    { type: "history", room_id: "r", events: [
      message(0, "websocket", "Hi", { participant_id: "alice", name: "Alice" }),
      message(1, "assistant", "Hello Alice"),
      message(2, "websocket", "Is my order 42 ready?", { participant_id: "alice", name: "Alice" }),
    ] },
    { type: "delta", stream_id: STREAM, from: "assistant", text: " is ready." },
    message(3, "assistant", "Yes, order 42 is ready.", { stream_id: STREAM }),
  ],
  // A row delivered before the history, which holds it too.
  "row before history": [
    message(3, "assistant", "Answer three"),
    { type: "history", room_id: "r", events: [
      message(1, "websocket", "Q one", { participant_id: "bob", name: "Bob" }),
      message(2, "assistant", "Answer two"),
      message(3, "assistant", "Answer three"),
    ] },
  ],
  // The row of one answer while another still streams.
  "two streams": [
    { type: "history", room_id: "r", events: [message(1, "websocket", "Q", { participant_id: "bob" })] },
    { type: "delta", stream_id: "a:1", from: "a", text: "A streaming" },
    { type: "delta", stream_id: "b:1", from: "b", text: "B streaming" },
    message(2, "b", "B done", { stream_id: "b:1" }),
    message(3, "a", "A done", { stream_id: "a:1" }),
  ],
};

const shown = {};
for (const [name, frames] of Object.entries(SCENARIOS)) {
  elements.name.value = "Bob";
  elements.join.dispatch("submit", { preventDefault() {} });
  const socket = sockets.at(-1);
  socket.readyState = 1;
  socket.dispatch("open", {});
  frames.forEach((frame) => socket.dispatch("message", { data: JSON.stringify(frame) }));
  shown[name] = elements.messages.children
    .filter((item) => !item.hidden)
    .map((item) => `${item.className}#${item.dataset.index ?? "-"}: ${item.textContent}`);
  elements.join.dispatch("submit", { preventDefault() {} });
}
process.stdout.write(JSON.stringify(shown));
