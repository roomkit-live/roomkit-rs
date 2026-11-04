//! Completed task cache for delegation dedup (RFC §23.4): Python's
//! `tasks.cache`.
//!
//! Prevents re-delegation of recently completed tasks by keeping what they
//! returned under the room, the worker, the task and what the call passed
//! with it: another context is other work. An entry also keeps the agent
//! whose call ran it, whose repeat alone it answers. Entries expire after a
//! configurable time to live.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use roomkit_core::model::{ChannelId, Metadata, RoomId};
use serde_json::Value;
use tokio::time::Instant;

use super::json::{json_line, sorted_keys};
use super::text::is_python_space;

/// What [`CompletedTaskCache::new`] takes: Python's
/// `CompletedTaskCache(ttl_seconds=300.0)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompletedTaskCacheOptions {
    /// Python `ttl_seconds=300.0`: how long a completed task is kept.
    pub ttl: Duration,
}

impl Default for CompletedTaskCacheOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
        }
    }
}

/// What [`CompletedTaskCache::put`] takes beside the task and its result:
/// Python's `put(room_id, agent_id, task, result, *, context=None)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PutOptions {
    /// Python `context=None`: what the call passed with the task.
    pub context: Option<Metadata>,
}

/// What [`CompletedTaskCache::get`] takes beside the task: Python's
/// `get(room_id, agent_id, task, *, context=None)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GetOptions {
    /// Python `context=None`: what the call passed with the task.
    pub context: Option<Metadata>,
}

/// What [`CompletedTaskCache::recent_context`] takes beside the room:
/// Python's `recent_context(room_id, limit=5)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecentContextOptions {
    /// Python `limit=5`: how many tasks at most.
    pub limit: usize,
}

impl Default for RecentContextOptions {
    fn default() -> Self {
        Self { limit: 5 }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    room_id: RoomId,
    agent_id: ChannelId,
    /// The task, whatever its case and outer spaces, and what was passed
    /// with it, whatever the order of its keys. Python hashes the same
    /// text; the key's value is no one else's to read.
    work: String,
    /// Whether the entry answers a repeat.
    answers: bool,
    /// The agent whose call it ran for: what it returned may hold what that
    /// agent passed or may read. An entry the host wrote has none.
    caller: Option<ChannelId>,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    result: Metadata,
    created_at: Instant,
    /// When it was kept, among the cache's entries: two may share an instant.
    order: u64,
    task_text: String,
}

#[derive(Debug, Default)]
struct Entries {
    held: HashMap<CacheKey, CacheEntry>,
    kept: u64,
}

/// An in-memory cache of completed delegation results, each for a time to
/// live: Python's `CompletedTaskCache`. Cloning it is cheap: the clones
/// share its entries.
#[derive(Clone, Debug)]
pub struct CompletedTaskCache {
    ttl: Duration,
    entries: Arc<Mutex<Entries>>,
}

impl Default for CompletedTaskCache {
    fn default() -> Self {
        Self::new(CompletedTaskCacheOptions::default())
    }
}

impl CompletedTaskCache {
    /// The cache, as `options` say.
    #[must_use]
    pub fn new(options: CompletedTaskCacheOptions) -> Self {
        let CompletedTaskCacheOptions { ttl } = options;
        Self {
            ttl,
            entries: Arc::default(),
        }
    }

    /// Stores a completed task's result, under `task` and the context it
    /// was run with: a task the host wrote, which answers any agent.
    pub fn put(
        &self,
        room_id: &RoomId,
        agent_id: &ChannelId,
        task: &str,
        result: Metadata,
        options: PutOptions,
    ) {
        let PutOptions { context } = options;
        let kept = Kept {
            room_id,
            agent_id,
            task,
            context: context.as_ref(),
        };
        self.keep(&kept, result, None, true);
    }

    /// Keeps a completed task, for the agent that called (`caller`):
    /// `answers` says whether what it returned answers a repeat, which one
    /// depending on when it ran does not.
    pub(crate) fn keep(
        &self,
        kept: &Kept<'_>,
        result: Metadata,
        caller: Option<&ChannelId>,
        answers: bool,
    ) {
        let key = CacheKey {
            room_id: kept.room_id.clone(),
            agent_id: kept.agent_id.clone(),
            work: work(kept.task, kept.context),
            answers,
            caller: caller.cloned(),
        };
        let mut entries = self.lock();
        entries.kept += 1;
        let order = entries.kept;
        entries.held.insert(
            key,
            CacheEntry {
                result,
                created_at: Instant::now(),
                order,
                task_text: kept.task.to_owned(),
            },
        );
    }

    /// The cached result of `task` run with `options.context`, if present
    /// and not expired, else `None`: the host's read, whichever agent's
    /// call ran it, the latest when several did.
    #[must_use]
    pub fn get(
        &self,
        room_id: &RoomId,
        agent_id: &ChannelId,
        task: &str,
        options: GetOptions,
    ) -> Option<Metadata> {
        let GetOptions { context } = options;
        let work = work(task, context.as_ref());
        let mut entries = self.lock();
        self.drop_expired(&mut entries);
        entries
            .held
            .iter()
            .filter(|(key, _)| {
                key.answers
                    && key.room_id == *room_id
                    && key.agent_id == *agent_id
                    && key.work == work
            })
            .max_by_key(|(_, entry)| entry.order)
            .map(|(_, entry)| entry.result.clone())
    }

    /// What answers `caller`'s repeat of `task`: its own call's result or
    /// one the host wrote, the latest of the two, never another agent's,
    /// which may hold what that agent passed or may read (RFC §23.4).
    pub(crate) fn answer_for(&self, kept: &Kept<'_>, caller: &ChannelId) -> Option<Metadata> {
        let work = work(kept.task, kept.context);
        let mut entries = self.lock();
        self.drop_expired(&mut entries);
        entries
            .held
            .iter()
            .filter(|(key, _)| {
                key.answers
                    && key.room_id == *kept.room_id
                    && key.agent_id == *kept.agent_id
                    && key.work == work
                    && key.caller.as_ref().is_none_or(|who| who == caller)
            })
            .max_by_key(|(_, entry)| entry.order)
            .map(|(_, entry)| entry.result.clone())
    }

    /// The tasks kept for `room_id`, newest first, up to `options.limit`,
    /// each once, whatever its case and outer spaces, however many times it
    /// ran; expired entries are skipped. Python's `recent_context`.
    #[must_use]
    pub fn recent_context(&self, room_id: &RoomId, options: RecentContextOptions) -> Vec<String> {
        let RecentContextOptions { limit } = options;
        self.recent(room_id, limit, |_| true)
    }

    /// [`recent_context`](Self::recent_context) for `caller`: its own tasks
    /// and those the host wrote, never another agent's, which may hold what
    /// `caller` may not read (RFC §23.4).
    pub(crate) fn recent_of(
        &self,
        room_id: &RoomId,
        caller: &ChannelId,
        limit: usize,
    ) -> Vec<String> {
        self.recent(room_id, limit, |who| who.is_none_or(|who| who == caller))
    }

    fn recent(
        &self,
        room_id: &RoomId,
        limit: usize,
        read: impl Fn(Option<&ChannelId>) -> bool,
    ) -> Vec<String> {
        let mut entries = self.lock();
        self.drop_expired(&mut entries);
        let mut relevant: Vec<&CacheEntry> = entries
            .held
            .iter()
            .filter(|(key, _)| key.room_id == *room_id && read(key.caller.as_ref()))
            .map(|(_, entry)| entry)
            .collect();
        relevant.sort_by_key(|entry| std::cmp::Reverse(entry.order));
        let mut seen = HashSet::new();
        relevant
            .into_iter()
            .filter(|entry| seen.insert(folded(&entry.task_text)))
            .take(limit)
            .map(|entry| entry.task_text.clone())
            .collect()
    }

    /// Removes all entries.
    pub fn clear(&self) {
        self.lock().held.clear();
    }

    fn drop_expired(&self, entries: &mut Entries) {
        let ttl = self.ttl;
        entries
            .held
            .retain(|_, entry| entry.created_at.elapsed() <= ttl);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What names a task in the cache.
pub(crate) struct Kept<'a> {
    pub(crate) room_id: &'a RoomId,
    pub(crate) agent_id: &'a ChannelId,
    pub(crate) task: &'a str,
    pub(crate) context: Option<&'a Metadata>,
}

/// `task` as a repeat reads it: its case and outer spaces aside.
fn folded(task: &str) -> String {
    task.trim_matches(is_python_space).to_lowercase()
}

/// What makes `task` and `context` the same work. It reads them as one JSON
/// array, so a context never reads as part of the task.
fn work(task: &str, context: Option<&Metadata>) -> String {
    let context = context.cloned().unwrap_or_default();
    json_line(&Value::Array(vec![
        Value::String(folded(task)),
        sorted_keys(&Value::Object(context)),
    ]))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn room(id: &str) -> RoomId {
        RoomId::new(id).expect("a room id")
    }

    fn agent(id: &str) -> ChannelId {
        ChannelId::new(id).expect("a channel id")
    }

    fn done(status: &str) -> Metadata {
        Metadata::from_iter([("status".to_owned(), json!(status))])
    }

    fn context(value: &Value) -> Metadata {
        value.as_object().cloned().expect("an object")
    }

    fn kept<'a>(room_id: &'a RoomId, agent_id: &'a ChannelId, task: &'a str) -> Kept<'a> {
        Kept {
            room_id,
            agent_id,
            task,
            context: None,
        }
    }

    fn put(cache: &CompletedTaskCache, room_id: &RoomId, task: &str, result: Metadata) {
        cache.put(
            room_id,
            &agent("agent-a"),
            task,
            result,
            PutOptions::default(),
        );
    }

    fn get(cache: &CompletedTaskCache, room_id: &RoomId, task: &str) -> Option<Metadata> {
        cache.get(room_id, &agent("agent-a"), task, GetOptions::default())
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_put_and_get
    #[test]
    fn put_and_get() {
        let cache = CompletedTaskCache::default();
        put(&cache, &room("room-1"), "search for cats", done("done"));
        assert_eq!(
            get(&cache, &room("room-1"), "search for cats"),
            Some(done("done"))
        );
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_get_returns_none_when_missing
    #[test]
    fn get_returns_none_when_missing() {
        let cache = CompletedTaskCache::default();
        assert_eq!(get(&cache, &room("room-1"), "unknown task"), None);
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_get_returns_none_when_expired
    #[tokio::test(start_paused = true)]
    async fn get_returns_none_when_expired() {
        let cache = CompletedTaskCache::new(CompletedTaskCacheOptions {
            ttl: Duration::from_millis(10),
        });
        put(&cache, &room("room-1"), "task", done("done"));
        tokio::time::advance(Duration::from_millis(20)).await;
        assert_eq!(get(&cache, &room("room-1"), "task"), None);
        assert_eq!(
            cache.recent_context(&room("room-1"), RecentContextOptions::default()),
            Vec::<String>::new()
        );
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_case_insensitive_hash
    #[test]
    fn a_task_reads_whatever_its_case_and_outer_spaces() {
        let cache = CompletedTaskCache::default();
        put(&cache, &room("room-1"), "Search For Cats", done("done"));
        assert_eq!(
            get(&cache, &room("room-1"), "  search for cats\u{1c}"),
            Some(done("done"))
        );
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_different_rooms_isolated
    #[test]
    fn different_rooms_are_isolated() {
        let cache = CompletedTaskCache::default();
        put(&cache, &room("room-1"), "task", done("r1"));
        assert_eq!(get(&cache, &room("room-2"), "task"), None);
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_the_context_is_part_of_the_key
    #[test]
    fn the_context_is_part_of_the_key() {
        let cache = CompletedTaskCache::default();
        cache.put(
            &room("room-1"),
            &agent("agent-a"),
            "task",
            done("done"),
            PutOptions {
                context: Some(context(
                    &json!({"x": 1, "y": {"b": 2, "a": [{"d": 1, "c": 2}]}}),
                )),
            },
        );
        let with = |value: Value| {
            cache.get(
                &room("room-1"),
                &agent("agent-a"),
                "task",
                GetOptions {
                    context: Some(context(&value)),
                },
            )
        };
        assert_eq!(get(&cache, &room("room-1"), "task"), None);
        assert_eq!(with(json!({"x": 2})), None);
        assert_eq!(
            with(json!({"y": {"a": [{"c": 2, "d": 1}], "b": 2}, "x": 1})),
            Some(done("done"))
        );
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_a_context_never_reads_as_part_of_the_task
    #[test]
    fn a_context_never_reads_as_part_of_the_task() {
        let cache = CompletedTaskCache::default();
        cache.put(
            &room("room-1"),
            &agent("agent-a"),
            "a",
            done("done"),
            PutOptions {
                context: Some(context(&json!({"x": 1}))),
            },
        );
        assert_eq!(get(&cache, &room("room-1"), "a\n{\"x\": 1}"), None);
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_the_host_reads_what_an_agent_s_call_kept
    #[test]
    fn the_host_reads_what_an_agent_s_call_kept() {
        let cache = CompletedTaskCache::default();
        let (room_id, worker) = (room("room-1"), agent("agent-a"));
        let named = kept(&room_id, &worker, "task");
        cache.keep(&named, done("kept"), Some(&agent("boss")), true);

        assert_eq!(get(&cache, &room_id, "task"), Some(done("kept")));
        assert_eq!(cache.answer_for(&named, &agent("boss")), Some(done("kept")));
        assert_eq!(cache.answer_for(&named, &agent("other")), None);
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_a_task_the_host_wrote_answers_any_agent
    #[test]
    fn a_task_the_host_wrote_answers_any_agent() {
        let cache = CompletedTaskCache::default();
        let (room_id, worker) = (room("room-1"), agent("agent-a"));
        put(&cache, &room_id, "task", done("written"));

        assert_eq!(
            cache.answer_for(&kept(&room_id, &worker, "task"), &agent("boss")),
            Some(done("written"))
        );
        assert_eq!(cache.recent_of(&room_id, &agent("boss"), 3), ["task"]);
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_a_kept_task_that_answers_no_repeat_is_read_by_no_get
    #[test]
    fn a_kept_task_that_answers_no_repeat_is_read_by_no_get() {
        let cache = CompletedTaskCache::default();
        let (room_id, worker) = (room("room-1"), agent("agent-a"));
        let named = kept(&room_id, &worker, "task");
        cache.keep(&named, Metadata::new(), Some(&agent("boss")), false);

        assert_eq!(get(&cache, &room_id, "task"), None);
        assert_eq!(cache.answer_for(&named, &agent("boss")), None);
        assert_eq!(
            cache.recent_context(&room_id, RecentContextOptions::default()),
            ["task"]
        );
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_the_latest_of_what_is_kept_answers
    #[test]
    fn the_latest_of_what_is_kept_answers() {
        let cache = CompletedTaskCache::default();
        let (room_id, worker) = (room("room-1"), agent("agent-a"));
        let named = kept(&room_id, &worker, "task");
        cache.keep(&named, done("first"), Some(&agent("boss")), true);
        cache.keep(&named, done("second"), Some(&agent("other")), true);
        put(&cache, &room_id, "task", done("the host's"));

        assert_eq!(get(&cache, &room_id, "task"), Some(done("the host's")));
        assert_eq!(
            cache.answer_for(&named, &agent("boss")),
            Some(done("the host's"))
        );

        cache.keep(&named, done("third"), Some(&agent("boss")), true);
        assert_eq!(
            cache.answer_for(&named, &agent("boss")),
            Some(done("third"))
        );
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_recent_context_lists_a_task_once
    #[test]
    fn recent_context_lists_a_task_once() {
        let cache = CompletedTaskCache::default();
        let (room_id, worker) = (room("room-1"), agent("agent-a"));
        put(&cache, &room_id, "Other.", Metadata::new());
        cache.put(
            &room_id,
            &worker,
            "Fix.",
            Metadata::new(),
            PutOptions {
                context: Some(context(&json!({"x": 1}))),
            },
        );
        cache.put(
            &room_id,
            &worker,
            " fix. ",
            Metadata::new(),
            PutOptions {
                context: Some(context(&json!({"x": 2}))),
            },
        );

        assert_eq!(
            cache.recent_context(&room_id, RecentContextOptions { limit: 2 }),
            [" fix. ", "Other."]
        );
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_recent_context
    #[test]
    fn recent_context_lists_the_room_s_newest_first() {
        let cache = CompletedTaskCache::default();
        let room_id = room("room-1");
        put(&cache, &room_id, "task 1", done("done"));
        put(&cache, &room_id, "task 2", done("done"));
        put(&cache, &room("room-2"), "task 3", done("done"));

        assert_eq!(
            cache.recent_context(&room_id, RecentContextOptions::default()),
            ["task 2", "task 1"]
        );
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_recent_context_respects_limit
    #[test]
    fn recent_context_respects_its_limit() {
        let cache = CompletedTaskCache::default();
        let room_id = room("room-1");
        for at in 0..10 {
            put(&cache, &room_id, &format!("task {at}"), done("done"));
        }
        assert_eq!(
            cache
                .recent_context(&room_id, RecentContextOptions { limit: 3 })
                .len(),
            3
        );
    }

    // Python: tests/tasks/test_cache.py::TestCompletedTaskCache::test_clear
    #[test]
    fn clear_removes_all_entries() {
        let cache = CompletedTaskCache::default();
        put(&cache, &room("room-1"), "task", done("done"));
        cache.clear();
        assert_eq!(get(&cache, &room("room-1"), "task"), None);
    }
}
