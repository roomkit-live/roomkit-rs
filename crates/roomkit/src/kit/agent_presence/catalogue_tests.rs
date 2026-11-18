//! A discussion confirms a declared agent's absence before dropping its work.
use std::collections::{BTreeSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::model::{ChannelId, RoomId};
use crate::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, ExecutorInfoOptions, ServedExecutor, TurnBackend,
    TurnBackendHandle, TurnFuture, TurnLink, TurnRequest,
};
use crate::{ChannelDeclaration, RoomKit, RoomKitError, RoomKitOptions};

#[derive(Clone, Copy)]
enum Snapshot {
    Hosts(&'static [&'static str]),
    Failed,
}

struct Catalogue {
    snapshots: Option<Mutex<VecDeque<Snapshot>>>,
    reads: AtomicUsize,
}

impl TurnBackend for Catalogue {
    fn submit(&self, _: TurnRequest) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        unreachable!("the catalogue is read before submission")
    }
    fn serve(&self, _: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async { Ok(()) })
    }
    fn control(
        &self,
        _: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        Box::pin(async { Ok(None) })
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async { Ok(()) })
    }
    // Keep the default executors_now: an application can implement only executors.
    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        let snapshots = self.snapshots.as_ref()?;
        Some(Box::pin(async {
            self.reads.fetch_add(1, Ordering::SeqCst);
            match snapshots
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra read")
            {
                Snapshot::Failed => Err(RoomKitError::Runtime("catalogue unavailable".into())),
                Snapshot::Hosts([]) => Ok(Vec::new()),
                Snapshot::Hosts(hosts) => Ok(vec![ExecutorInfo::new(
                    "peer",
                    ExecutorInfoOptions {
                        hosts: Some(hosts.iter().map(|id| (*id).to_owned()).collect()),
                        ..Default::default()
                    },
                )]),
            }
        }))
    }
}

async fn check(snapshots: Option<&[Snapshot]>, expected: &[&str]) {
    let transport = Arc::new(Catalogue {
        snapshots: snapshots.map(|states| Mutex::new(states.iter().copied().collect())),
        reads: AtomicUsize::new(0),
    });
    let kit = RoomKit::new(RoomKitOptions {
        turn_backend: Some(TurnBackendHandle::new(Arc::<Catalogue>::clone(&transport))),
        ..Default::default()
    });
    let ids = ["alice", "bob"].map(|id| ChannelId::new(id).unwrap());
    for id in &ids {
        kit.register_channel(ChannelDeclaration::new(id.clone()))
            .unwrap();
    }
    let actual = kit
        .known_offline(&ids, &RoomId::new("r").unwrap(), None)
        .await;
    assert_eq!(
        actual,
        expected
            .iter()
            .map(|id| ChannelId::new(*id).unwrap())
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(
        transport.reads.load(Ordering::SeqCst),
        snapshots.map_or(0, <[Snapshot]>::len)
    );
    kit.close().await.unwrap();
}

#[tokio::test]
async fn a_worker_that_appears_on_confirmation_keeps_its_discussion_turn() {
    use Snapshot::Hosts;
    check(Some(&[Hosts(&[]), Hosts(&["alice", "bob"])]), &[]).await;
    check(Some(&[Hosts(&["alice"]), Hosts(&[])]), &["bob"]).await;
    check(Some(&[Hosts(&[]), Hosts(&[])]), &["alice", "bob"]).await;
    check(Some(&[Hosts(&["alice", "bob"])]), &[]).await;
}

#[tokio::test]
async fn an_unavailable_catalogue_never_drops_discussion_work() {
    check(None, &[]).await;
    check(Some(&[Snapshot::Failed]), &[]).await;
    check(Some(&[Snapshot::Hosts(&[]), Snapshot::Failed]), &[]).await;
}
