//! An application transport controlled by each test from its worker end.
use roomkit::RoomKitError;
use roomkit::turns::wire::{Control, Frame};
use roomkit::turns::{
    ChannelControl, ChannelResult, RoomOwner, ServedExecutor, TurnBackend, TurnFuture, TurnLink,
    TurnRequest,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

#[derive(Default)]
pub(super) struct Backend {
    pub(super) registrations: Mutex<Vec<&'static str>>,
    pub(super) requests: Mutex<Vec<TurnRequest>>,
    pub(super) controls: Mutex<Vec<ChannelControl>>,
    pub(super) owner: Mutex<Option<Arc<dyn RoomOwner>>>,
    pub(super) released: Arc<AtomicUsize>,
    connections: Mutex<Vec<Connection>>,
    delay: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}
struct Connection {
    frames: mpsc::UnboundedReceiver<(u64, Frame)>,
    controls: mpsc::UnboundedSender<Control>,
}
pub(super) struct Worker {
    frames: mpsc::UnboundedSender<(u64, Frame)>,
    pub(super) controls: mpsc::UnboundedReceiver<Control>,
    seq: u64,
}
impl Worker {
    pub(super) fn send(&mut self, frame: Frame) {
        self.seq += 1;
        self.frames.send((self.seq, frame)).unwrap();
    }
}
impl Backend {
    pub(super) fn delay_next(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, wait) = oneshot::channel();
        let (resume, resumed) = oneshot::channel();
        *self.delay.lock().unwrap() = Some((entered, resumed));
        (wait, resume)
    }
    pub(super) fn connect(&self) -> Worker {
        let (frames, frame_rx) = mpsc::unbounded_channel();
        let (controls, control_rx) = mpsc::unbounded_channel();
        self.connections.lock().unwrap().push(Connection {
            frames: frame_rx,
            controls,
        });
        Worker {
            frames,
            controls: control_rx,
            seq: 0,
        }
    }
}
impl TurnBackend for Backend {
    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.registrations.lock().unwrap().push("attend");
            *self.owner.lock().unwrap() = Some(owner);
            Ok(())
        })
    }
    fn serve(&self, _: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async {
            self.registrations.lock().unwrap().push("serve");
            Ok(())
        })
    }
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move {
            let Some(io) = self.connections.lock().unwrap().pop() else {
                return Err(RoomKitError::Value("submission refused".into()));
            };
            request.hand_over();
            let barrier = self.delay.lock().unwrap().take();
            if let Some((entered, resume)) = barrier {
                entered.send(()).unwrap();
                resume.await.unwrap();
            }
            self.requests.lock().unwrap().push(request.clone());
            let link: Arc<dyn TurnLink> = Arc::new(Link {
                request,
                frames: tokio::sync::Mutex::new(io.frames),
                controls: io.controls,
                released: Arc::clone(&self.released),
            });
            Ok(link)
        })
    }
    fn control(
        &self,
        msg: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        Box::pin(async move {
            self.controls.lock().unwrap().push(msg);
            Ok(None)
        })
    }
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async {
            self.registrations.lock().unwrap().push("close");
            Ok(())
        })
    }
}
struct Link {
    request: TurnRequest,
    frames: tokio::sync::Mutex<mpsc::UnboundedReceiver<(u64, Frame)>>,
    controls: mpsc::UnboundedSender<Control>,
    released: Arc<AtomicUsize>,
}
impl TurnLink for Link {
    fn request(&self) -> &TurnRequest {
        &self.request
    }
    fn taken(&self) -> bool {
        self.request.handed()
    }
    fn receive(&self) -> TurnFuture<'_, Result<(u64, Frame), RoomKitError>> {
        Box::pin(async {
            self.frames
                .lock()
                .await
                .recv()
                .await
                .ok_or(RoomKitError::KitClosed)
        })
    }
    fn send(&self, control: Control) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.controls
                .send(control)
                .map_err(|_| RoomKitError::KitClosed)
        })
    }
    fn release(&self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}
