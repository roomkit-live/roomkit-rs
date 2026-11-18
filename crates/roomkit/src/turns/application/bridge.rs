//! Adapt a public owner link to the driver's numbered, bounded pipes.
use std::fmt;
use std::sync::Arc;
#[cfg(test)]
mod tests;

use super::super::pipe::{MessageOrder, PipeReceiver};
use super::super::wire::{Control, Frame};
use super::super::{MemoryTurn, TurnLink};
use crate::RoomKitError;

pub(crate) struct ActiveLink(std::sync::Weak<dyn TurnLink>);

impl ActiveLink {
    pub(in crate::turns) fn taken(&self) -> bool {
        self.0.upgrade().is_some_and(|link| link.taken())
    }
    pub(in crate::turns) fn beat_heard(&self) -> bool {
        self.0.upgrade().is_some_and(|link| link.beat_heard())
    }
}

impl fmt::Debug for ActiveLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ActiveLink")
    }
}

struct Release(Arc<dyn TurnLink>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub(in crate::turns) fn run_link(
    link: Arc<dyn TurnLink>,
    memory: Arc<MemoryTurn>,
    controls: PipeReceiver<Control>,
    mut pump: crate::channel::BoxFuture<'static, ()>,
) -> impl Future<Output = ()> + Send + 'static {
    // Own the release before spawning: shutdown may refuse to start this task.
    let release = Release(Arc::clone(&link));
    let _installed = memory
        .application_link
        .set(ActiveLink(Arc::downgrade(&link)));
    async move {
        let bridge = async {
            let result = tokio::select! {
                result = receive(&*link, &memory) => result,
                result = send(&*link, &memory, controls) => result,
            };
            if let Err(error) = result {
                drop(memory.frames.put(&Frame::Error {
                    error: super::super::error::kit_error(&error),
                }));
                drop(memory.frames.put(&Frame::End));
            }
        };
        tokio::select! {
            () = &mut pump => {},
            () = bridge => pump.await,
        }
        drop(release);
    }
}

async fn receive(link: &dyn TurnLink, memory: &MemoryTurn) -> Result<(), RoomKitError> {
    let mut order = MessageOrder::default();
    loop {
        let (seq, frame) = link.receive().await?;
        if order
            .admit(seq)
            .map_err(|e| super::super::TurnError::link(&e).into_kit_error())?
        {
            memory
                .frames
                .put(&frame)
                .map_err(|e| RoomKitError::TurnLink {
                    reason: "frame_invalid".into(),
                    message: Some(e.0),
                })?;
            if matches!(frame, Frame::End) {
                return Ok(());
            }
        }
    }
}

async fn send(
    link: &dyn TurnLink,
    memory: &MemoryTurn,
    mut controls: PipeReceiver<Control>,
) -> Result<(), RoomKitError> {
    while let Ok((_, control)) = controls.get().await {
        if matches!(control, Control::Pull { .. }) {
            let reader = crate::turns::lock(&memory.contexts).stream.clone();
            if let Some(context) = reader {
                context.scope(async { link.send(control).await }).await?;
                continue;
            }
        }
        link.send(control).await?;
    }
    Ok(())
}
