//! A between-turn verb can close its owner, or be interrupted by its owner's close.
use super::{Backend, accepted, output, run};
use roomkit::turns::wire::Frame;
use roomkit::turns::{
    OwnerHost, RoomCall, RoomOwner, TurnClient, TurnClientOptions, TurnFuture, TurnRequest,
};
use roomkit::{ChannelError, RoomKitError, model::Metadata};
use serde_json::Value;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;

struct Host {
    client: Mutex<Weak<TurnClient>>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
}
impl OwnerHost for Host {
    fn call<'a>(
        &'a self,
        _: &'a TurnRequest,
        _: &'a str,
        _: Metadata,
    ) -> TurnFuture<'a, Result<Value, ChannelError>> {
        Box::pin(async { Err(RoomKitError::Value("unknown".into()).into()) })
    }
    fn call_between_turns<'a>(
        &'a self,
        call: &'a RoomCall,
    ) -> TurnFuture<'a, Result<Value, ChannelError>> {
        Box::pin(async move {
            self.entered
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            if call.verb == "close" {
                let client = self.client.lock().unwrap().upgrade().unwrap();
                client.close().await?;
                Ok(Value::Bool(true))
            } else {
                std::future::pending().await
            }
        })
    }
}
#[tokio::test]
async fn closing_a_client_interrupts_other_calls_but_retains_the_call_which_closes_it() {
    for closes_itself in [false, true] {
        let backend = Arc::new(Backend::default());
        let (entered, waiting) = oneshot::channel();
        let host = Arc::new(Host {
            client: Mutex::default(),
            entered: Mutex::new(Some(entered)),
        });
        let client = Arc::new(TurnClient::new(
            Arc::<Backend>::clone(&backend),
            TurnClientOptions {
                host: Some(Arc::<Host>::clone(&host)),
                ..TurnClientOptions::default()
            },
        ));
        *host.client.lock().unwrap() = Arc::downgrade(&client);
        let mut worker = backend.connect();
        worker.send(accepted());
        worker.send(output(false));
        worker.send(Frame::End);
        run(&client).await.unwrap();
        let request = backend.requests.lock().unwrap()[0].clone();
        let call = RoomCall::new(
            request.channel_id,
            request.room_id,
            "application",
            request.turn_id,
            if closes_itself { "close" } else { "wait" },
        );
        let owner = Arc::clone(&client);
        let task = tokio::spawn(async move { owner.room_call(call).await });
        waiting.await.unwrap();
        if !closes_itself {
            client.close().await.unwrap();
        }
        let answer = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        if closes_itself {
            assert_eq!(answer.unwrap().result, Value::Bool(true));
        } else {
            assert!(matches!(answer, Err(RoomKitError::Cancelled)));
        }
    }
}
