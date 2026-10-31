use roomkit::model::RoomId;
use roomkit_core::rules::transcript::{Image, Message};
use std::{future::Future, pin::Pin};

/// Host-owned, room-authorized resolution of an opaque image reference (RFC §6.4).
/// Return a resolved data/HTTP image, or `None` for missing/inaccessible content.
/// Neither the resolver nor returned image bytes are persisted by the runtime.
pub trait ImageResolver: Send + Sync + 'static {
    /// Resolve only references authorized for `room`. Called afresh each turn;
    /// the runtime never reads a local path or caches the result across turns.
    fn resolve<'a>(
        &'a self,
        image: &'a Image,
        room: &'a RoomId,
    ) -> Pin<Box<dyn Future<Output = Option<Image>> + Send + 'a>>;
}

pub(crate) async fn resolve(
    messages: &mut [Message],
    vision: bool,
    resolver: Option<&dyn ImageResolver>,
    room: &RoomId,
) {
    for message in messages {
        let mut images = Vec::new();
        for image in std::mem::take(&mut message.images) {
            if !vision {
                continue;
            }
            if image.is_resolved() {
                images.push(image);
            } else if let Some(resolver) = resolver {
                match resolver
                    .resolve(&image, room)
                    .await
                    .filter(Image::is_resolved)
                {
                    Some(ready) => images.push(ready),
                    None => message.text.push_str("\n[Image unavailable]"),
                }
            } else {
                message.text.push_str("\n[Image unavailable]");
            }
        }
        message.images = images;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roomkit_core::rules::transcript::Role;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Resolver(AtomicUsize);
    impl ImageResolver for Resolver {
        fn resolve<'a>(
            &'a self,
            image: &'a Image,
            room: &'a RoomId,
        ) -> Pin<Box<dyn Future<Output = Option<Image>> + Send + 'a>> {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                if room == &RoomId::new("allowed").unwrap() && image.url() == "attachment://present"
                {
                    Image::new("data:image/png;base64,AQIDBA==", "image/png")
                } else {
                    None
                }
            })
        }
    }
    fn messages() -> Vec<Message> {
        let mut message = Message::new(Role::User, "Describe these images");
        for url in ["attachment://present", "attachment://missing"] {
            message.images.push(Image::new(url, "image/png").unwrap());
        }
        vec![message]
    }
    #[tokio::test]
    async fn resolution_is_opt_in_room_scoped_and_repeated_each_turn() {
        let resolver = Resolver(AtomicUsize::new(0));
        let mut input = messages();
        resolve(
            &mut input,
            false,
            Some(&resolver),
            &RoomId::new("allowed").unwrap(),
        )
        .await;
        assert_eq!(input.first().unwrap().images, Vec::<Image>::new());
        assert_eq!(resolver.0.load(Ordering::SeqCst), 0);
        for _ in 0..2 {
            let mut input = messages();
            resolve(
                &mut input,
                true,
                Some(&resolver),
                &RoomId::new("allowed").unwrap(),
            )
            .await;
            assert_eq!(input.first().unwrap().images.len(), 1);
            assert!(input.first().unwrap().text.ends_with("[Image unavailable]"));
        }
        assert_eq!(resolver.0.load(Ordering::SeqCst), 4);
        let mut input = messages();
        resolve(
            &mut input,
            true,
            Some(&resolver),
            &RoomId::new("foreign").unwrap(),
        )
        .await;
        assert_eq!(input.first().unwrap().images, Vec::<Image>::new());
    }
}
