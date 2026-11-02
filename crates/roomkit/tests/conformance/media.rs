//! The kit's readers of an author's media (roomkit-specs
//! `conformance/README.md`, "Setup"): `setup.kit.media_resolver` answers
//! each reference it names with its bytes, written in base64, recording
//! each call under `media_resolver`, and `setup.kit.attached_media` names
//! as an event's media the media contents its metadata lists under `key`
//! (RFC §6.4).

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use roomkit::model::EventContent;
use roomkit::{AttachedMedia, MediaResolver, RoomKitOptions};
use serde_json::{Value, json};

use crate::identity::RunSlot;
use crate::run::lock;

/// The kit's media readers a scenario's `setup.kit` names, over `options`:
/// the resolver records its calls into the run `slot` holds.
pub(crate) fn kit_options<S>(
    mut options: RoomKitOptions<S>,
    kit: &Value,
    slot: &RunSlot,
) -> RoomKitOptions<S> {
    if let Some(media) = kit["media_resolver"]["media"].as_object() {
        let held: HashMap<String, Vec<u8>> = media
            .iter()
            .filter_map(|(url, data)| Some((url.clone(), decoded(data.as_str()?)?)))
            .collect();
        let slot = slot.clone();
        options.media_resolver = Some(MediaResolver::new(move |media, room_id| {
            if let Some(run) = slot
                .run()
                .filter(|run| run.recording.load(Ordering::SeqCst))
            {
                let call = json!({ "url": media.url, "room_id": room_id });
                lock(&run.observed).media_resolver.push(call);
            }
            let bytes = held.get(&media.url).cloned();
            async move { Ok(bytes) }
        }));
    }
    if let Some(key) = kit["attached_media"]["key"].as_str() {
        let key = key.to_owned();
        options.attached_media = Some(AttachedMedia::new(move |event| {
            let records = event.metadata.get(&key).and_then(Value::as_array);
            Ok(records
                .into_iter()
                .flatten()
                .filter_map(|record| match serde_json::from_value(record.clone()) {
                    Ok(EventContent::Media(media)) => Some(media),
                    _ => None,
                })
                .collect())
        }));
    }
    options
}

/// The bytes `text` writes in standard base64, padded; `None` for a text
/// that is no such base64.
fn decoded(text: &str) -> Option<Vec<u8>> {
    let value = |byte: u8| match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    };
    let data = text.trim_end_matches('=').as_bytes();
    let mut bytes = Vec::with_capacity(data.len() * 3 / 4);
    let (mut word, mut bits) = (0u32, 0u32);
    for &byte in data {
        word = (word << 6) | u32::from(value(byte)?);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push(u8::try_from((word >> bits) & 0xff).ok()?);
        }
    }
    Some(bytes)
}
