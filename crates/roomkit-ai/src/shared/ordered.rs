//! Insertion-ordered mappings whose clones share the supplied container.

use std::fmt;
use std::marker::PhantomData;

use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::Shared;

#[derive(Clone)]
pub(crate) struct Ordered<T>(Shared<Vec<(String, T)>>);

impl<T: Clone + fmt::Debug> fmt::Debug for Ordered<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<T> Default for Ordered<T> {
    fn default() -> Self {
        Self(Shared::new(Vec::new()))
    }
}

impl<T: Clone> Ordered<T> {
    pub(crate) fn insert(&self, name: String, value: T) -> Option<T> {
        self.0.update(|items| {
            if let Some((_, previous)) = items.iter_mut().find(|(key, _)| *key == name) {
                return Some(std::mem::replace(previous, value));
            }
            items.push((name, value));
            None
        })
    }

    pub(crate) fn get(&self, name: &str) -> Option<T> {
        self.0.update(|items| {
            items
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, v)| v.clone())
        })
    }

    pub(crate) fn remove(&self, name: &str) -> Option<T> {
        self.0.update(|items| {
            let at = items.iter().position(|(key, _)| key == name)?;
            Some(items.remove(at).1)
        })
    }

    pub(crate) fn snapshot(&self) -> Vec<(String, T)> {
        self.0.snapshot()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.update(|items| items.len())
    }
}

impl<T: Clone + PartialEq> PartialEq for Ordered<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.0.same_object(&other.0) {
            return true;
        }
        let left = self.snapshot();
        let right = other.snapshot();
        left.len() == right.len()
            && left
                .iter()
                .all(|(key, value)| right.iter().any(|pair| pair.0 == *key && pair.1 == *value))
    }
}

impl<T: Clone + Serialize> Serialize for Ordered<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let items = self.snapshot();
        let mut map = serializer.serialize_map(Some(items.len()))?;
        for (key, value) in items {
            map.serialize_entry(&key, &value)?;
        }
        map.end()
    }
}

impl<'de, T: Clone + Deserialize<'de>> Deserialize<'de> for Ordered<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Mapping<T>(PhantomData<T>);
        impl<'de, T: Clone + Deserialize<'de>> Visitor<'de> for Mapping<T> {
            type Value = Ordered<T>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object with string keys")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut input: A) -> Result<Self::Value, A::Error> {
                let result = Ordered::default();
                while let Some((key, value)) = input.next_entry()? {
                    result.insert(key, value);
                }
                Ok(result)
            }
        }
        deserializer.deserialize_map(Mapping(PhantomData))
    }
}

macro_rules! ordered_mapping {
    ($(#[$attribute:meta])* $name:ident, $value:ty) => {
        $(#[$attribute])*
        #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(crate::shared::ordered::Ordered<$value>);

        impl $name {
            /// An independent empty mapping.
            pub fn new() -> Self { Self::default() }
            /// Inserts a value, replacing an existing entry in its original position.
            pub fn insert(&self, name: impl Into<String>, value: $value) -> Option<$value> {
                self.0.insert(name.into(), value)
            }
            /// Reads a value; any handles inside it retain their own sharing.
            pub fn get(&self, name: &str) -> Option<$value> { self.0.get(name) }
            /// Removes the named entry, preserving the order of every other entry.
            pub fn remove(&self, name: &str) -> Option<$value> { self.0.remove(name) }
            /// The current number of entries.
            pub fn len(&self) -> usize { self.0.len() }
            /// Whether there is no entry.
            pub fn is_empty(&self) -> bool { self.len() == 0 }
            /// An owned list of entries in insertion order, without retaining a lock.
            pub fn snapshot(&self) -> Vec<(String, $value)> { self.0.snapshot() }
            /// Iterates one snapshot, keeping the insertion order.
            pub fn iter(&self) -> std::vec::IntoIter<(String, $value)> {
                self.snapshot().into_iter()
            }
        }

        impl<S: Into<String>> FromIterator<(S, $value)> for $name {
            fn from_iter<I: IntoIterator<Item = (S, $value)>>(items: I) -> Self {
                let mapping = Self::new();
                for (key, value) in items { mapping.insert(key, value); }
                mapping
            }
        }

        impl IntoIterator for &$name {
            type Item = (String, $value);
            type IntoIter = std::vec::IntoIter<Self::Item>;
            fn into_iter(self) -> Self::IntoIter { self.iter() }
        }
    };
}

pub(crate) use ordered_mapping;
