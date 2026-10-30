//! Stateless Responses, with provider-owned output replay inside one tool loop.

mod folding;
mod items;
mod replay;
mod wire;

pub(crate) use folding::Folding;
pub(crate) use wire::{body, request};

#[cfg(test)]
mod tests;
