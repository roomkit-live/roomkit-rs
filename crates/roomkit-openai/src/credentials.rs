//! Host-owned bearer credentials, resolved separately for every request.
use std::{future::Future, pin::Pin};

/// An asynchronous credential lookup. Implementations must bound their I/O;
/// cancelling generation may drop this future before it completes.
pub type TokenFuture<'a> = Pin<Box<dyn Future<Output = Result<String, TokenError>> + Send + 'a>>;

/// A source of renewable bearer credentials. No OAuth or storage policy lives
/// in the engine. Errors use a closed vocabulary so secrets cannot reach logs.
pub trait TokenSource: Send + Sync {
    /// Resolve a credential before each HTTP request, including tool rounds
    /// and each try of a request under `OpenAiConfig::max_retries`.
    /// An error fails the generation; there is no fallback to the static key.
    fn token(&self) -> TokenFuture<'_>;
}

/// Safe credential failures that can be shown to a user.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TokenError {
    /// A temporary lookup or refresh failure.
    #[error("provider credentials are unavailable; check the connection and account")]
    Unavailable,
    /// The account must authenticate again.
    #[error("the provider account must sign in again")]
    SignInRequired,
    /// The account needs to grant inference access.
    #[error("the provider account has not authorized model usage; sign in to grant access")]
    PermissionRequired,
}
