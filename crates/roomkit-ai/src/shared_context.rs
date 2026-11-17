//! The generation context retained by hooks and thinkers.

use crate::AiContext;

crate::shared::shared_input!(
    /// The same mutable generation context, shared by its hook payload and
    /// thinker. Clones retain the object; [`snapshot`](Self::snapshot) explicitly
    /// reads an owned `AiContext`. Existing handles inside that context keep
    /// their own sharing semantics.
    SharedAiContext, AiContext
);
