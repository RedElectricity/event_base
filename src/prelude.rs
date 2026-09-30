//! The 90 % surface, one `use`.
//!
//! ```
//! use event_base::prelude::*;
//! ```
//!
//! Re‑exports the message/handler types you touch in every file, the user
//! macros (`#[handler]`, `send_msg!`, `start_system!`), and — with the default
//! `config` feature — the one‑line [`Bootstrap`](crate::Bootstrap) entry point.
//! Backend selection (`crossfire`, `redis_streams`, `mpmc`, WAL modules) stays
//! at the crate root so a program opts into exactly the machinery it uses.

pub use crate::core::error::CoreError;
pub use crate::core::handler::{Ack, EHandler};
pub use crate::core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
pub use crate::core::wal::wal::Wal;
pub use crate::core::{NodeType, set_node_name};

// The user macros. Re‑exported flat so `#[handler]` / `send_msg!` work after a
// single `use prelude::*`.
#[cfg(feature = "macro")]
pub use crate::{handler, send_msg, start_system};

#[cfg(feature = "config")]
pub use crate::{BootError, Bootstrap, NodeConfig, Running};
