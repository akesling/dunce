//! Storage backends for captured stream data

pub mod file;
pub mod handle;
pub mod memory;

pub use file::FileStorage;
pub use handle::StorageHandle;
pub use memory::MemoryStorage;

use crate::stream::{ChannelId, Result};

/// Core storage trait for captured stream data.
///
/// This trait defines the interface for storage backends that can persist
/// captured stream data across multiple named channels.
///
/// # Type Parameters
/// * `T` - The item type being stored
pub trait Storage<T>: Send + Sync {
    /// Registers a new channel and returns its ID.
    fn register_channel(&mut self, name: &str) -> ChannelId;

    /// Returns a list of all registered channel IDs.
    fn channels(&self) -> Vec<ChannelId>;

    /// Gets the channel ID for a named channel, if it exists.
    fn channel_id(&self, name: &str) -> Option<ChannelId>;

    /// Gets the name of a channel by its ID.
    fn channel_name(&self, id: ChannelId) -> Option<&str>;

    /// Appends an item to the specified channel.
    fn append(&mut self, channel: ChannelId, item: &T) -> Result<()>;

    /// Gets an item from a specific channel at the given index.
    fn get(&self, channel: ChannelId, index: usize) -> Result<Option<T>>;

    /// Gets an item from the global capture order at the given index.
    /// Returns `(ChannelId, T)` to identify which channel the item came from.
    fn get_globally(&self, global_index: usize) -> Result<Option<(ChannelId, T)>>;

    /// Returns the total number of items stored across all channels.
    fn len(&self) -> usize;

    /// Returns true if no items are stored.
    fn is_empty(&self) -> bool;

    /// Returns the number of items stored in the specified channel.
    fn channel_len(&self, channel: ChannelId) -> usize;

    /// Returns true if the specified channel is empty.
    fn channel_is_empty(&self, channel: ChannelId) -> bool;
}
