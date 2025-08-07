//! In-memory storage implementation

use super::Storage;
use crate::stream::{ChannelId, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// In-memory storage backend for captured stream data.
///
/// `MemoryStorage` stores all data in memory using standard Rust collections.
/// This provides fast access but data is not persisted between program runs.
/// It's ideal for temporary capture/replay scenarios and testing.
///
/// # Type Parameters
/// * `T` - The item type being stored
///
/// # Example
/// ```rust
/// use dcap::stream::{StorageHandle, MemoryStorage};
///
/// // Create memory storage for integers
/// let storage = StorageHandle::new(MemoryStorage::<i32>::new());
///
/// // Use with other dcap components
/// // let sink = Sink::new(storage.clone(), "data");
/// ```
#[derive(Debug)]
pub struct MemoryStorage<T> {
    channels: HashMap<ChannelId, Vec<T>>,
    channel_names: HashMap<String, ChannelId>,
    channel_lookup: HashMap<ChannelId, String>,
    next_channel_id: ChannelId,
    global_order: Vec<(ChannelId, usize)>,
}

impl<T> MemoryStorage<T> {
    /// Creates a new empty memory storage instance.
    ///
    /// # Example
    /// ```rust
    /// use dcap::stream::MemoryStorage;
    ///
    /// let storage = MemoryStorage::<String>::new();
    /// ```
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            channel_names: HashMap::new(),
            channel_lookup: HashMap::new(),
            next_channel_id: 0,
            global_order: Vec::new(),
        }
    }
}

impl<T> Default for MemoryStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Storage<T> for MemoryStorage<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
{
    fn register_channel(&mut self, name: &str) -> ChannelId {
        if let Some(&id) = self.channel_names.get(name) {
            return id;
        }

        let id = self.next_channel_id;
        self.next_channel_id += 1;
        self.channel_names.insert(name.to_string(), id);
        self.channel_lookup.insert(id, name.to_string());
        self.channels.insert(id, Vec::new());
        id
    }

    fn channel_id(&self, name: &str) -> Option<ChannelId> {
        self.channel_names.get(name).copied()
    }

    fn channel_name(&self, id: ChannelId) -> Option<&str> {
        self.channel_lookup.get(&id).map(|s| s.as_str())
    }

    fn append(&mut self, channel: ChannelId, item: &T) -> Result<()> {
        let channel_items = self.channels.entry(channel).or_default();
        let index = channel_items.len();
        channel_items.push(item.clone());
        self.global_order.push((channel, index));
        Ok(())
    }

    fn get(&self, channel: ChannelId, index: usize) -> Result<Option<T>> {
        Ok(self
            .channels
            .get(&channel)
            .and_then(|items| items.get(index))
            .cloned())
    }

    fn get_globally(&self, global_index: usize) -> Result<Option<(ChannelId, T)>> {
        if let Some(&(channel, idx)) = self.global_order.get(global_index) {
            if let Some(item) = self.get(channel, idx)? {
                return Ok(Some((channel, item)));
            }
        }
        Ok(None)
    }

    fn channels(&self) -> Vec<ChannelId> {
        self.channels.keys().copied().collect()
    }

    fn len(&self) -> usize {
        self.global_order.len()
    }

    fn channel_len(&self, channel: ChannelId) -> usize {
        self.channels.get(&channel).map_or(0, |v| v.len())
    }

    fn is_empty(&self) -> bool {
        self.global_order.is_empty()
    }

    fn channel_is_empty(&self, channel: ChannelId) -> bool {
        self.channels.get(&channel).is_none_or(|v| v.is_empty())
    }
}
