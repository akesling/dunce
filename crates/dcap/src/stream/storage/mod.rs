pub mod file;
pub mod handle;
pub mod memory;

pub use file::FileStorage;
pub use handle::StorageHandle;
pub use memory::MemoryStorage;

use crate::stream::{ChannelId, Result};

pub trait Storage<T>: Send + Sync {
    fn register_channel(&mut self, name: &str) -> ChannelId;
    fn channels(&self) -> Vec<ChannelId>;
    fn channel_id(&self, name: &str) -> Option<ChannelId>;
    fn channel_name(&self, id: ChannelId) -> Option<&str>;

    fn append(&mut self, channel: ChannelId, item: &T) -> Result<()>;

    fn get(&self, channel: ChannelId, index: usize) -> Result<Option<T>>;
    fn get_globally(&self, global_index: usize) -> Result<Option<(ChannelId, T)>>;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    fn channel_len(&self, channel: ChannelId) -> usize;
    fn channel_is_empty(&self, channel: ChannelId) -> bool;
}
