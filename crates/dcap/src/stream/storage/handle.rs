use super::Storage;
use crate::stream::{ChannelId, Result};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct StorageHandle<T> {
    pub(crate) inner: Arc<Mutex<dyn Storage<T>>>,
}

impl<T> StorageHandle<T> {
    pub fn new<S>(storage: S) -> Self
    where
        S: Storage<T> + 'static,
    {
        Self {
            inner: Arc::new(Mutex::new(storage)),
        }
    }

    pub fn register_channel(&self, name: &str) -> ChannelId {
        self.inner.lock().unwrap().register_channel(name)
    }

    pub fn channel_id(&self, name: &str) -> Option<ChannelId> {
        self.inner.lock().unwrap().channel_id(name)
    }

    pub fn channel_name(&self, id: ChannelId) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .channel_name(id)
            .map(|s| s.to_string())
    }

    pub fn append(&self, channel: ChannelId, item: &T) -> Result<()> {
        self.inner.lock().unwrap().append(channel, item)
    }

    pub fn get(&self, channel: ChannelId, index: usize) -> Result<Option<T>> {
        self.inner.lock().unwrap().get(channel, index)
    }

    pub fn get_globally(&self, global_index: usize) -> Result<Option<(ChannelId, T)>> {
        self.inner.lock().unwrap().get_globally(global_index)
    }

    pub fn channels(&self) -> Vec<ChannelId> {
        self.inner.lock().unwrap().channels()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn channel_len(&self, channel: ChannelId) -> usize {
        self.inner.lock().unwrap().channel_len(channel)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }

    pub fn channel_is_empty(&self, channel: ChannelId) -> bool {
        self.inner.lock().unwrap().channel_is_empty(channel)
    }
}

impl<T> std::fmt::Debug for StorageHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageHandle").finish_non_exhaustive()
    }
}
