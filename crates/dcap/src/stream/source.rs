use crate::stream::{ChannelId, Result, storage::StorageHandle};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct Source<T> {
    storage: StorageHandle<T>,
    channel: ChannelId,
    position: usize,
}

impl<T> Source<T>
where
    T: Serialize + for<'de> Deserialize<'de> + 'static,
{
    pub fn new(storage: StorageHandle<T>, channel_name: impl AsRef<str>) -> Option<Self> {
        let channel = storage.channel_id(channel_name.as_ref())?;
        Some(Self {
            storage,
            channel,
            position: 0,
        })
    }

    pub fn with_channel_id(storage: StorageHandle<T>, channel: ChannelId) -> Self {
        Self {
            storage,
            channel,
            position: 0,
        }
    }
}

impl<T> Stream for Source<T>
where
    T: Serialize + for<'de> Deserialize<'de> + 'static,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.storage.get(self.channel, self.position) {
            Ok(Some(item)) => {
                self.position += 1;
                Poll::Ready(Some(Ok(item)))
            }
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

pub struct SessionSource<T> {
    storage: StorageHandle<T>,
    position: usize,
}

impl<T> SessionSource<T>
where
    T: Serialize + for<'de> Deserialize<'de> + 'static,
{
    pub fn new(storage: StorageHandle<T>) -> Self {
        Self {
            storage,
            position: 0,
        }
    }
}

impl<T> Stream for SessionSource<T>
where
    T: Serialize + for<'de> Deserialize<'de> + 'static,
{
    type Item = Result<(ChannelId, T)>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.storage.get_globally(self.position) {
            Ok(Some(entry)) => {
                self.position += 1;
                Poll::Ready(Some(Ok(entry)))
            }
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}
