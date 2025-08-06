use crate::stream::{ChannelId, storage::StorageHandle};
use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};

#[pin_project]
#[derive(Debug, Clone)]
pub struct StreamInspector<S, T> {
    #[pin]
    inner: S,
    storage: StorageHandle<T>,
    channel: ChannelId,
}

impl<S, T> StreamInspector<S, T>
where
    S: Stream<Item = T>,
    T: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
{
    pub fn new(stream: S, storage: StorageHandle<T>, channel_name: impl AsRef<str>) -> Self {
        let channel = storage.register_channel(channel_name.as_ref());
        Self {
            inner: stream,
            storage,
            channel,
        }
    }

    pub fn with_channel_id(stream: S, storage: StorageHandle<T>, channel: ChannelId) -> Self {
        Self {
            inner: stream,
            storage,
            channel,
        }
    }
}

impl<S, T> Stream for StreamInspector<S, T>
where
    S: Stream<Item = T>,
    T: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let item_clone = item.clone();
                let _ = this.storage.append(*this.channel, &item_clone);
                Poll::Ready(Some(item))
            }
            other => other,
        }
    }
}

#[pin_project]
#[derive(Debug, Clone)]
pub struct StreamInspectorWithResults<S, T, E> {
    #[pin]
    inner: S,
    storage: StorageHandle<T>,
    channel: ChannelId,
    _error: std::marker::PhantomData<E>,
}

impl<S, T, E> StreamInspectorWithResults<S, T, E>
where
    S: Stream<Item = Result<T, E>>,
    T: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
    E: 'static,
{
    pub fn new(stream: S, storage: StorageHandle<T>, channel_name: impl AsRef<str>) -> Self {
        let channel = storage.register_channel(channel_name.as_ref());
        Self {
            inner: stream,
            storage,
            channel,
            _error: std::marker::PhantomData,
        }
    }
}

impl<S, T, E> Stream for StreamInspectorWithResults<S, T, E>
where
    S: Stream<Item = Result<T, E>>,
    T: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
    E: 'static,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                let item_clone = item.clone();
                let _ = this.storage.append(*this.channel, &item_clone);
                Poll::Ready(Some(Ok(item)))
            }
            other => other,
        }
    }
}
