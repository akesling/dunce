//! Stream inspection utilities for transparent monitoring and capture

use crate::stream::{ChannelId, storage::StorageHandle};
use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A transparent stream inspector that captures items while allowing them to pass through unchanged.
///
/// `Inspector` wraps an existing stream and captures each item to storage while
/// forwarding the items unchanged to downstream consumers. This enables transparent
/// monitoring and logging of stream data without affecting the normal flow.
///
/// # Type Parameters
/// * `S` - The inner stream type
/// * `T` - The item type, must be serializable
///
/// # Example
/// ```rust
/// use dcap::{Inspector};
/// use dcap::stream::{StorageHandle, MemoryStorage};
/// use futures::{StreamExt, stream};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = StorageHandle::new(MemoryStorage::new());
/// let data = vec![1, 2, 3];
/// let data_stream = stream::iter(data.clone());
///
/// // Wrap the stream with an inspector
/// let inspector = Inspector::new(data_stream, storage.clone(), "monitoring");
///
/// // Process the stream normally - inspector is transparent
/// let processed: Vec<i32> = inspector.collect().await;
/// assert_eq!(processed, data);
///
/// // The data was also captured to storage
/// assert_eq!(storage.len(), 3);
/// # Ok(())
/// # }
/// ```
#[pin_project]
#[derive(Debug, Clone)]
pub struct Inspector<S, T> {
    #[pin]
    inner: S,
    storage: StorageHandle<T>,
    channel: ChannelId,
}

impl<S, T> Inspector<S, T>
where
    S: Stream<Item = T>,
    T: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
{
    /// Creates a new inspector for the given stream.
    ///
    /// # Arguments
    /// * `stream` - The stream to wrap and monitor
    /// * `storage` - Storage handle for captured data
    /// * `channel_name` - Name of the channel to store data in
    ///
    /// # Example
    /// ```rust
    /// use dcap::Inspector;
    /// use dcap::stream::{StorageHandle, MemoryStorage};
    /// use futures::stream;
    ///
    /// let storage = StorageHandle::new(MemoryStorage::new());
    /// let data_stream = stream::iter(vec![1, 2, 3]);
    /// let inspector = Inspector::new(data_stream, storage, "my_channel");
    /// ```
    pub fn new(stream: S, storage: StorageHandle<T>, channel_name: impl AsRef<str>) -> Self {
        let channel = storage.register_channel(channel_name.as_ref());
        Self {
            inner: stream,
            storage,
            channel,
        }
    }

    /// Creates a new inspector using an existing channel ID.
    ///
    /// This is useful when you need precise control over channel management
    /// or when working with pre-registered channels.
    ///
    /// # Arguments
    /// * `stream` - The stream to wrap and monitor
    /// * `storage` - Storage handle for captured data
    /// * `channel` - The channel ID to use for storage
    pub fn with_channel_id(stream: S, storage: StorageHandle<T>, channel: ChannelId) -> Self {
        Self {
            inner: stream,
            storage,
            channel,
        }
    }
}

impl<S, T> Stream for Inspector<S, T>
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

/// A transparent inspector for streams that yield `Result<T, E>` values.
///
/// Similar to `Inspector`, but specifically designed for error-handling streams.
/// Only successful values (Ok variants) are captured to storage, while errors
/// are passed through unchanged.
///
/// # Type Parameters
/// * `S` - The inner stream type
/// * `T` - The success value type, must be serializable
/// * `E` - The error type
///
/// # Example
/// ```rust
/// use dcap::stream::{StorageHandle, MemoryStorage};
/// use dcap::stream::inspector::InspectorWithResults;
/// use futures::{StreamExt, stream};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = StorageHandle::new(MemoryStorage::new());
/// let results = vec![Ok(1), Err("error"), Ok(2), Ok(3)];
/// let result_stream = stream::iter(results);
///
/// let inspector = InspectorWithResults::new(result_stream, storage.clone(), "results");
/// let collected: Vec<_> = inspector.collect().await;
///
/// // All results pass through (including errors)
/// assert_eq!(collected.len(), 4);
/// // But only successful values were captured (3 items: 1, 2, 3)
/// assert_eq!(storage.len(), 3);
/// # Ok(())
/// # }
/// ```
#[pin_project]
#[derive(Debug, Clone)]
pub struct InspectorWithResults<S, T, E> {
    #[pin]
    inner: S,
    storage: StorageHandle<T>,
    channel: ChannelId,
    _error: std::marker::PhantomData<E>,
}

impl<S, T, E> InspectorWithResults<S, T, E>
where
    S: Stream<Item = Result<T, E>>,
    T: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
    E: 'static,
{
    /// Creates a new result inspector for the given stream.
    ///
    /// # Arguments
    /// * `stream` - The stream yielding `Result<T, E>` values to monitor
    /// * `storage` - Storage handle for captured successful values
    /// * `channel_name` - Name of the channel to store successful values in
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

impl<S, T, E> Stream for InspectorWithResults<S, T, E>
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
