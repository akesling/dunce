//! Stream sources for replaying captured data

use crate::stream::{ChannelId, Result, storage::StorageHandle};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A stream source for replaying captured data from a specific channel.
///
/// `Source` creates a stream that replays data previously captured in a channel.
/// It reads data sequentially from storage and yields it as a stream.
///
/// # Type Parameters
/// * `T` - The item type, must be serializable
///
/// # Example
/// ```rust
/// use dcap::{Sink, Source};
/// use dcap::stream::{StorageHandle, MemoryStorage};
/// use futures::{StreamExt, stream};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = StorageHandle::new(MemoryStorage::<i32>::new());
///
/// // First, capture some data
/// let original_data = vec![1, 2, 3, 4, 5];
/// let sink = Sink::new(storage.clone(), "test_data");
/// sink.capture(stream::iter(original_data.clone())).await?;
///
/// // Then replay it
/// let source = Source::new(storage, "test_data").unwrap();
/// let replayed_data: Vec<i32> = source.map(|r| r.unwrap()).collect().await;
///
/// assert_eq!(replayed_data, original_data);
/// # Ok(())
/// # }
/// ```
pub struct Source<T> {
    storage: StorageHandle<T>,
    channel: ChannelId,
    position: usize,
}

impl<T> Source<T>
where
    T: Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Creates a new source for replaying data from the specified channel.
    ///
    /// Returns `None` if the channel doesn't exist in storage.
    ///
    /// # Arguments
    /// * `storage` - Storage handle containing the captured data
    /// * `channel_name` - Name of the channel to replay from
    ///
    /// # Example
    /// ```rust
    /// use dcap::Source;
    /// use dcap::stream::{StorageHandle, MemoryStorage};
    ///
    /// let storage = StorageHandle::new(MemoryStorage::<i32>::new());
    /// // Assumes "my_data" channel exists and has data
    /// if let Some(source) = Source::new(storage, "my_data") {
    ///     // Use the source stream
    /// }
    /// ```
    pub fn new(storage: StorageHandle<T>, channel_name: impl AsRef<str>) -> Option<Self> {
        let channel = storage.channel_id(channel_name.as_ref())?;
        Some(Self {
            storage,
            channel,
            position: 0,
        })
    }

    /// Creates a new source using an existing channel ID.
    ///
    /// # Arguments
    /// * `storage` - Storage handle containing the captured data
    /// * `channel` - The channel ID to replay from
    ///
    /// # Example
    /// ```rust
    /// use dcap::Source;
    /// use dcap::stream::{StorageHandle, MemoryStorage};
    ///
    /// let storage = StorageHandle::new(MemoryStorage::<i32>::new());
    /// let channel_id = 0; // Assumes this channel exists
    /// let source = Source::with_channel_id(storage, channel_id);
    /// ```
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

/// A stream source for replaying data from all channels in capture order.
///
/// `SessionSource` creates a stream that replays data from all channels in the
/// order it was originally captured, regardless of which channel it came from.
/// This is useful for reconstructing the full session timeline.
///
/// Each yielded item is a tuple of `(ChannelId, T)` where the `ChannelId`
/// identifies which channel the item originated from.
///
/// # Type Parameters
/// * `T` - The item type, must be serializable
///
/// # Example
/// ```rust
/// use dcap::{Sink, SessionSource};
/// use dcap::stream::{StorageHandle, MemoryStorage};
/// use futures::{StreamExt, stream};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = StorageHandle::new(MemoryStorage::<i32>::new());
///
/// // Capture data to multiple channels
/// let sink1 = Sink::new(storage.clone(), "channel1");
/// sink1.capture(stream::iter(vec![1, 2])).await?;
///
/// let sink2 = Sink::new(storage.clone(), "channel2");
/// sink2.capture(stream::iter(vec![10, 20])).await?;
///
/// // Replay all data in capture order
/// let session_source = SessionSource::new(storage.clone());
/// let session_data: Vec<(usize, i32)> = session_source
///     .map(|r| r.unwrap())
///     .collect()
///     .await;
///
/// // Data is returned in the order it was captured across all channels
/// assert_eq!(session_data.len(), 4); // 2 + 2 items
/// # Ok(())
/// # }
/// ```
pub struct SessionSource<T> {
    storage: StorageHandle<T>,
    position: usize,
}

impl<T> SessionSource<T>
where
    T: Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Creates a new session source for replaying data from all channels.
    ///
    /// # Arguments
    /// * `storage` - Storage handle containing the captured data
    ///
    /// # Example
    /// ```rust
    /// use dcap::SessionSource;
    /// use dcap::stream::{StorageHandle, MemoryStorage};
    ///
    /// let storage = StorageHandle::new(MemoryStorage::<i32>::new());
    /// let session_source = SessionSource::new(storage);
    /// ```
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
