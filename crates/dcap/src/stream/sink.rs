//! Stream sink for capturing and storing stream data

use crate::stream::{ChannelId, Result, storage::StorageHandle};
use futures::{Stream, StreamExt, pin_mut};
use serde::{Deserialize, Serialize};

/// A stream sink for capturing and storing stream data.
///
/// `Sink` consumes an entire stream and stores all its items in the specified channel.
/// Unlike `Inspector`, which is transparent, `Sink` consumes the stream completely.
/// This is useful for dedicated capture operations where you want to store data
/// without processing it downstream.
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
/// let data = vec![1, 2, 3, 4, 5];
/// let data_stream = stream::iter(data.clone());
///
/// // Capture the entire stream
/// let sink = Sink::new(storage.clone(), "captured_data");
/// let count = sink.capture(data_stream).await?;
/// assert_eq!(count, 5);
///
/// // Later, replay the captured data
/// let source = Source::new(storage, "captured_data").unwrap();
/// let replayed: Vec<i32> = source.map(|r| r.unwrap()).collect().await;
/// assert_eq!(replayed, data);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Sink<T> {
    storage: StorageHandle<T>,
    channel: ChannelId,
}

impl<T> Sink<T>
where
    T: Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Creates a new sink for the specified channel.
    ///
    /// # Arguments
    /// * `storage` - Storage handle where captured data will be stored
    /// * `channel_name` - Name of the channel to store data in
    ///
    /// # Example
    /// ```rust
    /// use dcap::Sink;
    /// use dcap::stream::{StorageHandle, MemoryStorage};
    ///
    /// let storage = StorageHandle::new(MemoryStorage::<i32>::new());
    /// let sink = Sink::new(storage, "my_data");
    /// ```
    pub fn new(storage: StorageHandle<T>, channel_name: impl AsRef<str>) -> Self {
        let channel = storage.register_channel(channel_name.as_ref());
        Self { storage, channel }
    }

    /// Creates a new sink using an existing channel ID.
    ///
    /// # Arguments
    /// * `storage` - Storage handle where captured data will be stored
    /// * `channel` - The channel ID to use for storage
    pub fn with_channel_id(storage: StorageHandle<T>, channel: ChannelId) -> Self {
        Self { storage, channel }
    }

    /// Captures an entire stream, storing all items in the channel.
    ///
    /// This method consumes the stream completely and returns the number of items captured.
    ///
    /// # Arguments
    /// * `stream` - The stream to capture
    ///
    /// # Returns
    /// The number of items successfully captured
    ///
    /// # Example
    /// ```rust
    /// use dcap::Sink;
    /// use dcap::stream::{StorageHandle, MemoryStorage};
    /// use futures::stream;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let storage = StorageHandle::new(MemoryStorage::<i32>::new());
    /// let sink = Sink::new(storage, "numbers");
    /// let data_stream = stream::iter(vec![1, 2, 3, 4, 5]);
    ///
    /// let count = sink.capture(data_stream).await?;
    /// assert_eq!(count, 5);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn capture<S>(self, stream: S) -> Result<usize>
    where
        S: Stream<Item = T>,
    {
        let mut count = 0;
        pin_mut!(stream);

        while let Some(item) = stream.next().await {
            self.storage.append(self.channel, &item)?;
            count += 1;
        }

        Ok(count)
    }

    /// Captures a stream of `Result<T, E>` values, storing only successful items.
    ///
    /// This method handles error streams by capturing only the `Ok` values and
    /// counting the number of errors encountered.
    ///
    /// # Arguments
    /// * `stream` - The stream of results to capture
    ///
    /// # Returns
    /// A tuple of (success_count, error_count)
    ///
    /// # Example
    /// ```rust
    /// use dcap::Sink;
    /// use dcap::stream::{StorageHandle, MemoryStorage};
    /// use futures::stream;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let storage = StorageHandle::new(MemoryStorage::<i32>::new());
    /// let sink = Sink::new(storage, "results");
    ///
    /// let results = vec![Ok(1), Err("error"), Ok(2), Ok(3)];
    /// let result_stream = stream::iter(results);
    ///
    /// let (success_count, error_count) = sink.capture_with_errors(result_stream).await?;
    /// assert_eq!(success_count, 3);  // Three successful values
    /// assert_eq!(error_count, 1);    // One error
    /// # Ok(())
    /// # }
    /// ```
    pub async fn capture_with_errors<S, E>(self, stream: S) -> Result<(usize, usize)>
    where
        S: Stream<Item = std::result::Result<T, E>>,
        E: std::fmt::Debug,
    {
        let mut count = 0;
        let mut error_count = 0;
        pin_mut!(stream);

        while let Some(result) = stream.next().await {
            match result {
                Ok(item) => {
                    self.storage.append(self.channel, &item)?;
                    count += 1;
                }
                Err(_) => {
                    error_count += 1;
                }
            }
        }

        Ok((count, error_count))
    }
}
