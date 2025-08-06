use crate::stream::{ChannelId, Result, storage::StorageHandle};
use futures::{Stream, StreamExt, pin_mut};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct StreamSink<T> {
    storage: StorageHandle<T>,
    channel: ChannelId,
}

impl<T> StreamSink<T>
where
    T: Serialize + for<'de> Deserialize<'de> + 'static,
{
    pub fn new(storage: StorageHandle<T>, channel_name: impl AsRef<str>) -> Self {
        let channel = storage.register_channel(channel_name.as_ref());
        Self { storage, channel }
    }

    pub fn with_channel_id(storage: StorageHandle<T>, channel: ChannelId) -> Self {
        Self { storage, channel }
    }

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
