pub mod inspector;
pub mod sink;
pub mod source;
pub mod storage;
pub mod testing;

pub use inspector::{Inspector, InspectorWithResults};
pub use sink::Sink;
pub use source::{SessionSource, Source};
pub use storage::{FileStorage, MemoryStorage, Storage, StorageHandle};

pub type ChannelId = usize;
pub type Result<T> = std::result::Result<T, anyhow::Error>;

#[cfg(test)]
mod tests {
    use crate::stream::{MemoryStorage, StorageHandle, Inspector, Sink, Source};
    use futures::{StreamExt, stream};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: u32,
        message: String,
    }

    #[tokio::test]
    async fn example_single_channel_capture() {
        let storage = StorageHandle::new(MemoryStorage::<TestData>::new());

        let test_data = vec![
            TestData {
                id: 1,
                message: "Hello".to_string(),
            },
            TestData {
                id: 2,
                message: "World".to_string(),
            },
            TestData {
                id: 3,
                message: "Test".to_string(),
            },
        ];

        let data_stream = stream::iter(test_data.clone());
        let sink = Sink::new(storage.clone(), "test_channel");
        let count = sink.capture(data_stream).await.unwrap();
        println!("Captured {} items", count);

        let source = Source::new(storage, "test_channel").unwrap();
        let mut results = Vec::new();

        let mut source = Box::pin(source);
        while let Some(Ok(item)) = source.next().await {
            results.push(item);
        }

        assert_eq!(results, test_data);
    }

    #[tokio::test]
    async fn example_inspector() {
        let storage = StorageHandle::new(MemoryStorage::<i32>::new());

        let data = vec![1, 2, 3, 4, 5];
        let data_stream = stream::iter(data.clone());

        let inspector = Inspector::new(data_stream, storage.clone(), "numbers");

        let mut results = Vec::new();
        let mut inspector = Box::pin(inspector);
        while let Some(item) = inspector.next().await {
            results.push(item);
        }

        assert_eq!(results, data);
        assert_eq!(
            storage.channel_len(storage.channel_id("numbers").unwrap()),
            5
        );
    }
}
