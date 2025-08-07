use anyhow::anyhow;

pub mod stream;

pub use stream::{Inspector, Sink, Source, SessionSource};

pub static HAIKUS: [[&str; 3]; 10] = [
    [
        "Wires whisper slowly,",
        "Dunce records without a thought—",
        "Replay, glitch, repeat.",
    ],
    [
        "No thought, just capture.",
        "Streams pour into empty logs—",
        "Packets dream of loops.",
    ],
    [
        "It sees all the bytes,",
        "Not knowing what they all mean.",
        "Still, it plays them back.",
    ],
    [
        "Sync lost, logic thin.",
        "But Dunce logs the flow with pride—",
        "Truth in raw repeat.",
    ],
    [
        "Each frame is a guess,",
        "But still they return in time.",
        "Echoes of chaos.",
    ],
    [
        "Filters not in use,",
        "It listens to all the bits.",
        "Noise becomes signal.",
    ],
    [
        "TCP or not—shrug.",
        "It won't parse your clever stack.",
        "Just replays the blur.",
    ],
    [
        "Like rain on glass panes,",
        "Packets slide by unaware.",
        "Dunce jars them all whole.",
    ],
    [
        "A fool with a net,",
        "Scoops up seas of silent bits.",
        "Playback tides repeat.",
    ],
    [
        "Protocol blindfold,",
        "Context never understood—",
        "Still, perfect recall.",
    ],
];

/// Print a random project-related haiku
pub fn print_haiku(print_all: bool) -> anyhow::Result<()> {
    use rand::seq::SliceRandom as _;

    if print_all {
        for h in HAIKUS {
            println!("{}", h.join(" : "))
        }
    } else {
        let mut rng = rand::thread_rng();
        println!(
            "{}",
            HAIKUS
                .choose(&mut rng)
                .ok_or(anyhow!("at least one haiku"))?
                .join("\n")
        )
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::stream::*;
    use futures::{StreamExt, stream};
    use serde::{Deserialize, Serialize};

    use super::stream::testing::StreamSequencer;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct NetworkPacket {
        id: u64,
        src: String,
        dst: String,
        payload: Vec<u8>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct LogMessage {
        timestamp: u64,
        level: String,
        message: String,
    }

    #[tokio::test]
    async fn test_basic_capture_replay() {
        let storage = StorageHandle::new(MemoryStorage::new());

        let packets = vec![
            NetworkPacket {
                id: 1,
                src: "192.168.1.1".to_string(),
                dst: "192.168.1.100".to_string(),
                payload: b"hello".to_vec(),
            },
            NetworkPacket {
                id: 2,
                src: "192.168.1.100".to_string(),
                dst: "192.168.1.1".to_string(),
                payload: b"world".to_vec(),
            },
        ];

        let packet_stream = stream::iter(packets.clone());
        let sink = Sink::new(storage.clone(), "network");
        let captured_count = sink.capture(packet_stream).await.unwrap();

        assert_eq!(captured_count, 2);
        assert_eq!(storage.len(), 2);
        assert!(!storage.is_empty());

        let source = Source::new(storage.clone(), "network").unwrap();
        let replayed: Vec<_> = source.map(|result| result.unwrap()).collect().await;

        assert_eq!(replayed, packets);
    }

    #[tokio::test]
    async fn test_multi_channel_storage() {
        let storage = StorageHandle::new(MemoryStorage::new());

        let client_msgs = vec![
            LogMessage {
                timestamp: 1000,
                level: "INFO".to_string(),
                message: "Client connected".to_string(),
            },
            LogMessage {
                timestamp: 1002,
                level: "DEBUG".to_string(),
                message: "Sending request".to_string(),
            },
        ];
        let client_sink = Sink::new(storage.clone(), "client");
        client_sink
            .capture(stream::iter(client_msgs.clone()))
            .await
            .unwrap();

        let server_msgs = vec![
            LogMessage {
                timestamp: 1001,
                level: "INFO".to_string(),
                message: "New connection".to_string(),
            },
            LogMessage {
                timestamp: 1003,
                level: "DEBUG".to_string(),
                message: "Processing request".to_string(),
            },
        ];
        let server_sink = Sink::new(storage.clone(), "server");
        server_sink
            .capture(stream::iter(server_msgs.clone()))
            .await
            .unwrap();

        assert_eq!(storage.len(), 4);
        assert_eq!(
            storage.channel_len(storage.channel_id("client").unwrap()),
            2
        );
        assert_eq!(
            storage.channel_len(storage.channel_id("server").unwrap()),
            2
        );

        let client_replayed: Vec<_> = Source::new(storage.clone(), "client")
            .unwrap()
            .map(|result| result.unwrap())
            .collect()
            .await;
        assert_eq!(client_msgs, client_replayed);
        let server_replayed: Vec<_> = Source::new(storage.clone(), "server")
            .unwrap()
            .map(|result| result.unwrap())
            .collect()
            .await;
        assert_eq!(&server_msgs, &server_replayed);
    }

    /// Test transparent pass-through with StreamInspector
    #[tokio::test]
    async fn test_transparent_monitoring() {
        let storage = StorageHandle::new(MemoryStorage::new());

        let data = vec![1, 2, 3, 4, 5];
        let data_stream = stream::iter(data.clone());

        // Create inspector that captures while passing through
        let inspector = Inspector::new(data_stream, storage.clone(), "monitor");

        // Process the stream normally
        let processed_data: Vec<_> = inspector.collect().await;

        // Verify pass-through worked
        assert_eq!(processed_data, data);

        // Verify capture happened transparently
        assert_eq!(
            storage.channel_len(storage.channel_id("monitor").unwrap()),
            5
        );

        // Verify captured data matches
        let captured_source = Source::new(storage, "monitor").unwrap();
        let captured_data: Vec<_> = captured_source.map(|r| r.unwrap()).collect().await;
        assert_eq!(captured_data, data);
    }

    /// Test file-based persistence
    #[tokio::test]
    async fn test_file_persistence() {
        let temp_file = std::env::temp_dir().join("dunce_test.jsonl");

        // Clean up any existing file
        let _ = std::fs::remove_file(&temp_file);

        let messages = vec![
            "first message".to_string(),
            "second message".to_string(),
            "third message".to_string(),
        ];

        // Capture to file
        {
            let storage = StorageHandle::new(FileStorage::create(&temp_file).unwrap());
            let sink = Sink::new(storage.clone(), "messages");
            sink.capture(stream::iter(messages.clone())).await.unwrap();

            assert_eq!(storage.len(), 3);
            assert!(!storage.is_empty());
        }

        // Load from file and verify
        {
            let storage = StorageHandle::new(FileStorage::<String>::open(&temp_file).unwrap());
            assert_eq!(storage.len(), 3);

            // Check if channel exists
            assert!(storage.channel_id("messages").is_some());

            let source = Source::new(storage, "messages").unwrap();
            let loaded_messages: Vec<String> = source.map(|r| r.unwrap()).collect().await;

            assert_eq!(loaded_messages, messages);
        }

        // Clean up
        let _ = std::fs::remove_file(&temp_file);
    }

    /// Test error handling in streams
    #[tokio::test]
    async fn test_error_handling() {
        let storage = StorageHandle::new(MemoryStorage::new());

        // Create a stream with some errors
        let results: Vec<std::result::Result<String, &str>> = vec![
            Ok("success 1".to_string()),
            Err("error 1"),
            Ok("success 2".to_string()),
            Err("error 2"),
            Ok("success 3".to_string()),
        ];

        let result_stream = stream::iter(results);
        let sink = Sink::new(storage.clone(), "mixed");
        let (success_count, error_count) = sink.capture_with_errors(result_stream).await.unwrap();

        assert_eq!(success_count, 3);
        assert_eq!(error_count, 2);
        assert_eq!(storage.channel_len(storage.channel_id("mixed").unwrap()), 3);

        // Verify only successful items were stored
        let source = Source::new(storage, "mixed").unwrap();
        let stored_items: Vec<_> = source.map(|r| r.unwrap()).collect().await;

        assert_eq!(
            stored_items,
            vec![
                "success 1".to_string(),
                "success 2".to_string(),
                "success 3".to_string(),
            ]
        );
    }

    /// Test complex data structures
    #[tokio::test]
    async fn test_complex_data_structures() {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct ComplexEvent {
            id: u64,
            nested: NestedData,
            tags: Vec<String>,
        }

        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct NestedData {
            value: f64,
            metadata: std::collections::HashMap<String, String>,
        }

        let storage = StorageHandle::new(MemoryStorage::new());

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "sensor_1".to_string());
        metadata.insert("unit".to_string(), "celsius".to_string());

        let complex_events = vec![
            ComplexEvent {
                id: 1,
                nested: NestedData {
                    value: 23.5,
                    metadata: metadata.clone(),
                },
                tags: vec!["temperature".to_string(), "outdoor".to_string()],
            },
            ComplexEvent {
                id: 2,
                nested: NestedData {
                    value: 24.1,
                    metadata,
                },
                tags: vec!["temperature".to_string(), "indoor".to_string()],
            },
        ];

        let sink = Sink::new(storage.clone(), "sensors");
        sink.capture(stream::iter(complex_events.clone()))
            .await
            .unwrap();

        let source = Source::new(storage, "sensors").unwrap();
        let replayed_events: Vec<_> = source.map(|r| r.unwrap()).collect().await;

        assert_eq!(replayed_events, complex_events);
    }

    /// Test storage metadata operations
    #[tokio::test]
    async fn test_storage_metadata() {
        let storage = StorageHandle::new(MemoryStorage::new());

        // Test empty storage
        assert!(storage.is_empty());
        assert_eq!(storage.len(), 0);
        assert_eq!(storage.channels().len(), 0);

        // Add some data to different channels (same type)
        let sink1 = Sink::new(storage.clone(), "channel1");
        sink1.capture(stream::iter(vec![1, 2, 3])).await.unwrap();

        let sink2 = Sink::new(storage.clone(), "channel2");
        sink2.capture(stream::iter(vec![10, 20])).await.unwrap();

        // Test metadata after data
        assert!(!storage.is_empty());
        assert_eq!(storage.len(), 5);
        assert_eq!(storage.channels().len(), 2);

        let ch1_id = storage.channel_id("channel1").unwrap();
        let ch2_id = storage.channel_id("channel2").unwrap();

        assert_eq!(storage.channel_len(ch1_id), 3);
        assert_eq!(storage.channel_len(ch2_id), 2);
        assert!(!storage.channel_is_empty(ch1_id));
        assert!(!storage.channel_is_empty(ch2_id));

        assert_eq!(storage.channel_name(ch1_id), Some("channel1".to_string()));
        assert_eq!(storage.channel_name(ch2_id), Some("channel2".to_string()));

        // Test non-existent channel
        assert!(storage.channel_is_empty(999)); // Non-existent channel ID
        assert_eq!(storage.channel_len(999), 0);
        assert_eq!(storage.channel_name(999), None);
        assert_eq!(storage.channel_id("nonexistent"), None);
    }

    /// Test interleaved channel ordering using programmatic stream control
    #[tokio::test]
    async fn test_interleaved_channel_ordering() {
        let storage = StorageHandle::new(MemoryStorage::new());

        // Define separate stream data for each channel
        let client_msgs = vec![
            LogMessage {
                timestamp: 1000,
                level: "INFO".to_string(),
                message: "Client connected".to_string(),
            },
            LogMessage {
                timestamp: 1020,
                level: "DEBUG".to_string(),
                message: "Sending request".to_string(),
            },
            LogMessage {
                timestamp: 1040,
                level: "INFO".to_string(),
                message: "Request complete".to_string(),
            },
        ];

        let server_msgs = vec![
            LogMessage {
                timestamp: 1010,
                level: "INFO".to_string(),
                message: "New connection".to_string(),
            },
            LogMessage {
                timestamp: 1030,
                level: "DEBUG".to_string(),
                message: "Processing request".to_string(),
            },
            LogMessage {
                timestamp: 1050,
                level: "INFO".to_string(),
                message: "Response sent".to_string(),
            },
        ];

        // Create sequencer and controlled streams
        let sequencer = StreamSequencer::new();

        let client_stream = sequencer.create_controlled_stream(stream::iter(client_msgs.clone()));
        let client_id = client_stream.id();

        let server_stream = sequencer.create_controlled_stream(stream::iter(server_msgs.clone()));
        let server_id = server_stream.id();

        // Create inspectors that capture as streams naturally provide data
        let client_inspector = Inspector::new(client_stream, storage.clone(), "client");
        let server_inspector = Inspector::new(server_stream, storage.clone(), "server");

        // Define the interleaving sequence using the channel IDs
        let sequence = stream::iter(vec![
            client_id, server_id, client_id, server_id, client_id, server_id,
        ]);

        // Run the sequencer and process both streams concurrently
        // The sequencer coordinates the timing internally
        let (_, client_results, server_results) = tokio::join!(
            sequencer.run(sequence),
            client_inspector.collect::<Vec<_>>(),
            server_inspector.collect::<Vec<_>>()
        );

        // Each channel preserves ordering and is "complete"
        assert_eq!(client_results, client_msgs);
        assert_eq!(server_results, server_msgs);

        // All messages captured
        assert_eq!(storage.len(), 6);
        assert_eq!(
            storage.channel_len(storage.channel_id("client").unwrap()),
            3
        );
        assert_eq!(
            storage.channel_len(storage.channel_id("server").unwrap()),
            3
        );

        // Now replay the full session to verify the interleaved order is preserved
        let session = SessionSource::new(storage.clone());
        let session_events: Vec<(String, LogMessage)> = session
            .map(|result| {
                let (channel_id, event) = result.unwrap();
                let channel_name = storage.channel_name(channel_id).unwrap();
                (channel_name, event)
            })
            .collect()
            .await;

        // Verify we got all events
        assert_eq!(session_events.len(), 6);

        // The key test: verify the global order matches our manual interleaving
        let expected_timestamps = vec![1000, 1010, 1020, 1030, 1040, 1050];
        let expected_channels = vec!["client", "server", "client", "server", "client", "server"];
        let expected_messages = vec![
            "Client connected",
            "New connection",
            "Sending request",
            "Processing request",
            "Request complete",
            "Response sent",
        ];

        let actual_timestamps: Vec<u64> = session_events
            .iter()
            .map(|(_, event)| event.timestamp)
            .collect();

        let actual_channels: Vec<String> = session_events
            .iter()
            .map(|(channel, _)| channel.clone())
            .collect();

        let actual_messages: Vec<String> = session_events
            .iter()
            .map(|(_, event)| event.message.clone())
            .collect();

        // Verify the interleaved order is exactly as we manually captured it
        assert_eq!(actual_timestamps, expected_timestamps);
        assert_eq!(actual_channels, expected_channels);
        assert_eq!(actual_messages, expected_messages);

        // Verify individual channel replay maintains original order within each channel
        let client_source = Source::new(storage.clone(), "client").unwrap();
        let replayed_client: Vec<_> = client_source.map(|r| r.unwrap()).collect().await;
        assert_eq!(replayed_client, client_msgs);

        let server_source = Source::new(storage, "server").unwrap();
        let replayed_server: Vec<_> = server_source.map(|r| r.unwrap()).collect().await;
        assert_eq!(replayed_server, server_msgs);
    }
}
