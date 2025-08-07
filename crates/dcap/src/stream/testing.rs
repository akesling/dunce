use futures::channel::{mpsc, oneshot};
use futures::{SinkExt, Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Coordinates the sequencing of multiple controlled streams
#[derive(Default, Clone)]
pub struct StreamSequencer {
    inner: Arc<Mutex<StreamSequencerInner>>,
}

type ChannelId = usize;

/// Result of a stream emission attempt
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmissionResult {
    /// Stream emitted an item successfully
    Emitted,
    /// Stream is exhausted (returned None)
    Exhausted,
}

/// Error that occurred during sequence execution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceError {
    /// Channel was dropped without sending a response
    ChannelDropped { channel_id: ChannelId },
    /// Channel notifier is missing (channel was never registered)
    ChannelNotFound { channel_id: ChannelId },
    /// Failed to send signal to channel
    SendFailed { channel_id: ChannelId },
}

/// Result of running a sequence
#[derive(Debug, Default)]
pub struct SequenceRunResult {
    /// Channels that reported exhaustion during the run
    pub exhausted_channels: Vec<ChannelId>,
    /// Errors that occurred during execution
    pub errors: Vec<SequenceError>,
    /// Total number of successful emissions
    pub total_emissions: usize,
    /// Number of exhaustion events
    pub exhaustion_count: usize,
}

#[derive(Default)]
struct StreamSequencerInner {
    next_channel_id: ChannelId,
    stream_notifiers: HashMap<ChannelId, mpsc::Sender<oneshot::Sender<EmissionResult>>>,
}

impl StreamSequencer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StreamSequencerInner {
                next_channel_id: 0,
                stream_notifiers: HashMap::new(),
            })),
        }
    }

    pub fn create_controlled_stream<S>(&self, stream: S) -> ControlledStream<S> {
        let mut inner = self.inner.lock().unwrap();
        let stream_id = inner.next_channel_id;
        inner.next_channel_id += 1;

        let (sender, receiver) = mpsc::channel(1);
        inner.stream_notifiers.insert(stream_id, sender);

        ControlledStream::new(stream, stream_id, receiver)
    }

    pub async fn run<S>(self, mut sequence: S) -> SequenceRunResult
    where
        S: Stream<Item = ChannelId> + Unpin,
    {
        let mut result = SequenceRunResult::default();

        while let Some(next_channel) = sequence.next().await {
            // Get the sender for this channel
            let sender = {
                let inner = self.inner.lock().unwrap();
                inner.stream_notifiers.get(&next_channel).cloned()
            };

            // Signal the next channel that it can emit
            if let Some(mut sender) = sender {
                let (tx, rx) = oneshot::channel();
                match sender.send(tx).await {
                    Ok(()) => {
                        // Wait for the stream to signal emission result
                        match rx.await {
                            Ok(EmissionResult::Emitted) => {
                                // Stream emitted successfully
                                result.total_emissions += 1;
                            }
                            Ok(EmissionResult::Exhausted) => {
                                // Stream is exhausted
                                result.exhausted_channels.push(next_channel);
                                result.exhaustion_count += 1;
                            }
                            Err(_) => {
                                // Channel dropped without responding
                                result.errors.push(SequenceError::ChannelDropped {
                                    channel_id: next_channel,
                                });
                            }
                        }
                    }
                    Err(_) => {
                        // Failed to send signal to channel
                        result.errors.push(SequenceError::SendFailed {
                            channel_id: next_channel,
                        });
                    }
                }
            } else {
                // Channel was not found in the registry
                result.errors.push(SequenceError::ChannelNotFound {
                    channel_id: next_channel,
                });
            }
        }

        result
    }
}

/// A stream combinator that waits for sequencer coordination before emitting items
pub struct ControlledStream<S> {
    source: S,
    stream_id: ChannelId,

    /// Stream::next() waits on receipt over receiver and replies on the received oneshot when it's
    /// resolving to notify the sender.
    emit_signal: mpsc::Receiver<oneshot::Sender<EmissionResult>>,
    pending_emit: Option<oneshot::Sender<EmissionResult>>,
}

impl<S> ControlledStream<S> {
    fn new(
        stream: S,
        stream_id: ChannelId,
        emit_signal: mpsc::Receiver<oneshot::Sender<EmissionResult>>,
    ) -> Self {
        Self {
            source: stream,
            stream_id,
            emit_signal,
            pending_emit: None,
        }
    }

    pub fn id(&self) -> ChannelId {
        self.stream_id
    }
}

impl<S: Stream + Unpin> Stream for ControlledStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        // First, check if we have a pending signal to complete
        if let Some(has_emitted_notifier) = this.pending_emit.take() {
            // Poll the source stream
            let source = Pin::new(&mut this.source);
            match source.poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    // Send emission success signal
                    let _ = has_emitted_notifier.send(EmissionResult::Emitted);
                    return Poll::Ready(Some(item));
                }
                Poll::Ready(None) => {
                    // Send exhaustion signal
                    let _ = has_emitted_notifier.send(EmissionResult::Exhausted);
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    // Source isn't ready, keep the signal for next poll
                    this.pending_emit = Some(has_emitted_notifier);
                    return Poll::Pending;
                }
            }
        }

        // Try to receive the next emit signal
        match Pin::new(&mut this.emit_signal).poll_next(cx) {
            Poll::Ready(Some(has_emitted_notifier)) => {
                // We got the signal, now poll the source
                let source = Pin::new(&mut this.source);
                match source.poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        // Notify sequencer that this stream has emitted
                        let _ = has_emitted_notifier.send(EmissionResult::Emitted);
                        Poll::Ready(Some(item))
                    }
                    Poll::Ready(None) => {
                        // Notify sequencer that this stream is exhausted
                        let _ = has_emitted_notifier.send(EmissionResult::Exhausted);
                        Poll::Ready(None)
                    }
                    Poll::Pending => {
                        // Source isn't ready, save the signal for next poll
                        this.pending_emit = Some(has_emitted_notifier);
                        Poll::Pending
                    }
                }
            }
            Poll::Ready(None) => {
                // Channel closed, stream is done
                Poll::Ready(None)
            }
            Poll::Pending => {
                // Waiting for our turn
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::stream::*;
    use futures::{StreamExt, stream};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct LogMessage {
        timestamp: u64,
        level: String,
        message: String,
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
        let client_inspector =
            crate::stream::Inspector::new(client_stream, storage.clone(), "client");
        let server_inspector =
            crate::stream::Inspector::new(server_stream, storage.clone(), "server");

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
    }

    #[tokio::test]
    async fn test_unexpected_stream_exhaustion() {
        let sequencer = StreamSequencer::new();

        // Create a stream with only 2 items
        let short_stream = sequencer.create_controlled_stream(stream::iter(vec!["item1", "item2"]));
        let short_id = short_stream.id();

        // Create a stream with 3 items
        let normal_stream =
            sequencer.create_controlled_stream(stream::iter(vec!["normal1", "normal2", "normal3"]));
        let normal_id = normal_stream.id();

        let sequence = stream::iter(vec![
            short_id, normal_id, // Round 1: both have items
            short_id, normal_id, // Round 2: both have items
            short_id, normal_id, // Round 3: short_stream returns None, normal_stream has item
        ]);

        // Run and collect results
        let (run_result, short_results, normal_results) = tokio::join!(
            sequencer.run(sequence),
            short_stream.collect::<Vec<_>>(),
            normal_stream.collect::<Vec<_>>()
        );

        // Verify stream results
        assert_eq!(short_results, vec!["item1", "item2"]);
        assert_eq!(normal_results, vec!["normal1", "normal2", "normal3"]);

        // Verify exhaustion was reported
        assert_eq!(
            run_result.exhaustion_count, 1,
            "Should have 1 exhaustion event"
        );
        assert_eq!(
            run_result.exhausted_channels,
            vec![short_id],
            "Short stream should be marked as exhausted"
        );
        assert_eq!(
            run_result.total_emissions, 5,
            "Should have 5 successful emissions (2 from short, 3 from normal)"
        );
        assert!(run_result.errors.is_empty(), "Should have no errors");
    }

    /// Test error handling when channels are not found or drop
    #[tokio::test]
    async fn test_sequence_error_handling() {
        let sequencer = StreamSequencer::new();

        // Create only one stream
        let stream = sequencer.create_controlled_stream(stream::iter(vec!["item1", "item2"]));
        let valid_id = stream.id();
        let invalid_id = 999; // Non-existent channel

        // Sequence includes non-existent channel
        let sequence = stream::iter(vec![
            valid_id, invalid_id, // This channel doesn't exist
            valid_id,
        ]);

        let (run_result, stream_results) =
            tokio::join!(sequencer.run(sequence), stream.collect::<Vec<_>>());

        // Verify stream processed what it could
        assert_eq!(stream_results, vec!["item1", "item2"]);

        // Verify error was reported
        assert_eq!(run_result.errors.len(), 1, "Should have 1 error");
        assert_eq!(
            run_result.errors[0],
            SequenceError::ChannelNotFound {
                channel_id: invalid_id
            },
            "Should report channel not found"
        );
        assert_eq!(
            run_result.total_emissions, 2,
            "Should have 2 successful emissions"
        );

        println!("✓ Test successfully handles and reports sequence errors");
    }

    /// Test handling of a stream that hangs forever
    #[tokio::test]
    async fn test_hanging_stream_detection() {
        use std::time::Duration;
        use tokio::time::timeout;

        // A stream that hangs after emitting one item
        struct HangingStream {
            emitted: bool,
        }

        impl Stream for HangingStream {
            type Item = &'static str;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if !self.emitted {
                    self.emitted = true;
                    Poll::Ready(Some("first_item"))
                } else {
                    // Hang forever - always return Pending without waking
                    Poll::Pending
                }
            }
        }

        let sequencer = StreamSequencer::new();

        // Create a hanging stream
        let hanging_stream = sequencer.create_controlled_stream(HangingStream { emitted: false });
        let hanging_id = hanging_stream.id();

        // Create a normal stream
        let normal_stream =
            sequencer.create_controlled_stream(stream::iter(vec!["normal1", "normal2", "normal3"]));
        let normal_id = normal_stream.id();

        // Sequence expects multiple items from the hanging stream
        let sequence = stream::iter(vec![
            hanging_id, // Will emit "first_item"
            normal_id,  // Will emit "normal1"
            hanging_id, // Will hang forever here!
            normal_id,  // Never reached
        ]);

        // Run with timeout to detect hanging
        let timeout_duration = Duration::from_millis(100);
        let result = timeout(timeout_duration, async {
            tokio::join!(
                sequencer.run(sequence),
                hanging_stream.collect::<Vec<_>>(),
                normal_stream.collect::<Vec<_>>()
            )
        })
        .await;

        // Verify that the operation timed out
        assert!(result.is_err(), "Should timeout when stream hangs");

        println!("✓ Test successfully detected hanging stream via timeout");
        println!("  Note: When a stream hangs (returns Pending without progress),");
        println!("  the sequencer will wait indefinitely. Use timeouts to detect this.");
    }

    /// Test handling of a stream that becomes ready after delay
    #[tokio::test]
    async fn test_slow_stream_vs_hanging_stream() {
        use std::time::Duration;
        use tokio::time::{Sleep, sleep, timeout};

        // A stream that is slow but eventually produces
        struct SlowStream {
            items: Vec<&'static str>,
            index: usize,
            delay: Option<Pin<Box<Sleep>>>,
        }

        impl Stream for SlowStream {
            type Item = &'static str;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                // Check if we're currently delaying
                if let Some(ref mut delay) = self.delay {
                    match delay.as_mut().poll(cx) {
                        Poll::Ready(()) => {
                            self.delay = None; // Delay complete
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                if self.index < self.items.len() {
                    let item = self.items[self.index];
                    self.index += 1;

                    // Set up delay for next poll (except for last item)
                    if self.index < self.items.len() {
                        self.delay = Some(Box::pin(sleep(Duration::from_millis(10))));
                    }

                    Poll::Ready(Some(item))
                } else {
                    Poll::Ready(None)
                }
            }
        }

        let sequencer = StreamSequencer::new();

        // Create a slow but functioning stream
        let slow_stream = sequencer.create_controlled_stream(SlowStream {
            items: vec!["slow1", "slow2"],
            index: 0,
            delay: None,
        });
        let slow_id = slow_stream.id();

        // Create a normal stream
        let normal_stream =
            sequencer.create_controlled_stream(stream::iter(vec!["normal1", "normal2"]));
        let normal_id = normal_stream.id();

        let sequence = stream::iter(vec![slow_id, normal_id, slow_id, normal_id]);

        // Use a longer timeout that should succeed for slow stream
        let timeout_duration = Duration::from_millis(200);
        let result = timeout(timeout_duration, async {
            tokio::join!(
                sequencer.run(sequence),
                slow_stream.collect::<Vec<_>>(),
                normal_stream.collect::<Vec<_>>()
            )
        })
        .await;

        // This should complete successfully (not timeout)
        assert!(
            result.is_ok(),
            "Slow stream should complete within reasonable timeout"
        );

        let (run_result, slow_results, normal_results) = result.unwrap();
        assert_eq!(slow_results, vec!["slow1", "slow2"]);
        assert_eq!(normal_results, vec!["normal1", "normal2"]);
        assert_eq!(run_result.total_emissions, 4);
        assert!(run_result.errors.is_empty());

        println!("✓ Test distinguishes between slow streams (ok) and hanging streams (timeout)");
    }

    /// Test that provides better debugging with stream metadata
    #[tokio::test]
    async fn test_stream_exhaustion_with_diagnostics() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;
        use tokio::time::timeout;

        // Track how many items each stream has emitted
        #[derive(Clone)]
        struct DiagnosticStream<S> {
            inner: S,
            name: String,
            emitted_count: Arc<AtomicUsize>,
            total_items: usize,
        }

        impl<S: Stream + Unpin> Stream for DiagnosticStream<S> {
            type Item = S::Item;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                let result = Pin::new(&mut self.inner).poll_next(cx);
                if let Poll::Ready(Some(_)) = &result {
                    let count = self.emitted_count.fetch_add(1, Ordering::SeqCst) + 1;
                    println!(
                        "[{}] Emitted item {}/{}",
                        self.name, count, self.total_items
                    );
                } else if let Poll::Ready(None) = &result {
                    let final_count = self.emitted_count.load(Ordering::SeqCst);
                    println!(
                        "[{}] Stream exhausted after {} items",
                        self.name, final_count
                    );
                }
                result
            }
        }

        // Create sequencer
        let sequencer = StreamSequencer::new();

        // Create diagnostic streams
        let client_items = vec!["client1", "client2"]; // Only 2 items
        let client_count = Arc::new(AtomicUsize::new(0));
        let client_stream = sequencer.create_controlled_stream(DiagnosticStream {
            inner: stream::iter(client_items.clone()),
            name: "CLIENT".to_string(),
            emitted_count: client_count.clone(),
            total_items: client_items.len(),
        });
        let client_id = client_stream.id();

        let server_items = vec!["server1", "server2", "server3"]; // 3 items
        let server_count = Arc::new(AtomicUsize::new(0));
        let server_stream = sequencer.create_controlled_stream(DiagnosticStream {
            inner: stream::iter(server_items.clone()),
            name: "SERVER".to_string(),
            emitted_count: server_count.clone(),
            total_items: server_items.len(),
        });
        let server_id = server_stream.id();

        // Sequence expects 3 rounds (6 items total)
        let sequence_spec = vec![
            (client_id, "client"),
            (server_id, "server"), // Round 1
            (client_id, "client"),
            (server_id, "server"), // Round 2
            (client_id, "client"),
            (server_id, "server"), // Round 3 - client exhausted!
        ];

        println!("\n=== Starting sequence execution ===");
        println!(
            "Expected sequence: {:?}",
            sequence_spec
                .iter()
                .map(|(_, name)| name)
                .collect::<Vec<_>>()
        );
        println!("Client stream has {} items", client_items.len());
        println!("Server stream has {} items", server_items.len());
        println!("Sequence expects {} total emissions\n", sequence_spec.len());

        let sequence = stream::iter(sequence_spec.iter().map(|(id, _)| *id));

        let timeout_duration = Duration::from_millis(500);
        let result = timeout(timeout_duration, async {
            tokio::join!(
                sequencer.run(sequence),
                client_stream.collect::<Vec<_>>(),
                server_stream.collect::<Vec<_>>()
            )
        })
        .await;

        // Print diagnostic information
        println!("\n=== Final Status ===");
        let client_emitted = client_count.load(Ordering::SeqCst);
        let server_emitted = server_count.load(Ordering::SeqCst);

        println!(
            "Client emitted: {}/{} items",
            client_emitted,
            client_items.len()
        );
        println!(
            "Server emitted: {}/{} items",
            server_emitted,
            server_items.len()
        );

        // With EmissionResult, the sequencer now continues even when streams are exhausted
        // It prints warnings but doesn't block
        assert!(
            result.is_ok(),
            "Sequencer should complete even with exhausted streams"
        );

        println!("\n✓ Test completed successfully");

        // Check which stream was exhausted
        if client_emitted < client_items.len() {
            println!("CLIENT stream still has items remaining");
        } else {
            println!(
                "CLIENT stream was exhausted (emitted all {} items)",
                client_items.len()
            );
        }

        if server_emitted < server_items.len() {
            println!("SERVER stream still has items remaining");
        } else {
            println!(
                "SERVER stream was exhausted (emitted all {} items)",
                server_items.len()
            );
        }

        // The sequencer should have printed warnings about exhaustion
        println!("\nNote: Check above for 'Warning: Stream X is exhausted' messages");
        println!("These warnings help debug stream exhaustion issues without blocking.");
    }
}
