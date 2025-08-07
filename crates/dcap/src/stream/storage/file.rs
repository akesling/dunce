//! File-based storage implementation

use super::Storage;
use crate::stream::{ChannelId, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Serialize, Deserialize)]
struct StorageIndex {
    channels: HashMap<ChannelId, Vec<u64>>,
    global_order: Vec<(ChannelId, usize)>,
    channel_names: HashMap<String, ChannelId>,
    channel_lookup: HashMap<ChannelId, String>,
    next_channel_id: ChannelId,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredItem<T> {
    channel: ChannelId,
    channel_name: String,
    item: T,
}

/// File-based storage backend for captured stream data.
///
/// `FileStorage` persists data to disk in JSON Lines format, allowing data to survive
/// between program runs. It maintains an index for fast access while keeping the
/// capture order intact for session replay.
///
/// # Type Parameters
/// * `T` - The item type being stored, must be serializable
///
/// # Example
/// ```rust
/// use dcap::stream::{StorageHandle, FileStorage};
///
/// // Create file storage for new data
/// let storage = FileStorage::<String>::create("example.jsonl").unwrap();
/// let storage_handle = StorageHandle::new(storage);
/// // Use storage_handle with Sink, Source, etc...
/// ```
#[derive(Debug)]
pub struct FileStorage<T> {
    path: PathBuf,
    index: StorageIndex,
    phantom: PhantomData<T>,
}

impl<T> FileStorage<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        Ok(Self {
            path,
            index: StorageIndex::default(),
            phantom: PhantomData,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if !path.exists() {
            return Err(anyhow::anyhow!("File does not exist: {}", path.display()));
        }

        let mut storage = Self {
            path,
            index: StorageIndex::default(),
            phantom: PhantomData,
        };

        storage.load_index()?;
        Ok(storage)
    }

    fn load_index(&mut self) -> Result<()> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);

        let mut index = StorageIndex::default();
        let mut offset = 0u64;

        for line in reader.lines() {
            let line = line?;
            let stored: StoredItem<T> = serde_json::from_str(&line)?;

            // Register channel name if not seen before
            if !index.channel_names.contains_key(&stored.channel_name) {
                index
                    .channel_names
                    .insert(stored.channel_name.clone(), stored.channel);
                index
                    .channel_lookup
                    .insert(stored.channel, stored.channel_name.clone());
                if stored.channel >= index.next_channel_id {
                    index.next_channel_id = stored.channel + 1;
                }
            }

            let channel_offsets = index.channels.entry(stored.channel).or_default();
            let channel_index = channel_offsets.len();
            channel_offsets.push(offset);

            index.global_order.push((stored.channel, channel_index));

            offset += line.len() as u64 + 1;
        }

        self.index = index;
        Ok(())
    }
}

impl<T> Storage<T> for FileStorage<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    fn register_channel(&mut self, name: &str) -> ChannelId {
        if let Some(&id) = self.index.channel_names.get(name) {
            return id;
        }

        let id = self.index.next_channel_id;
        self.index.next_channel_id += 1;
        self.index.channel_names.insert(name.to_string(), id);
        self.index.channel_lookup.insert(id, name.to_string());
        id
    }

    fn channel_id(&self, name: &str) -> Option<ChannelId> {
        self.index.channel_names.get(name).copied()
    }

    fn channel_name(&self, id: ChannelId) -> Option<&str> {
        self.index.channel_lookup.get(&id).map(|s| s.as_str())
    }

    fn append(&mut self, channel: ChannelId, item: &T) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        let channel_name = self
            .index
            .channel_lookup
            .get(&channel)
            .ok_or_else(|| anyhow::anyhow!("Unknown channel: {}", channel))?
            .clone();
        let stored = StoredItem {
            channel,
            channel_name,
            item,
        };

        let offset = file.seek(SeekFrom::End(0))?;
        let line = serde_json::to_string(&stored)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;

        let channel_offsets = self.index.channels.entry(channel).or_default();
        let channel_index = channel_offsets.len();
        channel_offsets.push(offset);

        self.index.global_order.push((channel, channel_index));

        Ok(())
    }

    fn get(&self, channel: ChannelId, index: usize) -> Result<Option<T>> {
        if let Some(offsets) = self.index.channels.get(&channel) {
            if let Some(&offset) = offsets.get(index) {
                let mut file = File::open(&self.path)?;
                file.seek(SeekFrom::Start(offset))?;
                let mut reader = BufReader::new(file);
                let mut line = String::new();
                reader.read_line(&mut line)?;
                let stored: StoredItem<T> = serde_json::from_str(&line)?;
                return Ok(Some(stored.item));
            }
        }
        Ok(None)
    }

    fn get_globally(&self, global_index: usize) -> Result<Option<(ChannelId, T)>> {
        if let Some(&(channel, idx)) = self.index.global_order.get(global_index) {
            if let Some(item) = self.get(channel, idx)? {
                return Ok(Some((channel, item)));
            }
        }
        Ok(None)
    }

    fn channels(&self) -> Vec<ChannelId> {
        self.index.channel_names.values().copied().collect()
    }

    fn len(&self) -> usize {
        self.index.global_order.len()
    }

    fn channel_len(&self, channel: ChannelId) -> usize {
        self.index.channels.get(&channel).map_or(0, |v| v.len())
    }

    fn is_empty(&self) -> bool {
        self.index.global_order.is_empty()
    }

    fn channel_is_empty(&self, channel: ChannelId) -> bool {
        self.index
            .channels
            .get(&channel)
            .is_none_or(|v| v.is_empty())
    }
}
