use std::collections::BTreeMap;
use std::io::{self, Write};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};

use fjall::{Config, Keyspace, Partition, PartitionCreateOptions, Slice};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    EntryPayload, LeaderId, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};
use pack1::U64BE;
use watto::Pod;

use crate::{Response, TypeConfig};

type Request = <TypeConfig as openraft::RaftTypeConfig>::R;
type Entry = <TypeConfig as openraft::RaftTypeConfig>::Entry;
type NodeId = <TypeConfig as openraft::RaftTypeConfig>::NodeId;
type Node = <TypeConfig as openraft::RaftTypeConfig>::Node;
type SnapshotData = <TypeConfig as openraft::RaftTypeConfig>::SnapshotData;

#[cfg(test)]
mod tests;

struct FjallStore {
    db: Keyspace,
    state: Partition,
    log: Partition,
    snapshot: Mutex<Option<Snapshot<TypeConfig>>>,
}
impl FjallStore {
    pub fn new(path: &Path) -> Self {
        let db = Config::new(path).open().unwrap();
        let state = db
            .open_partition("state", PartitionCreateOptions::default())
            .unwrap();
        let log = db
            .open_partition("log", PartitionCreateOptions::default())
            .unwrap();

        Self {
            db,
            state,
            log,
            snapshot: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct StoreSnapshot {
    state: BTreeMap<Slice, Slice>,
    log: BTreeMap<Slice, Slice>,
}

#[repr(C)]
struct SerializedLogId {
    term: U64BE,
    node_id: U64BE,
    index: U64BE,
}
unsafe impl Pod for SerializedLogId {}

fn deserialize_logid(slice: &[u8]) -> (LogId<u64>, &[u8]) {
    let (log_id, rest) = SerializedLogId::ref_from_prefix(slice).unwrap();
    let (term, node_id, index) = (log_id.term.get(), log_id.node_id.get(), log_id.index.get());
    (
        LogId {
            leader_id: LeaderId { term, node_id },
            index,
        },
        rest,
    )
}

impl RaftLogReader<TypeConfig> for Arc<FjallStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<NodeId>> {
        let start = range.start_bound().map(|i| i.to_be_bytes());
        let end = range.end_bound().map(|i| i.to_be_bytes());
        let deserialized_log = self
            .log
            .range((start, end))
            .map(|res| {
                let (_k, v) = res.unwrap();
                let (log_id, rest) = deserialize_logid(&v);
                let payload = bincode::serde::decode_from_slice(rest, bincode::config::standard())
                    .unwrap()
                    .0;
                Entry { log_id, payload }
            })
            .collect();
        Ok(deserialized_log)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<FjallStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let state = self.applied_state().await?;

        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: state.0,
                last_membership: state.1,
                snapshot_id: "".into(),
            },
            snapshot: Box::new(StoreSnapshot {
                state: self.state.iter().map(|res| res.unwrap()).collect(),
                log: self.state.iter().map(|res| res.unwrap()).collect(),
            }),
        };
        *self.snapshot.lock().unwrap() = Some(snapshot.clone());

        Ok(snapshot)
    }
}

impl RaftLogStorage<TypeConfig> for Arc<FjallStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged_log_id = self
            .state
            .get("last_purged_log_id")
            .unwrap()
            .map(|v| deserialize_logid(&v).0);
        let last_log_id = self
            .log
            .last_key_value()
            .unwrap()
            .map(|(_k, v)| deserialize_logid(&v).0);

        Ok(LogState {
            last_purged_log_id,
            last_log_id: last_log_id.or(last_purged_log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let vote = bincode::serde::encode_to_vec(vote, bincode::config::standard()).unwrap();
        self.state.insert("vote", vote).unwrap();
        self.db.persist(fjall::PersistMode::SyncAll).unwrap();
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let vote = self.state.get("vote").unwrap().map(|v| {
            bincode::serde::decode_from_slice(&v, bincode::config::standard())
                .unwrap()
                .0
        });
        Ok(vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry>,
    {
        for Entry { log_id, payload } in entries {
            let mut writer = vec![];
            let key = SerializedLogId {
                term: log_id.leader_id.term.into(),
                node_id: log_id.leader_id.node_id.into(),
                index: log_id.index.into(),
            };
            writer.write_all(key.as_bytes()).unwrap();
            bincode::serde::encode_into_std_write(
                payload,
                &mut writer,
                bincode::config::standard(),
            )
            .unwrap();

            self.log.insert(log_id.index.to_be_bytes(), writer).unwrap();
        }

        let res = self.db.persist(fjall::PersistMode::SyncAll);
        callback.log_io_completed(res.map_err(io::Error::other));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let truncate_index = log_id.index;
        for key in self.log.keys().rev() {
            let key = key.unwrap();
            let index = u64::from_be_bytes(key.as_ref().try_into().unwrap());
            if index >= truncate_index {
                self.log.remove(key).unwrap();
            }
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let purge_index = log_id.index;
        for key in self.log.keys() {
            let key = key.unwrap();
            let index = u64::from_be_bytes(key.as_ref().try_into().unwrap());
            if index <= purge_index {
                self.log.remove(key).unwrap();
            }
        }
        let last_purged_log_id = SerializedLogId {
            term: log_id.leader_id.term.into(),
            node_id: log_id.leader_id.node_id.into(),
            index: log_id.index.into(),
        };
        self.state
            .insert("last_purged_log_id", last_purged_log_id.as_bytes())
            .unwrap();
        Ok(())
    }
}

impl RaftStateMachine<TypeConfig> for Arc<FjallStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        let last_applied_log = self
            .state
            .get("last_applied_log")
            .unwrap()
            .map(|v| deserialize_logid(&v).0);
        let last_membership = self
            .state
            .get("last_membership")
            .unwrap()
            .map(|last_membership| {
                bincode::serde::decode_from_slice(&last_membership, bincode::config::standard())
                    .unwrap()
                    .0
            })
            .unwrap_or_default();

        Ok((last_applied_log, last_membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry>,
    {
        let mut res = vec![];
        let mut last_applied_log = None;
        let mut last_membership = None;

        for entry in entries {
            last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Response { data: None }),
                EntryPayload::Normal(normal) => {
                    todo!()
                }
                EntryPayload::Membership(mem) => {
                    last_membership = Some(StoredMembership::new(Some(entry.log_id), mem));
                    res.push(Response { data: None })
                }
            };
        }

        if let Some(log_id) = last_applied_log {
            let last_applied_log = SerializedLogId {
                term: log_id.leader_id.term.into(),
                node_id: log_id.leader_id.node_id.into(),
                index: log_id.index.into(),
            };
            self.state
                .insert("last_applied_log", last_applied_log.as_bytes())
                .unwrap();
        }

        if let Some(last_membership) = last_membership {
            let last_membership =
                bincode::serde::encode_to_vec(&last_membership, bincode::config::standard())
                    .unwrap();
            self.state
                .insert("last_membership", last_membership)
                .unwrap();
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(StoreSnapshot {
            state: Default::default(),
            log: Default::default(),
        }))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        self.db.delete_partition(self.state.clone()).unwrap();
        self.db.delete_partition(self.log.clone()).unwrap();

        self.state
            .ingest(snapshot.state.iter().map(|(k, v)| (k.clone(), v.clone())))
            .unwrap();
        self.log
            .ingest(snapshot.log.iter().map(|(k, v)| (k.clone(), v.clone())))
            .unwrap();

        *self.snapshot.lock().unwrap() = Some(Snapshot {
            meta: meta.clone(),
            snapshot,
        });

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self.snapshot.lock().unwrap().clone())
    }
}
