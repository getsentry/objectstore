use std::io::{self, Cursor, Write};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use fjall::{Config, Keyspace, Partition, PartitionCreateOptions};
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

        Self { db, state, log }
    }
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
        let Bound::Included(start) = range.start_bound() else {
            unreachable!()
        };
        let end = match range.end_bound() {
            Bound::Included(end) => end + 1,
            Bound::Excluded(end) => *end,
            Bound::Unbounded => todo!(),
        };
        let start = start.to_be_bytes();
        let end = end.to_be_bytes();

        let deserialized_log = self
            .log
            .range(start..end)
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

// struct SnapshotBuilder {
//     state: fjall::Snapshot,
//     log: fjall::Snapshot,
// }

impl RaftSnapshotBuilder<TypeConfig> for Arc<FjallStore> {
    #[doc = " Build snapshot"]
    #[doc = ""]
    #[doc = " A snapshot has to contain state of all applied log, including membership. Usually it is just"]
    #[doc = " a serialized state machine."]
    #[doc = ""]
    #[doc = " Building snapshot can be done by:"]
    #[doc = " - Performing log compaction, e.g. merge log entries that operates on the same key, like a"]
    #[doc = "   LSM-tree does,"]
    #[doc = " - or by fetching a snapshot from the state machine."]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let state = self.applied_state().await?;

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: state.0,
                last_membership: state.1,
                snapshot_id: "".into(),
            },
            snapshot: Box::new(Cursor::new(vec![])),
        })
    }
}

impl RaftLogStorage<TypeConfig> for Arc<FjallStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged_log_id = self.state.get("last_purged_log_id").unwrap();
        let last_log_id = self.log.last_key_value().unwrap();

        Ok(LogState {
            last_purged_log_id: last_purged_log_id.map(|v| deserialize_logid(&v).0),
            last_log_id: last_log_id.map(|(_k, v)| deserialize_logid(&v).0),
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

    #[doc = " Get the snapshot builder for the state machine."]
    #[doc = ""]
    #[doc = " Usually it returns a snapshot view of the state machine(i.e., subsequent changes to the"]
    #[doc = " state machine won\'t affect the return snapshot view), or just a copy of the entire state"]
    #[doc = " machine."]
    #[doc = ""]
    #[doc = " The method is intentionally async to give the implementation a chance to use"]
    #[doc = " asynchronous sync primitives to serialize access to the common internal object, if"]
    #[doc = " needed."]
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
        // SnapshotBuilder {
        //     state: self.state.snapshot(),
        //     log: self.log.snapshot(),
        // }
    }

    #[doc = " Create a new blank snapshot, returning a writable handle to the snapshot object."]
    #[doc = ""]
    #[doc = " Openraft will use this handle to receive snapshot data."]
    #[doc = ""]
    #[doc = " See the [storage chapter of the guide][sto] for details on log compaction / snapshotting."]
    #[doc = ""]
    #[doc = " [sto]: crate::docs::getting_started#3-implement-raftlogstorage-and-raftstatemachine"]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotData>, StorageError<NodeId>> {
        todo!()
    }

    #[doc = " Install a snapshot which has finished streaming from the leader."]
    #[doc = ""]
    #[doc = " Before this method returns:"]
    #[doc = " - The state machine should be replaced with the new contents of the snapshot,"]
    #[doc = " - the input snapshot should be saved, i.e., [`Self::get_current_snapshot`] should return it."]
    #[doc = " - and all other snapshots should be deleted at this point."]
    #[doc = ""]
    #[doc = " ### snapshot"]
    #[doc = ""]
    #[doc = " A snapshot created from an earlier call to `begin_receiving_snapshot` which provided the"]
    #[doc = " snapshot."]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        todo!()
    }

    #[doc = " Get a readable handle to the current snapshot."]
    #[doc = ""]
    #[doc = " ### implementation algorithm"]
    #[doc = ""]
    #[doc = " Implementing this method should be straightforward. Check the configured snapshot"]
    #[doc = " directory for any snapshot files. A proper implementation will only ever have one"]
    #[doc = " active snapshot, though another may exist while it is being created. As such, it is"]
    #[doc = " recommended to use a file naming pattern which will allow for easily distinguishing between"]
    #[doc = " the current live snapshot, and any new snapshot which is being created."]
    #[doc = ""]
    #[doc = " A proper snapshot implementation will store last-applied-log-id and the"]
    #[doc = " last-applied-membership config as part of the snapshot, which should be decoded for"]
    #[doc = " creating this method\'s response data."]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(None)
    }
}
