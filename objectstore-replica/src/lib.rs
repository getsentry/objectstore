use openraft::{RaftNetwork, RaftNetworkFactory};
use serde::{Deserialize, Serialize};

mod fjallstore;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    Put { key: String, data: Vec<u8> },
    Get { key: String },
    Delete { key: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    data: Option<Vec<u8>>,
}

openraft::declare_raft_types!(
   pub TypeConfig:
       D            = Request,
       R            = Response,
       SnapshotData = fjallstore::StoreSnapshot,
);

// struct Network {}

// impl RaftNetwork<TypeConfig> for Network {
//     #[doc = " Send an AppendEntries RPC to the target."]
//     fn append_entries(
//         &mut self,
//         rpc: AppendEntriesRequest<C>,
//         option: RPCOption,
//     ) -> impl std::future::Future<
//         Output = Result<
//             AppendEntriesResponse<C::NodeId>,
//             RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>,
//         >,
//     > + Send {
//         todo!()
//     }

//     #[doc = " Send a RequestVote RPC to the target."]
//     fn vote(
//         &mut self,
//         rpc: VoteRequest<C::NodeId>,
//         option: RPCOption,
//     ) -> impl std::future::Future<
//         Output = Result<
//             VoteResponse<C::NodeId>,
//             RPCError<C::NodeId, C::Node, RaftError<C::NodeId>>,
//         >,
//     > + Send {
//         todo!()
//     }

//     #[doc = " Send a complete Snapshot to the target."]
//     #[doc = ""]
//     #[doc = " This method is responsible to fragment the snapshot and send it to the target node."]
//     #[doc = " Before returning from this method, the snapshot should be completely transmitted and"]
//     #[doc = " installed on the target node, or rejected because of `vote` being smaller than the"]
//     #[doc = " remote one."]
//     #[doc = ""]
//     #[doc = " The default implementation just calls several `install_snapshot` RPC for each fragment."]
//     #[doc = ""]
//     #[doc = " The `vote` is the leader vote which is used to check if the leader is still valid by a"]
//     #[doc = " follower."]
//     #[doc = " When the follower finished receiving snapshot, it calls `Raft::install_full_snapshot()`"]
//     #[doc = " with this vote."]
//     #[doc = ""]
//     #[doc = " `cancel` get `Ready` when the caller decides to cancel this snapshot transmission."]
//     #[doc = ""]
//     #[doc = " To implement a more generic snapshot transmission, you can use the `generic-snapshot-data`"]
//     #[doc = " feature. Enabling this feature allows you to send any type of snapshot data."]
//     #[doc = " See the [generic snapshot"]
//     #[doc = " data](crate::docs::feature_flags#feature-flag-generic-snapshot-data) chapter for"]
//     #[doc = " details."]
//     #[cfg(feature = "generic-snapshot-data")]
//     fn full_snapshot(
//         &mut self,
//         vote: Vote<C::NodeId>,
//         snapshot: Snapshot<C>,
//         cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
//         option: RPCOption,
//     ) -> impl std::future::Future<
//         Output = Result<SnapshotResponse<C::NodeId>, StreamingError<C, Fatal<C::NodeId>>>,
//     > + Send {
//         todo!()
//     }
// }

// impl RaftNetworkFactory<TypeConfig> for Network {
//     #[doc = " Actual type of the network handling a single connection."]
//     type Network = Network;

//     #[doc = " Create a new network instance sending RPCs to the target node."]
//     #[doc = ""]
//     #[doc = " This function should **not** create a connection but rather a client that will connect when"]
//     #[doc = " required. Therefore there is chance it will build a client that is unable to send out"]
//     #[doc = " anything, e.g., in case the Node network address is configured incorrectly. But this method"]
//     #[doc = " does not return an error because openraft can only ignore it."]
//     #[doc = ""]
//     #[doc = " The method is intentionally async to give the implementation a chance to use asynchronous"]
//     #[doc = " sync primitives to serialize access to the common internal object, if needed."]
//     fn new_client(
//         &mut self,
//         target: TypeConfig::NodeId,
//         node: &TypeConfig::Node,
//     ) -> impl std::future::Future<Output = Self::Network> + Send {
//         todo!()
//     }
// }
