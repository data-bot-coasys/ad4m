//! GraphQL types and resolvers for the SFU service.

#[cfg(feature = "sfu")]
pub mod types {
    use coasys_juniper::GraphQLObject;

    /// Information about an active SFU room.
    #[derive(GraphQLObject, Debug, Clone)]
    pub struct SfuRoomGql {
        pub neighbourhood_url: String,
        pub room_name: String,
        pub participant_count: i32,
        pub participants: Vec<SfuParticipantGql>,
    }

    /// Information about a participant in an SFU room.
    #[derive(GraphQLObject, Debug, Clone)]
    pub struct SfuParticipantGql {
        pub agent_did: String,
        pub has_audio: bool,
        pub has_video: bool,
        pub is_active_speaker: bool,
    }

    /// Result of joining a call via the SFU.
    #[derive(GraphQLObject, Debug, Clone)]
    pub struct CallSessionGql {
        pub room_name: String,
        pub neighbourhood_url: String,
        pub participant_id: String,
        pub sdp_answer: String,
        /// If set, client should disconnect and reconnect to this SFU node's DID instead.
        pub redirect_to: Option<String>,
        /// Maps stream IDs to participant DIDs so the client knows who each track belongs to.
        /// Format: Vec of "streamId:did" pairs.
        pub stream_mapping: Vec<String>,
    }

    /// SFU configuration for a neighbourhood (from Social DNA).
    #[derive(GraphQLObject, Debug, Clone)]
    pub struct SfuConfigGql {
        pub mode: String,
        pub designated_peer: Option<String>,
        pub sfu_peers: Vec<String>,
        pub fallback: String,
        pub max_mesh_participants: i32,
        pub max_participants_per_node: Option<i32>,
    }

    /// Information about an SFU node in a cascaded cluster.
    #[derive(GraphQLObject, Debug, Clone)]
    pub struct SfuNodeGql {
        pub did: String,
        pub participant_count: i32,
        pub capacity_hint: i32,
    }

    /// Participant join/leave event for subscriptions.
    #[derive(GraphQLObject, Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct CallParticipantEvent {
        pub room_id: String,
        pub agent_did: String,
        pub event_type: String, // "joined" | "left"
    }

    impl crate::graphql::graphql_types::GetValue for CallParticipantEvent {
        type Value = CallParticipantEvent;
        fn get_value(&self) -> Self::Value {
            self.clone()
        }
    }

    impl crate::graphql::graphql_types::GetFilter for CallParticipantEvent {
        fn get_filter(&self) -> Option<String> {
            Some(self.room_id.clone())
        }
    }

    /// Stream add/remove event for subscriptions.
    #[derive(GraphQLObject, Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct CallStreamEvent {
        pub room_id: String,
        pub agent_did: String,
        pub track_kind: String, // "audio" | "video"
        pub event_type: String, // "added" | "removed"
    }

    impl crate::graphql::graphql_types::GetValue for CallStreamEvent {
        type Value = CallStreamEvent;
        fn get_value(&self) -> Self::Value {
            self.clone()
        }
    }

    impl crate::graphql::graphql_types::GetFilter for CallStreamEvent {
        fn get_filter(&self) -> Option<String> {
            Some(self.room_id.clone())
        }
    }

    /// Server-initiated SDP renegotiation offer event.
    /// Published to clients when new tracks need to be added (peer join)
    /// or removed (peer leave).
    #[derive(GraphQLObject, Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct RenegotiationOfferEvent {
        /// The room ID (format: "neighbourhood_url:room_name")
        pub room_id: String,
        /// The DID of the peer this offer is for (targeted delivery)
        pub agent_did: String,
        /// The SDP offer JSON string
        pub sdp_offer: String,
        /// Track mapping entries, each formatted as "mid:ownerDid:kind"
        /// e.g. "3:did:key:z6MkABC:audio", "4:did:key:z6MkABC:video"
        pub track_mapping: Vec<String>,
    }

    impl crate::graphql::graphql_types::GetValue for RenegotiationOfferEvent {
        type Value = RenegotiationOfferEvent;
        fn get_value(&self) -> Self::Value {
            self.clone()
        }
    }

    impl crate::graphql::graphql_types::GetFilter for RenegotiationOfferEvent {
        fn get_filter(&self) -> Option<String> {
            // Filter by agent_did so each client only receives their own offers
            Some(self.agent_did.clone())
        }
    }
}
