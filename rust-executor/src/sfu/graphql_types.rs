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
    }

    /// SFU configuration for a neighbourhood (from Social DNA).
    #[derive(GraphQLObject, Debug, Clone)]
    pub struct SfuConfigGql {
        pub mode: String,
        pub designated_peer: Option<String>,
        pub fallback: String,
        pub max_mesh_participants: i32,
    }

    /// Participant join/leave event for subscriptions.
    #[derive(GraphQLObject, Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct CallParticipantEvent {
        pub room_id: String,
        pub agent_did: String,
        pub event_type: String, // "joined" | "left"
    }

    /// Stream add/remove event for subscriptions.
    #[derive(GraphQLObject, Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct CallStreamEvent {
        pub room_id: String,
        pub agent_did: String,
        pub track_kind: String, // "audio" | "video"
        pub event_type: String, // "added" | "removed"
    }
}
