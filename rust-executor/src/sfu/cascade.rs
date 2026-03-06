//! Cascaded SFU — manages pipe transports between SFU nodes in a cluster.
//!
//! When multiple executor nodes act as SFU peers for the same neighbourhood,
//! they establish str0m peer connections ("pipe transports") between each other
//! to relay media tracks across the cluster.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use log::{debug, info, warn};
use str0m::change::SdpOffer;
use str0m::media::{MediaData, MediaKind, Mid};
use str0m::{Candidate, Event, IceConnectionState, Input, Output, Rtc};

use super::room::RoomId;

/// Represents a remote SFU node in the cascade cluster.
#[derive(Debug, Clone)]
pub struct SfuNodeInfo {
    pub did: String,
    pub participant_count: u32,
    pub capacity_hint: u32,
}

/// A pipe transport — a str0m peer connection to a remote SFU node.
pub struct PipeTransport {
    pub remote_did: String,
    pub rtc: Rtc,
    pub room_id: RoomId,
    /// Tracks being received from the remote SFU (mid -> kind)
    pub tracks_in: HashMap<Mid, MediaKind>,
    /// Tracks being sent to the remote SFU (local mid -> source mid)
    pub tracks_out: HashMap<Mid, Mid>,
    pub established: bool,
}

impl std::fmt::Debug for PipeTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipeTransport")
            .field("remote_did", &self.remote_did)
            .field("room_id", &self.room_id)
            .field("established", &self.established)
            .field("tracks_in", &self.tracks_in.len())
            .field("tracks_out", &self.tracks_out.len())
            .finish()
    }
}

/// Messages used for SFU cluster discovery and pipe transport signalling.
/// These are sent via neighbourhood signalling channels.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum CascadeSignal {
    /// Broadcast: "I am an SFU node for this room"
    #[serde(rename = "sfu-announce")]
    Announce {
        did: String,
        room_id: String,
        participant_count: u32,
        capacity_hint: u32,
    },
    /// SDP offer to establish a pipe transport between two SFU nodes
    #[serde(rename = "sfu-pipe-offer")]
    PipeOffer {
        from_did: String,
        to_did: String,
        room_id: String,
        sdp_offer: String,
    },
    /// SDP answer for pipe transport
    #[serde(rename = "sfu-pipe-answer")]
    PipeAnswer {
        from_did: String,
        to_did: String,
        room_id: String,
        sdp_answer: String,
    },
    /// SFU node is leaving the cluster
    #[serde(rename = "sfu-leave")]
    Leave { did: String, room_id: String },
}

/// Manages the cascade cluster for a single SFU node.
///
/// Tracks known peer SFU nodes, manages pipe transports, and handles
/// forwarding decisions for cross-node media relay.
pub struct CascadeManager {
    /// Our DID
    local_did: String,
    /// Local SFU server address for creating pipe transport RTCs
    local_addr: SocketAddr,
    /// Known SFU nodes per room: room_id_str -> (did -> node_info)
    known_nodes: HashMap<String, HashMap<String, SfuNodeInfo>>,
    /// Active pipe transports: (room_id_str, remote_did) -> PipeTransport
    pipes: HashMap<(String, String), PipeTransport>,
    /// Max participants this node will accept per room
    max_participants_per_node: u32,
}

impl CascadeManager {
    pub fn new(local_did: String, local_addr: SocketAddr, max_participants_per_node: u32) -> Self {
        Self {
            local_did,
            local_addr,
            known_nodes: HashMap::new(),
            pipes: HashMap::new(),
            max_participants_per_node,
        }
    }

    /// Generate an announce signal for broadcasting.
    pub fn announce_sfu_node(&self, room_id: &RoomId, participant_count: u32) -> CascadeSignal {
        CascadeSignal::Announce {
            did: self.local_did.clone(),
            room_id: room_id.to_string(),
            participant_count,
            capacity_hint: self.max_participants_per_node,
        }
    }

    /// Handle an incoming SFU announce from a peer node.
    pub fn handle_sfu_announce(
        &mut self,
        did: String,
        room_id: String,
        participant_count: u32,
        capacity_hint: u32,
    ) {
        if did == self.local_did {
            return; // Ignore our own announces
        }

        let nodes = self.known_nodes.entry(room_id).or_default();
        nodes.insert(
            did.clone(),
            SfuNodeInfo {
                did,
                participant_count,
                capacity_hint,
            },
        );
    }

    /// Create an SDP offer to establish a pipe transport to a remote SFU node.
    pub fn establish_pipe(
        &mut self,
        remote_did: &str,
        room_id: &RoomId,
    ) -> Result<CascadeSignal, String> {
        let room_key = room_id.to_string();
        let pipe_key = (room_key.clone(), remote_did.to_string());

        if self.pipes.contains_key(&pipe_key) {
            return Err("Pipe transport already exists".to_string());
        }

        let mut rtc = Rtc::builder().build(Instant::now());
        let candidate = Candidate::host(self.local_addr, "udp")
            .map_err(|e| format!("Failed to create candidate: {}", e))?;
        rtc.add_local_candidate(candidate)
            .map_err(|e| format!("Failed to add candidate: {}", e))?;

        // Create offer via SDP API — the remote side will add media lines on accept
        let offer = rtc
            .sdp_api()
            .apply()
            .map_err(|e| format!("Failed to create pipe offer: {}", e))?;

        let sdp_offer = serde_json::to_string(&offer)
            .map_err(|e| format!("Failed to serialize offer: {}", e))?;

        let pipe = PipeTransport {
            remote_did: remote_did.to_string(),
            rtc,
            room_id: room_id.clone(),
            tracks_in: HashMap::new(),
            tracks_out: HashMap::new(),
            established: false,
        };

        self.pipes.insert(pipe_key, pipe);

        Ok(CascadeSignal::PipeOffer {
            from_did: self.local_did.clone(),
            to_did: remote_did.to_string(),
            room_id: room_key,
            sdp_offer,
        })
    }

    /// Handle an incoming pipe offer from a remote SFU node.
    pub fn handle_pipe_offer(
        &mut self,
        from_did: &str,
        room_id: &str,
        sdp_offer_json: &str,
    ) -> Result<CascadeSignal, String> {
        let offer: SdpOffer = serde_json::from_str(sdp_offer_json)
            .map_err(|e| format!("Invalid pipe SDP offer: {}", e))?;

        let mut rtc = Rtc::builder().build(Instant::now());
        let candidate = Candidate::host(self.local_addr, "udp")
            .map_err(|e| format!("Failed to create candidate: {}", e))?;
        rtc.add_local_candidate(candidate)
            .map_err(|e| format!("Failed to add candidate: {}", e))?;

        let answer = rtc
            .sdp_api()
            .accept_offer(offer)
            .map_err(|e| format!("Failed to accept pipe offer: {}", e))?;

        let sdp_answer = serde_json::to_string(&answer)
            .map_err(|e| format!("Failed to serialize pipe answer: {}", e))?;

        // Parse room_id from the string format "neighbourhood_url:room_name"
        let (nh_url, room_name) = room_id
            .split_once(':')
            .unwrap_or((room_id, "default"));

        let pipe = PipeTransport {
            remote_did: from_did.to_string(),
            rtc,
            room_id: RoomId::new(nh_url, room_name),
            tracks_in: HashMap::new(),
            tracks_out: HashMap::new(),
            established: true,
        };

        let pipe_key = (room_id.to_string(), from_did.to_string());
        self.pipes.insert(pipe_key, pipe);

        Ok(CascadeSignal::PipeAnswer {
            from_did: self.local_did.clone(),
            to_did: from_did.to_string(),
            room_id: room_id.to_string(),
            sdp_answer,
        })
    }

    /// Handle an incoming pipe answer from a remote SFU node.
    pub fn handle_pipe_answer(
        &mut self,
        from_did: &str,
        room_id: &str,
        sdp_answer_json: &str,
    ) -> Result<(), String> {
        let pipe_key = (room_id.to_string(), from_did.to_string());
        let pipe = self
            .pipes
            .get_mut(&pipe_key)
            .ok_or_else(|| "No pending pipe transport for this node".to_string())?;

        let answer: str0m::change::SdpAnswer = serde_json::from_str(sdp_answer_json)
            .map_err(|e| format!("Invalid pipe SDP answer: {}", e))?;

        pipe.rtc
            .sdp_api()
            .accept_answer(answer)
            .map_err(|e| format!("Failed to accept pipe answer: {}", e))?;

        pipe.established = true;
        info!(
            "Pipe transport established to SFU node {} for room {}",
            from_did, room_id
        );

        Ok(())
    }

    /// Forward media data to all pipe transports for a room, excluding the origin node.
    pub fn forward_to_pipes(
        &mut self,
        room_id: &str,
        data: &MediaData,
        exclude_node: Option<&str>,
    ) {
        for ((pipe_room, pipe_did), pipe) in self.pipes.iter_mut() {
            if pipe_room != room_id {
                continue;
            }
            if let Some(exclude) = exclude_node {
                if pipe_did == exclude {
                    continue; // Don't forward back to origin SFU node
                }
            }
            if !pipe.established || !pipe.rtc.is_alive() {
                continue;
            }

            // Find matching outgoing track on the pipe
            for (&out_mid, _) in &pipe.tracks_out {
                if let Some(writer) = pipe.rtc.writer(out_mid) {
                    if let Err(e) = writer.write(data.network_time, data.time, &data.data) {
                        debug!("Failed to forward to pipe {}: {:?}", pipe_did, e);
                    }
                    break;
                }
            }
        }
    }

    /// Remove an SFU node from the cluster (handles sfu-leave).
    pub fn remove_node(&mut self, did: &str) {
        // Remove from known nodes
        for nodes in self.known_nodes.values_mut() {
            nodes.remove(did);
        }

        // Remove and disconnect pipe transports
        let keys_to_remove: Vec<_> = self
            .pipes
            .keys()
            .filter(|(_, d)| d == did)
            .cloned()
            .collect();

        for key in keys_to_remove {
            if let Some(mut pipe) = self.pipes.remove(&key) {
                pipe.rtc.disconnect();
                info!("Removed pipe transport to SFU node {}", did);
            }
        }
    }

    /// Get known SFU nodes for a room (for the sfuNodesForRoom query).
    pub fn nodes_for_room(&self, room_id: &str) -> Vec<SfuNodeInfo> {
        self.known_nodes
            .get(room_id)
            .map(|nodes| nodes.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Pick the least-loaded SFU node for a new participant.
    /// Returns None if this local node should accept the participant.
    pub fn pick_redirect_node(&self, room_id: &str, local_count: u32) -> Option<&SfuNodeInfo> {
        if local_count < self.max_participants_per_node {
            // We have capacity — only redirect if a remote node is significantly less loaded
            let nodes = self.known_nodes.get(room_id)?;
            let least_loaded = nodes
                .values()
                .filter(|n| n.participant_count + 1 <= n.capacity_hint)
                .min_by_key(|n| n.participant_count)?;

            if least_loaded.participant_count + 2 < local_count {
                return Some(least_loaded);
            }
            return None;
        }

        // We're at capacity — must redirect
        let nodes = self.known_nodes.get(room_id)?;
        nodes
            .values()
            .filter(|n| n.participant_count < n.capacity_hint)
            .min_by_key(|n| n.participant_count)
    }

    /// Get mutable access to all pipe transports (for driving in the event loop).
    pub fn pipes_mut(&mut self) -> impl Iterator<Item = (&(String, String), &mut PipeTransport)> {
        self.pipes.iter_mut()
    }

    /// Check if we're in cascaded mode for a room (have known peer nodes).
    pub fn is_cascaded(&self, room_id: &str) -> bool {
        self.known_nodes
            .get(room_id)
            .map(|n| !n.is_empty())
            .unwrap_or(false)
    }
}
