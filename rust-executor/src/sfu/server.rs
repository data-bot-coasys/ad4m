//! SFU server — manages the shared UDP socket, str0m Rtc instances,
//! and the Sans I/O event loop driven by tokio.

use std::collections::HashMap;
use std::net::SocketAddr;

use std::time::{Duration, Instant};

use log::{debug, error, info, warn};
use str0m::change::{SdpAnswer, SdpOffer, SdpPendingOffer};
use str0m::media::{Direction, KeyframeRequest, KeyframeRequestKind, MediaData, MediaKind, Mid};
use str0m::net::Protocol;
use str0m::{net::Receive, Candidate, Event, IceConnectionState, Input, Output, Rtc};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};


use super::relay::MediaRelay;
use super::room::{ParticipantId, RoomId};

#[cfg(feature = "sfu")]
use crate::pubsub::{get_global_pubsub_sync, SFU_CALL_PARTICIPANTS_TOPIC, SFU_CALL_STREAMS_TOPIC, SFU_RENEGOTIATION_OFFER_TOPIC};

/// Format a track mapping entry as "mid:ownerDid:kind"
fn format_track_mapping(mid: &Mid, owner_did: &str, kind: MediaKind) -> String {
    format!("{}:{}:{}", mid, owner_did, match kind { MediaKind::Audio => "audio", MediaKind::Video => "video" })
}

/// A connected WebRTC peer managed by the SFU server.
#[derive(Debug)]
pub struct SfuPeer {
    pub id: ParticipantId,
    pub room_id: RoomId,
    pub agent_did: String,
    pub rtc: Rtc,
    /// Maps incoming Mid (media the peer sends us) to MediaKind
    pub tracks_in: HashMap<Mid, MediaKind>,
    /// Maps outgoing Mid (media we send to the peer) to the source (origin participant, origin mid)
    pub tracks_out: HashMap<Mid, (ParticipantId, Mid)>,
    /// If this peer is a pipe transport to another SFU node (not a real participant)
    pub is_pipe_transport: bool,
    /// The DID of the remote SFU node (only set for pipe transports)
    pub pipe_remote_did: Option<String>,
    /// Tracks we've already created outgoing mids for: (source_pid, kind) → our outgoing mid
    pub outgoing_tracks: HashMap<(ParticipantId, MediaKind), Mid>,
    /// Pending SDP offer waiting for client answer
    pub pending_offer: Option<SdpPendingOffer>,
}

/// Commands sent to the SFU event loop from the GraphQL API / signalling layer.

pub enum SfuCommand {
    /// A new peer has completed SDP negotiation and should be added to the event loop.
    AddPeer(SfuPeer),
    /// A peer is leaving (explicit leave or disconnect).
    RemovePeer(ParticipantId),
    /// Set quality preference for a participant's received video.
    SetQualityPreference {
        participant_id: ParticipantId,
        /// "high", "medium", "low", or "auto"
        preference: String,
    },
    /// Renegotiate SDP for an existing peer (client-initiated, e.g. track add/remove).
    RenegotiatePeer {
        agent_did: String,
        room_id: RoomId,
        sdp_offer: SdpOffer,
        response_tx: oneshot::Sender<Result<String, String>>,
    },
    /// Client's answer to a server-initiated SDP offer.
    AnswerServerOffer {
        agent_did: String,
        room_id: RoomId,
        sdp_answer: SdpAnswer,
        response_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Shut down the SFU server.
    Shutdown,
}

/// Configuration for the SFU server.
#[derive(Debug, Clone)]
pub struct SfuServerConfig {
    /// Address to bind the UDP socket to. Use 0.0.0.0:0 for auto-assignment.
    pub bind_addr: SocketAddr,
    /// STUN server URLs for ICE candidates.
    pub stun_servers: Vec<String>,
    /// TURN server URLs for ICE relay candidates.
    pub turn_servers: Vec<TurnServer>,
}

#[derive(Debug, Clone)]
pub struct TurnServer {
    pub url: String,
    pub username: String,
    pub credential: String,
}

impl Default for SfuServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:0".parse().unwrap(),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            turn_servers: vec![],
        }
    }
}

/// The SFU server. Owns the UDP socket and drives the str0m event loop.
pub struct SfuServer {
    /// The bound local address of the UDP socket.
    pub local_addr: SocketAddr,
    /// Channel to send commands to the event loop.
    pub command_tx: mpsc::Sender<SfuCommand>,
}

impl SfuServer {
    /// Start the SFU server. Binds a UDP socket and spawns the event loop on tokio.
    pub async fn start(config: SfuServerConfig) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(config.bind_addr).await?;
        let raw_addr = socket.local_addr()?;
        // Resolve 0.0.0.0 to actual network interface IP for cascade pipe transports
        let local_addr = if raw_addr.ip().is_unspecified() {
            let probe = std::net::UdpSocket::bind("0.0.0.0:0").ok()
                .and_then(|s| { s.connect("8.8.8.8:80").ok()?; s.local_addr().ok() })
                .map(|a| a.ip())
                .unwrap_or(raw_addr.ip());
            std::net::SocketAddr::new(probe, raw_addr.port())
        } else {
            raw_addr
        };
        info!("SFU server bound to UDP {} (resolved: {})", raw_addr, local_addr);

        let (command_tx, command_rx) = mpsc::channel(256);

        tokio::spawn(Self::event_loop(socket, command_rx, local_addr));

        Ok(Self {
            local_addr,
            command_tx,
        })
    }

    /// Create an Rtc instance for a new peer, process the SDP offer, and return the answer.
    /// The Rtc instance is NOT yet added to the event loop — call `add_peer` after.
    pub fn create_rtc_for_offer(
        offer: SdpOffer,
        local_addr: SocketAddr,
    ) -> Result<(Rtc, String), String> {
        let mut rtc = Rtc::builder().build(Instant::now());

        // If bound to 0.0.0.0, resolve to actual network interface IP
        let resolved_addr = if local_addr.ip().is_unspecified() {
            let probe = std::net::UdpSocket::bind("0.0.0.0:0").ok()
                .and_then(|s| { s.connect("8.8.8.8:80").ok()?; s.local_addr().ok() })
                .map(|a| a.ip())
                .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            std::net::SocketAddr::new(probe, local_addr.port())
        } else {
            local_addr
        };
        info!("SFU creating host candidate with addr: {}", resolved_addr);
        let candidate = Candidate::host(resolved_addr, "udp")
            .map_err(|e| format!("Failed to create host candidate: {}", e))?;
        rtc.add_local_candidate(candidate);

        let answer = rtc
            .sdp_api()
            .accept_offer(offer)
            .map_err(|e| format!("Failed to accept SDP offer: {}", e))?;

        let answer_json = serde_json::to_string(&answer)
            .map_err(|e| format!("Failed to serialize SDP answer: {}", e))?;

        Ok((rtc, answer_json))
    }

    /// For a given peer, add SendOnly media lines for each source track they don't have yet.
    /// Returns list of (mid, source_pid, kind) for newly added tracks.
    /// Determine which receive tracks are missing for this peer.
    /// Returns a list of (source_pid, source_mid, kind, source_did) that need to be added.
    fn find_missing_receive_tracks(
        peer: &SfuPeer,
        sources: &[(ParticipantId, String, Vec<(Mid, MediaKind)>)],
    ) -> Vec<(ParticipantId, Mid, MediaKind, String)> {
        let mut missing = Vec::new();

        for (source_pid, source_did, source_tracks) in sources {
            if *source_pid == peer.id {
                continue;
            }
            for (source_mid, kind) in source_tracks {
                let key = (source_pid.clone(), *kind);
                if peer.outgoing_tracks.contains_key(&key) {
                    continue;
                }
                missing.push((source_pid.clone(), *source_mid, *kind, source_did.clone()));
            }
        }

        missing
    }



    /// Generate a server offer for a peer (after adding tracks) and publish it.
    /// Stores the pending offer on the peer.
    fn generate_and_publish_offer(peer: &mut SfuPeer, track_mappings: Vec<String>) {
        let prev_pending = peer.pending_offer.take();
        let had_pending = prev_pending.is_some();
        let mut api = peer.rtc.sdp_api();

        // If there's an existing pending offer, merge it
        if let Some(prev) = prev_pending {
            api.merge(prev);
        }

        info!("SFU: attempting to generate offer for peer {} with {} track mappings, has_pending={}", 
            peer.id, track_mappings.len(), had_pending);
        match api.apply() {
            Some((offer, pending)) => {
                peer.pending_offer = Some(pending);

                let offer_json = match serde_json::to_string(&offer) {
                    Ok(j) => j,
                    Err(e) => {
                        error!("SFU: failed to serialize server offer: {}", e);
                        return;
                    }
                };

                let event = super::graphql_types::types::RenegotiationOfferEvent {
                    room_id: peer.room_id.to_string(),
                    agent_did: peer.agent_did.clone(),
                    sdp_offer: offer_json,
                    track_mapping: track_mappings,
                };

                if let Ok(json) = serde_json::to_string(&event) {
                    info!("SFU: publishing server offer to peer {} ({}) with {} track mappings",
                        peer.id, peer.agent_did, event.track_mapping.len());
                    get_global_pubsub_sync().publish_sync(&SFU_RENEGOTIATION_OFFER_TOPIC, &json);
                }
            }
            None => {
                info!("SFU: no changes to apply for peer {} — no offer generated", peer.id);
            }
        }
    }

    /// Perform renegotiation for all peers in a room. Called when a peer joins/leaves.
    fn renegotiate_room(
        peers: &mut HashMap<ParticipantId, SfuPeer>,
        room_id: &RoomId,
    ) {
        // Collect all source tracks in the room: (pid, did, [(mid, kind)])
        let sources: Vec<(ParticipantId, String, Vec<(Mid, MediaKind)>)> = peers.iter()
            .filter(|(_, p)| &p.room_id == room_id)
            .map(|(pid, p)| {
                let tracks: Vec<(Mid, MediaKind)> = p.tracks_in.iter()
                    .map(|(mid, kind)| (*mid, *kind))
                    .collect();
                (pid.clone(), p.agent_did.clone(), tracks)
            })
            .collect();

        // For each peer in the room, find and add missing receive tracks
        let peer_ids: Vec<ParticipantId> = peers.iter()
            .filter(|(_, p)| &p.room_id == room_id && !p.is_pipe_transport)
            .map(|(pid, _)| pid.clone())
            .collect();

        for pid in &peer_ids {
            let peer = match peers.get_mut(pid) {
                Some(p) => p,
                None => continue,
            };

            let missing = Self::find_missing_receive_tracks(peer, &sources);
            info!("SFU: renegotiate_room for peer {} - {} missing receive tracks", pid, missing.len());

            if !missing.is_empty() {
                // Use a single SdpApi for adding tracks AND applying the offer
                let prev_pending = peer.pending_offer.take();
                let had_pending = prev_pending.is_some();
                
                // Phase 1: Create SdpApi, add media lines, apply → get offer
                // We can't update peer tracking maps while SdpApi borrows peer.rtc,
                // so we collect (new_mid, source info) from the api calls first.
                let mut api = peer.rtc.sdp_api();
                if let Some(prev) = prev_pending {
                    api.merge(prev);
                }

                let mut new_mids: Vec<(Mid, ParticipantId, Mid, MediaKind, String)> = Vec::new();
                for (source_pid, source_mid, kind, source_did) in &missing {
                    let new_mid = api.add_media(
                        *kind,
                        Direction::SendOnly,
                        None,
                        None,
                        None,
                    );
                    new_mids.push((new_mid, source_pid.clone(), *source_mid, *kind, source_did.clone()));
                    info!(
                        "SFU: added SendOnly {} mid={} to peer {} for source peer {} mid={}",
                        match kind { MediaKind::Audio => "audio", MediaKind::Video => "video" },
                        new_mid, peer.id, source_pid, source_mid
                    );
                }

                info!("SFU: attempting to generate offer for peer {} with {} new tracks, has_pending={}",
                    peer.id, new_mids.len(), had_pending);
                let apply_result = api.apply();
                // api is consumed by apply(), peer.rtc borrow is released

                // Phase 2: Update tracking maps (now safe to borrow peer mutably)
                for (new_mid, source_pid, source_mid, kind, _source_did) in &new_mids {
                    let key = (source_pid.clone(), *kind);
                    peer.outgoing_tracks.insert(key, *new_mid);
                    peer.tracks_out.insert(*new_mid, (source_pid.clone(), *source_mid));
                }

                // Build track mappings
                let track_mappings: Vec<String> = new_mids.iter()
                    .map(|(mid, _src_pid, _src_mid, kind, src_did)| {
                        format_track_mapping(mid, src_did, *kind)
                    })
                    .collect();

                // Include previously mapped tracks
                let mut all_mappings = track_mappings;
                for (out_mid, (src_pid, _src_mid)) in &peer.tracks_out {
                    if let Some(src) = sources.iter().find(|(p, _, _)| p == src_pid) {
                        let kind = peer.outgoing_tracks.iter()
                            .find(|((_p, _k), m)| *m == out_mid)
                            .map(|((_, k), _)| *k);
                        if let Some(kind) = kind {
                            let entry = format_track_mapping(out_mid, &src.1, kind);
                            if !all_mappings.contains(&entry) {
                                all_mappings.push(entry);
                            }
                        }
                    }
                }

                // Phase 3: Process apply result
                match apply_result {
                    Some((offer, pending)) => {
                        peer.pending_offer = Some(pending);
                        let offer_json = match serde_json::to_string(&offer) {
                            Ok(j) => j,
                            Err(e) => {
                                error!("SFU: failed to serialize server offer: {}", e);
                                continue;
                            }
                        };
                        let event = super::graphql_types::types::RenegotiationOfferEvent {
                            room_id: peer.room_id.to_string(),
                            agent_did: peer.agent_did.clone(),
                            sdp_offer: offer_json,
                            track_mapping: all_mappings,
                        };
                        if let Ok(json) = serde_json::to_string(&event) {
                            info!("SFU: publishing server offer to peer {} ({}) with {} track mappings",
                                peer.id, peer.agent_did, event.track_mapping.len());
                            get_global_pubsub_sync().publish_sync(&SFU_RENEGOTIATION_OFFER_TOPIC, &json);
                        }
                    }
                    None => {
                        info!("SFU: no changes to apply for peer {} — no offer generated", peer.id);
                    }
                }
            }
        }

        // Also handle pipe transports
        let pipe_ids: Vec<ParticipantId> = peers.iter()
            .filter(|(_, p)| &p.room_id == room_id && p.is_pipe_transport)
            .map(|(pid, _)| pid.clone())
            .collect();

        for pid in &pipe_ids {
            let peer = match peers.get_mut(pid) {
                Some(p) => p,
                None => continue,
            };

            let missing = Self::find_missing_receive_tracks(peer, &sources);
            if !missing.is_empty() {
                let prev_pending = peer.pending_offer.take();
                let mut api = peer.rtc.sdp_api();
                if let Some(prev) = prev_pending {
                    api.merge(prev);
                }
                let mut new_mids: Vec<(Mid, ParticipantId, Mid, MediaKind, String)> = Vec::new();
                for (source_pid, source_mid, kind, source_did) in &missing {
                    let new_mid = api.add_media(*kind, Direction::SendOnly, None, None, None);
                    new_mids.push((new_mid, source_pid.clone(), *source_mid, *kind, source_did.clone()));
                }
                let apply_result = api.apply();

                for (new_mid, source_pid, source_mid, kind, _) in &new_mids {
                    peer.outgoing_tracks.insert((source_pid.clone(), *kind), *new_mid);
                    peer.tracks_out.insert(*new_mid, (source_pid.clone(), *source_mid));
                }
                let track_mappings: Vec<String> = new_mids.iter()
                    .map(|(mid, _src_pid, _src_mid, kind, src_did)| {
                        format_track_mapping(mid, src_did, *kind)
                    })
                    .collect();

                match apply_result {
                    Some((offer, pending)) => {
                        peer.pending_offer = Some(pending);
                        let offer_json = match serde_json::to_string(&offer) {
                            Ok(j) => j,
                            Err(e) => {
                                error!("SFU: failed to serialize pipe offer: {}", e);
                                continue;
                            }
                        };
                        let event = super::graphql_types::types::RenegotiationOfferEvent {
                            room_id: peer.room_id.to_string(),
                            agent_did: peer.agent_did.clone(),
                            sdp_offer: offer_json,
                            track_mapping: track_mappings,
                        };
                        if let Ok(json) = serde_json::to_string(&event) {
                            get_global_pubsub_sync().publish_sync(&SFU_RENEGOTIATION_OFFER_TOPIC, &json);
                        }
                    }
                    None => {}
                }
            }
        }
    }

    /// The main event loop. Reads UDP packets, drives str0m, and relays media.
    async fn event_loop(
        socket: UdpSocket,
        mut command_rx: mpsc::Receiver<SfuCommand>,
        local_addr: SocketAddr,
    ) {
        let mut peers: HashMap<ParticipantId, SfuPeer> = HashMap::new();
        let mut relay = MediaRelay::new();
        let mut quality_preferences: HashMap<ParticipantId, String> = HashMap::new();
        let mut buf = vec![0u8; 2000];
        // Rooms that need renegotiation (deferred to end of command processing)
        let mut rooms_to_renegotiate: Vec<RoomId> = Vec::new();

        let mut pending_renegotiations: HashMap<RoomId, std::time::Instant> = HashMap::new();
        // Resolve 0.0.0.0 to actual IP for ICE candidate matching
        let resolved_local_addr = if local_addr.ip().is_unspecified() {
            let probe = std::net::UdpSocket::bind("0.0.0.0:0").ok()
                .and_then(|s| { s.connect("8.8.8.8:80").ok()?; s.local_addr().ok() })
                .map(|a| a.ip())
                .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            std::net::SocketAddr::new(probe, local_addr.port())
        } else {
            local_addr
        };
        info!("SFU event loop started on {} (resolved: {})", local_addr, resolved_local_addr);

        loop {
            rooms_to_renegotiate.clear();

            // Process commands
            loop {
                match command_rx.try_recv() {
                    Ok(SfuCommand::AddPeer(peer)) => {
                        let pid = peer.id.clone();
                        let room_id = peer.room_id.clone();
                        let is_pipe = peer.is_pipe_transport;
                        let agent_did_for_event = peer.agent_did.clone();
                        info!(
                            "SFU: peer {} (DID: {}) joined room {}{}",
                            pid, peer.agent_did, room_id,
                            if is_pipe { " [pipe transport]" } else { "" }
                        );

                        if is_pipe {
                            relay.register_pipe_transport(pid.clone());
                        }

                        // Publish participant joined event (non-pipe only)
                        if !is_pipe {
                            let event = super::graphql_types::types::CallParticipantEvent {
                                room_id: room_id.to_string(),
                                agent_did: agent_did_for_event,
                                event_type: "joined".to_string(),
                            };
                            if let Ok(json) = serde_json::to_string(&event) {
                                get_global_pubsub_sync().publish_sync(&SFU_CALL_PARTICIPANTS_TOPIC, &json);
                            }
                        }

                        // Don't renegotiate immediately on AddPeer.
                        // Instead, we wait for MediaAdded (when this peer starts sending)
                        // which triggers renegotiation naturally. This avoids a race where
                        // the renegotiation offer is published before the joiner's
                        // GraphQL subscription is established.
                        //
                        // For the JOINER who needs existing peers' tracks:
                        // We schedule a delayed renegotiation via a flag on the peer.
                        // The next poll tick (100ms later) will check for pending renegotiations.
                        pending_renegotiations.insert(room_id.clone(), std::time::Instant::now() + std::time::Duration::from_millis(500));

                        peers.insert(pid, peer);
                    }
                    Ok(SfuCommand::RemovePeer(pid)) => {
                        if let Some(peer) = peers.remove(&pid) {
                            let room_id = peer.room_id.clone();
                            info!("SFU: peer {} left room {}", pid, room_id);
                            // Publish participant left event (non-pipe only)
                            if !peer.is_pipe_transport {
                                let event = super::graphql_types::types::CallParticipantEvent {
                                    room_id: peer.room_id.to_string(),
                                    agent_did: peer.agent_did.clone(),
                                    event_type: "left".to_string(),
                                };
                                if let Ok(json) = serde_json::to_string(&event) {
                                    get_global_pubsub_sync().publish_sync(&SFU_CALL_PARTICIPANTS_TOPIC, &json);
                                }

                                // Publish stream removed events for all tracks from this peer
                                for kind in peer.tracks_in.values() {
                                    let stream_event = super::graphql_types::types::CallStreamEvent {
                                        room_id: peer.room_id.to_string(),
                                        agent_did: peer.agent_did.clone(),
                                        track_kind: match kind { MediaKind::Audio => "audio", MediaKind::Video => "video" }.to_string(),
                                        event_type: "removed".to_string(),
                                    };
                                    if let Ok(json) = serde_json::to_string(&stream_event) {
                                        get_global_pubsub_sync().publish_sync(&SFU_CALL_STREAMS_TOPIC, &json);
                                    }
                                }
                            }
                            relay.remove_participant(&pid);
                            quality_preferences.remove(&pid);

                            // Remove outgoing tracks that pointed to this departed peer,
                            // mark their mids as inactive, and schedule renegotiation
                            let mut peers_needing_offer: Vec<ParticipantId> = Vec::new();
                            for (other_pid, other_peer) in peers.iter_mut() {
                                if other_peer.room_id == room_id && !other_peer.is_pipe_transport {
                                    // Find mids that pointed to the departed peer
                                    let departed_mids: Vec<Mid> = other_peer.tracks_out.iter()
                                        .filter(|(_, (src_pid, _))| *src_pid == pid)
                                        .map(|(mid, _)| *mid)
                                        .collect();
                                    
                                    if !departed_mids.is_empty() {
                                        // Mark departed mids as inactive in SDP
                                        for mid in &departed_mids {
                                            other_peer.rtc.sdp_api().set_direction(*mid, Direction::Inactive);
                                        }
                                        
                                        // Remove from tracking maps
                                        other_peer.tracks_out.retain(|_, (src_pid, _)| *src_pid != pid);
                                        other_peer.outgoing_tracks.retain(|(src_pid, _), _| *src_pid != pid);
                                        
                                        peers_needing_offer.push(other_pid.clone());
                                    }
                                }
                            }
                            
                            // Collect DID map for track mapping generation (avoids borrow conflict)
                            let did_map: HashMap<ParticipantId, String> = peers.iter()
                                .filter(|(_, p)| p.room_id == room_id)
                                .map(|(pid, p)| (pid.clone(), p.agent_did.clone()))
                                .collect();
                            
                            // Generate renegotiation offers for affected peers
                            for other_pid in peers_needing_offer {
                                if let Some(other_peer) = peers.get_mut(&other_pid) {
                                    // Build full track mapping for remaining tracks
                                    let all_mappings: Vec<String> = other_peer.tracks_out.iter()
                                        .filter_map(|(out_mid, (src_pid, _))| {
                                            let kind = other_peer.outgoing_tracks.iter()
                                                .find(|((p, _k), m)| p == src_pid && *m == out_mid)
                                                .map(|((_, k), _)| *k);
                                            let src_did = did_map.get(src_pid);
                                            match (kind, src_did) {
                                                (Some(k), Some(d)) => Some(format_track_mapping(out_mid, d, k)),
                                                _ => None,
                                            }
                                        })
                                        .collect();
                                    
                                    Self::generate_and_publish_offer(other_peer, all_mappings);
                                    info!("SFU: departure renegotiation offer sent to peer {}", other_pid);
                                }
                            }
                        }
                    }
                    Ok(SfuCommand::SetQualityPreference { participant_id, preference }) => {
                        info!("SFU: peer {} quality preference set to '{}'", participant_id, preference);
                        quality_preferences.insert(participant_id, preference);
                    }
                    Ok(SfuCommand::RenegotiatePeer { agent_did, room_id, sdp_offer, response_tx }) => {
                        // Find existing peer by DID + room
                        let existing_pid = peers.iter()
                            .find(|(_, p)| p.agent_did == agent_did && p.room_id == room_id)
                            .map(|(pid, _)| pid.clone());

                        if let Some(pid) = existing_pid {
                            if let Some(peer) = peers.get_mut(&pid) {
                                info!("SFU: renegotiating peer {} (DID: {}) in room {}", pid, agent_did, room_id);
                                match peer.rtc.sdp_api().accept_offer(sdp_offer) {
                                    Ok(answer) => {
                                        match serde_json::to_string(&answer) {
                                            Ok(answer_json) => { let _ = response_tx.send(Ok(answer_json)); }
                                            Err(e) => { let _ = response_tx.send(Err(format!("Failed to serialize answer: {}", e))); }
                                        }
                                    }
                                    Err(e) => {
                                        let _ = response_tx.send(Err(format!("Failed to accept renegotiation offer: {}", e)));
                                    }
                                }
                            } else {
                                let _ = response_tx.send(Err("Peer not found in event loop".to_string()));
                            }
                        } else {
                            let _ = response_tx.send(Err(format!("No existing peer for DID {} in room {}", agent_did, room_id)));
                        }
                    }
                    Ok(SfuCommand::AnswerServerOffer { agent_did, room_id, sdp_answer, response_tx }) => {
                        let existing_pid = peers.iter()
                            .find(|(_, p)| p.agent_did == agent_did && p.room_id == room_id)
                            .map(|(pid, _)| pid.clone());

                        if let Some(pid) = existing_pid {
                            if let Some(peer) = peers.get_mut(&pid) {
                                if let Some(pending) = peer.pending_offer.take() {
                                    info!("SFU: applying server offer answer from peer {} (DID: {})", pid, agent_did);
                                    match peer.rtc.sdp_api().accept_answer(pending, sdp_answer) {
                                        Ok(()) => {
                                            info!("SFU: server offer answer accepted for peer {}", pid);
                                            
                                            // Collect keyframe targets before dropping the peer borrow
                                            let keyframe_targets: Vec<(ParticipantId, Mid)> = peer.tracks_out.iter()
                                                .filter_map(|(_, (src_pid, src_mid))| {
                                                    peer.outgoing_tracks.iter()
                                                        .find(|((p, k), _)| p == src_pid && *k == MediaKind::Video)
                                                        .map(|_| (src_pid.clone(), *src_mid))
                                                })
                                                .collect();
                                            
                                            let _ = response_tx.send(Ok(()));
                                            
                                            // Drop the peer borrow so we can borrow other peers
                                            let _ = peer;
                                            
                                            // Request keyframes from source peers
                                            for (src_pid, src_mid) in keyframe_targets {
                                                if let Some(src_peer) = peers.get_mut(&src_pid) {
                                                    if let Some(mut writer) = src_peer.rtc.writer(src_mid) {
                                                        info!("SFU: requesting post-answer keyframe from peer {} mid={}", src_pid, src_mid);
                                                        let _ = writer.request_keyframe(None, KeyframeRequestKind::Pli);
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("SFU: failed to accept server offer answer for peer {}: {}", pid, e);
                                            let _ = response_tx.send(Err(format!("Failed to accept answer: {}", e)));
                                        }
                                    }
                                } else {
                                    warn!("SFU: no pending offer for peer {} — answer ignored", pid);
                                    let _ = response_tx.send(Err("No pending server offer".to_string()));
                                }
                            } else {
                                let _ = response_tx.send(Err("Peer not found".to_string()));
                            }
                        } else {
                            let _ = response_tx.send(Err(format!("No peer for DID {} in room {}", agent_did, room_id)));
                        }
                    }
                    Ok(SfuCommand::Shutdown) => {
                        info!("SFU event loop shutting down");
                        return;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        info!("SFU command channel closed, shutting down");
                        return;
                    }
                }
            }

            // Perform deferred room renegotiations
            for room_id in &rooms_to_renegotiate {
                Self::renegotiate_room(&mut peers, room_id);
            }

            // Clean out disconnected peers
            let mut disconnected_rooms: Vec<RoomId> = Vec::new();
            peers.retain(|pid, peer| {
                if !peer.rtc.is_alive() {
                    info!("SFU: peer {} disconnected", pid);
                    if !peer.is_pipe_transport {
                        let event = super::graphql_types::types::CallParticipantEvent {
                            room_id: peer.room_id.to_string(),
                            agent_did: peer.agent_did.clone(),
                            event_type: "left".to_string(),
                        };
                        if let Ok(json) = serde_json::to_string(&event) {
                            get_global_pubsub_sync().publish_sync(&SFU_CALL_PARTICIPANTS_TOPIC, &json);
                        }
                        for kind in peer.tracks_in.values() {
                            let stream_event = super::graphql_types::types::CallStreamEvent {
                                room_id: peer.room_id.to_string(),
                                agent_did: peer.agent_did.clone(),
                                track_kind: match kind { MediaKind::Audio => "audio", MediaKind::Video => "video" }.to_string(),
                                event_type: "removed".to_string(),
                            };
                            if let Ok(json) = serde_json::to_string(&stream_event) {
                                get_global_pubsub_sync().publish_sync(&SFU_CALL_STREAMS_TOPIC, &json);
                            }
                        }
                    }
                    disconnected_rooms.push(peer.room_id.clone());
                    relay.remove_participant(pid);
                    false
                } else {
                    true
                }
            });

            // Poll all peers for output
            let mut earliest_timeout = Instant::now() + Duration::from_millis(100);
            let mut media_to_relay: Vec<(ParticipantId, MediaData)> = Vec::new();
            let mut keyframe_requests: Vec<(ParticipantId, KeyframeRequest)> = Vec::new();
            let mut new_tracks: Vec<(ParticipantId, RoomId, Mid, MediaKind)> = Vec::new();

            for (pid, peer) in peers.iter_mut() {
                loop {
                    if !peer.rtc.is_alive() {
                        break;
                    }

                    match peer.rtc.poll_output() {
                        Ok(Output::Transmit(transmit)) => {
                            if let Err(e) = socket
                                .try_send_to(&transmit.contents, transmit.destination)
                                .map_err(|e| e)
                            {
                                debug!(
                                    "SFU: failed to send UDP to {}: {}",
                                    transmit.destination, e
                                );
                            }
                        }
                        Ok(Output::Timeout(t)) => {
                            earliest_timeout = earliest_timeout.min(t);
                            break;
                        }
                        Ok(Output::Event(event)) => match event {
                            Event::IceConnectionStateChange(state) => {
                                info!("SFU: peer {} ICE state: {:?}", pid, state);
                                if state == IceConnectionState::Disconnected {
                                    peer.rtc.disconnect();
                                }
                            }
                            Event::MediaAdded(e) => {
                                info!("SFU: peer {} added {:?} track mid={}", pid, e.kind, e.mid);
                                peer.tracks_in.insert(e.mid, e.kind);

                                // Request keyframe for newly added video tracks
                                if matches!(e.kind, MediaKind::Video) {
                                    if let Some(mut writer) = peer.rtc.writer(e.mid) {
                                        info!("SFU: requesting keyframe from peer {} for new video track mid={}", pid, e.mid);
                                        let _ = writer.request_keyframe(None, KeyframeRequestKind::Pli);
                                    }
                                }

                                // Publish stream event
                                let stream_event = super::graphql_types::types::CallStreamEvent {
                                    room_id: peer.room_id.to_string(),
                                    agent_did: peer.agent_did.clone(),
                                    track_kind: match e.kind { MediaKind::Audio => "audio", MediaKind::Video => "video" }.to_string(),
                                    event_type: "added".to_string(),
                                };
                                if let Ok(json) = serde_json::to_string(&stream_event) {
                                    get_global_pubsub_sync().publish_sync(&SFU_CALL_STREAMS_TOPIC, &json);
                                }

                                // Track new track for renegotiation
                                new_tracks.push((pid.clone(), peer.room_id.clone(), e.mid, e.kind));
                            }
                            Event::MediaData(data) => {
                                media_to_relay.push((pid.clone(), data));
                            }
                            Event::KeyframeRequest(req) => {
                                keyframe_requests.push((pid.clone(), req));
                            }
                            _ => {}
                        },
                        Err(e) => {
                            debug!("SFU: peer {} poll error (non-fatal): {:?}", pid, e);
                            break;
                        }
                    }
                }
            }

            // If new tracks appeared, trigger renegotiation for their rooms
            if !new_tracks.is_empty() {
                let mut rooms_needing_reneg: Vec<RoomId> = Vec::new();
                for (_, room_id, _, _) in &new_tracks {
                    if !rooms_needing_reneg.contains(room_id) {
                        rooms_needing_reneg.push(room_id.clone());
                    }
                }
                for room_id in &rooms_needing_reneg {
                    Self::renegotiate_room(&mut peers, room_id);
                    // Cancel any pending renegotiation for this room since we just did it
                    pending_renegotiations.remove(room_id);
                }
            }
            
            // Check for delayed renegotiations (from AddPeer)
            let now_instant = std::time::Instant::now();
            let due: Vec<RoomId> = pending_renegotiations.iter()
                .filter(|(_, when)| now_instant >= **when)
                .map(|(room_id, _)| room_id.clone())
                .collect();
            for room_id in &due {
                pending_renegotiations.remove(room_id);
                info!("SFU: executing delayed renegotiation for room {}", room_id);
                Self::renegotiate_room(&mut peers, room_id);
            }

            // Relay media data to other peers in the same room
            for (origin_pid, data) in &media_to_relay {
                let origin_room = match peers.get(origin_pid) {
                    Some(p) => p.room_id.clone(),
                    None => continue,
                };

                // Update relay with voice activity for active speaker detection
                if data.params.spec().codec.is_audio() {
                    relay.update_voice_activity(origin_pid, data);
                }

                let origin_is_pipe = peers.get(origin_pid).map(|p| p.is_pipe_transport).unwrap_or(false);
                let origin_pipe_did = peers.get(origin_pid).and_then(|p| p.pipe_remote_did.clone());

                // Forward to all other peers in the same room
                for (target_pid, target_peer) in peers.iter_mut() {
                    if target_pid == origin_pid {
                        continue;
                    }
                    if target_peer.room_id != origin_room {
                        continue;
                    }

                    // Don't forward media from a pipe back to itself
                    if origin_is_pipe && target_peer.is_pipe_transport
                        && target_peer.pipe_remote_did == origin_pipe_did {
                        continue;
                    }

                    // Apply quality preference filtering for video
                    if data.params.spec().codec.is_video() {
                        if let Some(rid) = &data.rid {
                            let pref = quality_preferences.get(target_pid).map(|s| s.as_str()).unwrap_or("high");
                            let rid_str = rid.to_string();
                            let skip = match pref {
                                "low" => rid_str != "low" && rid_str != "q",
                                "medium" => rid_str == "high" || rid_str == "f",
                                _ => false,
                            };
                            if skip {
                                continue;
                            }
                        }
                    }

                    // Find the correct outgoing mid for this source track
                    let target_mid = target_peer.tracks_out.iter()
                        .find(|(_, (src_pid, src_mid))| src_pid == origin_pid && *src_mid == data.mid)
                        .map(|(out_mid, _)| *out_mid);

                    // Fallback: for audio, try any audio out track mapped to origin
                    let target_mid = target_mid.or_else(|| {
                        if data.params.spec().codec.is_audio() {
                            // Look for an outgoing audio track mapped to origin_pid
                            target_peer.outgoing_tracks.iter()
                                .find(|((src_pid, kind), _)| src_pid == origin_pid && *kind == MediaKind::Audio)
                                .map(|(_, mid)| *mid)
                        } else {
                            // Look for an outgoing video track mapped to origin_pid
                            target_peer.outgoing_tracks.iter()
                                .find(|((src_pid, kind), _)| src_pid == origin_pid && *kind == MediaKind::Video)
                                .map(|(_, mid)| *mid)
                        }
                    });

                    if let Some(mid) = target_mid {
                        if let Some(writer) = target_peer.rtc.writer(mid) {
                            if let Err(e) = writer.write(data.pt, data.network_time, data.time, data.data.clone()) {
                                debug!(
                                    "SFU: failed to write media to peer {} mid {}: {:?}",
                                    target_pid, mid, e
                                );
                            }
                        } else {
                            debug!(
                                "SFU: writer() returned None for peer {} mid {} — direction may not allow sending or answer not yet received",
                                target_pid, mid
                            );
                        }
                    } else {
                        // No track mapping yet — this can happen before renegotiation completes
                        debug!(
                            "SFU: no outgoing track for peer {} to receive from {} — renegotiation may be pending",
                            target_pid, origin_pid
                        );
                    }
                }
            }

            // Handle keyframe requests — route to the originating peer
            for (requesting_pid, req) in &keyframe_requests {
                let requesting_peer = match peers.get(requesting_pid) {
                    Some(p) => p,
                    None => continue,
                };

                if let Some((origin_pid, origin_mid)) =
                    requesting_peer.tracks_out.get(&req.mid).cloned()
                {
                    if let Some(origin_peer) = peers.get_mut(&origin_pid) {
                        if let Some(mut writer) = origin_peer.rtc.writer(origin_mid) {
                            let _ = writer.request_keyframe(None, KeyframeRequestKind::Pli);
                        }
                    }
                }
            }

            // Read from the UDP socket with timeout
            let duration = (earliest_timeout - Instant::now()).max(Duration::from_millis(1));

            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((n, source)) => {
                            let data = &buf[..n];
                            if let Ok(contents) = data.try_into() {
                                let input = Input::Receive(
                                    Instant::now(),
                                    Receive {
                                        proto: Protocol::Udp,
                                        source,
                                        destination: resolved_local_addr,
                                        contents,
                                    },
                                );

                                if let Some((pid, peer)) = peers.iter_mut().find(|(_, p)| p.rtc.accepts(&input)) {
                                    if let Err(e) = peer.rtc.handle_input(input) {
                                        debug!("SFU: peer {} input error (non-fatal): {:?}", pid, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            if e.kind() == std::io::ErrorKind::WouldBlock {
                            } else {
                                error!("SFU: UDP recv error: {:?}", e);
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(duration) => {
                }
            }

            // Drive time forward for all peers
            let now = Instant::now();
            for (_pid, peer) in peers.iter_mut() {
                let _ = peer.rtc.handle_input(Input::Timeout(now));
            }
        }
    }
}
