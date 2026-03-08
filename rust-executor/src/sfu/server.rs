//! SFU server — manages the shared UDP socket, str0m Rtc instances,
//! and the Sans I/O event loop driven by tokio.

use std::collections::HashMap;
use std::net::SocketAddr;

use std::time::{Duration, Instant};

use log::{debug, error, info, warn};
use str0m::change::SdpOffer;
use str0m::media::{KeyframeRequest, KeyframeRequestKind, MediaData, MediaKind, Mid};
use str0m::net::Protocol;
use str0m::{net::Receive, Candidate, Event, IceConnectionState, Input, Output, Rtc};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};


use super::relay::MediaRelay;
use super::room::{ParticipantId, RoomId};

#[cfg(feature = "sfu")]
use crate::pubsub::{get_global_pubsub_sync, SFU_CALL_PARTICIPANTS_TOPIC, SFU_CALL_STREAMS_TOPIC};

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
    /// Renegotiate SDP for an existing peer (track add/remove).
    RenegotiatePeer {
        agent_did: String,
        room_id: RoomId,
        sdp_offer: SdpOffer,
        response_tx: oneshot::Sender<Result<String, String>>,
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
            // Use UDP connect trick to find the default outbound IP
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

                        // tracks_out are set up during SDP creation in call_join
                        peers.insert(pid, peer);
                    }
                    Ok(SfuCommand::RemovePeer(pid)) => {
                        if let Some(peer) = peers.remove(&pid) {
                            info!("SFU: peer {} left room {}", pid, peer.room_id);
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

            // Clean out disconnected peers
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
                            warn!("SFU: peer {} poll error: {:?}", pid, e);
                            peer.rtc.disconnect();
                            break;
                        }
                    }
                }
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

                // Cache origin pipe info before the mutable borrow loop
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
                                _ => false, // "high" and "auto" forward all
                            };
                            if skip {
                                continue;
                            }
                        }
                    }

                    // Find a matching mid on the target peer to forward the media
                    // For audio: use the target peer's audio mid (sendrecv allows bidirectional)
                    // For video: use the target peer's video mid if present
                    let target_mid = if data.params.spec().codec.is_audio() {
                        target_peer.tracks_in.iter()
                            .find(|(_, kind)| matches!(kind, MediaKind::Audio))
                            .map(|(mid, _)| *mid)
                    } else {
                        // For video, try tracks_out first (explicit mapping), then fall back to video mid
                        target_peer.tracks_out.iter()
                            .find(|(_, (src_pid, src_mid))| src_pid == origin_pid && *src_mid == data.mid)
                            .map(|(out_mid, _)| *out_mid)
                            .or_else(|| target_peer.tracks_in.iter()
                                .find(|(_, kind)| matches!(kind, MediaKind::Video))
                                .map(|(mid, _)| *mid))
                    };

                    if let Some(mid) = target_mid {
                        if let Some(writer) = target_peer.rtc.writer(mid) {
                            if let Err(e) = writer.write(data.pt, data.network_time, data.time, data.data.clone()) {
                                debug!(
                                    "SFU: failed to write media to peer {} mid {}: {:?}",
                                    target_pid, mid, e
                                );
                            }
                        }
                    } else if target_peer.is_pipe_transport {
                        debug!(
                            "SFU: no target mid for pipe peer {} (tracks_in: {:?}), cannot forward {:?} from {}",
                            target_pid, target_peer.tracks_in, if data.params.spec().codec.is_audio() { "audio" } else { "video" }, origin_pid
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

                // The keyframe request is for an outgoing track on the requesting peer.
                // Find which origin peer owns that track.
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

                                // Demultiplex: find which peer accepts this packet
                                if let Some((_pid, peer)) = peers.iter_mut().find(|(_, p)| p.rtc.accepts(&input)) {
                                    if let Err(e) = peer.rtc.handle_input(input) {
                                        warn!("SFU: peer input error: {:?}", e);
                                        peer.rtc.disconnect();
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            if e.kind() == std::io::ErrorKind::WouldBlock {
                                // Non-blocking mode, expected
                            } else {
                                error!("SFU: UDP recv error: {:?}", e);
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(duration) => {
                    // Timeout — drive all peers forward
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
