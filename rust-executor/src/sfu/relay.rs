//! Media relay — selective forwarding and active speaker detection.
//!
//! The relay tracks voice activity per participant and provides
//! active speaker detection based on audio energy levels.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use str0m::media::{MediaData, MediaKind};

use super::room::ParticipantId;

/// Tracks voice activity for active speaker detection.
#[derive(Debug)]
struct VoiceActivityState {
    /// Smoothed audio level (0.0 = silence, 1.0 = max)
    level: f64,
    /// Last time we received audio from this participant
    last_audio: Instant,
    /// Whether currently considered "speaking"
    is_speaking: bool,
}

impl Default for VoiceActivityState {
    fn default() -> Self {
        Self {
            level: 0.0,
            last_audio: Instant::now(),
            is_speaking: false,
        }
    }
}

/// The media relay tracks voice activity and determines active speakers.
///
/// Actual media forwarding is handled in the server event loop (server.rs),
/// since it needs direct access to the str0m Rtc instances. The relay module
/// provides the intelligence layer: which participant is the active speaker,
/// voice activity tracking, etc.
#[derive(Debug)]
pub struct MediaRelay {
    voice_activity: HashMap<ParticipantId, VoiceActivityState>,
    /// The current active speaker (participant with highest sustained audio level)
    active_speaker: Option<ParticipantId>,
    /// Set of participant IDs that represent pipe transports (not real users).
    /// Media from these should not be forwarded back to the originating pipe.
    pipe_transport_ids: HashSet<ParticipantId>,
}

/// Threshold for considering a participant as "speaking"
const SPEAKING_THRESHOLD: f64 = 0.01;
/// Time after last audio before a participant is no longer considered speaking
const SPEAKING_TIMEOUT_MS: u128 = 500;
/// Approximate size of a "full energy" Opus packet for normalization
const OPUS_FULL_ENERGY_SIZE: f64 = 200.0;

impl MediaRelay {
    pub fn new() -> Self {
        Self {
            voice_activity: HashMap::new(),
            active_speaker: None,
            pipe_transport_ids: HashSet::new(),
        }
    }

    /// Update voice activity for a participant based on incoming audio data.
    /// Uses a simple energy estimation from the first few bytes of the RTP payload.
    pub fn update_voice_activity(&mut self, pid: &ParticipantId, data: &MediaData) {
        if data.kind != MediaKind::Audio {
            return;
        }

        let state = self.voice_activity.entry(pid.clone()).or_default();

        // Simple audio energy estimation from RTP payload
        // This is a rough heuristic — real VAD would decode the Opus frame
        let energy = if data.data.len() > 2 {
            // Opus silence packets are very small (typically 1-3 bytes)
            // Larger packets generally indicate voice activity
            let size_energy = (data.data.len() as f64 / OPUS_FULL_ENERGY_SIZE).min(1.0);
            size_energy
        } else {
            0.0
        };

        // Exponential moving average for smoothing
        state.level = state.level * 0.7 + energy * 0.3;
        state.last_audio = Instant::now();
        state.is_speaking = state.level > SPEAKING_THRESHOLD;

        // Update active speaker
        self.update_active_speaker();
    }

    /// Determine the current active speaker.
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn update_active_speaker(&mut self) {
        let now = Instant::now();
        let mut best: Option<(ParticipantId, f64)> = None;

        for (pid, state) in &self.voice_activity {
            // Skip if no recent audio
            if now.duration_since(state.last_audio).as_millis() > SPEAKING_TIMEOUT_MS {
                continue;
            }

            if state.is_speaking {
                match &best {
                    Some((_, best_level)) if state.level > *best_level => {
                        best = Some((pid.clone(), state.level));
                    }
                    None => {
                        best = Some((pid.clone(), state.level));
                    }
                    _ => {}
                }
            }
        }

        self.active_speaker = best.map(|(pid, _)| pid);
    }

    /// Get the current active speaker, if any.
    pub fn active_speaker(&self) -> Option<&ParticipantId> {
        self.active_speaker.as_ref()
    }

    /// Check if a specific participant is currently speaking.
    pub fn is_speaking(&self, pid: &ParticipantId) -> bool {
        self.voice_activity
            .get(pid)
            .map(|s| {
                s.is_speaking
                    && Instant::now().duration_since(s.last_audio).as_millis()
                        <= SPEAKING_TIMEOUT_MS
            })
            .unwrap_or(false)
    }

    /// Remove a participant from voice activity tracking.
    pub fn remove_participant(&mut self, pid: &ParticipantId) {
        self.voice_activity.remove(pid);
        self.pipe_transport_ids.remove(pid);
        if self.active_speaker.as_ref() == Some(pid) {
            self.active_speaker = None;
            self.update_active_speaker();
        }
    }

    /// Register a participant ID as a pipe transport endpoint.
    pub fn register_pipe_transport(&mut self, pid: ParticipantId) {
        self.pipe_transport_ids.insert(pid);
    }

    /// Check if a participant ID represents a pipe transport.
    pub fn is_pipe_transport(&self, pid: &ParticipantId) -> bool {
        self.pipe_transport_ids.contains(pid)
    }

    /// Unregister a pipe transport participant.
    pub fn unregister_pipe_transport(&mut self, pid: &ParticipantId) {
        self.pipe_transport_ids.remove(pid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::RangeInclusive;
    use std::time::{Duration, Instant};
    use str0m::media::{MediaData, Mid};
    use str0m::rtp::{ExtensionValues, MediaTime, Pt, SeqNo};
    use str0m::format::{Codec, CodecSpec, FormatParams, PayloadParams};
    use str0m::packet::CodecExtra;

    fn make_audio_data(size: usize) -> MediaData {
        let pt = Pt::new_with_value(111);
        let spec = CodecSpec {
            codec: Codec::Opus,
            clock_rate: str0m::rtp::Frequency::FORTY_EIGHT_KHZ,
            channels: Some(2),
            format: FormatParams::default(),
        };
        let params = PayloadParams::new(pt, None, spec);
        MediaData {
            mid: Mid::new(),
            pt,
            rid: None,
            params,
            time: MediaTime::new(0, str0m::rtp::Frequency::FORTY_EIGHT_KHZ),
            network_time: Instant::now(),
            seq_range: RangeInclusive::new(SeqNo::from(1u64), SeqNo::from(1u64)),
            contiguous: true,
            data: vec![0u8; size],
            ext_vals: ExtensionValues::default(),
            codec_extra: CodecExtra::None,
            last_sender_info: None,
            audio_start_of_talk_spurt: false,
        }
    }

    #[test]
    fn test_active_speaker_detection() {
        let relay = MediaRelay::new();
        assert!(relay.active_speaker().is_none());
    }

    #[test]
    fn test_voice_activity_detection() {
        let mut relay = MediaRelay::new();
        let p1 = ParticipantId::next();
        let p2 = ParticipantId::next();

        // Silence packet should not trigger speaking
        let silent = make_audio_data(1);
        relay.update_voice_activity(&p1, &silent);
        assert!(!relay.is_speaking(&p1));

        // Larger packet should trigger speaking
        let loud = make_audio_data(200);
        relay.update_voice_activity(&p1, &loud);
        assert!(relay.is_speaking(&p1));

        // Another participant with higher energy becomes active speaker
        let louder = make_audio_data(400);
        relay.update_voice_activity(&p2, &louder);
        assert_eq!(relay.active_speaker(), Some(&p2));
    }

    #[test]
    fn test_pipe_transport_registration() {
        let mut relay = MediaRelay::new();
        let p1 = ParticipantId::next();
        relay.register_pipe_transport(p1.clone());
        assert!(relay.is_pipe_transport(&p1));
        relay.unregister_pipe_transport(&p1);
        assert!(!relay.is_pipe_transport(&p1));
    }

    #[test]
    fn test_speaking_timeout() {
        let mut relay = MediaRelay::new();
        let p1 = ParticipantId::next();

        let loud = make_audio_data(200);
        relay.update_voice_activity(&p1, &loud);
        assert!(relay.is_speaking(&p1));

        // Simulate timeout by rewinding last_audio
        if let Some(state) = relay.voice_activity.get_mut(&p1) {
            state.last_audio = Instant::now() - Duration::from_millis(SPEAKING_TIMEOUT_MS as u64 + 10);
        }

        assert!(!relay.is_speaking(&p1));
    }

    #[test]
    fn test_multiple_participants_speaking() {
        let mut relay = MediaRelay::new();
        let p1 = ParticipantId::next();
        let p2 = ParticipantId::next();
        let p3 = ParticipantId::next();

        // Feed varying energy levels
        let small = make_audio_data(50);
        let medium = make_audio_data(150);
        let large = make_audio_data(400);

        relay.update_voice_activity(&p1, &small);
        relay.update_voice_activity(&p2, &large);
        relay.update_voice_activity(&p3, &medium);

        // p2 has highest energy → active speaker
        assert_eq!(relay.active_speaker(), Some(&p2));
    }

    #[test]
    fn test_voice_activity_with_direct_state() {
        let mut relay = MediaRelay::new();
        let p1 = ParticipantId::next();
        let p2 = ParticipantId::next();

        relay.voice_activity.insert(p1.clone(), VoiceActivityState {
            level: 0.8,
            last_audio: Instant::now(),
            is_speaking: true,
        });
        relay.voice_activity.insert(p2.clone(), VoiceActivityState {
            level: 0.2,
            last_audio: Instant::now(),
            is_speaking: true,
        });

        relay.update_active_speaker();
        assert_eq!(relay.active_speaker(), Some(&p1));
    }

    #[test]
    fn test_remove_participant() {
        let mut relay = MediaRelay::new();
        let p1 = ParticipantId::next();

        // Add some voice activity state
        relay.voice_activity.insert(
            p1.clone(),
            VoiceActivityState {
                level: 0.5,
                last_audio: Instant::now(),
                is_speaking: true,
            },
        );
        relay.active_speaker = Some(p1.clone());

        relay.remove_participant(&p1);
        assert!(relay.active_speaker().is_none());
        assert!(!relay.is_speaking(&p1));
    }
}
