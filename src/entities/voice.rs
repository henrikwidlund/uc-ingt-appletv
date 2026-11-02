// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! Voice assistant entity: Siri on the Apple TV.
//!
//! Siri shows its answer on the TV. No text or speech is sent back to the Remote.

use crate::controller::VOICE_SAMPLE_RATE;
use crate::device::state::Snapshot;
use serde_json::{Map, Value};
use uc_api::intg::AvailableIntgEntity;
use uc_api::{AudioConfiguration, SampleFormat, VoiceAssistantEntityOptions};

pub fn definition(mut entity: AvailableIntgEntity) -> AvailableIntgEntity {
    entity.features = Some(Vec::new());
    let options = VoiceAssistantEntityOptions {
        audio_cfg: Some(AudioConfiguration {
            channels: 1,
            sample_rate: VOICE_SAMPLE_RATE,
            sample_format: SampleFormat::I16,
        }),
        profiles: None,
        preferred_profile: None,
    };
    if let Ok(Value::Object(map)) = serde_json::to_value(options) {
        entity.options = Some(map);
    }
    entity
}

/// Insert the state attribute.
pub fn attributes(s: &Snapshot, out: &mut Map<String, Value>) {
    // Siri needs MRP.
    let state = if s.connected && s.mrp {
        "ON"
    } else {
        "UNAVAILABLE"
    };
    out.insert("state".into(), state.into());
}
