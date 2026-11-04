// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! Apple TV devices of the controller: entity states, subscriptions and commands.

use crate::config_store::DeviceConfig;
use crate::controller::{Controller, R2AudioChunkMsg, R2RequestMsg};
use crate::device::{
    AppleTvActor, AudioChunk, Browse, Command, Connect, DeviceEvent, DeviceStatus, Disconnect, Stop,
};
use crate::entities::{self, EntityKind};
use crate::errors::ServiceError;
use crate::util::{DeserializeMsgData, return_fut_err};
use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, ResponseFuture};
use log::{debug, info, warn};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use uc_api::EntityType;
use uc_api::intg::ws::BrowseMediaMsgData;
use uc_api::intg::{AssistantEvent, AvailableIntgEntity, DeviceState, EntityCommand};
use uc_api::ws::{EventCategory, WsMessage, WsResultMsgData};

/// Attribute that changes with each position update.
const POSITION_UPDATED_AT: &str = "media_position_updated_at";

/// `entity_change` data. Borrows the attributes, so that they are not copied to serialize them.
#[derive(Serialize)]
pub(super) struct EntityChangeRef<'a> {
    entity_type: EntityType,
    entity_id: &'a str,
    attributes: &'a Map<String, Value>,
}

/// A voice command failed: forget the voice session.
#[derive(Message)]
#[rtype(result = "()")]
struct EndVoiceSession(u32);

/// One configured Apple TV.
pub(super) struct Device {
    pub cfg: DeviceConfig,
    pub addr: Addr<AppleTvActor>,
    pub status: DeviceStatus,
}

impl Controller {
    /// Start the actor of a device.
    pub(super) fn add_device(&mut self, cfg: DeviceConfig, ctx: &Context<Self>) {
        let key: Arc<str> = cfg.key.as_str().into();
        let addr = AppleTvActor::new(
            key.clone(),
            cfg.clone(),
            self.device_ctx.clone(),
            ctx.address().recipient(),
        )
        .start();
        // Remove first: `insert` keeps the old key, and events are matched by the key instance.
        if let Some(old) = self.devices.remove(&*key) {
            old.addr.do_send(Stop);
        }
        self.devices.insert(
            key,
            Device {
                cfg,
                addr,
                status: DeviceStatus::Disconnected,
            },
        );
    }

    /// True if the key belongs to the running actor of the device.
    fn is_current(&self, key: &Arc<str>) -> bool {
        self.devices
            .get_key_value(&**key)
            .is_some_and(|(k, _)| Arc::ptr_eq(k, key))
    }

    /// Stop the actor of a device and forget its entities.
    pub(super) fn remove_device(&mut self, key: &str) -> Option<DeviceConfig> {
        let device = self.devices.remove(key)?;
        device.addr.do_send(Stop);
        self.entities
            .retain(|id, _| EntityKind::parse(id).is_none_or(|(k, _)| k != key));
        self.voice_sessions.retain(|_, k| &**k != key);
        self.store.remove_cache(key);
        // The removed device might have had an error.
        self.update_device_state();
        Some(device.cfg)
    }

    /// The configuration of all devices, sorted by name.
    pub(super) fn sorted_configs(&self) -> Vec<DeviceConfig> {
        let mut devices: Vec<DeviceConfig> = self.devices.values().map(|d| d.cfg.clone()).collect();
        devices.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        devices
    }

    /// Save the configuration of all devices.
    pub(super) fn save_devices(&self) -> Result<(), ServiceError> {
        self.store.save_devices(&self.sorted_configs())?;
        Ok(())
    }

    /// Save the configuration of all devices without waiting for the disk.
    fn queue_devices(&self) -> Result<(), ServiceError> {
        self.store.queue_devices(&self.sorted_configs())?;
        Ok(())
    }

    pub(super) fn available_entities(&self) -> Vec<AvailableIntgEntity> {
        self.devices
            .values()
            .flat_map(|d| entities::definitions(&d.cfg))
            .collect()
    }

    /// States of the subscribed entities, or of all entities without subscriptions.
    pub(super) fn entity_states(&self, ws_id: &str) -> Vec<EntityChangeRef<'_>> {
        let subscribed = self.sessions.get(ws_id).map(|s| &s.subscribed_entities);
        self.entities
            .iter()
            .filter(|(id, _)| subscribed.is_none_or(|s| s.is_empty() || s.contains(*id)))
            .filter_map(|(id, attrs)| entity_change(id, attrs))
            .collect()
    }

    pub(super) fn subscribe(&mut self, ws_id: &str, entity_ids: Vec<String>) {
        let Some(session) = self.sessions.get_mut(ws_id) else {
            return;
        };
        let mut new = Vec::new();
        for id in entity_ids {
            if session.subscribed_entities.insert(id.clone()) {
                new.push(id);
            }
        }
        // Send the current states, and connect the devices.
        for id in &new {
            if let Some(change) = self.entities.get(id).and_then(|a| entity_change(id, a)) {
                self.send_entity_change(ws_id, &change);
            }
            if let Some((key, _)) = EntityKind::parse(id)
                && let Some(device) = self.devices.get(key)
            {
                device.addr.do_send(Connect);
            }
        }
    }

    pub(super) fn unsubscribe(&mut self, ws_id: &str, entity_ids: &[String]) {
        let Some(session) = self.sessions.get_mut(ws_id) else {
            return;
        };
        for id in entity_ids {
            session.subscribed_entities.remove(id);
        }
        // Disconnect devices without subscribed entities.
        for (key, device) in &self.devices {
            let used = self.sessions.values().any(|s| {
                s.subscribed_entities
                    .iter()
                    .any(|id| EntityKind::parse(id).is_some_and(|(k, _)| k == &**key))
            });
            if !used {
                debug!(
                    "[{}] no subscribed entities, disconnecting",
                    device.cfg.name
                );
                device.addr.do_send(Disconnect);
            }
        }
    }

    pub(super) fn connect_devices(&self) {
        for device in self.devices.values() {
            device.addr.do_send(Connect);
        }
    }

    pub(super) fn disconnect_devices(&self) {
        for device in self.devices.values() {
            device.addr.do_send(Disconnect);
        }
    }

    pub(super) fn entity_command(
        &mut self,
        msg: R2RequestMsg,
        _resp_msg: &'static str,
        ctx: &Context<Self>,
    ) -> ResponseFuture<Result<Option<WsMessage>, ServiceError>> {
        let req_id = msg.req_id;
        let command: EntityCommand = match msg.deserialize() {
            Ok(c) => c,
            Err(e) => return_fut_err!(e.into()),
        };
        let addr = match self.device_for(&command.entity_id) {
            Ok(v) => v,
            Err(e) => return_fut_err!(e),
        };
        let (addr, kind) = addr;
        if kind.entity_type() != command.entity_type {
            return_fut_err!(ServiceError::BadRequest(format!(
                "Invalid entity type for {}",
                command.entity_id
            )));
        }
        let voice_session = self.voice_session(&command);
        let controller = ctx.address();
        Box::pin(async move {
            let result = addr
                .send(Command {
                    kind,
                    cmd_id: command.cmd_id,
                    params: command.params,
                })
                .await
                .unwrap_or_else(|e| Err(e.into()));
            if let Err(e) = result {
                if let Some(session_id) = voice_session {
                    controller.do_send(EndVoiceSession(session_id));
                }
                return Err(e);
            }
            Ok(Some(WsMessage::response(
                req_id,
                "result",
                WsResultMsgData::new("OK", "Command sent"),
            )))
        })
    }

    pub(super) fn browse_media(
        &self,
        msg: R2RequestMsg,
        resp_msg: &'static str,
    ) -> ResponseFuture<Result<Option<WsMessage>, ServiceError>> {
        let req_id = msg.req_id;
        let data: BrowseMediaMsgData = match msg.deserialize() {
            Ok(d) => d,
            Err(e) => return_fut_err!(e.into()),
        };
        let (addr, kind) = match self.device_for(&data.entity_id) {
            Ok(v) => v,
            Err(e) => return_fut_err!(e),
        };
        if kind != EntityKind::MediaPlayer {
            return_fut_err!(ServiceError::BadRequest(
                "Only media players can be browsed".into()
            ));
        }
        Box::pin(async move {
            let result = addr
                .send(Browse {
                    media_id: data.media_id,
                    paging: data.paging.unwrap_or_default(),
                })
                .await??;
            Ok(Some(WsMessage::response(req_id, resp_msg, result)))
        })
    }

    fn device_for(
        &self,
        entity_id: &str,
    ) -> Result<(Addr<AppleTvActor>, EntityKind), ServiceError> {
        let (key, kind) = EntityKind::parse(entity_id)
            .ok_or_else(|| ServiceError::NotFound(format!("Unknown entity: {entity_id}")))?;
        let device = self
            .devices
            .get(key)
            .ok_or_else(|| ServiceError::NotFound(format!("Unknown entity: {entity_id}")))?;
        Ok((device.addr.clone(), kind))
    }

    fn send_entity_change(&self, ws_id: &str, change: &EntityChangeRef<'_>) {
        match serde_json::to_value(change) {
            Ok(data) => self.send_r2_msg(
                WsMessage::event("entity_change", EventCategory::Entity, data),
                ws_id,
            ),
            Err(e) => warn!("entity_change serialize error: {e}"),
        }
    }

    /// Send an event to all Remotes.
    fn broadcast(&self, msg_name: &str, category: EventCategory, data: &Value) {
        for ws_id in self.sessions.keys() {
            self.send_r2_msg(WsMessage::event(msg_name, category, data.clone()), ws_id);
        }
    }

    /// Store changed attributes and send them to the subscribed Remotes.
    fn apply_changes(&mut self, key: &str, updates: Vec<(EntityKind, Map<String, Value>)>) {
        // Late event of a removed device.
        if !self.devices.contains_key(key) {
            return;
        }
        let mut entity_id = String::new();
        for (kind, attrs) in updates {
            entity_id.clear();
            kind.write_entity_id(key, &mut entity_id);
            let subscribed = self
                .sessions
                .values()
                .any(|s| s.subscribed_entities.contains(&entity_id));
            let changed = if let Some(cache) = self.entities.get_mut(&entity_id) {
                diff(cache, attrs, subscribed)
            } else {
                let changed = if subscribed {
                    attrs.clone()
                } else {
                    Map::new()
                };
                self.entities.insert(entity_id.clone(), attrs);
                changed
            };
            if !changed.is_empty() {
                self.send_changes(kind, &entity_id, changed);
            }
        }
    }

    /// Send `entity_change` to the Remotes that subscribed the entity.
    fn send_changes(&self, kind: EntityKind, entity_id: &str, attributes: Map<String, Value>) {
        let mut targets = self
            .sessions
            .iter()
            .filter(|(_, s)| s.subscribed_entities.contains(entity_id))
            .map(|(ws_id, _)| &**ws_id);
        let Some(mut target) = targets.next() else {
            return;
        };
        let mut data = Map::new();
        data.insert("entity_type".into(), json!(kind.entity_type()));
        data.insert("entity_id".into(), entity_id.into());
        data.insert("attributes".into(), Value::Object(attributes));
        let data = Value::Object(data);
        // The data is only copied for more than one Remote.
        for next in targets {
            self.send_r2_msg(
                WsMessage::event("entity_change", EventCategory::Entity, data.clone()),
                target,
            );
            target = next;
        }
        self.send_r2_msg(
            WsMessage::event("entity_change", EventCategory::Entity, data),
            target,
        );
    }

    /// Driver state: error while one device has invalid credentials.
    fn update_device_state(&mut self) {
        let state = if self
            .devices
            .values()
            .any(|d| d.status == DeviceStatus::AuthError)
        {
            DeviceState::Error
        } else {
            DeviceState::Connected
        };
        if state != self.device_state {
            self.device_state = state;
            for ws_id in self.sessions.keys() {
                self.send_device_state(ws_id);
            }
        }
    }
}

/// Merge new attributes into the cache. Returns the attributes that changed if `collect` is set.
fn diff(
    cache: &mut Map<String, Value>,
    attrs: Map<String, Value>,
    collect: bool,
) -> Map<String, Value> {
    let mut changed = Map::new();
    let mut updated_at = None;
    let mut new_position = false;
    for (name, value) in attrs {
        if name == POSITION_UPDATED_AT {
            updated_at = Some(value);
            continue;
        }
        if cache.get(&name) != Some(&value) {
            new_position |= name == "media_position" || name == "media_duration";
            if collect {
                cache.insert(name.clone(), value.clone());
                changed.insert(name, value);
            } else {
                cache.insert(name, value);
            }
        }
    }
    // The timestamp is only sent with a new position.
    if let Some(ts) = updated_at
        && new_position
    {
        if collect {
            changed.insert(POSITION_UPDATED_AT.into(), ts.clone());
        }
        cache.insert(POSITION_UPDATED_AT.into(), ts);
    }
    changed
}

fn entity_change<'a>(
    entity_id: &'a str,
    attributes: &'a Map<String, Value>,
) -> Option<EntityChangeRef<'a>> {
    let (_, kind) = EntityKind::parse(entity_id)?;
    Some(EntityChangeRef {
        entity_type: kind.entity_type(),
        entity_id,
        attributes,
    })
}

impl Handler<DeviceEvent> for Controller {
    type Result = ();

    fn handle(&mut self, msg: DeviceEvent, _: &mut Self::Context) {
        if let DeviceEvent::Changes { key, .. }
        | DeviceEvent::Config { key, .. }
        | DeviceEvent::Status { key, .. } = &msg
            && !self.is_current(key)
        {
            // From a removed or replaced actor.
            return;
        }
        match msg {
            DeviceEvent::Changes { key, changes } => self.apply_changes(&key, changes),
            DeviceEvent::Config { key, cfg } => {
                let Some(device) = self.devices.get_mut(&*key) else {
                    return;
                };
                // Only take the values the device finds on the network. The event can be older
                // than the credentials and settings of the controller, for example after pairing
                // again.
                let own = &mut device.cfg;
                let address = own.address != cfg.address;
                if own.name == cfg.name && own.known_ids == cfg.known_ids && !address {
                    return;
                }
                info!("[{}] saving changed configuration", cfg.name);
                own.name = cfg.name;
                own.known_ids = cfg.known_ids;
                if address {
                    own.address = cfg.address;
                }
                // The device actors share this thread: do not wait for the disk.
                if let Err(e) = self.queue_devices() {
                    warn!("Saving the configuration failed: {e}");
                }
            }
            DeviceEvent::Status { key, status } => {
                if let Some(device) = self.devices.get_mut(&*key) {
                    device.status = status;
                    self.update_device_state();
                }
            }
            DeviceEvent::Assistant(event) => {
                if let AssistantEvent::Finished { session_id, .. }
                | AssistantEvent::Error { session_id, .. } = &event
                {
                    self.voice_sessions.remove(session_id);
                }
                match serde_json::to_value(event) {
                    Ok(data) => self.broadcast("assistant_event", EventCategory::Entity, &data),
                    Err(e) => warn!("assistant_event serialize error: {e}"),
                }
            }
        }
    }
}

impl Handler<R2AudioChunkMsg> for Controller {
    type Result = Result<(), ServiceError>;

    fn handle(&mut self, msg: R2AudioChunkMsg, _: &mut Self::Context) -> Self::Result {
        let key = self.voice_sessions.get(&msg.session_id).ok_or_else(|| {
            ServiceError::NotFound(format!("Unknown voice session: {}", msg.session_id))
        })?;
        let device = self.devices.get(key).ok_or_else(|| {
            ServiceError::NotFound(format!("Unknown voice session: {}", msg.session_id))
        })?;
        device.addr.do_send(AudioChunk {
            session_id: msg.session_id,
            data: msg.data,
        });
        Ok(())
    }
}

impl Handler<EndVoiceSession> for Controller {
    type Result = ();

    fn handle(&mut self, msg: EndVoiceSession, _: &mut Self::Context) {
        self.voice_sessions.remove(&msg.0);
    }
}

impl Controller {
    /// Remember the device of a voice session, for the audio chunks. Returns the session id.
    pub(super) fn voice_session(&mut self, command: &EntityCommand) -> Option<u32> {
        if command.entity_type != EntityType::VoiceAssistant {
            return None;
        }
        let session_id = command
            .params
            .as_ref()
            .and_then(|p| p.get("session_id"))
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok());
        if let (Some(session_id), Some((key, _))) =
            (session_id, EntityKind::parse(&command.entity_id))
            && let Some((key, _)) = self.devices.get_key_value(key)
        {
            self.voice_sessions.insert(session_id, key.clone());
            return Some(session_id);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(v: Value) -> Map<String, Value> {
        v.as_object().cloned().expect("object")
    }

    #[test]
    fn diff_keeps_only_changes() {
        let mut cache = map(json!({ "state": "ON", "media_position": 1 }));
        let changed = diff(
            &mut cache,
            map(json!({ "state": "ON", "media_title": "Song" })),
            true,
        );
        assert_eq!(map(json!({ "media_title": "Song" })), changed);
        assert_eq!(Some(&json!("Song")), cache.get("media_title"));
    }

    #[test]
    fn position_timestamp_only_with_new_position() {
        let mut cache = map(json!({ "media_position": 1 }));
        let changed = diff(
            &mut cache,
            map(json!({ "media_position": 1, POSITION_UPDATED_AT: "t1" })),
            true,
        );
        assert!(changed.is_empty());
        let changed = diff(
            &mut cache,
            map(json!({ "media_position": 2, POSITION_UPDATED_AT: "t2" })),
            true,
        );
        assert_eq!(
            map(json!({ "media_position": 2, POSITION_UPDATED_AT: "t2" })),
            changed
        );
    }

    #[test]
    fn diff_without_collect_updates_cache_only() {
        let mut cache = map(json!({ "media_position": 1 }));
        let changed = diff(
            &mut cache,
            map(json!({ "media_position": 2, POSITION_UPDATED_AT: "t2" })),
            false,
        );
        assert!(changed.is_empty());
        assert_eq!(
            map(json!({ "media_position": 2, POSITION_UPDATED_AT: "t2" })),
            cache
        );
    }
}
