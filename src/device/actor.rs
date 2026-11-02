// Copyright (c) 2026 Henrik Widlund
// SPDX-License-Identifier: MPL-2.0

//! Actor for one Apple TV: connection, state updates and commands.

use crate::config_store::{DeviceConfig, fnv1a};
use crate::device::commands::{Call, Planner, Seek, exec};
use crate::device::state::{ARTWORK_SIZE, AccountInfo, AppInfo, Dirty, ItemArtwork, Snapshot};
use crate::device::{
    AudioChunk, Browse, Command, Connect, DeviceContext, DeviceEvent, DeviceStatus, Disconnect,
    Stop, UpdateConfig, browse, discovery, now,
};
use crate::entities::{self, Capabilities, EntityKind, media_player, remote, select};
use crate::errors::ServiceError;
use actix::prelude::*;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use log::{Level, debug, info, log_enabled, warn};
use ratv::companion::{AttentionState, Event};
use ratv::mrp::{NowPlaying, PlaybackState, Volume};
use ratv::{AppleTv, ClientOptions, Endpoints};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::BroadcastStream;
use uc_api::intg::{AssistantError, AssistantErrorCode, AssistantEvent};

/// Interval for the app, account and AirPlay polls.
const POLL_INTERVAL: Duration = Duration::from_secs(300);
/// Interval for a full reconnect while a protocol is missing.
const FULL_RECONNECT_INTERVAL: Duration = Duration::from_secs(600);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Siri ends if no audio arrives in this time.
const SIRI_AUDIO_TIMEOUT: Duration = Duration::from_secs(10);
/// Audio chunks kept while Siri starts.
const SIRI_QUEUE: usize = 256;
/// Screen events come in bursts. Act after the last one.
const SCREEN_SETTLE: Duration = Duration::from_millis(1500);
/// Now playing changes right after the Home Screen shows belong to the switch.
const HOME_GRACE: Duration = Duration::from_secs(1);
/// Screen changes in this time after an app launch come from the launch.
const LAUNCH_GRACE: Duration = Duration::from_secs(5);

/// Lists that are kept between restarts.
#[derive(Debug, Default, Serialize, Deserialize)]
struct DeviceCache {
    apps: Vec<AppInfo>,
    airplay_devices: Vec<(String, String)>,
}

/// A watch channel has a new value.
#[derive(Debug, Clone, Copy, Message)]
#[rtype(result = "()")]
enum WatchChanged {
    NowPlaying(u64),
    Volume(u64),
}

/// Companion event, or `None` if events were lost.
#[derive(Debug, Message)]
#[rtype(result = "()")]
struct CompanionEvent {
    generation: u64,
    event: Option<Event>,
}

struct SiriSession {
    session_id: u32,
    audio: mpsc::Sender<Bytes>,
}

#[derive(Default)]
struct Timers {
    reconnect: Option<SpawnHandle>,
    poll: Option<SpawnHandle>,
    chapter: Option<SpawnHandle>,
    full_reconnect: Option<SpawnHandle>,
    screen: Option<SpawnHandle>,
}

pub struct AppleTvActor {
    cfg: DeviceConfig,
    /// Incremented on each configuration change.
    cfg_version: u64,
    key: Arc<str>,
    ctx: Arc<DeviceContext>,
    controller: Recipient<DeviceEvent>,
    tv: Option<AppleTv>,
    s: Snapshot,
    caps: Capabilities,
    status: DeviceStatus,
    /// The controller wants a connection.
    want: bool,
    attempts: u32,
    /// Incremented on each connect and disconnect. Results of older connections are ignored.
    generation: u64,
    np: Option<watch::Receiver<NowPlaying>>,
    volume: Option<watch::Receiver<Volume>>,
    attention: Option<AttentionState>,
    seek: Option<Seek>,
    siri: Option<SiriSession>,
    /// Commands run one after the other.
    cmd_lock: Arc<tokio::sync::Mutex<()>>,
    timers: Timers,
    apps_fetched: Option<Instant>,
    /// Incremented on each artwork update. Results of older fetches are ignored.
    artwork_seq: u64,
    /// The Home Screen shows: now playing is cleared since then.
    home_since: Option<Instant>,
    /// Time of the last app launch through the driver.
    launched_at: Option<Instant>,
    /// Last media control flags from Companion. 0 when no app has media controls.
    media_flags: Option<u32>,
    /// Name of the shown artwork file.
    shown_file: Option<Box<str>>,
    item_art: ItemArtwork,
}

impl AppleTvActor {
    pub fn new(
        cfg: DeviceConfig,
        ctx: Arc<DeviceContext>,
        controller: Recipient<DeviceEvent>,
    ) -> Self {
        let cache: DeviceCache = ctx.store.load_cache(&cfg.key).unwrap_or_default();
        let mut s = Snapshot {
            name: cfg.name.clone(),
            global_volume: cfg.global_volume,
            ..Snapshot::default()
        };
        s.set_apps(cache.apps);
        s.set_airplay_devices(cache.airplay_devices);
        Self {
            key: cfg.key.as_str().into(),
            cfg,
            cfg_version: 0,
            ctx,
            controller,
            tv: None,
            s,
            caps: Capabilities::default(),
            status: DeviceStatus::Disconnected,
            want: false,
            attempts: 0,
            generation: 0,
            np: None,
            volume: None,
            attention: None,
            seek: None,
            siri: None,
            artwork_seq: 0,
            home_since: None,
            launched_at: None,
            media_flags: None,
            shown_file: None,
            item_art: ItemArtwork::default(),
            cmd_lock: Arc::default(),
            timers: Timers::default(),
            apps_fetched: None,
        }
    }

    /// Send the attributes of the dirty groups to the controller.
    fn emit(&self, dirty: Dirty) {
        if dirty.is_empty() {
            return;
        }
        let now = now();
        let changes: Vec<_> = EntityKind::all()
            .filter_map(|kind| {
                let mut attrs = Map::new();
                entities::attributes(&self.s, kind, dirty, now, &mut attrs);
                (!attrs.is_empty()).then_some((kind, attrs))
            })
            .collect();
        if !changes.is_empty() {
            self.controller.do_send(DeviceEvent::Changes {
                key: self.key.clone(),
                changes,
            });
        }
    }

    fn set_status(&mut self, status: DeviceStatus) {
        if self.status != status {
            self.status = status;
            self.controller.do_send(DeviceEvent::Status {
                key: self.key.clone(),
                status,
            });
        }
    }

    fn connect(&mut self, ctx: &mut Context<Self>) {
        if !self.want
            || matches!(
                self.status,
                DeviceStatus::Connecting | DeviceStatus::Connected
            )
        {
            return;
        }
        self.set_status(DeviceStatus::Connecting);
        let mut cfg = self.cfg.clone();
        let version = self.cfg_version;
        let mut options = ClientOptions::default();
        options.name.clone_from(&self.ctx.client_name);

        let fut = async move {
            let device = match discovery::find(&cfg).await {
                Ok(Some(d)) => d,
                Ok(None) => return (None, Err(None)),
                Err(e) => return (None, Err(Some(e))),
            };
            let changed = discovery::update_config(&mut cfg, &device);
            let result =
                AppleTv::connect(&Endpoints::from(&device), &cfg.credentials, &options).await;
            (changed.then_some(cfg), result.map_err(Some))
        };
        ctx.spawn(fut.into_actor(self).map(move |(cfg, result), act, ctx| {
            if let Some(cfg) = cfg
                && act.cfg_version == version
            {
                act.cfg = cfg.clone();
                act.controller.do_send(DeviceEvent::Config(cfg));
            }
            match result {
                Ok(tv) if act.want && act.status == DeviceStatus::Connecting => {
                    act.on_connected(tv, ctx);
                }
                Ok(tv) => {
                    actix::spawn(async move {
                        let _ = tv.close().await;
                    });
                }
                Err(Some(e)) if e.is_auth() => {
                    warn!(
                        "[{}] pairing is not valid anymore, pair again: {e}",
                        act.cfg.name
                    );
                    act.lost_connection(ctx);
                    act.set_status(DeviceStatus::AuthError);
                }
                Err(e) => {
                    match e {
                        Some(e) => warn!("[{}] connect failed: {e}", act.cfg.name),
                        None => info!("[{}] not found on the network", act.cfg.name),
                    }
                    act.lost_connection(ctx);
                    act.schedule_reconnect(ctx);
                }
            }
        }));
    }

    fn on_connected(&mut self, tv: AppleTv, ctx: &mut Context<Self>) {
        self.generation += 1;
        let generation = self.generation;
        self.attempts = 0;
        info!("[{}] connected", self.cfg.name);

        let missing = match (tv.companion().is_some(), tv.mrp().is_some()) {
            (true, true) => None,
            (false, _) => {
                Some("Companion: power state, apps, accounts and swipes are not available")
            }
            (_, false) => Some("MRP: now playing, volume, outputs and Siri are not available"),
        };
        if let Some(missing) = missing {
            warn!("[{}] not connected: {missing}", self.cfg.name);
            // `closed()` fires only for connected protocols, so the missing one needs a full reconnect.
            self.timers.full_reconnect =
                Some(ctx.run_interval(FULL_RECONNECT_INTERVAL, Self::full_reconnect));
        }

        let mut dirty = Dirty::CONNECTION;
        self.s.connected = true;
        self.s.mrp = tv.mrp().is_some();
        let own_uid = tv.mrp().and_then(|m| m.device_info().device_uid.clone());
        if self.s.own_output_uid != own_uid {
            self.s.own_output_uid = own_uid;
            dirty |= Dirty::OUTPUTS;
        }

        if let Some(rx) = tv.now_playing() {
            ctx.add_message_stream(changes(rx.clone(), WatchChanged::NowPlaying(generation)));
            self.np = Some(rx);
        }
        if let Some(rx) = tv.volume() {
            ctx.add_message_stream(changes(rx.clone(), WatchChanged::Volume(generation)));
            self.volume = Some(rx);
        }
        if let Some(companion) = tv.companion() {
            let events = BroadcastStream::new(companion.events()).map(move |e| CompanionEvent {
                generation,
                event: e.ok(),
            });
            ctx.add_message_stream(events);
        }

        let closed = tv.clone();
        ctx.spawn(async move { closed.closed().await }.into_actor(self).map(
            move |reason, act, ctx| {
                if act.generation == generation {
                    match reason {
                        Some(e) => warn!("[{}] connection closed: {e}", act.cfg.name),
                        None => info!("[{}] connection closed", act.cfg.name),
                    }
                    act.lost_connection(ctx);
                    act.schedule_reconnect(ctx);
                }
            },
        ));

        self.tv = Some(tv);
        self.set_status(DeviceStatus::Connected);
        self.query_power(ctx);
        dirty |= self.update_now_playing(ctx) | self.update_volume();
        self.emit(dirty);

        self.poll(ctx);
        self.timers.poll = Some(ctx.run_interval(POLL_INTERVAL, Self::poll));
    }

    /// Drop the connection and mark the entities unavailable.
    fn lost_connection(&mut self, ctx: &mut Context<Self>) {
        self.close(ctx);
        self.set_status(DeviceStatus::Disconnected);
        if self.s.connected {
            let dirty = self.s.disconnected();
            self.emit(dirty);
        }
    }

    /// Close the connection and stop all tasks of it.
    fn close(&mut self, ctx: &mut Context<Self>) {
        self.generation += 1;
        self.np = None;
        self.volume = None;
        self.attention = None;
        self.seek = None;
        self.siri = None;
        self.home_since = None;
        self.media_flags = None;
        for handle in [
            self.timers.poll.take(),
            self.timers.chapter.take(),
            self.timers.full_reconnect.take(),
            self.timers.screen.take(),
        ]
        .into_iter()
        .flatten()
        {
            ctx.cancel_future(handle);
        }
        if let Some(tv) = self.tv.take() {
            actix::spawn(async move {
                let _ = tv.close().await;
            });
        }
    }

    fn cancel_timers(&mut self, ctx: &mut Context<Self>) {
        let t = std::mem::take(&mut self.timers);
        for handle in [t.reconnect, t.poll, t.chapter, t.full_reconnect, t.screen]
            .into_iter()
            .flatten()
        {
            ctx.cancel_future(handle);
        }
    }

    fn schedule_reconnect(&mut self, ctx: &mut Context<Self>) {
        if !self.want {
            return;
        }
        self.cancel_timers(ctx);
        self.attempts = self.attempts.saturating_add(1);
        let delay = Duration::from_secs(u64::from(self.attempts) * 2).min(MAX_BACKOFF);
        debug!("[{}] reconnect in {}s", self.cfg.name, delay.as_secs());
        self.timers.reconnect = Some(ctx.run_later(delay, |act, ctx| {
            act.timers.reconnect = None;
            act.connect(ctx);
        }));
    }

    /// Reconnect all protocols. Skipped while Siri or a fast forward or rewind is active.
    fn full_reconnect(&mut self, ctx: &mut Context<Self>) {
        if self.siri.is_some() || self.seek.is_some() {
            debug!("[{}] full reconnect skipped: device is busy", self.cfg.name);
            return;
        }
        info!("[{}] reconnecting to get all protocols", self.cfg.name);
        // Keep the entity states: they become unavailable only if the connect fails.
        self.close(ctx);
        self.status = DeviceStatus::Disconnected;
        self.connect(ctx);
    }

    fn query_power(&self, ctx: &mut Context<Self>) {
        let Some(tv) = self.tv.clone() else { return };
        if tv.companion().is_none() {
            return;
        }
        let generation = self.generation;
        ctx.spawn(
            async move {
                match tv.companion() {
                    Some(c) => c.power_state().await,
                    None => Err(ratv::Error::NotConnected("Companion")),
                }
            }
            .into_actor(self)
            .map(move |result, act, _| match result {
                Ok(state) if act.generation == generation => act.set_attention(state),
                Ok(_) => {}
                Err(e) => warn!("[{}] power state failed: {e}", act.cfg.name),
            }),
        );
    }

    fn set_attention(&mut self, state: AttentionState) {
        self.attention = Some(state);
        let dirty = self.s.set_power(state.into());
        self.emit(dirty);
    }

    fn update_now_playing(&mut self, ctx: &mut Context<Self>) -> Dirty {
        if self.home_since.is_some_and(|t| t.elapsed() < HOME_GRACE) {
            return Dirty::NONE;
        }
        let Some(rx) = &self.np else {
            return Dirty::NONE;
        };
        if self.home_since.take().is_some() {
            debug!(
                "[{}] now playing again after the Home Screen",
                self.cfg.name
            );
        }
        let dirty = self.s.update_now_playing(&rx.borrow());
        self.apply_now_playing(dirty, ctx)
    }

    fn apply_now_playing(&mut self, dirty: Dirty, ctx: &mut Context<Self>) -> Dirty {
        if dirty.any(Dirty::LANGUAGES) && log_enabled!(Level::Debug) {
            self.log_languages();
        }
        if dirty.any(Dirty::METADATA | Dirty::ARTWORK) {
            debug!(
                "[{}] item: {:?} / {:?} / {:?}, artwork id {:?}",
                self.cfg.name, self.s.title, self.s.artist, self.s.album, self.s.artwork_id
            );
        }
        if dirty.any(Dirty::ARTWORK) {
            self.update_artwork(ctx);
        }
        if dirty.any(Dirty::CHAPTERS | Dirty::POSITION | Dirty::PLAYBACK) {
            self.schedule_chapter(ctx);
        }
        dirty
    }

    /// The screen changed. If nothing plays and no app has media controls, the Home Screen
    /// (or an app without media) shows: clear now playing.
    fn screen_changed(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.timers.screen.take() {
            ctx.cancel_future(handle);
        }
        let generation = self.generation;
        self.timers.screen = Some(ctx.run_later(SCREEN_SETTLE, move |act, ctx| {
            act.timers.screen = None;
            // Another screen opened after the launch: the launched app may not be in front.
            if act.launched_at.is_some_and(|t| t.elapsed() > LAUNCH_GRACE)
                && act.s.launched_app.take().is_some()
            {
                act.launched_at = None;
                act.emit(Dirty::APPS);
            }
            if act.generation != generation
                || act.home_since.is_some()
                || act.s.state == Some(PlaybackState::Playing)
                || act.media_flags.is_some_and(|f| f != 0)
            {
                return;
            }
            debug!("[{}] Home Screen: clearing now playing", act.cfg.name);
            act.home_since = Some(Instant::now());
            let dirty = act.s.update_now_playing(&NowPlaying::default());
            let dirty = act.apply_now_playing(dirty, ctx);
            act.emit(dirty);
        }));
    }

    /// An app has media controls again: show now playing.
    fn leave_home(&mut self, ctx: &mut Context<Self>) {
        if self.home_since.is_some() {
            let dirty = self.update_now_playing(ctx);
            self.emit(dirty);
        }
    }

    fn update_volume(&mut self) -> Dirty {
        let Some(rx) = &self.volume else {
            return Dirty::NONE;
        };
        let dirty = self.s.update_volume(&rx.borrow());
        if dirty.any(Dirty::OUTPUTS) && log_enabled!(Level::Debug) {
            let outputs: Vec<(&str, &str)> = self
                .s
                .outputs
                .iter()
                .map(|o| (o.uid.as_str(), o.name.as_str()))
                .collect();
            debug!(
                "[{}] outputs: {outputs:?}, own uid: {:?}",
                self.cfg.name, self.s.own_output_uid
            );
        }
        // `None` means unknown: keep the features.
        if let Some(available) = self.s.volume_available
            && available != self.caps.volume
        {
            info!("[{}] volume control available: {available}", self.cfg.name);
            self.caps.volume = available;
            self.controller.do_send(DeviceEvent::Capabilities {
                key: self.key.clone(),
                caps: self.caps,
            });
        }
        dirty
    }

    /// Current and available languages, to compare the ids.
    fn log_languages(&self) {
        let ids = |list: &[ratv::mrp::LanguageOption]| -> Vec<(Option<String>, Option<String>)> {
            list.iter()
                .map(|o| (o.identifier.clone(), o.language_tag.clone()))
                .collect()
        };
        let id = |o: Option<&ratv::mrp::LanguageOption>| {
            o.map(|o| (o.identifier.clone(), o.language_tag.clone()))
        };
        debug!(
            "[{}] audio: {:?} of {:?}, subtitles: {:?} of {:?}, auto: {}",
            self.cfg.name,
            id(self.s.audio_language.as_ref()),
            ids(&self.s.audio_languages),
            id(self.s.subtitle_language.as_ref()),
            ids(&self.s.subtitle_languages),
            self.s.subtitles_auto
        );
    }

    /// Use the artwork URL, or fetch the image and serve it from a file.
    ///
    /// The image is also fetched without an artwork id: some apps send none, but the Apple TV
    /// has the image.
    fn update_artwork(&mut self, ctx: &mut Context<Self>) {
        self.artwork_seq += 1;
        if !self.s.artwork_fetch {
            if let Some(url) = &self.s.artwork_url {
                debug!("[{}] artwork url: {url}", self.cfg.name);
            }
            self.shown_file = None;
            return;
        }
        let Some(tv) = self.tv.clone().filter(|_| self.s.state.is_some()) else {
            debug!("[{}] no artwork: nothing plays", self.cfg.name);
            return;
        };
        let id = self.s.artwork_id.clone();
        debug!("[{}] fetching artwork {id:?}", self.cfg.name);
        let generation = self.generation;
        let seq = self.artwork_seq;
        let item = self.s.item_seq;
        let item_key = self.s.item_key();
        // The Apple TV scales the image.
        ctx.spawn(
            async move { tv.artwork(f64::from(ARTWORK_SIZE), -1.0).await }
                .into_actor(self)
                .map(move |result, act, ctx| {
                    if act.generation != generation || act.artwork_seq != seq {
                        return;
                    }
                    match result {
                        Ok(Some(art)) => {
                            // Without an artwork id, the image data gives a stable file name.
                            let id = id.unwrap_or_else(|| format!("{:016x}", fnv1a(&art.data)));
                            act.save_artwork(id, art, seq, (item, item_key), ctx);
                        }
                        // Some apps drop the image of the same item. Keep it until the item changes.
                        Ok(None) if act.s.artwork_item == item => {
                            debug!(
                                "[{}] no artwork, keeping the image of this item",
                                act.cfg.name
                            );
                        }
                        Ok(None) if act.s.keep_artwork(item) => {
                            debug!(
                                "[{}] no artwork, keeping the image of the short item before ({:?})",
                                act.cfg.name, act.s.prev_item_lasted
                            );
                            act.s.artwork_item = item;
                            act.s.artwork_kept = true;
                            if let Some(name) = act.shown_file.clone() {
                                act.item_art.remember(item_key, name);
                            }
                        }
                        Ok(None) => {
                            if let Some(name) = act.cached_art(item_key) {
                                debug!("[{}] no artwork, using the image seen before for this item", act.cfg.name);
                                act.show_file(name, item);
                                return;
                            }
                            debug!(
                                "[{}] no artwork for this item, item before lasted {:?}",
                                act.cfg.name, act.s.prev_item_lasted
                            );
                            act.shown_file = None;
                            if act.s.artwork_url.take().is_some() {
                                act.emit(Dirty::ARTWORK);
                            }
                        }
                        Err(e) => warn!("[{}] artwork failed: {e}", act.cfg.name),
                    }
                }),
        );
    }

    /// Save the image. `item` is the item sequence and the item key at fetch time.
    fn save_artwork(
        &self,
        id: String,
        art: ratv::mrp::Artwork,
        seq: u64,
        (item, item_key): (u32, u64),
        ctx: &mut Context<Self>,
    ) {
        let store = self.ctx.artwork.clone();
        let key = self.key.clone();
        ctx.spawn(
            async move {
                store
                    .save(&key, &id, art.mime_type.as_deref(), &art.data)
                    .await
            }
            .into_actor(self)
            .map(move |result, act, _| {
                let name: Box<str> = match result {
                    Ok(name) => name.into(),
                    Err(e) => {
                        warn!("[{}] artwork save failed: {e}", act.cfg.name);
                        return;
                    }
                };
                act.item_art.remember(item_key, name.clone());
                if act.artwork_seq == seq {
                    act.show_file(name, item);
                }
            }),
        );
    }

    /// Show a kept artwork file.
    fn show_file(&mut self, name: Box<str>, item: u32) {
        let Some(base) = self.http_base() else {
            warn!("[{}] no local address for the artwork URL", self.cfg.name);
            return;
        };
        let url = format!("{base}/artwork/{}/{name}", self.key);
        debug!("[{}] artwork file: {url}", self.cfg.name);
        self.s.artwork_url = Some(url);
        self.s.artwork_item = item;
        self.s.artwork_kept = false;
        self.shown_file = Some(name);
        self.emit(Dirty::ARTWORK);
    }

    /// The file seen before for the item, if it is still kept.
    fn cached_art(&self, item_key: u64) -> Option<Box<str>> {
        let name = self.item_art.get(item_key)?;
        self.ctx.artwork.touch(&self.key, name).then(|| name.into())
    }

    /// Base URL of the HTTP server, with an address on the network of the Apple TV.
    fn http_base(&self) -> Option<String> {
        let ip = self.ctx.http_address.or_else(|| {
            // No packet is sent: connect only selects the local address.
            let target = SocketAddr::new(self.cfg.address?, 9);
            let socket = UdpSocket::bind((IpAddr::from([0, 0, 0, 0]), 0)).ok()?;
            socket.connect(target).ok()?;
            Some(socket.local_addr().ok()?.ip())
        })?;
        Some(format!(
            "http://{}",
            SocketAddr::new(ip, self.ctx.http_port)
        ))
    }

    /// Update the chapter select when the next chapter starts.
    fn schedule_chapter(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.timers.chapter.take() {
            ctx.cancel_future(handle);
        }
        let rate = f64::from(self.s.rate.unwrap_or(0.0));
        if rate <= 0.0 || self.s.chapters.is_empty() {
            return;
        }
        let Some(pos) = self.s.position(now()) else {
            return;
        };
        let Some(next) = self
            .s
            .chapters
            .iter()
            .filter_map(|c| c.start)
            .find(|&start| start > pos + 0.5)
        else {
            return;
        };
        let delay = Duration::from_secs_f64(((next - pos) / rate).max(0.5));
        self.timers.chapter = Some(ctx.run_later(delay, |act, ctx| {
            act.timers.chapter = None;
            act.emit(Dirty::CHAPTERS);
            act.schedule_chapter(ctx);
        }));
    }

    /// Fetch the apps, accounts and AirPlay devices.
    fn poll(&mut self, ctx: &mut Context<Self>) {
        let Some(tv) = self.tv.clone() else { return };
        let generation = self.generation;

        if tv.companion().is_some() {
            if self
                .apps_fetched
                .is_none_or(|t| t.elapsed() >= POLL_INTERVAL)
            {
                let apps_tv = tv.clone();
                ctx.spawn(async move { apps_tv.apps().await }.into_actor(self).map(
                    move |result, act, _| match result {
                        Ok(apps) if act.generation == generation => {
                            act.apps_fetched = Some(Instant::now());
                            let apps = apps
                                .into_iter()
                                .map(|a| AppInfo {
                                    bundle_id: a.bundle_id,
                                    name: a.name,
                                })
                                .collect();
                            let dirty = act.s.set_apps(apps);
                            if !dirty.is_empty() {
                                act.save_cache();
                                act.emit(dirty);
                            }
                        }
                        Ok(_) => {}
                        Err(e) => warn!("[{}] app list failed: {e}", act.cfg.name),
                    },
                ));
            }
            let accounts_tv = tv.clone();
            ctx.spawn(
                async move { accounts_tv.accounts().await }
                    .into_actor(self)
                    .map(move |result, act, _| match result {
                        Ok(accounts) if act.generation == generation => {
                            let accounts = accounts
                                .into_iter()
                                .map(|a| AccountInfo {
                                    id: a.id,
                                    name: a.name,
                                })
                                .collect();
                            let dirty = act.s.set_accounts(accounts);
                            act.emit(dirty);
                        }
                        Ok(_) => {}
                        Err(e) => warn!("[{}] account list failed: {e}", act.cfg.name),
                    }),
            );
        }

        if tv.mrp().is_some() {
            let own_uid = self.s.own_output_uid.clone();
            ctx.spawn(
                async move { discovery::scan_airplay_outputs(own_uid.as_deref()).await }
                    .into_actor(self)
                    .map(move |result, act, _| match result {
                        Ok(devices) if act.generation == generation => {
                            let dirty = act.s.set_airplay_devices(devices);
                            if !dirty.is_empty() {
                                act.save_cache();
                                act.emit(dirty);
                            }
                        }
                        Ok(_) => {}
                        Err(e) => warn!("[{}] AirPlay scan failed: {e}", act.cfg.name),
                    }),
            );
        }
    }

    fn save_cache(&self) {
        #[derive(Serialize)]
        struct CacheRef<'a> {
            apps: &'a [AppInfo],
            airplay_devices: &'a [(String, String)],
        }
        let cache = CacheRef {
            apps: &self.s.apps,
            airplay_devices: &self.s.airplay_devices,
        };
        if let Err(e) = self.ctx.store.save_cache(&self.key, &cache) {
            warn!("[{}] cache save failed: {e}", self.cfg.name);
        }
    }

    /// Turn an entity command into calls.
    fn plan(&mut self, msg: &Command) -> Result<Vec<Call>, ServiceError> {
        let params = msg.params.as_ref();
        let run = match msg.kind {
            EntityKind::MediaPlayer => remote::RemoteRun {
                actions: vec![media_player::command(&msg.cmd_id, params)?],
                repeat: 1,
                delay: Duration::ZERO,
            },
            EntityKind::Remote => remote::command(&msg.cmd_id, params)?,
            EntityKind::Select(kind) => remote::RemoteRun {
                actions: vec![media_player::Action::Select(
                    kind,
                    select::command(&msg.cmd_id, params)?,
                )],
                repeat: 1,
                delay: Duration::ZERO,
            },
            EntityKind::Sensor(_) | EntityKind::VoiceAssistant => {
                return Err(ServiceError::NotSupported(msg.cmd_id.clone()));
            }
        };
        let mut planner = Planner {
            s: &self.s,
            seek: &mut self.seek,
            screensaver: self.attention == Some(AttentionState::Screensaver),
            powered: false,
        };
        let mut calls = Vec::with_capacity(run.actions.len() * run.repeat as usize);
        for i in 0..run.repeat {
            for (j, action) in run.actions.iter().enumerate() {
                if !run.delay.is_zero() && (i > 0 || j > 0) {
                    calls.push(Call::Sleep(run.delay));
                }
                planner.plan(action, &mut calls)?;
            }
        }
        Ok(calls)
    }

    fn voice_start(
        &mut self,
        params: Option<&Map<String, Value>>,
        ctx: &mut Context<Self>,
    ) -> Result<(), ServiceError> {
        let session_id = params
            .and_then(|p| p.get("session_id"))
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| ServiceError::BadRequest("Missing parameter: session_id".into()))?;
        let tv = self.tv.clone().ok_or(ServiceError::NotConnected)?;
        if tv.mrp().is_none() {
            return Err(ServiceError::ServiceUnavailable("Siri needs MRP".into()));
        }
        if let Some(old) = self.siri.take() {
            info!(
                "[{}] Siri session {} replaced by {session_id}",
                self.cfg.name, old.session_id
            );
        }
        let (audio, mut rx) = mpsc::channel::<Bytes>(SIRI_QUEUE);
        self.siri = Some(SiriSession { session_id, audio });

        let entity_id = EntityKind::VoiceAssistant.entity_id(&self.key);
        let controller = self.controller.clone();
        let ready_id = entity_id.clone();
        let fut = async move {
            let mut siri = tv.start_siri().await?;
            controller.do_send(DeviceEvent::Assistant(AssistantEvent::Ready {
                entity_id: ready_id,
                session_id,
            }));
            let mut pcm = Vec::new();
            loop {
                match tokio::time::timeout(SIRI_AUDIO_TIMEOUT, rx.recv()).await {
                    Ok(Some(chunk)) if !chunk.is_empty() => {
                        pcm.clear();
                        pcm.extend(
                            chunk
                                .as_chunks::<2>()
                                .0
                                .iter()
                                .map(|b| i16::from_le_bytes(*b)),
                        );
                        siri.push_pcm(&pcm).await?;
                    }
                    // End of audio, session replaced or timeout.
                    _ => break,
                }
            }
            siri.finish().await
        };
        ctx.spawn(fut.into_actor(self).map(move |result, act, _| {
            if act
                .siri
                .as_ref()
                .is_some_and(|s| s.session_id == session_id)
            {
                act.siri = None;
            }
            let event = match result {
                Ok(()) => AssistantEvent::Finished {
                    entity_id,
                    session_id,
                },
                Err(e) => {
                    warn!("[{}] Siri failed: {e}", act.cfg.name);
                    AssistantEvent::Error {
                        entity_id,
                        session_id,
                        data: AssistantError {
                            code: AssistantErrorCode::UnexpectedError,
                            message: e.to_string(),
                        },
                    }
                }
            };
            act.controller.do_send(DeviceEvent::Assistant(event));
        }));
        Ok(())
    }
}

/// Stream that yields `msg` each time the watch value changes.
fn changes<T: Send + Sync + 'static>(
    rx: watch::Receiver<T>,
    msg: WatchChanged,
) -> impl Stream<Item = WatchChanged> + 'static {
    futures::stream::unfold(rx, move |mut rx| async move {
        rx.changed().await.ok()?;
        Some((msg, rx))
    })
}

impl Actor for AppleTvActor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        self.emit(Dirty::ALL);
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        self.close(ctx);
        self.ctx.artwork.remove(&self.key);
    }
}

impl Handler<WatchChanged> for AppleTvActor {
    type Result = ();

    fn handle(&mut self, msg: WatchChanged, ctx: &mut Self::Context) {
        let dirty = match msg {
            WatchChanged::NowPlaying(g) if g == self.generation => self.update_now_playing(ctx),
            WatchChanged::Volume(g) if g == self.generation => self.update_volume(),
            _ => return,
        };
        self.emit(dirty);
    }
}

impl Handler<CompanionEvent> for AppleTvActor {
    type Result = ();

    fn handle(&mut self, msg: CompanionEvent, ctx: &mut Self::Context) {
        if msg.generation != self.generation {
            return;
        }
        match msg.event {
            Some(Event::SystemStatus(state)) => self.set_attention(state),
            Some(Event::ScreenChanged { .. }) => self.screen_changed(ctx),
            Some(Event::MediaControl(flags)) => {
                self.media_flags = Some(flags.0);
                if flags.0 != 0 {
                    self.leave_home(ctx);
                }
            }
            Some(_) => {}
            // Events were lost: ask again.
            None => self.query_power(ctx),
        }
    }
}

impl Handler<Connect> for AppleTvActor {
    type Result = ();

    fn handle(&mut self, _: Connect, ctx: &mut Self::Context) {
        self.want = true;
        if self.status == DeviceStatus::AuthError {
            self.set_status(DeviceStatus::Disconnected);
        }
        if self.status == DeviceStatus::Disconnected {
            if let Some(handle) = self.timers.reconnect.take() {
                ctx.cancel_future(handle);
            }
            self.attempts = 0;
            self.connect(ctx);
        }
    }
}

impl Handler<Disconnect> for AppleTvActor {
    type Result = ();

    fn handle(&mut self, _: Disconnect, ctx: &mut Self::Context) {
        self.want = false;
        self.cancel_timers(ctx);
        self.lost_connection(ctx);
    }
}

impl Handler<Stop> for AppleTvActor {
    type Result = ();

    fn handle(&mut self, _: Stop, ctx: &mut Self::Context) {
        self.want = false;
        ctx.stop();
    }
}

impl Handler<UpdateConfig> for AppleTvActor {
    type Result = ();

    fn handle(&mut self, msg: UpdateConfig, ctx: &mut Self::Context) {
        let cfg = msg.0;
        let reconnect = cfg.credentials != self.cfg.credentials
            || cfg.address != self.cfg.address
            || cfg.known_ids != self.cfg.known_ids;
        if cfg.global_volume != self.cfg.global_volume {
            self.s.global_volume = cfg.global_volume;
            self.emit(Dirty::VOLUME);
        }
        self.cfg = cfg;
        self.cfg_version += 1;
        if reconnect && self.want {
            info!("[{}] configuration changed, reconnecting", self.cfg.name);
            self.lost_connection(ctx);
            self.attempts = 0;
            self.connect(ctx);
        }
    }
}

impl Handler<Command> for AppleTvActor {
    type Result = ResponseActFuture<Self, Result<(), ServiceError>>;

    fn handle(&mut self, msg: Command, ctx: &mut Self::Context) -> Self::Result {
        if msg.kind == EntityKind::VoiceAssistant {
            let result = match msg.cmd_id.as_str() {
                "voice_start" => self.voice_start(msg.params.as_ref(), ctx),
                _ => Err(ServiceError::NotSupported(msg.cmd_id)),
            };
            return Box::pin(fut::ready(result));
        }
        let Some(tv) = self.tv.clone() else {
            return Box::pin(fut::ready(Err(ServiceError::NotConnected)));
        };
        let calls = match self.plan(&msg) {
            Ok(calls) => calls,
            Err(e) => return Box::pin(fut::ready(Err(e))),
        };
        debug!("[{}] {} -> {calls:?}", self.cfg.name, msg.cmd_id);
        // The current account is not reported by the Apple TV, so it is set after a switch.
        let account = calls.iter().rev().find_map(|c| match c {
            Call::SwitchAccount(_, name) => Some(name.clone()),
            _ => None,
        });
        // The app in front is not reported either.
        let launched = calls.iter().rev().find_map(|c| match c {
            Call::LaunchApp(id) => Some(id.clone()),
            _ => None,
        });
        let lock = self.cmd_lock.clone();
        let fut = async move {
            let _guard = lock.lock().await;
            for call in calls {
                exec(&tv, call).await?;
            }
            Ok(())
        };
        Box::pin(
            fut.into_actor(self)
                .map(move |result: Result<(), ratv::Error>, act, _| {
                    if result.is_ok()
                        && let Some(name) = account
                    {
                        act.s.current_account = Some(name);
                        act.emit(Dirty::ACCOUNTS);
                    }
                    if result.is_ok()
                        && let Some(id) = launched
                    {
                        act.s.launched_app = Some(id);
                        act.launched_at = Some(Instant::now());
                        act.emit(Dirty::APPS);
                    }
                    result.map_err(ServiceError::from)
                }),
        )
    }
}

impl Handler<Browse> for AppleTvActor {
    type Result = Result<uc_api::intg::ws::BrowseMediaResponseMsgData, ServiceError>;

    fn handle(&mut self, msg: Browse, _: &mut Self::Context) -> Self::Result {
        browse::browse(&self.s, msg.media_id.as_deref(), &msg.paging)
    }
}

impl Handler<AudioChunk> for AppleTvActor {
    type Result = ();

    fn handle(&mut self, msg: AudioChunk, _: &mut Self::Context) {
        let Some(siri) = &self.siri else {
            debug!(
                "[{}] audio for unknown session {}",
                self.cfg.name, msg.session_id
            );
            return;
        };
        if siri.session_id != msg.session_id {
            debug!(
                "[{}] audio for old session {}",
                self.cfg.name, msg.session_id
            );
            return;
        }
        let end = msg.data.is_empty();
        if siri.audio.try_send(msg.data).is_err() {
            warn!("[{}] Siri audio queue full, chunk dropped", self.cfg.name);
        }
        if end {
            // Dropping the sender also ends the session if the end marker was dropped.
            self.siri = None;
        }
    }
}
